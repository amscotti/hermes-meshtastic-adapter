"""
Connection-lifecycle tests for the Meshtastic platform adapter.

Extracted from test_meshtastic.py: connect/disconnect/reconnect, the
transport worker and open/close serialization, keepalive and link-drop
classification, cron/standalone delivery, and pause.
"""

import asyncio
import json
import os
import socket
import sys
import tempfile
import threading
import time
import unittest
from concurrent.futures import Future as ConcurrentFuture
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import connection

# Add CWD to system path to ensure local imports resolve
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
hermes_agent_path = os.getenv("HERMES_AGENT_PATH", os.path.expanduser("~/.hermes/hermes-agent"))
if os.path.isdir(hermes_agent_path):
    sys.path.append(hermes_agent_path)

# Register the platform inside the registry so that Platform("meshtastic") resolves correctly in venv
from gateway.platform_registry import PlatformEntry, platform_registry

platform_registry.register(
    PlatformEntry(
        name="meshtastic",
        label="Meshtastic",
        adapter_factory=lambda cfg: None,
        check_fn=lambda: True,
    )
)

import importlib.util

# Load local mesh_tools.py dynamically, exposing it under the logical name
# "meshtastic_tools" (kept for back-compat with the module singleton). Reuse an
# instance another test module already loaded: unittest imports every module
# up front, so a fresh load would rebind the adapter singleton link and break
# the other file's handler aliases.
if "meshtastic_tools" in sys.modules:
    meshtastic_tools = sys.modules["meshtastic_tools"]
else:
    tools_spec = importlib.util.spec_from_file_location(
        "meshtastic_tools",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "mesh_tools.py"),
    )
    meshtastic_tools = importlib.util.module_from_spec(tools_spec)
    sys.modules["meshtastic_tools"] = meshtastic_tools
    tools_spec.loader.exec_module(meshtastic_tools)

import telemetry_db
import transport
from adapter import (
    HAS_MESHTASTIC,
    AckStatus,
    MeshtasticAdapter,
    _DaemonTransportExecutor,
    _env_enablement,
    _standalone_send,
)
from telemetry_db import init_db

handle_mesh_pause = meshtastic_tools.handle_mesh_pause
handle_mesh_resume = meshtastic_tools.handle_mesh_resume


_BLANK_ENV = {
    "MESHTASTIC_SERIAL_PORT": "",
    "MESHTASTIC_BAUD_RATE": "",
    "MESHTASTIC_ALLOWED_NODES": "",
    "MESHTASTIC_ALLOWED_USERS": "",
    "MESHTASTIC_ALLOW_ALL_USERS": "",
    "MESHTASTIC_HOME_CHANNEL": "",
    "MESHTASTIC_CHUNK_BYTES": "",
    "MESHTASTIC_CHUNK_DELAY": "0",
    "MESHTASTIC_ACK_TIMEOUT": "",
    "MESHTASTIC_SEND_RETRIES": "",
    "MESHTASTIC_RETRY_BACKOFF": "0",
    "MESHTASTIC_TELEMETRY_RETENTION_DAYS": "",
    "MESHTASTIC_TCP_HOST": "",
    "MESHTASTIC_TCP_PORT": "",
}


class TestLifecycle(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._env_patcher = patch.dict(os.environ, _BLANK_ENV)
        self._env_patcher.start()

        # Isolate SQLite database from the user's live Hermes profile.
        self._tmp_db = tempfile.NamedTemporaryFile(delete=False)
        self._tmp_db.close()
        telemetry_db.DB_PATH = self._tmp_db.name
        init_db()

        # Configure platform mock
        self.config = MagicMock()
        self.config.extra = {
            "serial_port": "mock_port",
            "baud_rate": 115200,
            "allowed_users": "!ab12cd34,!da1b1613",
            "allow_all_users": False,
            "home_channel": "meshtastic:channel:0",
        }

        # Instantiate Adapter
        self.adapter = MeshtasticAdapter(self.config)

        # Mock gateway runner's handle_message
        self.adapter.handle_message = AsyncMock()

        # Connect to mock interface
        await self.adapter.connect()
        # Give reconnect task time to initialize mock interface
        await asyncio.sleep(0.1)

    async def asyncTearDown(self):
        await self.adapter.disconnect()
        self._env_patcher.stop()
        try:
            os.unlink(self._tmp_db.name)
        except Exception:
            pass

    def test_pause_resume_state(self):
        """pause_link/resume_link flip the flag and report it via pause_state."""
        self.assertFalse(self.adapter._paused)
        state = self.adapter.pause_link()
        self.assertTrue(state["paused"])
        self.assertIsNone(state["resumes_at"])  # untimed
        self.assertTrue(self.adapter._paused)

        state = self.adapter.resume_link()
        self.assertFalse(state["paused"])
        self.assertFalse(self.adapter._paused)

    def test_pause_timed_reports_window_and_auto_expires(self):
        """A timed pause reports a resume time and _pause_expired auto-resumes."""
        state = self.adapter.pause_link(minutes=30)
        self.assertTrue(state["paused"])
        self.assertIsNotNone(state["resumes_at"])
        self.assertAlmostEqual(state["resumes_in_minutes"], 30, delta=1)

        # Not yet expired.
        self.assertFalse(self.adapter._pause_expired())
        self.assertTrue(self.adapter._paused)
        # Force the deadline into the past → auto-resume.
        self.adapter._pause_until = time.time() - 1
        self.assertTrue(self.adapter._pause_expired())
        self.assertFalse(self.adapter._paused)

    def test_pause_resumes_in_minutes_clamped_at_zero(self):
        """A past deadline reports 0.0, never negative — before expiry clears it."""
        self.adapter.pause_link(minutes=1)
        self.adapter._pause_until = time.time() - 5
        state = self.adapter.pause_state()
        self.assertTrue(state["paused"])
        self.assertEqual(state["resumes_in_minutes"], 0.0)

    async def test_mesh_pause_and_resume_tools(self):
        """The tools drive pause_link/resume_link and report the state."""
        out = json.loads(await handle_mesh_pause({"minutes": 5}))
        self.assertTrue(out["paused"])
        self.assertIn("note", out)
        self.assertTrue(self.adapter._paused)

        out = json.loads(await handle_mesh_resume({}))
        self.assertFalse(out["paused"])
        self.assertFalse(self.adapter._paused)

    async def test_mesh_pause_rejects_bad_minutes(self):
        """Non-numeric / non-positive minutes are rejected, not paused."""
        for bad in ("soon", 0, -5):
            out = json.loads(await handle_mesh_pause({"minutes": bad}))
            self.assertIn("error", out)
        self.assertFalse(self.adapter._paused)

    def test_pause_link_rejects_non_finite_minutes_defensively(self):
        """The adapter itself rejects a NaN/Inf deadline (which could never
        auto-resume and would crash pause_state's localtime)."""
        for minutes in (float("nan"), float("inf")):
            with self.subTest(minutes=minutes):
                with self.assertRaises(ValueError):
                    self.adapter.pause_link(minutes=minutes)
                self.assertFalse(self.adapter._paused)
                self.assertIsNone(self.adapter._pause_until)

    def test_pause_link_zero_minutes_is_noop(self):
        """minutes=0 is falsy, so without an explicit guard it would set
        _pause_until=None (an indefinite pause). A zero-minute pause must be a
        no-op instead — the link stays unpaused."""
        self.assertFalse(self.adapter._paused)
        state = self.adapter.pause_link(minutes=0)
        self.assertFalse(state["paused"])
        self.assertFalse(self.adapter._paused)
        self.assertIsNone(self.adapter._pause_until)
        # Same for 0.0.
        state = self.adapter.pause_link(minutes=0.0)
        self.assertFalse(state["paused"])
        self.assertFalse(self.adapter._paused)

    def test_pause_link_none_is_untimed(self):
        """minutes=None still produces the documented indefinite (untimed) pause."""
        state = self.adapter.pause_link(minutes=None)
        self.assertTrue(state["paused"])
        self.assertIsNone(state["resumes_at"])
        self.assertTrue(self.adapter._paused)
        self.assertIsNone(self.adapter._pause_until)
        self.adapter.resume_link()

    def test_pause_state_survives_huge_finite_deadline(self):
        """A large-but-finite deadline must not crash pause_state for callers."""
        self.adapter.pause_link(minutes=1)
        self.adapter._pause_until = 1e19
        state = self.adapter.pause_state()
        self.assertTrue(state["paused"])
        self.assertIsNone(state["resumes_at"])

    async def test_standalone_send(self):
        """Test that cron standalone ephemeral send routes through adapter.send."""
        res = await _standalone_send(
            self.config, "meshtastic:!ab12cd34", "Cron standalone message check"
        )
        self.assertTrue(res.get("success"))

    def test_keepalive_socket_handle_and_serial_stream(self):
        """Liveness follows the socket handle and the pyserial stream's is_open."""
        self.assertTrue(self.adapter._interface_is_alive(SimpleNamespace(socket=object())))
        self.assertFalse(self.adapter._interface_is_alive(SimpleNamespace(socket=None)))
        alive = SimpleNamespace(stream=SimpleNamespace(is_open=True))
        dead = SimpleNamespace(stream=SimpleNamespace(is_open=False))
        self.assertTrue(self.adapter._interface_is_alive(alive))
        self.assertFalse(self.adapter._interface_is_alive(dead))

    def test_keepalive_isconnected_is_event_not_method(self):
        """meshtastic's isConnected is a threading.Event attribute, not a callable.

        Spec'd stub (only ``isConnected``, no socket/stream) reproduces the real
        interface layout — a plain MagicMock would make ``isConnected()`` return a
        truthy Mock and hide the regression this guards against.
        """
        event = threading.Event()
        iface = SimpleNamespace(isConnected=event)
        self.assertFalse(self.adapter._interface_is_alive(iface))  # cleared == dropped
        event.set()
        self.assertTrue(self.adapter._interface_is_alive(iface))

    def test_keepalive_mock_interface_defaults_alive(self):
        """An interface with no known liveness handle is treated as alive."""
        self.assertTrue(self.adapter._interface_is_alive(self.adapter.get_interfaces()[0]))

    def test_tcp_keepalive_armed_once_per_socket(self):
        """SO_KEEPALIVE is set on connect and re-armed only when the socket changes."""

        class FakeSocket:
            def __init__(self):
                self.opts = []
                self.ioctls = []

            def setsockopt(self, level, opt, value):
                self.opts.append((level, opt, value))

            def ioctl(self, control, args):
                self.ioctls.append((control, args))

        sock = FakeSocket()
        iface = SimpleNamespace(socket=sock)
        self.adapter._apply_tcp_keepalive(iface)
        self.assertIn((socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1), sock.opts)
        # Idle/interval tuning goes through whichever knob the platform exposes.
        self.assertTrue(len(sock.opts) > 1 or sock.ioctls)

        # A second poll on the same socket must not re-issue the options.
        before = len(sock.opts) + len(sock.ioctls)
        self.adapter._apply_tcp_keepalive(iface)
        self.assertEqual(len(sock.opts) + len(sock.ioctls), before)

        # ...but the library's self-heal swaps the socket, which must re-arm.
        iface.socket = FakeSocket()
        self.adapter._apply_tcp_keepalive(iface)
        self.assertIn((socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1), iface.socket.opts)

    def test_tcp_keepalive_survives_unsupported_socket(self):
        """A socket that rejects the options must not break the connection."""

        class RejectingSocket:
            def setsockopt(self, *args):
                raise OSError("not supported here")

        # Serial interfaces have no socket at all — also a no-op, not a crash.
        self.adapter._apply_tcp_keepalive(SimpleNamespace(socket=None))
        self.adapter._apply_tcp_keepalive(SimpleNamespace(socket=RejectingSocket()))
        self.assertIsNone(self.adapter._keepalive_socket_id)

    def test_link_recovery_separates_socket_resets_from_absent_nodes(self):
        """A drop is classified by how long the node stayed unreachable."""
        target = "tcp://192.168.1.69:4403"

        # Back within seconds: the node stayed up, only the socket died.
        self.adapter._note_link_drop(target)
        self.adapter._report_link_recovery(target)
        self.assertEqual(self.adapter._link_drop_counts["socket_reset"], 1)
        self.assertEqual(self.adapter._link_drop_counts["node_absent"], 0)

        # Gone for minutes: the node itself was away (reboot / WiFi / power).
        self.adapter._note_link_drop(target)
        self.adapter._link_down_since[target] -= 120
        self.adapter._report_link_recovery(target)
        self.assertEqual(self.adapter._link_drop_counts["socket_reset"], 1)
        self.assertEqual(self.adapter._link_drop_counts["node_absent"], 1)

        # A connect that never followed a drop (first ever) reports nothing.
        self.adapter._report_link_recovery(target)
        self.assertEqual(sum(self.adapter._link_drop_counts.values()), 2)
        self.assertFalse(self.adapter._link_down_since)

    async def test_stale_link_drop_cleared_on_new_connect(self):
        """A drop surviving a disconnect must not misreport the next lifecycle."""
        target = "tcp://192.168.1.69:4403"
        self.adapter._note_link_drop(target)
        self.assertTrue(self.adapter._link_down_since)

        # Lifecycle turnover: simulate the adapter being disconnected and
        # re-connected without ever recovering the dropped link.
        with self.adapter._lifecycle_lock:
            self.adapter._running = False
            self.adapter._lifecycle_id += 1
        await self.adapter.connect()

        # The stale timestamp is gone; recovery on the new lifecycle counts
        # nothing (an old outage is not this lifecycle's outage).
        self.assertFalse(self.adapter._link_down_since)
        self.adapter._report_link_recovery(target)
        self.assertEqual(sum(self.adapter._link_drop_counts.values()), 0)

    def test_local_node_id_from_dict_myinfo(self):
        """Verify local node ID extraction handles dict-shaped myInfo."""
        iface = SimpleNamespace(myInfo={"my_node_num": 0xAB12CD34})

        self.assertEqual(self.adapter._get_interface_node_id(iface), "!ab12cd34")

    async def test_stale_interface_open_is_closed_not_registered(self):
        """An open completing after lifecycle turnover must close its result."""
        lifecycle_id = self.adapter._lifecycle_id
        opened = SimpleNamespace(close=MagicMock())
        self.adapter._open_interface = MagicMock(return_value=opened)
        self.adapter._running = False
        self.adapter._lifecycle_id += 1

        result = await asyncio.to_thread(
            self.adapter._open_and_register_interface, "stale_port", lifecycle_id
        )
        self.assertIsNone(result)
        opened.close.assert_called_once()
        self.assertNotIn(opened, self.adapter.get_interfaces())

    async def test_open_register_recovers_existing_iface_after_timeout_race(self):
        """A retry open must not discard a live iface registered by a late first open.

        Sequence: timed-out open finishes and registers; a concurrent retry
        opens a second iface, loses the register race, closes the orphan, and
        returns the already-registered interface so reconnect can poll.
        """
        target = "race_port"
        first = SimpleNamespace(close=MagicMock(), name="first")
        second = SimpleNamespace(close=MagicMock(), name="second")
        with self.adapter._iface_lock:
            self.adapter._interfaces[target] = first
        self.adapter._open_interface = MagicMock(return_value=second)

        result = await asyncio.to_thread(
            self.adapter._open_and_register_interface,
            target,
            self.adapter._lifecycle_id,
        )

        self.assertIs(result, first)
        second.close.assert_called_once()
        first.close.assert_not_called()
        with self.adapter._iface_lock:
            self.assertIs(self.adapter._interfaces[target], first)

    async def test_reconnect_loop_adopts_late_registered_iface_instead_of_exiting(self):
        """``None`` from open must not kill reconnect when the map already has the target.

        Models the open-timeout race where the retry returns None but a late
        first open already registered the interface — the loop must keep
        polling rather than exiting with an unmonitored live iface.
        """
        target = "late_reg_port"
        iface = SimpleNamespace(close=MagicMock())
        open_calls = 0
        poll_calls = 0

        async def open_injects(_target, _lifecycle_id):
            # Simulate: open await returned None, but a concurrent late open
            # already registered the target on the worker thread.
            nonlocal open_calls
            open_calls += 1
            with self.adapter._iface_lock:
                self.adapter._interfaces[target] = iface
            return None

        async def poll_once(_target, _lifecycle_id):
            nonlocal poll_calls
            poll_calls += 1
            with self.adapter._lifecycle_lock:
                self.adapter._running = False
            return False

        with (
            patch.object(self.adapter, "_open_interface_for_lifecycle", side_effect=open_injects),
            patch.object(self.adapter, "_poll_interface_until_drop", side_effect=poll_once),
            patch.object(self.adapter, "_apply_tcp_keepalive"),
            patch.object(self.adapter, "_report_link_recovery"),
            patch.object(self.adapter, "_warn_missing_node_key"),
        ):
            await self.adapter._reconnect_loop(target, self.adapter._lifecycle_id)

        self.assertEqual(open_calls, 1)
        self.assertEqual(poll_calls, 1)
        iface.close.assert_not_called()

    async def test_stale_reconnect_cleanup_cannot_pop_replacement_interface(self):
        """Exception cleanup from an old generation cannot remove a new interface."""
        old_lifecycle = self.adapter._lifecycle_id
        replacement = SimpleNamespace(close=MagicMock())
        with self.adapter._lifecycle_lock:
            self.adapter._lifecycle_id += 1
            with self.adapter._iface_lock:
                self.adapter._interfaces["replacement"] = replacement

        active, dropped = self.adapter._pop_interface_for_lifecycle("replacement", old_lifecycle)

        self.assertFalse(active)
        self.assertIsNone(dropped)
        with self.adapter._iface_lock:
            self.assertIs(self.adapter._interfaces["replacement"], replacement)

    async def test_reconnect_exception_closes_dropped_interface(self):
        """A poll exception pops the dead interface and closes it (not leaked)."""
        iface = SimpleNamespace(close=MagicMock())
        with self.adapter._iface_lock:
            self.adapter._interfaces["poll_break_port"] = iface

        async def poll_raises(target, lifecycle_id):
            raise OSError("link poll failed")

        async def open_none(target, lifecycle_id):
            return None

        with (
            patch.object(self.adapter, "_poll_interface_until_drop", side_effect=poll_raises),
            patch.object(self.adapter, "_open_interface_for_lifecycle", side_effect=open_none),
            patch.object(connection, "INITIAL_BACKOFF", 0.001),
            patch.object(connection, "next_backoff", return_value=0.001),
        ):
            await self.adapter._reconnect_loop("poll_break_port", self.adapter._lifecycle_id)

        iface.close.assert_called_once()
        with self.adapter._iface_lock:
            self.assertNotIn("poll_break_port", self.adapter._interfaces)

    async def test_cancelled_open_preserves_cancelled_error_over_constructor_failure(self):
        """A constructor raising during the cancel wait must not replace CancelledError."""
        started = threading.Event()

        def open_raises(*_args):
            started.set()
            time.sleep(0.05)
            raise OSError("port gone")

        self.adapter._open_and_register_interface = MagicMock(side_effect=open_raises)
        task = asyncio.create_task(
            self.adapter._reconnect_loop("gone_port", self.adapter._lifecycle_id)
        )
        self.assertTrue(await asyncio.to_thread(started.wait, 1))
        with patch.dict(os.environ, {"MESHTASTIC_OPEN_CANCEL_TIMEOUT": "1"}):
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

    async def test_connect_is_idempotent(self):
        """A repeated connect must not replace live lifecycle tasks."""
        consumer = self.adapter._incoming_consumer_task
        drain = self.adapter._queue_drain_task
        reconnects = dict(self.adapter._reconnect_tasks)
        lifecycle_id = self.adapter._lifecycle_id

        self.assertTrue(await self.adapter.connect())
        self.assertIs(self.adapter._incoming_consumer_task, consumer)
        self.assertIs(self.adapter._queue_drain_task, drain)
        self.assertEqual(self.adapter._reconnect_tasks, reconnects)
        self.assertEqual(self.adapter._lifecycle_id, lifecycle_id)

    async def test_connect_refused_while_disconnect_in_progress(self):
        """The lifecycle guard that refuses a connect() while a disconnect is
        tearing down (returns False) is exercised here against a fresh adapter
        with _disconnecting set — no reconnect/drain tasks may be spawned. A
        regression that deleted the guard would silently allow a connect to
        race the teardown."""
        adapter = MeshtasticAdapter(self.config)
        adapter.handle_message = AsyncMock()
        adapter._disconnecting = True  # simulate an in-progress teardown

        self.assertFalse(await adapter.connect())

        # No lifecycle state advanced, no tasks spawned.
        self.assertFalse(adapter._running)
        self.assertEqual(adapter._reconnect_tasks, {})
        self.assertIsNone(adapter._queue_drain_task)
        self.assertIsNone(adapter._incoming_consumer_task)

    async def test_adapter_can_reconnect_after_disconnect(self):
        """Lifecycle teardown keeps the transport worker reusable."""
        await self.adapter.disconnect()
        self.assertTrue(await self.adapter.connect())
        for _ in range(100):
            if self.adapter.get_interfaces():
                break
            await asyncio.sleep(0.01)
        self.assertTrue(self.adapter.get_interfaces())
        res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content="after reconnect")
        self.assertTrue(res.success)

    async def test_stopped_old_drain_task_exits_after_lifecycle_turnover(self):
        """Restarting an old loop cannot drain the replacement lifecycle's queue."""
        old_lifecycle = self.adapter._lifecycle_id
        other_loop = asyncio.new_event_loop()
        task_holder: list[asyncio.Task] = []
        task_created = threading.Event()
        start_loop = threading.Event()

        def run_old_loop():
            asyncio.set_event_loop(other_loop)
            task = other_loop.create_task(self.adapter._drain_queue_loop(old_lifecycle))
            task_holder.append(task)
            task_created.set()
            start_loop.wait(timeout=1)
            other_loop.run_until_complete(task)

        thread = threading.Thread(target=run_old_loop, daemon=True)
        thread.start()
        try:
            self.assertTrue(task_created.wait(1))
            with self.adapter._queue_lock:
                self.adapter._outbound_queue.append(
                    {"chat_id": "meshtastic:!ab12cd34", "content": "new lifecycle"}
                )
            with self.adapter._lifecycle_lock:
                self.adapter._lifecycle_id += 1
            with patch.object(self.adapter, "_send_immediate", new_callable=AsyncMock) as send:
                start_loop.set()
                thread.join(timeout=1)
                self.assertFalse(thread.is_alive())
                send.assert_not_awaited()
            with self.adapter._queue_lock:
                self.assertEqual(self.adapter._outbound_queue[-1]["content"], "new lifecycle")
            self.assertTrue(task_holder[0].done())
        finally:
            start_loop.set()
            thread.join(timeout=1)
            other_loop.close()

    async def test_pubsub_callback_ignored_after_disconnect(self):
        """A stale pubsub callback cannot enqueue into a stopped consumer."""
        queue = self.adapter._incoming_queue
        await self.adapter.disconnect()
        self.adapter._on_receive_pubsub({"id": 1})
        self.assertIsNone(self.adapter._incoming_queue)
        self.assertIsNotNone(queue)
        self.assertTrue(queue.empty())

    async def test_old_consumer_drops_packet_after_lifecycle_turnover(self):
        """A queue wakeup cannot dispatch after its consumer generation goes stale."""
        lifecycle_id = self.adapter._lifecycle_id
        queue: asyncio.Queue = asyncio.Queue()
        consumer = asyncio.create_task(self.adapter._consume_incoming_queue(lifecycle_id, queue))
        await asyncio.sleep(0)
        with self.adapter._lifecycle_lock:
            self.adapter._lifecycle_id += 1

        await queue.put(({"id": 99881}, None))
        await asyncio.wait_for(consumer, timeout=1)

        self.adapter.handle_message.assert_not_awaited()
        await asyncio.wait_for(queue.join(), timeout=1)

    async def test_consume_incoming_queue_pubsub_ack_upgrade_no_deadlock(self):
        """Implicit→real ACK upgrade via the queue consumer must not hang.

        Production used to hold non-reentrant ``_lifecycle_lock`` across
        ``_on_receive``. The multi-hop path upgrades via
        ``_maybe_record_pubsub_ack`` → ``_record_ack_response``, which
        re-acquires the same lock — freezing the platform loop. The consumer
        must validate generation under the lock then dispatch unlocked.
        """
        dest = "!ab12cd34"
        relay = "!da1b1613"
        pkt_id = "99882"
        cf = self.adapter._track_pending_ack(pkt_id, dest, "hi", create_future=True)
        self.adapter._record_ack_response(
            {
                "fromId": relay,
                "decoded": {"requestId": int(pkt_id), "routing": {"errorReason": "NONE"}},
            },
            dest,
            "hi",
        )
        self.assertEqual(self.adapter.get_ack_status(pkt_id)["status"], AckStatus.IMPLICIT_ACK)
        self.assertFalse(cf.done())

        lifecycle_id = self.adapter._lifecycle_id
        queue: asyncio.Queue = asyncio.Queue()
        consumer = asyncio.create_task(self.adapter._consume_incoming_queue(lifecycle_id, queue))
        await asyncio.sleep(0)
        direct = {
            "fromId": dest,
            "hopStart": 1,
            "hopLimit": 1,
            "decoded": {"requestId": int(pkt_id), "routing": {"errorReason": "NONE"}},
        }
        await queue.put((direct, self.adapter.get_interfaces()[0]))
        record = await asyncio.wait_for(asyncio.wrap_future(cf), timeout=2)
        self.assertEqual(record["status"], AckStatus.ACK)
        self.assertEqual(self.adapter.get_ack_status(pkt_id)["status"], AckStatus.ACK)
        consumer.cancel()
        try:
            await consumer
        except asyncio.CancelledError:
            pass

    async def test_many_concurrent_disconnects_do_not_exhaust_default_executor(self):
        """Follower disconnects poll shared completion without occupying pool workers."""
        iface = self.adapter.get_interfaces()[0]
        started = threading.Event()
        release = threading.Event()

        def close():
            started.set()
            release.wait(timeout=2)

        iface.close = close
        primary = asyncio.create_task(self.adapter.disconnect())
        self.assertTrue(await asyncio.to_thread(started.wait, 1))
        followers = [asyncio.create_task(self.adapter.disconnect()) for _ in range(40)]
        await asyncio.sleep(0.05)
        self.assertFalse(any(task.done() for task in followers))
        release.set()
        await asyncio.wait_for(asyncio.gather(primary, *followers), timeout=2)

    async def test_close_timeout_does_not_cancel_queued_close(self):
        """A close queued behind a blocked transport job still runs after timeout."""
        executor = self.adapter._transport_executor
        self.assertIsNotNone(executor)
        started = threading.Event()
        release = threading.Event()
        closed = threading.Event()

        def blocked():
            started.set()
            release.wait(timeout=2)

        executor.submit(blocked)
        self.assertTrue(await asyncio.to_thread(started.wait, 1))
        iface = SimpleNamespace(close=closed.set)
        with patch.dict(os.environ, {"MESHTASTIC_EXECUTOR_SHUTDOWN_TIMEOUT": "0.02"}):
            await self.adapter._close_interfaces([iface])
        self.assertFalse(closed.is_set())
        release.set()
        self.assertTrue(await asyncio.to_thread(closed.wait, 1))

    async def test_close_interfaces_waits_for_shutting_down_executor(self):
        """Shutdown-race fallback cannot close concurrently with accepted work."""
        executor = _DaemonTransportExecutor("meshtastic-test-closing")
        started = threading.Event()
        release = threading.Event()
        closed = threading.Event()

        def blocked():
            started.set()
            release.wait(timeout=2)

        def close():
            closed.set()

        executor.submit(blocked)
        self.assertTrue(await asyncio.to_thread(started.wait, 1))
        executor.shutdown(wait=False)
        iface = SimpleNamespace(close=close)
        with (
            patch.object(self.adapter, "_transport_executor", executor),
            patch.dict(os.environ, {"MESHTASTIC_EXECUTOR_SHUTDOWN_TIMEOUT": "0.02"}),
        ):
            await self.adapter._close_interfaces([iface])

        self.assertFalse(closed.is_set())
        release.set()
        self.assertTrue(await asyncio.to_thread(closed.wait, 1))

    async def test_close_interfaces_busy_queue_does_not_shut_down_executor(self):
        """A transient full-queue (TransportBusyError) during close must NOT
        permanently shut down the executor. The close runs on a daemon thread
        (without touching executor state), so a recoverable worker keeps
        accepting work afterwards. This is the counterpart to the shutdown-race
        test above — the two submit outcomes must be distinguished."""
        executor = _DaemonTransportExecutor("meshtastic-test-busy-close")
        started = threading.Event()
        release = threading.Event()
        closed = threading.Event()

        def wedged():
            started.set()
            release.wait(timeout=5)

        # Wedge the worker with one blocking job, then fill the 256-deep queue
        # so the next submit raises TransportBusyError (transient backpressure).
        executor.submit(wedged)
        self.assertTrue(await asyncio.to_thread(started.wait, 1))
        for _ in range(transport.TRANSPORT_JOB_QUEUE_MAXSIZE):
            executor.submit(lambda: None)

        iface = SimpleNamespace(close=closed.set)
        with patch.object(self.adapter, "_transport_executor", executor):
            await self.adapter._close_interfaces([iface])

        # The close ran on a daemon thread despite the full queue.
        self.assertTrue(closed.is_set())
        # The executor was NOT permanently shut down (the bug path called
        # executor.shutdown(wait=True) here, which sets _closed=True for life).
        self.assertFalse(executor._closed)
        self.assertTrue(executor.is_alive())
        # Release the wedge so the queued trivial jobs can drain.
        release.set()
        # The worker recovers: once the queue drains, submit succeeds again
        # (TransportBusyError is transient). The old bug path would have raised
        # TransportShutdownError permanently instead.
        fut = None
        deadline = time.monotonic() + 5
        while fut is None and time.monotonic() < deadline:
            try:
                fut = executor.submit(lambda: 42)
            except transport.TransportBusyError:
                await asyncio.sleep(0.02)
        self.assertIsNotNone(fut, "executor never recovered after releasing the wedge")
        result = await asyncio.wait_for(asyncio.wrap_future(fut), 5)
        self.assertEqual(result, 42)
        executor.shutdown(wait=True, timeout=5)
        """Every accepted job is before shutdown sentinel and completes."""
        executor = _DaemonTransportExecutor("meshtastic-test-race")
        barrier = threading.Barrier(2)
        accepted: list[ConcurrentFuture] = []

        def submit():
            barrier.wait()
            try:
                accepted.append(executor.submit(lambda: 42))
            except RuntimeError:
                pass

        thread = threading.Thread(target=submit)
        thread.start()
        barrier.wait()
        executor.shutdown(wait=False)
        thread.join(timeout=1)
        for future in accepted:
            self.assertEqual(await asyncio.wait_for(asyncio.wrap_future(future), 1), 42)

    async def test_transport_executor_future_carries_job_exception(self):
        """A raising job surfaces its exception on the returned future."""
        executor = _DaemonTransportExecutor("meshtastic-test-jobexc")
        self.addCleanup(executor.shutdown, wait=True, timeout=1)

        def boom():
            raise ValueError("job failed")

        future = executor.submit(boom)
        with self.assertRaises(ValueError):
            await asyncio.wait_for(asyncio.wrap_future(future), 1)

    async def test_transport_executor_skips_future_cancelled_before_run(self):
        """A future cancelled while queued never executes its job."""
        executor = _DaemonTransportExecutor("meshtastic-test-precancel")
        self.addCleanup(executor.shutdown, wait=True, timeout=1)
        started = threading.Event()
        release = threading.Event()

        def blocked():
            started.set()
            release.wait(timeout=2)

        executor.submit(blocked)
        self.assertTrue(await asyncio.to_thread(started.wait, 1))
        ran = threading.Event()
        cancelled = executor.submit(ran.set)
        cancelled.cancel()
        release.set()
        await asyncio.sleep(0.05)
        self.assertTrue(cancelled.cancelled())
        self.assertFalse(ran.is_set())

    async def test_close_interfaces_serialized_logs_and_continues_on_error(self):
        """A failing close does not prevent closing the remaining interfaces."""
        closed: list[str] = []

        def bad_close():
            raise OSError("close failed")

        first = SimpleNamespace(close=bad_close)
        second = SimpleNamespace(close=lambda: closed.append("second"))
        self.adapter._close_interfaces_serialized([first, second])
        self.assertEqual(closed, ["second"])

    async def test_close_interfaces_without_executor_uses_daemon_thread(self):
        """No transport executor: close still runs off the event-loop thread."""
        closed = threading.Event()
        threads: list[str] = []

        def close():
            threads.append(threading.current_thread().name)
            closed.set()

        with patch.object(self.adapter, "_transport_executor", None):
            await self.adapter._close_interfaces([SimpleNamespace(close=close)])

        self.assertTrue(closed.is_set())
        self.assertEqual(threads, ["meshtastic-close"])

    async def test_close_interfaces_on_daemon_thread_tolerates_close_error(self):
        """Per-interface close errors are logged, not raised, on the fallback thread."""
        closed: list[str] = []

        def bad_close():
            raise OSError("close failed")

        with patch.object(self.adapter, "_transport_executor", None):
            await self.adapter._close_interfaces(
                [
                    SimpleNamespace(close=bad_close),
                    SimpleNamespace(close=lambda: closed.append("second")),
                ]
            )

        self.assertEqual(closed, ["second"])

    async def test_close_interfaces_after_executor_tolerates_close_error(self):
        """Per-interface close errors are logged, not raised, after executor drain."""
        executor = _DaemonTransportExecutor("meshtastic-test-closeerr")
        executor.shutdown(wait=True)
        closed: list[str] = []

        def bad_close():
            raise OSError("close failed")

        with patch.object(self.adapter, "_transport_executor", executor):
            await self.adapter._close_interfaces(
                [
                    SimpleNamespace(close=bad_close),
                    SimpleNamespace(close=lambda: closed.append("second")),
                ]
            )

        self.assertEqual(closed, ["second"])

    def test_pop_interface_for_lifecycle_active_pops(self):
        """An active generation owns the pop and receives the interface."""
        marker = SimpleNamespace()
        with self.adapter._iface_lock:
            self.adapter._interfaces["pop_target"] = marker

        active, popped = self.adapter._pop_interface_for_lifecycle(
            "pop_target", self.adapter._lifecycle_id
        )

        self.assertTrue(active)
        self.assertIs(popped, marker)
        with self.adapter._iface_lock:
            self.assertNotIn("pop_target", self.adapter._interfaces)

    def test_drop_interface_close_error_is_logged_not_raised(self):
        """A close error on a dead interface is logged; removal still returns False."""
        dead = SimpleNamespace(
            stream=SimpleNamespace(is_open=False),
            close=MagicMock(side_effect=OSError("close failed")),
        )
        with self.adapter._iface_lock:
            self.adapter._interfaces["dead_target"] = dead

        self.assertFalse(self.adapter._drop_interface_if_dead_serialized("dead_target", dead))
        dead.close.assert_called_once()
        with self.adapter._iface_lock:
            self.assertNotIn("dead_target", self.adapter._interfaces)

    def test_open_cancel_timeout_parsing(self):
        """Bad/negative MESHTASTIC_OPEN_CANCEL_TIMEOUT values fall back safely."""
        for raw, expected in (("2.5", 2.5), ("0", 0.0), ("-3", 0.0), ("bogus", 5.0), ("", 5.0)):
            with patch.dict(os.environ, {"MESHTASTIC_OPEN_CANCEL_TIMEOUT": raw}):
                self.assertEqual(self.adapter._open_cancel_timeout(), expected, f"raw={raw!r}")

    def test_executor_shutdown_timeout_parsing(self):
        """Bad/negative MESHTASTIC_EXECUTOR_SHUTDOWN_TIMEOUT values fall back safely."""
        for raw, expected in (("1.5", 1.5), ("0", 0.0), ("-3", 0.0), ("bogus", 5.0), ("", 5.0)):
            with patch.dict(os.environ, {"MESHTASTIC_EXECUTOR_SHUTDOWN_TIMEOUT": raw}):
                self.assertEqual(
                    self.adapter._executor_shutdown_timeout(), expected, f"raw={raw!r}"
                )

    async def test_shutdown_transport_executor_warns_but_does_not_hang(self):
        """A busy worker past the timeout logs a warning and teardown continues."""
        executor = _DaemonTransportExecutor("meshtastic-test-busy")
        started = threading.Event()
        release = threading.Event()

        def blocked():
            started.set()
            release.wait(timeout=2)

        executor.submit(blocked)
        self.assertTrue(await asyncio.to_thread(started.wait, 1))
        with patch.dict(os.environ, {"MESHTASTIC_EXECUTOR_SHUTDOWN_TIMEOUT": "0.02"}):
            with self.assertLogs("adapter", level="WARNING") as cm:
                await self.adapter._shutdown_transport_executor(executor)
        self.assertTrue(any("still busy" in line for line in cm.output))
        self.assertTrue(executor.is_alive())
        release.set()
        executor._thread.join(timeout=1)
        self.assertFalse(executor.is_alive())

    def test_drop_interface_if_dead_serialized_outcomes(self):
        """Probe: target-changed → None, alive → True, dead → pop+close+False."""
        other = SimpleNamespace()
        with self.adapter._iface_lock:
            self.adapter._interfaces["probe_target"] = other
        self.assertIsNone(
            self.adapter._drop_interface_if_dead_serialized("probe_target", SimpleNamespace())
        )

        alive = SimpleNamespace()
        with self.adapter._iface_lock:
            self.adapter._interfaces["probe_target"] = alive
        self.assertTrue(self.adapter._drop_interface_if_dead_serialized("probe_target", alive))

        dead = SimpleNamespace(stream=SimpleNamespace(is_open=False), close=MagicMock())
        with self.adapter._iface_lock:
            self.adapter._interfaces["probe_target"] = dead
        self.assertFalse(self.adapter._drop_interface_if_dead_serialized("probe_target", dead))
        dead.close.assert_called_once()
        with self.adapter._iface_lock:
            self.assertNotIn("probe_target", self.adapter._interfaces)

    def test_cancel_task_threadsafe_swallows_closed_loop(self):
        """A closed task loop cannot raise out of cross-thread cancellation."""
        loop = MagicMock()
        loop.is_closed.return_value = False
        loop.call_soon_threadsafe.side_effect = RuntimeError("Event loop is closed")
        task = MagicMock()
        task.done.return_value = False
        task.get_loop.return_value = loop

        self.adapter._cancel_task_threadsafe(task)
        loop.call_soon_threadsafe.assert_called_once()

    async def test_disconnect_takes_over_cancelled_owner_loop_task(self):
        """A cancelled/stranded owner-loop teardown is restarted by a waiter."""
        completion = ConcurrentFuture()
        cancelled = asyncio.create_task(asyncio.sleep(0))
        cancelled.cancel()
        await asyncio.gather(cancelled, return_exceptions=True)
        self.adapter._disconnecting = True
        self.adapter._disconnect_future = completion
        self.adapter._disconnect_task = cancelled
        # Simulate stopped owner loop so disconnect must take over locally.
        owner_loop = MagicMock()
        owner_loop.is_running.return_value = False
        self.adapter.loop = owner_loop

        await asyncio.wait_for(self.adapter.disconnect(), timeout=2)
        self.assertTrue(completion.done())
        self.assertFalse(self.adapter._disconnecting)

    async def test_disconnect_takeover_cancels_superseded_pending_task(self):
        """A superseded pending teardown task is cancelled, not just abandoned."""
        old_loop = asyncio.new_event_loop()
        old_task = old_loop.create_task(asyncio.sleep(30))
        completion = ConcurrentFuture()
        self.adapter._disconnecting = True
        self.adapter._disconnect_future = completion
        with self.adapter._lifecycle_lock:
            self.adapter._disconnect_owner_loop = old_loop
            self.adapter._disconnect_task = old_task

        self.adapter._start_disconnect_task(completion)

        self.assertIsNot(self.adapter._disconnect_task, old_task)

        def drain_old_loop():
            asyncio.set_event_loop(old_loop)
            old_loop.run_until_complete(asyncio.gather(old_task, return_exceptions=True))

        await asyncio.to_thread(drain_old_loop)
        self.assertTrue(old_task.cancelled())
        old_loop.close()
        await asyncio.wait_for(asyncio.wrap_future(completion), 2)

    async def test_cancelled_disconnect_impl_task_is_restarted_by_follower(self):
        """Cancelling the teardown task itself cannot wedge disconnect forever."""
        iface = self.adapter.get_interfaces()[0]
        started = threading.Event()
        release = threading.Event()

        def close():
            started.set()
            release.wait(timeout=2)

        iface.close = close
        primary = asyncio.create_task(self.adapter.disconnect())
        self.assertTrue(await asyncio.to_thread(started.wait, 1))
        self.adapter._disconnect_task.cancel()
        # The primary caller's own poll loop detects the cancelled impl task
        # and starts a takeover teardown; do not assert on the transient task.

        follower = asyncio.create_task(self.adapter.disconnect())
        release.set()
        await asyncio.wait_for(asyncio.gather(primary, follower, return_exceptions=True), timeout=3)
        self.assertFalse(self.adapter._disconnecting)

    async def test_disconnect_failure_propagates_and_resets_state(self):
        """A close error surfaces on the completion and leaves state consistent."""
        with patch.object(self.adapter, "_close_interfaces", side_effect=OSError("close exploded")):
            with self.assertRaises(OSError):
                await asyncio.wait_for(self.adapter.disconnect(), timeout=2)

        self.assertFalse(self.adapter._disconnecting)
        self.assertIsNone(self.adapter._disconnect_task)
        self.assertTrue(self.adapter._disconnect_done.is_set())
        self.assertEqual(self.adapter._interfaces, {})

    async def test_cancelled_open_cancel_wait_is_bounded_by_env(self):
        """MESHTASTIC_OPEN_CANCEL_TIMEOUT bounds the post-cancel cleanup wait."""
        started = threading.Event()

        def open_hangs(*_args):
            started.set()
            time.sleep(0.2)
            return SimpleNamespace(close=MagicMock())

        self.adapter._open_and_register_interface = MagicMock(side_effect=open_hangs)
        for timeout in ("0.02", "0"):
            with patch.dict(os.environ, {"MESHTASTIC_OPEN_CANCEL_TIMEOUT": timeout}):
                task = asyncio.create_task(
                    self.adapter._reconnect_loop("slow_port", self.adapter._lifecycle_id)
                )
                self.assertTrue(await asyncio.to_thread(started.wait, 1))
                started.clear()
                start = time.monotonic()
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                self.assertLess(time.monotonic() - start, 0.15)

    async def test_open_success_path_has_bounded_wait(self):
        """A wedged constructor cannot pin ``_open_interface_for_lifecycle``.

        The success-path open is awaited with MESHTASTIC_OPEN_TIMEOUT; expiry
        surfaces as a connect failure (the reconnect loop backs off) instead of
        pinning the await forever.
        """
        started = threading.Event()

        def open_hangs(*_args):
            started.set()
            time.sleep(5)

        self.adapter._open_and_register_interface = MagicMock(side_effect=open_hangs)
        with patch.dict(
            os.environ,
            {"MESHTASTIC_OPEN_TIMEOUT": "0.05", "MESHTASTIC_EXECUTOR_SHUTDOWN_TIMEOUT": "0"},
        ):
            with self.assertRaises(TimeoutError):
                await asyncio.wait_for(
                    self.adapter._open_interface_for_lifecycle(
                        "slow_port", self.adapter._lifecycle_id
                    ),
                    timeout=2,
                )

    async def test_serial_discovery_does_not_block_platform_loop(self):
        """auto serial discovery runs off the event-loop thread.

        Blocking USB enumeration must not stall connect(): the loop stays
        responsive (a concurrent tick fires) while discovery is in flight.
        """
        await self.adapter.disconnect()
        self.adapter.serial_port = "auto"

        started = threading.Event()
        release = threading.Event()

        def blocking_discovery():
            started.set()
            release.wait(timeout=2)
            return []

        ticks: list[float] = []

        async def ticker():
            start = time.monotonic()
            for _ in range(30):
                await asyncio.sleep(0.01)
                ticks.append(time.monotonic() - start)
            return True

        ticker_task = asyncio.create_task(ticker())
        with (
            patch("adapter.transport.discover_serial_ports", side_effect=blocking_discovery),
            patch.dict(os.environ, {"MESHTASTIC_EXECUTOR_SHUTDOWN_TIMEOUT": "0"}),
        ):
            connect_task = asyncio.create_task(self.adapter.connect())
            self.assertTrue(await asyncio.to_thread(started.wait, 1))
            # While discovery blocks, the loop must keep ticking.
            self.assertTrue(await asyncio.wait_for(ticker_task, timeout=2))
            self.assertGreaterEqual(len(ticks), 3)
            release.set()
            await asyncio.wait_for(connect_task, timeout=2)

    async def test_reconnect_loop_exits_quietly_on_executor_shutdown(self):
        """Submit rejection at teardown breaks the loop without a failure log."""
        dead = _DaemonTransportExecutor("meshtastic-test-submit-dead")
        dead.shutdown(wait=True)
        with patch.object(self.adapter, "_transport_executor", dead):
            with self.assertNoLogs("adapter", level="ERROR"):
                await asyncio.wait_for(
                    self.adapter._reconnect_loop("gone_port", self.adapter._lifecycle_id),
                    timeout=2,
                )

    async def test_reconnect_loop_backs_off_quietly_on_busy_executor(self):
        """A wedged worker (full job queue) retries with a WARNING, not an ERROR."""
        executor = MagicMock()
        executor.submit.side_effect = transport.TransportBusyError(
            "transport worker job queue is full"
        )
        real_sleep = asyncio.sleep

        def tick(_seconds):
            return real_sleep(0.001)  # collapse backoff sleeps; still yields

        with patch.object(self.adapter, "_transport_executor", executor):
            with (
                patch("asyncio.sleep", new=tick),
                self.assertLogs("adapter", level="WARNING") as cm,
            ):
                task = asyncio.create_task(
                    self.adapter._reconnect_loop("gone_port", self.adapter._lifecycle_id)
                )
                await asyncio.sleep(0.2)
                with self.adapter._lifecycle_lock:
                    self.adapter._lifecycle_id += 1  # stale generation ends the loop
                await asyncio.wait_for(task, timeout=2)

        self.assertFalse(any("Failed to connect" in line for line in cm.output))
        self.assertTrue(any("busy" in line for line in cm.output))

    async def test_reconnect_loop_reraises_unexpected_runtime_error(self):
        """A non-shutdown RuntimeError from submit takes the failure path."""
        executor = MagicMock()
        executor.submit.side_effect = RuntimeError("something else broke")
        with patch.object(self.adapter, "_transport_executor", executor):
            with self.assertLogs("adapter", level="ERROR") as cm:
                task = asyncio.create_task(
                    self.adapter._reconnect_loop("gone_port", self.adapter._lifecycle_id)
                )
                await asyncio.sleep(0.1)
                with self.adapter._lifecycle_lock:
                    self.adapter._lifecycle_id += 1  # stale generation ends the loop
                await asyncio.wait_for(task, timeout=2)

        self.assertTrue(any("Failed to connect" in line for line in cm.output))

    async def test_reconnect_loop_pause_releases_interface_and_holds(self):
        """Paused: the loop drops the node and stays off; resume reconnects."""
        real_sleep = asyncio.sleep

        def tick(_seconds):
            return real_sleep(0.001)  # collapse 1s/2s loop sleeps; still yields

        task = asyncio.create_task(
            self.adapter._reconnect_loop("mock_port", self.adapter._lifecycle_id)
        )
        try:
            self.adapter.pause_link()
            with patch("asyncio.sleep", new=tick):
                for _ in range(200):
                    if not self.adapter.get_interfaces():
                        break
                    await asyncio.sleep(0.01)
                self.assertEqual(self.adapter.get_interfaces(), [])
                for _ in range(10):
                    await asyncio.sleep(0.01)
                self.assertEqual(self.adapter.get_interfaces(), [])
                self.adapter.resume_link()
                for _ in range(200):
                    if self.adapter.get_interfaces():
                        break
                    await asyncio.sleep(0.01)
                self.assertEqual(len(self.adapter.get_interfaces()), 1)
        finally:
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

    async def test_pubsub_packet_from_detached_interface_is_ignored(self):
        """A packet from an interface no longer registered is dropped."""
        foreign_iface = SimpleNamespace()
        real_iface = self.adapter.get_interfaces()[0]
        received: list[tuple] = []

        def capture(packet, interface=None):
            received.append((packet, interface))

        with patch.object(self.adapter, "_on_receive", side_effect=capture):
            self.adapter._on_receive_pubsub({"id": 424242}, interface=foreign_iface)
            self.adapter._on_receive_pubsub({"id": 424243}, interface=real_iface)
            for _ in range(100):
                if received:
                    break
                await asyncio.sleep(0.01)

        self.assertEqual(len(received), 1)
        self.assertEqual(received[0][0]["id"], 424243)
        self.assertIs(received[0][1], real_iface)

    async def test_cancel_task_threadsafe_cancels_foreign_running_loop_task(self):
        """A task on a foreign running loop is cancelled via call_soon_threadsafe."""
        other_loop = asyncio.new_event_loop()
        task_holder: list[asyncio.Task] = []
        loop_ready = threading.Event()

        def run_loop():
            asyncio.set_event_loop(other_loop)
            task_holder.append(other_loop.create_task(asyncio.sleep(30)))
            other_loop.call_soon(loop_ready.set)
            other_loop.run_forever()

        thread = threading.Thread(target=run_loop, daemon=True, name="foreign-loop")
        thread.start()
        try:
            self.assertTrue(loop_ready.wait(1))
            foreign = task_holder[0]

            self.adapter._cancel_task_threadsafe(foreign)

            for _ in range(100):
                if foreign.cancelled():
                    break
                await asyncio.sleep(0.01)
            self.assertTrue(foreign.cancelled())
        finally:
            if other_loop.is_running():
                other_loop.call_soon_threadsafe(other_loop.stop)
            thread.join(timeout=1)
            other_loop.close()

    async def test_cancel_task_threadsafe_queues_on_stopped_loop(self):
        """Cancellation queued on a stopped loop takes effect when it restarts."""
        other_loop = asyncio.new_event_loop()
        foreign = other_loop.create_task(asyncio.sleep(30))

        self.adapter._cancel_task_threadsafe(foreign)

        def run_loop():
            asyncio.set_event_loop(other_loop)
            other_loop.run_until_complete(asyncio.gather(foreign, return_exceptions=True))

        thread = threading.Thread(target=run_loop, daemon=True)
        thread.start()
        thread.join(timeout=1)
        try:
            self.assertFalse(thread.is_alive())
            self.assertTrue(foreign.cancelled())
        finally:
            if thread.is_alive():
                other_loop.call_soon_threadsafe(other_loop.stop)
                thread.join(timeout=1)
            other_loop.close()

    async def test_reserved_live_disconnect_owner_is_not_stolen_before_callback(self):
        """task=None can mean a queued owner-loop callback, not absent ownership."""
        completion = ConcurrentFuture()
        owner_loop = MagicMock()
        owner_loop.is_running.return_value = True
        with self.adapter._lifecycle_lock:
            self.adapter._disconnect_owner_loop = owner_loop
            self.adapter._disconnect_task = None

        self.adapter._start_disconnect_task(completion)
        self.assertIsNone(self.adapter._disconnect_task)

        completion.set_result(None)
        self.adapter._start_disconnect_task(completion)
        self.assertIsNone(self.adapter._disconnect_task)

    async def test_cancelled_reserved_disconnect_starts_local_teardown(self):
        """Caller cancellation cannot strand a queued foreign-loop reservation."""
        real_consumer = self.adapter._incoming_consumer_task
        fake_loop = MagicMock()
        fake_loop.is_running.return_value = True
        callback_reserved = threading.Event()

        def reserve_callback(*_args):
            callback_reserved.set()

        fake_loop.call_soon_threadsafe.side_effect = reserve_callback
        fake_consumer = MagicMock()
        fake_consumer.get_loop.return_value = fake_loop
        self.adapter._incoming_consumer_task = fake_consumer

        caller = asyncio.create_task(self.adapter.disconnect())
        self.assertTrue(await asyncio.to_thread(callback_reserved.wait, 1))
        caller.cancel()
        await asyncio.gather(caller, return_exceptions=True)

        completion = self.adapter._disconnect_future
        self.assertIsNotNone(completion)
        await asyncio.wait_for(asyncio.wrap_future(completion), timeout=2)
        self.assertFalse(self.adapter._disconnecting)

        if real_consumer is not None and not real_consumer.done():
            real_consumer.cancel()
            await asyncio.gather(real_consumer, return_exceptions=True)

    async def test_cancelled_primary_disconnect_is_completed_by_follower(self):
        """Caller cancellation cannot advertise or permanently abort teardown."""
        iface = self.adapter.get_interfaces()[0]
        started = threading.Event()
        release = threading.Event()

        def close():
            started.set()
            release.wait(timeout=2)

        iface.close = close
        primary = asyncio.create_task(self.adapter.disconnect())
        self.assertTrue(await asyncio.to_thread(started.wait, 1))
        primary.cancel()
        await asyncio.gather(primary, return_exceptions=True)
        self.assertFalse(self.adapter._disconnect_done.is_set())

        follower = asyncio.create_task(self.adapter.disconnect())
        release.set()
        await asyncio.wait_for(follower, timeout=2)
        self.assertTrue(self.adapter._disconnect_done.is_set())
        self.assertIsNone(self.adapter._transport_executor)

    async def test_disconnect_falls_back_when_platform_loop_callback_rejected(self):
        """call_soon_threadsafe RuntimeError starts teardown on the caller loop."""
        real_consumer = self.adapter._incoming_consumer_task
        fake_loop = MagicMock()
        fake_loop.is_running.return_value = True
        fake_loop.call_soon_threadsafe.side_effect = RuntimeError("loop closed")
        fake_consumer = MagicMock()
        fake_consumer.get_loop.return_value = fake_loop
        self.adapter._incoming_consumer_task = fake_consumer

        try:
            await asyncio.wait_for(self.adapter.disconnect(), timeout=2)
        finally:
            self.adapter._incoming_consumer_task = real_consumer

        self.assertFalse(self.adapter._disconnecting)

    def test_tcp_liveness_prefers_isconnected_over_socket(self):
        """A TCP iface mid-self-heal (socket=None, isConnected set) reads alive.

        The library clears socket during its internal reconnect but leaves
        isConnected set; tearing down on the raw socket probe would race the
        self-heal. isConnected is the authoritative signal.
        """
        import threading

        evt = threading.Event()
        evt.set()
        tcp_iface = SimpleNamespace(socket=None, isConnected=evt)
        self.assertTrue(self.adapter._interface_is_alive(tcp_iface))
        # A real drop clears isConnected -> dead.
        evt.clear()
        self.assertFalse(self.adapter._interface_is_alive(tcp_iface))

    def test_connection_lifecycle_handlers_log_without_raising(self):
        """The connection.lost/established pubsub handlers are safe no-ops."""
        with self.assertLogs("adapter", level="WARNING"):
            self.adapter._on_connection_lost(interface="tcp")
        with self.assertLogs("adapter", level="INFO"):
            self.adapter._on_connection_established(interface="tcp")

    def test_get_interface_node_id_prefers_getMyNodeInfo(self):
        """Real MeshInterface exposes getMyNodeInfo, not getMyNodeId."""
        iface = MagicMock()
        # Simulate library shape: no getMyNodeId, yes getMyNodeInfo.
        del iface.getMyNodeId
        iface.getMyNodeInfo.return_value = {
            "num": 0xDA1B1613,
            "user": {"id": "!DA1B1613"},
        }
        self.assertEqual(self.adapter._get_interface_node_id(iface), "!da1b1613")

    def test_discover_serial_ports_prefers_meshtastic_findPorts(self):
        """auto discovery should use meshtastic.util.findPorts when available."""
        with patch("transport.HAS_MESHTASTIC", True):
            with patch(
                "meshtastic.util.findPorts", return_value=["/dev/cu.usbserial-mesh"]
            ) as find_ports:
                ports = self.adapter._discover_serial_ports()
        self.assertEqual(ports, ["/dev/cu.usbserial-mesh"])
        find_ports.assert_called_once_with(True)


class TestLifecycleTcpTransport(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Isolate telemetry writes (MeshtasticAdapter.__init__ calls init_db()).
        self._tmp_db = tempfile.NamedTemporaryFile(delete=False)
        self._tmp_db.close()
        telemetry_db.DB_PATH = self._tmp_db.name
        init_db()

    async def asyncTearDown(self):
        try:
            os.unlink(self._tmp_db.name)
        except Exception:
            pass

    def _adapter(self, **env):
        merged = {**_BLANK_ENV, **env}
        with patch.dict(os.environ, merged):
            config = MagicMock()
            config.extra = {}
            return MeshtasticAdapter(config)

    def test_tcp_host_selected_as_target(self):
        """A configured TCP host produces a single tcp:// target, skipping serial."""
        adapter = self._adapter(
            MESHTASTIC_SERIAL_PORT="/dev/ttyUSB0",
            MESHTASTIC_TCP_HOST="192.168.1.50",
            MESHTASTIC_TCP_PORT="4403",
        )
        self.assertEqual(adapter.tcp_host, "192.168.1.50")
        self.assertEqual(adapter.tcp_port, 4403)
        self.assertEqual(adapter._connection_targets(), ["tcp://192.168.1.50:4403"])

    def test_serial_target_when_no_tcp_host(self):
        """Without a TCP host the adapter keeps the existing serial behaviour."""
        adapter = self._adapter(MESHTASTIC_SERIAL_PORT="/dev/ttyUSB0")
        self.assertEqual(adapter._connection_targets(), ["/dev/ttyUSB0"])

    def test_tcp_port_defaults_to_4403(self):
        adapter = self._adapter(MESHTASTIC_TCP_HOST="meshgw.local")
        self.assertEqual(adapter.tcp_port, 4403)
        self.assertEqual(adapter._connection_targets(), ["tcp://meshgw.local:4403"])

    def test_ipv6_target_round_trip(self):
        """IPv6 literals are bracketed when built and unbracketed when parsed."""
        adapter = self._adapter(MESHTASTIC_TCP_HOST="2001:db8::1", MESHTASTIC_TCP_PORT="8080")
        self.assertEqual(adapter._connection_targets(), ["tcp://[2001:db8::1]:8080"])
        self.assertEqual(
            transport.parse_tcp_target("tcp://[2001:db8::1]:8080"),
            ("2001:db8::1", 8080),
        )
        # Bracketed literal without a port falls back to the default.
        self.assertEqual(
            transport.parse_tcp_target("tcp://[fe80::1]"),
            ("fe80::1", 4403),
        )

    def test_env_enablement_for_tcp_only(self):
        """The platform enables on a TCP host even without a serial port."""
        with patch.dict(os.environ, {**_BLANK_ENV, "MESHTASTIC_TCP_HOST": "10.0.0.7"}):
            env_config = _env_enablement()
        self.assertIsNotNone(env_config)
        self.assertEqual(env_config["tcp_host"], "10.0.0.7")
        self.assertEqual(env_config["tcp_port"], 4403)

    @unittest.skipUnless(HAS_MESHTASTIC, "meshtastic library not installed")
    async def test_connect_opens_tcp_interface(self):
        """connect() routes a TCP target through TCPInterface with host/port."""
        adapter = self._adapter(MESHTASTIC_TCP_HOST="192.168.1.50", MESHTASTIC_TCP_PORT="4403")
        adapter.handle_message = AsyncMock()

        fake_iface = MagicMock()
        fake_iface.nodes = {}

        with patch("meshtastic.tcp_interface.TCPInterface", return_value=fake_iface) as tcp_ctor:
            await adapter.connect()
            await asyncio.sleep(0.1)
            try:
                tcp_ctor.assert_called_once_with(hostname="192.168.1.50", portNumber=4403)
                self.assertEqual(adapter.get_interfaces(), [fake_iface])
            finally:
                await adapter.disconnect()


if __name__ == "__main__":
    unittest.main()
