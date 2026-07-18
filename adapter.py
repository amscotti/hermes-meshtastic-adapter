"""
Meshtastic Platform Adapter for Hermes Agent.

Connects to Meshtastic LoRa nodes over USB-serial or TCP/IP and bridges them
with the Hermes gateway runner.
"""

import asyncio
import importlib
import logging
import os
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future as ConcurrentFuture
from concurrent.futures import InvalidStateError as ConcurrentInvalidStateError
from datetime import datetime
from types import ModuleType
from typing import Any, cast

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)

try:
    from . import ack_state
except ImportError:
    import ack_state

try:
    from . import telemetry_db
except ImportError:
    import telemetry_db

try:
    from . import chunking
except ImportError:
    import chunking

try:
    from . import mock_interface
except ImportError:
    import mock_interface

try:
    from . import node_freshness
except ImportError:
    import node_freshness

try:
    from . import transport
except ImportError:
    import transport

logger = logging.getLogger(__name__)

# Value snapshots re-exported so existing imports/tests keep resolving.
# transport reads its own module attributes at call time, so tests must
# patch transport.HAS_MESHTASTIC / transport.pub, not these adapter copies.
HAS_MESHTASTIC = transport.HAS_MESHTASTIC
pub = transport.pub
DEFAULT_TCP_PORT = transport.DEFAULT_TCP_PORT
_DaemonTransportExecutor = transport._DaemonTransportExecutor

# ACK bookkeeping state machine lives in ack_state; AckStatus is re-exported
# here so existing imports from adapter keep resolving.
AckStatus = ack_state.AckStatus


# --- Mock Implementation for Testing / Dry Run ---
MockSerialInterface = mock_interface.MockSerialInterface
MockLocalNode = mock_interface.MockLocalNode


class MeshtasticAdapter(BasePlatformAdapter):
    """
    Meshtastic platform adapter. Bridges Meshtastic LoRa radios
    with Hermes async message routing.
    """

    MAX_MESSAGE_LENGTH = chunking.MAX_MESSAGE_LENGTH
    DEFAULT_CHUNK_BYTES = chunking.DEFAULT_CHUNK_BYTES

    # This adapter chunks long replies natively in send() (numbered LoRa-safe
    # chunks), so the gateway delivery router must hand us the full payload
    # instead of truncating it at max_message_length.
    splits_long_messages = True

    # Upper bound on retained ACK/NACK bookkeeping records to avoid unbounded
    # memory growth on a long-running gateway. Oldest non-pending records evict
    # first. Aliases ack_state.ACK_RECORD_LIMIT (single source of truth); kept as
    # a class attribute so tests can override it per-instance, and AckTracker
    # reads it via its adapter back-reference.
    ACK_RECORD_LIMIT = ack_state.ACK_RECORD_LIMIT

    # Upper bound on the per-node "observed" overlay (live last_heard / signal
    @property
    def message_len_fn(self):
        return lambda text: len(str(text).encode("utf-8"))

    # --- ACK state delegates -------------------------------------------------
    # The ACK bookkeeping dicts and _ack_lock live on self._ack_tracker
    # (ack_state.AckTracker). send() and tests read them through these
    # read-only properties, and the state-machine methods route through the
    # thin delegates below. Lock acquisition order and lifecycle checks remain
    # entirely inside AckTracker.

    @property
    def _pending_acks(self) -> dict[str, dict[str, Any]]:
        return self._ack_tracker._pending_acks

    @property
    def _ack_responses(self) -> dict[str, dict[str, Any]]:
        return self._ack_tracker._ack_responses

    @property
    def _ack_tokens(self) -> dict[str, object]:
        return self._ack_tracker._ack_tokens

    @property
    def _ack_response_tokens(self) -> dict[str, object]:
        return self._ack_tracker._ack_response_tokens

    @property
    def _ack_inflight_tokens(self) -> dict[object, int]:
        return self._ack_tracker._ack_inflight_tokens

    @property
    def _early_ack_packets(self) -> dict[object, tuple[dict, str, str, int]]:
        return self._ack_tracker._early_ack_packets

    @property
    def _ack_futures(self) -> dict[str, ConcurrentFuture]:
        return self._ack_tracker._ack_futures

    @property
    def _ack_lock(self) -> threading.Lock:
        return self._ack_tracker._ack_lock

    def _maybe_record_pubsub_ack(self, packet: dict) -> bool:
        return self._ack_tracker._maybe_record_pubsub_ack(packet)

    def _track_pending_ack(
        self,
        pkt_id: str | None,
        dest: str,
        content: str,
        *,
        create_future: bool = False,
        send_token: object | None = None,
    ) -> ConcurrentFuture | None:
        return self._ack_tracker._track_pending_ack(
            pkt_id, dest, content, create_future=create_future, send_token=send_token
        )

    def _fail_pending_acks(self, reason: str = "DISCONNECTED") -> None:
        self._ack_tracker._fail_pending_acks(reason)

    def get_ack_status(self, packet_id: str) -> dict[str, Any] | None:
        return self._ack_tracker.get_ack_status(packet_id)

    def _make_ack_callback(self, dest: str, content: str):
        return self._ack_tracker._make_ack_callback(dest, content)

    def _make_ack_callback_for_send(
        self,
        dest: str,
        content: str,
        send_token: object | None,
        lifecycle_id: int | None = None,
    ):
        return self._ack_tracker._make_ack_callback_for_send(
            dest, content, send_token, lifecycle_id
        )

    def _record_ack_response(
        self,
        packet: dict,
        dest: str,
        content: str,
        *,
        send_token: object | None = None,
        lifecycle_id: int | None = None,
    ) -> None:
        self._ack_tracker._record_ack_response(
            packet, dest, content, send_token=send_token, lifecycle_id=lifecycle_id
        )

    def _set_ack_future_result(self, future: ConcurrentFuture, record: dict[str, Any]) -> None:
        self._ack_tracker._set_ack_future_result(future, record)

    async def _wait_for_ack(
        self,
        pkt_id: str,
        future: ConcurrentFuture,
        timeout: float,
    ) -> dict[str, Any]:
        return await self._ack_tracker._wait_for_ack(pkt_id, future, timeout)

    def _is_retriable_failure(self, result: SendResult) -> bool:
        return ack_state.is_retriable_failure(result)

    def _ack_wait_config(self, metadata: dict[str, Any] | None) -> tuple[bool, float]:
        return ack_state.ack_wait_config(metadata)

    def _send_retries(self, metadata: dict[str, Any] | None) -> int:
        return ack_state.send_retries(metadata)

    def _retry_backoff(self) -> float:
        return ack_state.retry_backoff()

    @property
    def enforces_own_access_policy(self) -> bool:
        """This adapter gates inbound traffic itself in ``_on_receive``.

        Tells the gateway's ``_is_user_authorized`` that it may trust an
        already-gated Meshtastic event. The gateway only actually trusts when
        ``_dm_policy`` resolves to ``"allowlist"`` (see below), mirroring
        WeCom/Weixin/WhatsApp — defense-in-depth on top of the env allowlist
        wired via the registry's ``allowed_users_env``.
        """
        return True

    @property
    def _dm_policy(self) -> str:
        """Effective DM access policy read by the gateway trust path.

        ``"allowlist"`` when a node allowlist is active (the gateway then trusts
        the adapter's own intake gate); ``"open"`` when ``allow_all`` is set.
        With no allowlist and ``allow_all=False`` the adapter default-denies at
        intake, so the gateway never sees such traffic — "open" is inert there.
        """
        if self.allowed_nodes and not self.allow_all:
            return "allowlist"
        return "open"

    @property
    def _group_policy(self) -> str:
        """Effective group/channel access policy read by the gateway trust path.

        Meshtastic channel broadcasts map to ``chat_type="group"`` and pass
        through the same ``_is_authorized_node`` intake gate as DMs, so the
        effective policy is identical.
        """
        return self._dm_policy

    def format_tool_event(
        self, event: Any, *, mode: str = "all", preview_max_len: int = 40
    ) -> str | None:
        """Suppress tool-progress chrome over LoRa.

        The base default renders per-tool progress text (emoji + name + preview),
        which would become its own LoRa chunk(s) — real airtime cost on a ~170-
        byte/4-s-per-chunk channel. Return None so tool events are dropped before
        they reach the mesh (the final answer still delivers in full).
        """
        del event, mode, preview_max_len
        return None

    def __init__(self, config: PlatformConfig, **kwargs):
        platform = Platform("meshtastic")
        super().__init__(config=config, platform=platform)

        # Read plugin configuration from env or config.yaml extra
        extra = getattr(config, "extra", {}) or {}

        self.serial_port = os.getenv("MESHTASTIC_SERIAL_PORT") or extra.get("serial_port") or "auto"
        self.baud_rate = int(os.getenv("MESHTASTIC_BAUD_RATE") or extra.get("baud_rate", 115200))
        # meshtastic.serial_interface hardcodes 115200 on the pyserial open —
        # MESHTASTIC_BAUD_RATE is accepted for setup-UI parity / future use but
        # is not applied to the library constructor today.
        if self.baud_rate != 115200:
            logger.warning(
                "MESHTASTIC_BAUD_RATE=%s is ignored: the meshtastic library always "
                "opens serial at 115200.",
                self.baud_rate,
            )

        # Optional TCP/IP transport for WiFi/Ethernet-capable nodes. When a host
        # is configured the adapter connects over TCP instead of serial; the two
        # transports are mutually exclusive (one connection at a time).
        self.tcp_host = (os.getenv("MESHTASTIC_TCP_HOST") or extra.get("tcp_host") or "").strip()
        self.tcp_port = int(
            os.getenv("MESHTASTIC_TCP_PORT") or extra.get("tcp_port") or DEFAULT_TCP_PORT
        )

        # Access control list (Allowed node IDs, e.g. '!da1b1613')
        allowed_nodes_raw = (
            os.getenv("MESHTASTIC_ALLOWED_NODES")
            or os.getenv("MESHTASTIC_ALLOWED_USERS")
            or extra.get("allowed_nodes")
            or extra.get("allowed_users")
            or ""
        )
        self.allow_all = (
            os.getenv("MESHTASTIC_ALLOW_ALL_USERS", "").lower() in ("1", "true", "yes")
            if os.getenv("MESHTASTIC_ALLOW_ALL_USERS")
            else extra.get("allow_all_users", False)
        )

        # Whether to answer channel/broadcast messages. Default False: the agent
        # replies to direct messages only and never posts into a shared public
        # channel (which wastes mesh airtime and is visible to everyone). Set
        # MESHTASTIC_ALLOW_CHANNELS=true to opt in.
        self.allow_channels = (
            os.getenv("MESHTASTIC_ALLOW_CHANNELS", "").lower() in ("1", "true", "yes")
            if os.getenv("MESHTASTIC_ALLOW_CHANNELS")
            else extra.get("allow_channels", False)
        )

        self.allowed_nodes: set[str] = set()
        if allowed_nodes_raw:
            parts = [p.strip().lower() for p in str(allowed_nodes_raw).split(",") if p.strip()]
            for p in parts:
                self.allowed_nodes.add(p)
                # If they omitted the leading '!', support matching it too
                if not p.startswith("!"):
                    self.allowed_nodes.add(f"!{p}")
                else:
                    self.allowed_nodes.add(p.lstrip("!"))

        # Hermes gateway re-checks the allowlist env with exact string equality
        # (no case fold, no bang-normalization). Expand the env so its second
        # gate accepts the same forms we accept at intake. See authz_mixin.
        self._expand_allowlist_env_for_gateway()

        # Live-observed per-node overlay (last_heard / signal learned from the
        # packet stream), keyed by node id. Fed in _on_receive for EVERY heard
        # node and layered over the library's node DB by the mesh_* tools.
        self._node_freshness = self._create_node_freshness()

        # Active hardware connections mapping: devPath -> interface.
        # _iface_lock protects only short map/state operations; slow Meshtastic
        # I/O is serialized by the lifecycle's single daemon transport worker.
        self._interfaces: dict[str, Any] = {}
        self._iface_lock = threading.Lock()
        self._transport_executor: _DaemonTransportExecutor | None = None
        self._pubsub_subscribed = False
        self._lifecycle_id = 0
        self._lifecycle_lock = threading.Lock()
        self._disconnecting = False
        # Waiters poll the shared completion future (_disconnect_future), not
        # this Event, so concurrent disconnects never occupy default-executor
        # workers. The Event remains only as a teardown-completion signal for
        # tests/diagnostics.
        self._disconnect_done = threading.Event()
        self._disconnect_done.set()
        self._disconnect_future: ConcurrentFuture | None = None
        self._disconnect_task: asyncio.Task | None = None
        self._disconnect_owner_loop: asyncio.AbstractEventLoop | None = None
        self._disconnect_interfaces: list[tuple[str, Any]] = []
        self._disconnect_close_started = False

        # Outbound message queue for temporary drops (Phase 3 Task 2)
        # Bounded at 100 messages, oldest-first eviction
        self._outbound_queue: list[dict[str, Any]] = []
        self._queue_lock = threading.Lock()
        # ACK/NACK tracking state machine: owns the 7 ACK dicts + _ack_lock.
        # Exposed on the adapter via read-only property delegates below so
        # send() and tests can keep reading self._ack_lock / self._pending_acks.
        self._ack_tracker = ack_state.AckTracker(self)

        # Platform loop: set in connect(). Owns _incoming_queue, reconnect /
        # drain tasks, and the pubsub→queue bridge. Send/ACK waiters may run on
        # a *different* loop (agent session). ACK completion uses
        # concurrent.futures (loop-independent); transport I/O is serialized
        # on the daemon worker (not the platform loop).
        self.loop: asyncio.AbstractEventLoop | None = None
        self._cross_loop_send_logged = False
        self._reconnect_tasks: dict[str, asyncio.Task] = {}
        self._queue_drain_task: asyncio.Task | None = None
        self._running = False

        # Incoming queue and tasks for thread-safe bridge
        self._incoming_queue: asyncio.Queue | None = None
        self._incoming_consumer_task: asyncio.Task | None = None
        self._message_tasks: set[asyncio.Task] = set()

        # Initialise SQLite telemetry DB
        telemetry_db.init_db()
        logger.info("MeshtasticAdapter initialized.")

    @property
    def name(self) -> str:
        return "Meshtastic"

    def get_interfaces(self) -> list[Any]:
        """Return the active serial/BLE interface instances."""
        with self._iface_lock:
            return list(self._interfaces.values())

    def _has_interfaces(self) -> bool:
        with self._iface_lock:
            return bool(self._interfaces)

    def _register_interface(
        self, target: str, iface: Any, *, lifecycle_id: int | None = None
    ) -> bool:
        """Register ``iface`` only if this lifecycle still owns ``target``."""
        with self._iface_lock:
            if (
                not self._running
                or target in self._interfaces
                or (lifecycle_id is not None and lifecycle_id != self._lifecycle_id)
            ):
                return False
            self._interfaces[target] = iface
        return True

    def _pop_interface(self, target: str) -> Any | None:
        """Remove and return one interface without performing blocking I/O."""
        with self._iface_lock:
            return self._interfaces.pop(target, None)

    def _pop_interface_for_lifecycle(
        self, target: str, lifecycle_id: int
    ) -> tuple[bool, Any | None]:
        """Remove ``target`` only while ``lifecycle_id`` still owns adapter state."""
        with self._lifecycle_lock:
            if lifecycle_id != self._lifecycle_id or not self._running:
                return False, None
            with self._iface_lock:
                return True, self._interfaces.pop(target, None)

    def _close_interfaces_serialized(self, interfaces: list[Any]) -> None:
        """Close interfaces on a worker thread, serialized with sendText."""
        for iface in interfaces:
            try:
                iface.close()
            except Exception as exc:
                logger.error("Error closing Meshtastic interface: %s", exc)

    @staticmethod
    async def _await_concurrent_future(
        future: ConcurrentFuture, timeout: float | None = None
    ) -> Any:
        """Await without propagating asyncio cancellation into queued worker jobs.

        Polling (rather than ``asyncio.shield(asyncio.wrap_future(...))``) keeps
        the implementation trivial and guarantees a caller ``CancelledError``
        cannot reach the daemon worker job. The 10ms cadence is a deliberate
        tradeoff: awaits only cover slow, infrequent transport open/close/drain
        calls, so the wakeup cost is negligible relative to the I/O latency.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while not future.done():
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError
            await asyncio.sleep(0.01)
        return future.result()

    async def _close_interfaces_on_daemon_thread(
        self, interfaces: list[Any], timeout: float
    ) -> None:
        """Close via a short-lived daemon thread — never on the event-loop thread."""
        close_fut: ConcurrentFuture = ConcurrentFuture()

        def _close() -> None:
            try:
                self._close_interfaces_serialized(interfaces)
            except BaseException as exc:
                try:
                    close_fut.set_exception(exc)
                except ConcurrentInvalidStateError:
                    pass
            else:
                try:
                    close_fut.set_result(None)
                except ConcurrentInvalidStateError:
                    pass

        threading.Thread(target=_close, name="meshtastic-close", daemon=True).start()
        await self._await_concurrent_future(close_fut, timeout)

    async def _close_interfaces_after_executor(
        self,
        executor: _DaemonTransportExecutor,
        interfaces: list[Any],
        timeout: float,
    ) -> None:
        """Close only after a shutting-down worker drains accepted transport work."""
        close_fut: ConcurrentFuture = ConcurrentFuture()

        def _drain_then_close() -> None:
            try:
                # Unbounded join is deliberate: a bounded join could let close
                # run concurrently with an in-flight sendText, violating the
                # transport serialization this path exists to preserve. The
                # caller's await is time-bounded and this thread is a daemon,
                # so a stuck worker still cannot pin process exit.
                executor.shutdown(wait=True)
                self._close_interfaces_serialized(interfaces)
            except BaseException as exc:
                try:
                    close_fut.set_exception(exc)
                except ConcurrentInvalidStateError:
                    pass
            else:
                try:
                    close_fut.set_result(None)
                except ConcurrentInvalidStateError:
                    pass

        threading.Thread(
            target=_drain_then_close,
            name="meshtastic-close-after-worker",
            daemon=True,
        ).start()
        await self._await_concurrent_future(close_fut, timeout)

    async def _close_interfaces_via_executor(
        self,
        executor: _DaemonTransportExecutor,
        interfaces: list[Any],
        timeout: float,
    ) -> None:
        """Close on the transport worker; drain-then-close if it is mid-shutdown."""
        try:
            close_fut = executor.submit(self._close_interfaces_serialized, interfaces)
        except RuntimeError:
            # Executor shut down between the read and submit. Wait for its
            # accepted work to drain before closing, preserving the
            # no-concurrent-sendText/close transport invariant.
            await self._close_interfaces_after_executor(executor, interfaces, timeout)
            return
        await self._await_concurrent_future(close_fut, timeout)

    async def _close_interfaces(self, interfaces: list[Any]) -> None:
        """Close interfaces off the event-loop thread, time-bounded.

        Dispatch (each path never runs close on the caller's loop):
          1. ``_close_interfaces_via_executor`` — the lifecycle transport worker
             (normal path, serialized against sendText).
          2. ``_close_interfaces_on_daemon_thread`` — short-lived daemon thread
             when no worker exists (never connected / already torn down).
        A TimeoutError only abandons the *await*; the daemon close still runs.
        """
        if not interfaces:
            return
        timeout = self._executor_shutdown_timeout()
        with self._lifecycle_lock:
            executor = self._transport_executor
        try:
            if executor is not None:
                await self._close_interfaces_via_executor(executor, interfaces, timeout)
            else:
                await self._close_interfaces_on_daemon_thread(interfaces, timeout)
        except TimeoutError:
            logger.warning(
                "Meshtastic interface close still running after %.1fs; disconnect continues "
                "(daemon transport worker will finish in the background)",
                timeout,
            )

    @staticmethod
    def _open_cancel_timeout() -> float:
        """Seconds to wait for a cancelled open before abandoning the await.

        ``0`` means do not wait (abandon immediately). The constructor still
        runs on the daemon transport worker and closes a stale result via
        lifecycle_id. Override with MESHTASTIC_OPEN_CANCEL_TIMEOUT.
        """
        raw = os.getenv("MESHTASTIC_OPEN_CANCEL_TIMEOUT") or "5"
        try:
            return max(0.0, float(raw))
        except (TypeError, ValueError):
            return 5.0

    @staticmethod
    def _executor_shutdown_timeout() -> float:
        """Seconds to wait for transport-worker drain / close during disconnect.

        ``0`` means do not wait. Override with MESHTASTIC_EXECUTOR_SHUTDOWN_TIMEOUT.
        """
        raw = os.getenv("MESHTASTIC_EXECUTOR_SHUTDOWN_TIMEOUT") or "5"
        try:
            return max(0.0, float(raw))
        except (TypeError, ValueError):
            return 5.0

    async def _shutdown_transport_executor(self, executor: _DaemonTransportExecutor) -> None:
        """Shut down the daemon transport worker without hanging forever."""
        timeout = self._executor_shutdown_timeout()
        executor.shutdown(wait=False)
        deadline = time.monotonic() + timeout
        # Poll without blocking the platform loop. Daemon worker means a stuck
        # operation cannot pin process exit after this bounded wait expires.
        while executor.is_alive() and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        if executor.is_alive():
            logger.warning(
                "Meshtastic transport executor still busy after %.1fs during disconnect; "
                "continuing (daemon worker will finish in the background)",
                timeout,
            )

    def _drop_interface_if_dead_serialized(self, target: str, iface: Any) -> bool | None:
        """Atomically probe and close a dead interface.

        Returns None if the target changed, True if alive, and False after a
        dead interface was removed and closed. This runs on the single daemon
        transport worker, so probe-through-removal is serialized against
        sendText: a send cannot slip between them.
        """
        with self._iface_lock:
            if self._interfaces.get(target) is not iface:
                return None
        if self._interface_is_alive(iface):
            return True
        with self._iface_lock:
            if self._interfaces.get(target) is not iface:
                return None
            self._interfaces.pop(target, None)
        try:
            iface.close()
        except Exception as exc:
            logger.error("Error closing dropped Meshtastic interface: %s", exc)
        return False

    def _open_and_register_interface(self, target: str, lifecycle_id: int) -> Any | None:
        """Open on a worker, then atomically adopt or close a stale result."""
        iface = self._open_interface(target)
        if self._register_interface(target, iface, lifecycle_id=lifecycle_id):
            return iface
        self._close_interfaces_serialized([iface])
        return None

    def _subscribe_pubsub(self) -> None:
        """Subscribe once per adapter lifecycle, independent of interface count."""
        if not HAS_MESHTASTIC or not pub or self._pubsub_subscribed:
            return
        pub.subscribe(self._on_receive_pubsub, "meshtastic.receive")
        pub.subscribe(self._on_connection_lost, "meshtastic.connection.lost")
        pub.subscribe(self._on_connection_established, "meshtastic.connection.established")
        self._pubsub_subscribed = True

    def _unsubscribe_pubsub(self) -> None:
        if not HAS_MESHTASTIC or not pub or not self._pubsub_subscribed:
            return
        pub.unsubscribe(self._on_receive_pubsub, "meshtastic.receive")
        pub.unsubscribe(self._on_connection_lost, "meshtastic.connection.lost")
        pub.unsubscribe(self._on_connection_established, "meshtastic.connection.established")
        self._pubsub_subscribed = False

    def _schedule_on_loop(
        self,
        loop: asyncio.AbstractEventLoop | None,
        callback: Callable[..., Any],
        *args: Any,
        what: str = "callback",
    ) -> bool:
        """Thread-safe schedule onto ``loop``. Returns False if skipped.

        Used from meshtastic pubsub / radio callback threads to touch asyncio
        state. Logs at debug when the target loop is missing, not running, or
        closes between the check and ``call_soon_threadsafe`` (TOCTOU race).
        """
        if loop is None:
            logger.debug("Skipping %s: target loop is None", what)
            return False
        try:
            if not loop.is_running():
                logger.debug(
                    "Skipping %s: target loop not running (loop=%r)",
                    what,
                    loop,
                )
                return False
            loop.call_soon_threadsafe(callback, *args)
            return True
        except RuntimeError as exc:
            # Loop closed between is_running() and call_soon_threadsafe.
            logger.debug("Skipping %s: %s", what, exc)
            return False

    def _cancel_task_threadsafe(self, task: asyncio.Task) -> None:
        """Cancel a task regardless of which loop owns it.

        ``Task.cancel()`` touches the owning loop's internal state and is only
        safe to call from that loop's thread. Disconnect teardown may run on a
        follower loop that took over after the platform loop's owner task was
        cancelled/stranded; in that case foreign-loop tasks are cancelled via
        ``call_soon_threadsafe``. A stopped-but-open loop accepts the callback
        and cancels the task before it can resume into a later lifecycle.
        """
        loop = task.get_loop()
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None
        if loop is current_loop:
            task.cancel()
            return
        if task.done() or loop.is_closed():
            return
        try:
            # Unlike _schedule_on_loop (the inbound bridge), cancellation must
            # also be queued on a stopped loop in case it is restarted later.
            loop.call_soon_threadsafe(task.cancel)
        except RuntimeError as exc:
            logger.debug("Skipping task cancellation: %s", exc)

    def _lifecycle_is_active(self, lifecycle_id: int) -> bool:
        """Return whether ``lifecycle_id`` still owns adapter loop tasks."""
        with self._lifecycle_lock:
            return self._running and self._lifecycle_id == lifecycle_id

    def _run_db_write(self, fn: Callable[[], None]) -> None:
        """Run a blocking telemetry DB write off the event loop when one is available.

        Inbound processing runs on the platform loop. The target callables
        swallow their own exceptions, so the executor future is intentionally
        fire-and-forget.
        """
        loop = self.loop
        if loop is not None:
            loop.run_in_executor(None, fn)
        else:
            fn()

    def _is_authorized_node(self, node_id: str) -> bool:
        """Check if a node ID is permitted to speak with the bot."""
        if self.allow_all:
            return True
        nid = node_id.strip().lower()
        return nid in self.allowed_nodes or nid.lstrip("!") in self.allowed_nodes

    @staticmethod
    def _normalize_node_id(node_id: Any) -> str | None:
        """Canonicalize a Meshtastic node id to ``!`` + lowercase 8-hex when possible.

        Hermes gateway allowlist matching is exact (no case fold / bang
        normalization), so inbound ``user_id`` / DM chat_ids must be stable and
        match the ``!aabbccdd`` form operators put in MESHTASTIC_ALLOWED_NODES.
        Numeric node numbers and ``!``-prefixed hex (any case) are normalized;
        other string forms are lowercased as-is.
        """
        if node_id is None:
            return None
        # bool is a subclass of int — don't treat True/False as node numbers.
        if isinstance(node_id, bool):
            return str(node_id).lower()
        if isinstance(node_id, int):
            return f"!{node_id:08x}"
        text = str(node_id).strip()
        if not text:
            return None
        low = text.lower()
        bare = low[1:] if low.startswith("!") else low
        if len(bare) == 8 and all(c in "0123456789abcdef" for c in bare):
            return f"!{bare}"
        return low

    @staticmethod
    def _expand_allowlist_env_for_gateway() -> None:
        """Expand MESHTASTIC_ALLOWED_NODES so Hermes' exact-match gate accepts our forms.

        The adapter accepts node ids with/without ``!`` and any case. Hermes
        ``_is_user_authorized`` reads ``allowed_users_env`` (MESHTASTIC_ALLOWED_NODES)
        with exact equality and no normalization. Expanding that env keeps the
        gateway double-check aligned with adapter intake. Legacy
        MESHTASTIC_ALLOWED_USERS is still read for adapter-local allowlisting but
        is not the gateway's auth env, so it is left untouched here.
        """
        raw = os.getenv("MESHTASTIC_ALLOWED_NODES", "").strip()
        if not raw:
            return
        expanded: set[str] = set()
        for part in raw.split(","):
            p = part.strip()
            if not p:
                continue
            expanded.add(p)
            low = p.lower()
            expanded.add(low)
            bare = low.lstrip("!")
            if bare:
                expanded.add(bare)
                expanded.add(f"!{bare}")
        os.environ["MESHTASTIC_ALLOWED_NODES"] = ",".join(sorted(expanded))

    def _create_node_freshness(self) -> node_freshness.NodeFreshness:
        """Factory seam so tests/subclasses can substitute a bounded overlay."""
        return node_freshness.NodeFreshness()

    def _update_observed(
        self,
        node_id: str,
        rx_time: Any,
        snr: Any,
        rssi: Any,
        hop_count: int | None,
    ) -> None:
        self._node_freshness.update(node_id, rx_time, snr, rssi, hop_count)

    def get_observed_node(self, node_id: str) -> dict[str, Any]:
        return self._node_freshness.get(node_id)

    def _get_interface_node_id(self, interface: Any) -> str | None:
        """Return the local Meshtastic node ID for an interface, if known.

        Prefers the library's ``getMyNodeInfo()`` (real MeshInterface) which
        returns the node-DB entry including ``user.id``. Falls back to
        ``myInfo.my_node_num`` (protobuf) and the mock's ``getMyNodeId()``.
        """
        if hasattr(interface, "getMyNodeInfo") and callable(interface.getMyNodeInfo):
            try:
                info = interface.getMyNodeInfo()
            except Exception:
                info = None
            if isinstance(info, dict):
                user = info.get("user") or {}
                user_id = user.get("id") if isinstance(user, dict) else None
                if isinstance(user_id, str) and user_id:
                    return self._normalize_node_id(user_id) or user_id
                num = info.get("num")
                if isinstance(num, int):
                    return f"!{num:08x}"

        my_info = getattr(interface, "myInfo", None)
        my_node_num = None
        if isinstance(my_info, dict):
            my_node_num = my_info.get("my_node_num")
        elif my_info is not None:
            my_node_num = getattr(my_info, "my_node_num", None)

        if isinstance(my_node_num, int):
            return f"!{my_node_num:08x}"
        if my_node_num is not None:
            try:
                return f"!{int(my_node_num):08x}"
            except (TypeError, ValueError):
                pass

        get_my = getattr(interface, "getMyNodeId", None)
        if callable(get_my):
            try:
                return self._normalize_node_id(get_my())
            except Exception:
                return None
        return None

    def _load_tools_module(self) -> ModuleType:
        """Load the companion tools module without colliding with Hermes' tools package."""
        import sys

        if "meshtastic_tools" in sys.modules:
            return sys.modules["meshtastic_tools"]
        if __package__:
            return importlib.import_module(f"{__package__}.tools")
        return importlib.import_module("tools")

    def _tools_set_adapter_fn(self) -> Callable[[object | None], None]:
        """Return the companion tools module's set_adapter function."""
        attr_name = "set_adapter"
        return cast(Callable[[object | None], None], getattr(self._load_tools_module(), attr_name))

    def _set_tools_adapter(self, adapter: object | None) -> None:
        """Update the active adapter reference in the companion tools module."""
        self._tools_set_adapter_fn()(adapter)

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Connect to the Meshtastic node(s) and start listening."""
        # is_reconnect is part of the base-class contract but ignored here: the
        # only outbound buffer is in-memory and persists across in-process
        # reconnects, so there is no server-side queue to preserve.
        del is_reconnect
        with self._lifecycle_lock:
            if self._running:
                logger.debug("Meshtastic adapter is already connected/connecting")
                return True
            if self._disconnecting:
                logger.warning("Cannot connect Meshtastic adapter while disconnect is in progress")
                return False
            self._running = True
            self._lifecycle_id += 1
            lifecycle_id = self._lifecycle_id
            if self._transport_executor is None:
                self._transport_executor = _DaemonTransportExecutor(name="meshtastic-transport")
        self.loop = asyncio.get_running_loop()
        self._cross_loop_send_logged = False

        self._set_tools_adapter(self)
        self._subscribe_pubsub()

        # Pass the generation and queue explicitly so a task stranded on an old
        # loop cannot consume a replacement lifecycle's queue after restart.
        incoming_queue = asyncio.Queue()
        self._incoming_queue = incoming_queue
        self._incoming_consumer_task = asyncio.create_task(
            self._consume_incoming_queue(lifecycle_id, incoming_queue)
        )

        # Determine connection targets to open
        targets = self._connection_targets()
        logger.info(f"Connecting to Meshtastic targets: {targets}")

        # Start connection routine for each target
        self._reconnect_tasks.clear()
        for target in targets:
            self._reconnect_tasks[target] = asyncio.create_task(
                self._reconnect_loop(target, lifecycle_id)
            )

        # Start queue drain monitoring
        self._queue_drain_task = asyncio.create_task(self._drain_queue_loop(lifecycle_id))

        self._mark_connected()
        return True

    def _connection_targets(self) -> list[str]:
        """Resolve the connection target keys to open.

        A configured TCP host takes precedence over serial: the two transports
        are mutually exclusive. Targets are opaque keys understood by
        ``_reconnect_loop`` and ``_open_interface`` — a ``tcp://host:port`` URL
        for TCP, otherwise a serial device path (or ``mock_port`` fallback).
        """
        return transport.connection_targets(self.tcp_host, self.tcp_port, self.serial_port)

    def _open_interface(self, target: str) -> Any:
        """Open the serial/TCP interface for a connection target.

        Runs the blocking Meshtastic constructors; callers offload this to an
        executor. Falls back to the mock interface when the Meshtastic libraries
        are unavailable so the plugin still loads.
        """
        return transport.open_interface(target)

    def _discover_serial_ports(self) -> list[str]:
        """Discover likely Meshtastic serial devices cross-platform.

        Prefer ``meshtastic.util.findPorts`` (VID whitelist for known radios,
        then non-blacklisted ports) so ``auto`` does not open every USB-serial
        gadget on the host. Fall back to pyserial / glob when the library is
        unavailable.
        """
        return transport.discover_serial_ports()

    async def _reconnect_loop(self, target: str, lifecycle_id: int):
        """Exponential backoff reconnect loop for one connection target."""
        backoff = 1.0
        while self._lifecycle_is_active(lifecycle_id):
            try:
                with self._iface_lock:
                    needs_connect = target not in self._interfaces
                if needs_connect:
                    logger.info(f"Attempting to connect to Meshtastic target: {target}...")
                    # Constructors can block. The worker adopts the result only
                    # if this lifecycle still wants it; canceled/stale opens are
                    # closed before the worker returns.
                    with self._lifecycle_lock:
                        executor = self._transport_executor
                    if executor is None:
                        break
                    try:
                        open_cf = executor.submit(
                            lambda t=target, lid=lifecycle_id: self._open_and_register_interface(
                                t, lid
                            )
                        )
                    except RuntimeError as exc:
                        # Executor shutdown mid-teardown — not a connection
                        # failure, so no backoff/retry: just exit the loop.
                        if "cannot schedule new futures after shutdown" in str(exc).lower():
                            break
                        raise
                    try:
                        iface = await self._await_concurrent_future(open_cf)
                    except asyncio.CancelledError:
                        # Constructor work cannot be canceled once running.
                        # Wait briefly for stale-lifecycle cleanup; if the open
                        # is hung (or timeout is 0), abandon the await so
                        # disconnect can finish. Daemon worker still closes a
                        # late result via lifecycle_id.
                        timeout = self._open_cancel_timeout()
                        if timeout > 0:
                            try:
                                await self._await_concurrent_future(open_cf, timeout)
                            except TimeoutError:
                                logger.warning(
                                    "Meshtastic open for %s still running after %.1fs cancel "
                                    "wait; disconnect continues (stale result will be closed)",
                                    target,
                                    timeout,
                                )
                            except Exception:
                                # The constructor failed after we were cancelled.
                                # Preserve the CancelledError — that is the
                                # meaningful outcome for the caller; the worker
                                # already logged the open failure.
                                logger.debug(
                                    "Cancelled Meshtastic open for %s also raised",
                                    target,
                                    exc_info=True,
                                )
                        else:
                            logger.warning(
                                "Meshtastic open for %s abandoned immediately on cancel "
                                "(MESHTASTIC_OPEN_CANCEL_TIMEOUT=0); stale result will be closed",
                                target,
                            )
                        raise
                    if iface is None:
                        break
                    backoff = 1.0  # Reset backoff on success
                    logger.info(f"Successfully connected to Meshtastic on {target}")

                    # Security warnings for local node
                    my_node = getattr(iface, "localNode", None)
                    if my_node:
                        # Try to read info dictionary
                        nodes = getattr(iface, "nodes", {}) or {}
                        my_id = self._get_interface_node_id(iface) or ""

                        my_info = nodes.get(my_id, {})
                        if not my_info.get("user", {}).get("publicKey"):
                            logger.warning(
                                f"!!! WARNING: Local node {my_id} has no initialized public/private key. "
                                "DMs WILL FAIL. Please pair/connect the node to the official Meshtastic mobile app "
                                "at least once to complete encryption setup."
                            )

            except Exception as e:
                logger.error(f"Failed to connect to Meshtastic on {target}: {e}")
                active_lifecycle, dropped = self._pop_interface_for_lifecycle(target, lifecycle_id)
                if not active_lifecycle:
                    break
                if dropped is not None:
                    await self._close_interfaces([dropped])

                # Sleep with exponential backoff
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)
                continue

            # If successfully connected, poll until the connection drops
            while self._lifecycle_is_active(lifecycle_id):
                with self._iface_lock:
                    if target not in self._interfaces:
                        break
                    iface = self._interfaces[target]
                with self._lifecycle_lock:
                    executor = self._transport_executor
                if executor is None:
                    break
                alive = await asyncio.wrap_future(
                    executor.submit(self._drop_interface_if_dead_serialized, target, iface)
                )
                if alive is None:
                    break
                if not alive:
                    logger.warning(f"Meshtastic target {target} dropped connection!")
                    break

                await asyncio.sleep(2.0)

    def _interface_is_alive(self, iface: Any) -> bool:
        """Best-effort liveness probe for a connected interface.

        Probe transport-specific handles first. meshtastic's
        ``MeshInterface.isConnected`` is a ``threading.Event`` *attribute* (not a
        method) present on every real interface, so it must be checked LAST and
        via ``is_set()``: checking it first would shadow the TCP/serial branches,
        and calling it raises (an Event is not callable) — masking real drops on
        both transports.
        """
        # TCP: TCPInterface exposes the live socket, but its _readBytes self-heals
        # dead sockets (close -> sleep 1 -> reconnect), creating a brief
        # socket=None window. Probing the raw socket in that window would falsely
        # report a drop and tear the interface down mid-self-heal. Trust the
        # library's authoritative isConnected Event instead — it is cleared only
        # in _disconnected() (a real drop), not during the self-heal window.
        is_connected = getattr(iface, "isConnected", None)
        if hasattr(iface, "socket"):
            if is_connected is not None and hasattr(is_connected, "is_set"):
                return bool(is_connected.is_set())
            return iface.socket is not None
        # Serial: pyserial stream exposes is_open / isOpen().
        stream = getattr(iface, "stream", None)
        if stream is not None:
            if hasattr(stream, "isOpen"):
                return bool(stream.isOpen())
            if hasattr(stream, "is_open"):
                return bool(stream.is_open)
            return True
        # Fallback: meshtastic's threading.Event liveness flag. Reuses the
        # is_connected binding from the top of this function (the attribute
        # hasn't changed); no need to re-read it.
        if hasattr(is_connected, "is_set"):
            return bool(is_connected.is_set())
        # No known liveness handle (e.g. the mock interface) — assume alive.
        return True

    async def _drain_queue_loop(self, lifecycle_id: int):
        """Monitor and drain the outbound messages queue when connections are active."""
        while self._lifecycle_is_active(lifecycle_id):
            if self._has_interfaces() and self._outbound_queue:
                with self._queue_lock:
                    item = self._outbound_queue.pop(0)

                try:
                    logger.info(f"Draining queued message to {item['chat_id']}")
                    # Shield the executor-backed transport call so disconnect
                    # can await its real result: requeue only when it definitely
                    # did not send, avoiding loss or a duplicate after teardown.
                    send_task = asyncio.create_task(
                        self._send_immediate(item["chat_id"], item["content"])
                    )
                    try:
                        res = await asyncio.shield(send_task)
                    except asyncio.CancelledError:
                        timeout = self._executor_shutdown_timeout()
                        done, _ = await asyncio.wait({send_task}, timeout=timeout)
                        if done:
                            try:
                                res = send_task.result()
                            except Exception:
                                with self._queue_lock:
                                    self._outbound_queue.insert(0, item)
                            else:
                                if not res.success:
                                    with self._queue_lock:
                                        self._outbound_queue.insert(0, item)
                        else:
                            # Delivery is indeterminate: do not requeue and risk
                            # a duplicate. The daemon worker may still complete.
                            logger.warning(
                                "Queued Meshtastic send still running after %.1fs during "
                                "disconnect; not requeueing (delivery indeterminate)",
                                timeout,
                            )
                        raise
                    if not res.success:
                        with self._queue_lock:
                            self._outbound_queue.insert(0, item)
                        await asyncio.sleep(5.0)
                    else:
                        delay = float(os.getenv("MESHTASTIC_CHUNK_DELAY", "4.0"))
                        await asyncio.sleep(delay)
                except Exception as e:
                    logger.error(f"Error draining queued message: {e}")
                    with self._queue_lock:
                        self._outbound_queue.insert(0, item)
                    await asyncio.sleep(5.0)
            else:
                await asyncio.sleep(1.0)

    def _start_disconnect_task(self, completion: ConcurrentFuture) -> None:
        """Start teardown on the event loop that owns platform tasks."""
        current_loop = asyncio.get_running_loop()
        with self._lifecycle_lock:
            if completion.done():
                return
            owner_loop = self._disconnect_owner_loop
            task = self._disconnect_task
            if (
                owner_loop is not None
                and owner_loop is not current_loop
                and owner_loop.is_running()
            ):
                # task=None means a call_soon_threadsafe callback is reserved
                # but has not run yet. A live task likewise still owns teardown.
                if task is None or not task.done():
                    return
            needs_task = task is None or task.done() or not task.get_loop().is_running()
            if needs_task:
                # Check + assign under one cross-thread lock so concurrent
                # takeover callers cannot launch duplicate teardown tasks.
                if task is not None and not task.done():
                    self._cancel_task_threadsafe(task)
                self._disconnect_owner_loop = current_loop
                self._disconnect_task = current_loop.create_task(self._disconnect_impl(completion))

    async def disconnect(self) -> None:
        """Request platform-loop teardown and await its shared completion."""
        platform_loop: asyncio.AbstractEventLoop | None = None
        with self._lifecycle_lock:
            if self._disconnecting:
                completion = self._disconnect_future
                start_teardown = False
            else:
                completion = ConcurrentFuture()
                self._disconnect_future = completion
                self._disconnecting = True
                self._disconnect_done.clear()
                self._running = False
                self._lifecycle_id += 1
                start_teardown = True
                # Snapshot the platform loop under the lock alongside the
                # _running flip so teardown is dispatched to the loop that owns
                # the lifecycle tasks.
                consumer_task = self._incoming_consumer_task
                platform_loop = consumer_task.get_loop() if consumer_task is not None else self.loop

        if completion is None:
            return
        if start_teardown:
            current_loop = asyncio.get_running_loop()
            if platform_loop is current_loop:
                with self._lifecycle_lock:
                    self._disconnect_owner_loop = current_loop
                self._start_disconnect_task(completion)
            elif platform_loop is not None and platform_loop.is_running():
                with self._lifecycle_lock:
                    self._disconnect_owner_loop = platform_loop
                try:
                    platform_loop.call_soon_threadsafe(self._start_disconnect_task, completion)
                except RuntimeError:
                    with self._lifecycle_lock:
                        self._disconnect_owner_loop = current_loop
                    self._start_disconnect_task(completion)
            else:
                # Platform loop already stopped: fallback cleanup can still
                # cancel (without awaiting) old-loop tasks and close transport.
                with self._lifecycle_lock:
                    self._disconnect_owner_loop = current_loop
                self._start_disconnect_task(completion)
        else:
            logger.debug("Waiting for Meshtastic disconnect already in progress")

        # Polling does not tie completion to the caller loop and caller
        # cancellation cannot cancel the shared teardown. If the owner loop
        # stops after accepting the callback, take over cleanup here.
        try:
            while not completion.done():
                # Detect cancelled/done owner tasks even when their loop is still
                # running, plus tasks stranded on a stopped loop.
                self._start_disconnect_task(completion)
                await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            # If this caller reserved a foreign-loop callback that never ran,
            # transfer the empty reservation locally before propagating caller
            # cancellation. The teardown task is independent of this waiter.
            current_loop = asyncio.get_running_loop()
            with self._lifecycle_lock:
                take_over = not completion.done() and self._disconnect_task is None
                if take_over:
                    self._disconnect_owner_loop = current_loop
            if take_over:
                self._start_disconnect_task(completion)
            raise
        completion.result()

    async def _disconnect_impl(self, completion: ConcurrentFuture) -> None:
        """Teardown implementation; always owned by the platform loop when live.

        Ownership-gate pattern: this task can be superseded at any time by a
        follower caller that takes over teardown (see ``_start_disconnect_task``).
        So every stage boundary re-checks, under one ``_lifecycle_lock`` hold,
        that ALL of the following still hold before touching shared state:
          1. ``self._disconnecting`` is still set (no completed teardown);
          2. ``self._disconnect_future is completion`` (same teardown epoch);
          3. ``self._disconnect_task is current_task`` (this task is the owner).
        Failing any gate means a newer owner exists, so this task returns
        ``superseded`` without mutating the newer lifecycle's state. The
        ``finally`` block only settles ``completion`` and clears flags when the
        same three checks pass, so a stale task can never advertise completion
        or wipe a successor's bookkeeping.
        """
        failure: BaseException | None = None
        cancelled = False
        superseded = False
        current_task = asyncio.current_task()
        try:
            with self._lifecycle_lock:
                if (
                    not self._disconnecting
                    or self._disconnect_future is not completion
                    or self._disconnect_task is not current_task
                ):
                    superseded = True
                    return
                # Claim all lifecycle-owned state atomically while ownership is
                # valid. A stale task may later finish work on this snapshot,
                # but can never reach a newly connected lifecycle's state.
                self._set_tools_adapter(None)
                self._unsubscribe_pubsub()
                with self._iface_lock:
                    detached = list(self._interfaces.items())
                    self._interfaces.clear()
                if detached:
                    # Accumulate (do NOT clear on supersede): a follower task
                    # continuing this same teardown epoch must close what any
                    # superseded owner detached. The list is only cleared when
                    # an owner actually settles completion. Double-close cannot
                    # occur — _disconnect_close_started gates who runs close.
                    self._disconnect_interfaces.extend(detached)
                ports = list(self._disconnect_interfaces)
                self._fail_pending_acks(reason="DISCONNECTED")
                lifecycle_tasks = list(self._reconnect_tasks.values())
                if self._queue_drain_task:
                    lifecycle_tasks.append(self._queue_drain_task)
                if self._incoming_consumer_task:
                    lifecycle_tasks.append(self._incoming_consumer_task)
                self._reconnect_tasks.clear()
                self._queue_drain_task = None
                self._incoming_consumer_task = None
                self._incoming_queue = None
            # Cancel on each task's owning loop. A takeover teardown may run on
            # a follower loop while the platform loop is still running; calling
            # Task.cancel() directly from another thread is not safe, so foreign-
            # loop tasks are marshalled via call_soon_threadsafe.
            current_loop = asyncio.get_running_loop()
            for task in lifecycle_tasks:
                self._cancel_task_threadsafe(task)
            local_lifecycle_tasks = [
                task for task in lifecycle_tasks if task.get_loop() is current_loop
            ]
            if local_lifecycle_tasks:
                await asyncio.gather(*local_lifecycle_tasks, return_exceptions=True)
            with self._lifecycle_lock:
                if (
                    not self._disconnecting
                    or self._disconnect_future is not completion
                    or self._disconnect_task is not current_task
                ):
                    superseded = True
                    return
                message_tasks = list(self._message_tasks)
                self._message_tasks.clear()
            for task in message_tasks:
                self._cancel_task_threadsafe(task)
            local_message_tasks = [
                task for task in message_tasks if task.get_loop() is current_loop
            ]
            if local_message_tasks:
                await asyncio.gather(*local_message_tasks, return_exceptions=True)
            with self._lifecycle_lock:
                if (
                    not self._disconnecting
                    or self._disconnect_future is not completion
                    or self._disconnect_task is not current_task
                ):
                    superseded = True
                    return
                should_start_close = not self._disconnect_close_started
                if should_start_close:
                    self._disconnect_close_started = True
            if should_start_close:
                await self._close_interfaces([iface for _, iface in ports])
            with self._lifecycle_lock:
                if (
                    not self._disconnecting
                    or self._disconnect_future is not completion
                    or self._disconnect_task is not current_task
                ):
                    superseded = True
                    return
                executor = self._transport_executor
                self._transport_executor = None
            if executor is not None:
                # Bounded join on a daemon worker — safe to call from the loop
                # thread because shutdown(wait) only joins with a timeout.
                await self._shutdown_transport_executor(executor)
            logger.info("Disconnected Meshtastic Platform.")
        except asyncio.CancelledError:
            # Do not advertise completion. Cleanup is idempotent; a polling
            # caller will atomically start a takeover task on a live loop.
            cancelled = True
            raise
        except BaseException as exc:
            failure = exc
            logger.error("Error disconnecting Meshtastic platform: %s", exc, exc_info=True)
        finally:
            if cancelled or superseded:
                with self._lifecycle_lock:
                    if self._disconnect_task is current_task:
                        self._disconnect_task = None
                        self._disconnect_owner_loop = None
            else:
                with self._lifecycle_lock:
                    if (
                        self._disconnecting
                        and self._disconnect_future is completion
                        and self._disconnect_task is current_task
                    ):
                        # Settle completion before releasing ownership so polling
                        # callers cannot observe task=None with completion pending.
                        try:
                            if failure is None:
                                completion.set_result(None)
                            else:
                                completion.set_exception(failure)
                        except ConcurrentInvalidStateError:
                            pass
                        self._disconnecting = False
                        self._disconnect_done.set()
                        self._disconnect_interfaces.clear()
                        self._disconnect_close_started = False
                        self._disconnect_task = None
                        self._disconnect_owner_loop = None

    def _on_receive_pubsub(self, packet, interface=None):
        """Wrapper callback called by the pubsub framework (running on PySub background thread).

        Always marshals onto the *platform* loop (``self.loop``): that is the
        loop that owns ``_incoming_queue``. There is no running loop on the
        pubsub thread, and a send-loop queue would never be drained.
        """
        if not self._running or self._incoming_queue is None:
            return
        if interface is not None:
            with self._iface_lock:
                if not any(active is interface for active in self._interfaces.values()):
                    logger.debug("Ignoring packet from detached Meshtastic interface")
                    return
        self._schedule_on_loop(
            self.loop,
            self._incoming_queue.put_nowait,
            (packet, interface),
            what="inbound packet enqueue",
        )

    def _on_connection_lost(self, interface=None):
        """Log Meshtastic-reported connection drops (pubsub background thread).

        The library fires ``meshtastic.connection.lost`` from ``_disconnected()``
        — e.g. on a reader-thread exit or a device reboot — cases the liveness
        poll can miss or lag. This is observability-only; the reconnect loop's
        ``_interface_is_alive`` poll still owns teardown to avoid racing the
        library's own TCP self-heal.
        """
        logger.warning("Meshtastic reported connection lost (interface=%s).", interface)

    def _on_connection_established(self, interface=None):
        """Log Meshtastic-reported connection establishment (pubsub background thread)."""
        logger.info("Meshtastic reported connection established (interface=%s).", interface)

    async def _consume_incoming_queue(self, lifecycle_id: int, incoming_queue: asyncio.Queue):
        """Consume incoming packets from the asyncio Queue."""
        while self._lifecycle_is_active(lifecycle_id):
            try:
                packet, interface = await incoming_queue.get()
                try:
                    # Keep lifecycle ownership through synchronous dispatch so
                    # disconnect/reconnect cannot advance the generation in the
                    # gap between validation and _on_receive side effects.
                    with self._lifecycle_lock:
                        if lifecycle_id != self._lifecycle_id or not self._running:
                            break
                        self._on_receive(packet, interface)
                finally:
                    incoming_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in incoming queue consumer: {e}", exc_info=True)

    def _handle_message_done(self, task: asyncio.Task):
        """Callback to discard finished task and log exceptions."""
        self._message_tasks.discard(task)
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in handle_message task: {e}", exc_info=True)

    @staticmethod
    def _channel_field(ch: Any, key: str) -> Any:
        """Read a channel field from a dict (mock) or a protobuf Channel (hardware).

        ``localNode.channels`` is a list of dicts under the mock interface but a
        list of protobuf ``Channel`` objects on real hardware — those have no
        ``.get()``, and their name lives under ``settings`` (``ch.settings.name``).
        """
        if isinstance(ch, dict):
            return ch.get(key)
        if key == "name":
            settings = getattr(ch, "settings", None)
            return getattr(settings, "name", None) if settings is not None else None
        return getattr(ch, key, None)

    def _on_receive(self, packet: dict, interface: Any = None):
        """Processes incoming packet in the main loop thread."""
        try:
            # Meshtastic response callbacks are one-shot. A relay can consume
            # onAckNak with an implicit ACK before the destination's real ACK,
            # so also feed matching routing packets from pubsub into existing
            # outbound ACK records. This precedes conversation authorization:
            # the request id must already be one of ours.
            self._maybe_record_pubsub_ack(packet)

            # Canonical node id (! + lowercase 8-hex) so Hermes gateway
            # allowlist exact-match and session keys stay consistent.
            from_id = self._normalize_node_id(packet.get("fromId") or packet.get("from"))
            if not from_id:
                return

            # Link metadata from the packet envelope.
            # Prefer rx* keys from the radio envelope; use is-not-None so a
            # legitimate 0.0 SNR (or 0 RSSI) is not treated as missing.
            snr = packet.get("rxSnr")
            if snr is None:
                snr = packet.get("snr")
            rssi = packet.get("rxRssi")
            if rssi is None:
                rssi = packet.get("rssi")
            hop_limit = packet.get("hopLimit")
            hop_start = packet.get("hopStart")
            hop_count = None
            if hop_limit is not None and hop_start is not None:
                hop_count = max(0, hop_start - hop_limit)

            # Track observed freshness for EVERY heard node — BEFORE the auth
            # gate, so last_heard/signal stay current even for nodes that aren't
            # allowed to talk to Hermes (e.g. a node the user just wants to watch).
            self._update_observed(from_id, packet.get("rxTime"), snr, rssi, hop_count)

            # Restriction check BEFORE any further processing
            if not self._is_authorized_node(from_id):
                logger.warning(f"Unauthorized node ID {from_id} skipped.")
                return

            # echo filtering (avoid bot replying to itself)
            my_node_id = None
            if interface:
                my_node_id = self._get_interface_node_id(interface)

            if my_node_id and from_id == my_node_id:
                return

            decoded = packet.get("decoded", {})
            portnum = decoded.get("portnum")

            # Log signal qualities immediately if present
            if snr is not None or rssi is not None:
                self._run_db_write(lambda: telemetry_db.log_signal(from_id, snr, rssi, hop_count))

            # Telemetry: portnums_pb2.PortNum.TELEMETRY_APP == 67 (MessageToDict
            # usually emits the string name). Older code treated 4/33 as
            # telemetry/env — those are NODEINFO_APP and IP_TUNNEL_APP.
            if portnum in ("TELEMETRY_APP", 67):
                self._run_db_write(lambda: self._handle_telemetry_packet(from_id, decoded))
                return

            # Position: portnums_pb2.PortNum.POSITION_APP == 3
            if portnum in ("POSITION_APP", 3):
                self._run_db_write(lambda: self._handle_position_packet(from_id, decoded))
                return

            # We only bridge TEXT messages (TEXT_MESSAGE_APP == 1)
            if portnum not in ("TEXT_MESSAGE_APP", 1, "TEXT_MESSAGE"):
                return

            # Library may expose decoded text and/or raw payload bytes.
            payload = decoded.get("payload")
            text_field = decoded.get("text")
            if payload is None and text_field is None:
                return

            if isinstance(payload, bytes):
                text = payload.decode("utf-8", errors="replace")
            elif payload is not None:
                text = str(payload)
            else:
                text = str(text_field)

            # Determine scopes (DM vs Channel)
            to_id = packet.get("toId") or packet.get("to")
            is_broadcast = False
            if to_id in (4294967295, 0xFFFFFFFF):
                is_broadcast = True
            elif isinstance(to_id, str):
                to_id_clean = to_id.strip().lower()
                if to_id_clean in (
                    "^all",
                    "broadcast",
                    "4294967295",
                    "0xffffffff",
                    "ffffffff",
                    "!ffffffff",
                ):
                    is_broadcast = True

            if isinstance(to_id, int):
                to_id = "^all" if is_broadcast else f"!{to_id:08x}"

            # By default the agent only answers direct messages — never a shared
            # channel/broadcast (avoids spamming a public channel's airtime).
            if is_broadcast and not self.allow_channels:
                logger.info(
                    "Ignoring channel/broadcast message from %s "
                    "(set MESHTASTIC_ALLOW_CHANNELS=true to answer channels)",
                    from_id,
                )
                return

            channel_index = packet.get("channel", 0)

            if is_broadcast or to_id == "^all" or to_id == "broadcast":
                # Scoped channel group chat session
                channel_name = str(channel_index)
                if (
                    interface
                    and hasattr(interface, "localNode")
                    and hasattr(interface.localNode, "channels")
                ):
                    for ch in interface.localNode.channels:
                        if self._channel_field(
                            ch, "index"
                        ) == channel_index and self._channel_field(ch, "name"):
                            channel_name = self._channel_field(ch, "name")
                            break
                chat_id = f"meshtastic:channel:{channel_name}"
                chat_type = "group"
            else:
                # Private direct message session
                chat_id = f"meshtastic:{from_id}"
                chat_type = "dm"

            # Fetch sender display names
            sender_name = from_id
            if interface and hasattr(interface, "nodes") and from_id in interface.nodes:
                user = interface.nodes[from_id].get("user", {})
                sender_name = user.get("longName") or user.get("shortName") or from_id

            # Build packet context for the agent.  Keep this compact but include
            # the LoRa metadata that matters for decisions/debugging.
            meta_lines = ["[Meshtastic packet metadata]"]
            meta_lines.append(f"from: {from_id} ({sender_name})")
            meta_lines.append(f"to: {to_id}")
            meta_lines.append(f"chat_scope: {chat_id} ({chat_type})")
            meta_lines.append(f"channel: {channel_index}")
            if snr is not None:
                meta_lines.append(f"rx_snr: {snr} dB")
            if rssi is not None:
                meta_lines.append(f"rx_rssi: {rssi} dBm")
            if hop_count is not None:
                meta_lines.append(f"hop_count: {hop_count}")
            if hop_limit is not None:
                meta_lines.append(f"hop_limit: {hop_limit}")
            if hop_start is not None:
                meta_lines.append(f"hop_start: {hop_start}")
            for key in (
                "id",
                "rxTime",
                "priority",
                "wantAck",
                "pkiEncrypted",
                "publicKey",
                "nextHop",
                "relayNode",
                "transportMechanism",
            ):
                if key in packet:
                    val = packet.get(key)
                    if key == "publicKey":
                        val = "present" if val else "absent"
                    meta_lines.append(f"{key}: {val}")
            packet_context = "\n".join(meta_lines)

            # Build Hermes MessageEvent
            source = self.build_source(
                chat_id=chat_id,
                user_id=from_id,
                user_name=sender_name,
                chat_type=chat_type,
            )

            # Prefer the radio's receive time so session history reflects when the
            # packet actually arrived over the air, not when the loop drained it
            # (packets can sit in the incoming queue across reconnects). A skewed
            # or garbage rxTime must never drop the message — fall back to now().
            event_ts = datetime.now()
            rx_time = packet.get("rxTime")
            if rx_time:
                try:
                    event_ts = datetime.fromtimestamp(float(rx_time))
                except (TypeError, ValueError, OverflowError, OSError):
                    pass

            # If the phone app sent this as a reply, surface the replied-to packet
            # id so the agent/gateway has reply context.
            reply_id = decoded.get("replyId")

            # Resolve a packet id; explicitly distinguish "absent" (None) from a
            # falsy-but-valid 0, since `or` would skip an id of 0.
            pkt_id = packet.get("id")
            if pkt_id is None:
                pkt_id = packet.get("rxTime") or time.time()
            event = MessageEvent(
                text=text,
                message_type=MessageType.TEXT,
                source=source,
                raw_message=packet,
                message_id=str(pkt_id),
                channel_context=packet_context,
                timestamp=event_ts,
                reply_to_message_id=str(reply_id) if reply_id is not None else None,
            )

            # Bridge to Hermes Gateway
            task = asyncio.create_task(self.handle_message(event))
            self._message_tasks.add(task)
            task.add_done_callback(self._handle_message_done)

        except Exception as e:
            logger.error(f"Error handling inbound Meshtastic packet: {e}", exc_info=True)

    @staticmethod
    def _first_not_none(*values: Any) -> Any:
        """Return the first value that is not None (0 / 0.0 / False are kept).

        Mirrored as ``tools._first_not_none`` (loaded as ``meshtastic_tools``);
        keep both in sync — tools cannot import adapter at module load without
        risking a cycle through the gateway stack.
        """
        for value in values:
            if value is not None:
                return value
        return None

    def _handle_telemetry_packet(self, node_id: str, decoded: dict):
        """Helper to process and log sensor/metrics telemetry."""
        try:
            # Check for device metrics or environment metrics nested
            telemetry = decoded.get("telemetry", {})
            if not telemetry:
                # If parsed differently by protobufs
                telemetry = decoded

            metrics = telemetry.get("deviceMetrics", {}) or {}
            env = telemetry.get("environmentMetrics", {}) or {}

            # batteryLevel 0 means external power on many devices — must not use
            # truthiness. Real mesh dicts use uptimeSeconds (MessageToDict);
            # accept legacy "uptime" too (mock / older payloads).
            battery = self._first_not_none(
                metrics.get("batteryLevel"), telemetry.get("batteryLevel")
            )
            voltage = self._first_not_none(metrics.get("voltage"), telemetry.get("voltage"))
            uptime = self._first_not_none(
                metrics.get("uptimeSeconds"),
                metrics.get("uptime"),
                telemetry.get("uptimeSeconds"),
                telemetry.get("uptime"),
            )

            temp = self._first_not_none(
                env.get("temperature"),
                env.get("barometric_temperature"),
                telemetry.get("temperature"),
            )
            humidity = self._first_not_none(
                env.get("relativeHumidity"), telemetry.get("relativeHumidity")
            )
            pressure = self._first_not_none(
                env.get("barometricPressure"), telemetry.get("barometricPressure")
            )

            if any(val is not None for val in (battery, voltage, temp, humidity, pressure, uptime)):
                telemetry_db.log_telemetry(
                    node_id=node_id,
                    battery_level=battery,
                    voltage=voltage,
                    temperature=temp,
                    humidity=humidity,
                    pressure=pressure,
                    uptime=uptime,
                )
                logger.debug(f"Logged telemetry for node {node_id}")
        except Exception as e:
            logger.error(f"Error logging telemetry packet: {e}")

    def _handle_position_packet(self, node_id: str, decoded: dict):
        """Helper to process and log position updates."""
        try:
            pos = decoded.get("position", {}) or decoded
            lat = pos.get("latitude")
            lon = pos.get("longitude")
            alt = pos.get("altitude")

            if lat is not None and lon is not None:
                # Real coordinates inside meshtastic packages are scaled down or decimals
                # protobuf stores them scaled by 1e7
                if abs(lat) > 90.0 or abs(lon) > 180.0:
                    lat = lat / 1e7
                    lon = lon / 1e7
                    if alt is not None:
                        alt = alt / 1.0  # standard float

                telemetry_db.log_position(
                    node_id=node_id, latitude=lat, longitude=lon, altitude=alt
                )
                logger.debug(f"Logged position for node {node_id}")
        except Exception as e:
            logger.error(f"Error logging position packet: {e}")

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: str | None = None,
        metadata: dict[str, Any] | None = None,
        allow_queueing: bool = True,
    ) -> SendResult:
        """
        Send a message. Queue it if not connected.
        Splits oversized payloads into numbered chunks automatically.
        """
        # Meshtastic reply threading: reply_to is a prior packet id (string); the
        # radio's replyId is an int. Only valid integer ids become threaded replies.
        reply_id = self._parse_reply_id(reply_to)
        wait_for_ack, ack_timeout = self._ack_wait_config(metadata)
        retries = self._send_retries(metadata)

        # Retry applies to direct messages only: broadcasts have no per-recipient
        # ACK, so re-sending them would flood the shared channel.
        dest = chat_id.split(":", 2)[1] if ":" in chat_id else ""
        is_dm = dest.startswith("!")

        # Retrying is only meaningful when we can observe delivery, so enabling
        # retries for a DM implies waiting for its ACK.
        if retries > 0 and is_dm and not wait_for_ack:
            wait_for_ack = True
            if ack_timeout <= 0:
                ack_timeout = 30.0

        max_attempts = retries + 1 if (retries > 0 and wait_for_ack and is_dm) else 1
        retry_backoff = self._retry_backoff()

        chunks = self._chunk_message(content)
        logger.info(
            "Sending message to %s. Splitting into %d chunks (bytes=%d).",
            chat_id,
            len(chunks),
            len((content or "").encode("utf-8")),
        )

        last_msg_id = None
        sent_ids = []
        raw_chunks = []
        for idx, chunk in enumerate(chunks):
            # Multi-packet LoRa delivery needs real pacing; too-fast writes are
            # accepted by the local serial API but get dropped/overwritten on air.
            if idx > 0:
                delay = float(os.getenv("MESHTASTIC_CHUNK_DELAY", "4.0"))
                logger.info(
                    "Waiting %.1fs before Meshtastic chunk %d/%d", delay, idx + 1, len(chunks)
                )
                await asyncio.sleep(delay)

            # Deliver this chunk, re-sending un-ACKed transient failures up to
            # ``max_attempts`` times (1 == no retry, the default).
            attempt = 0
            while True:
                attempt += 1
                res = await self._send_chunk(
                    chat_id,
                    chunk,
                    allow_queueing=allow_queueing,
                    wait_for_ack=wait_for_ack,
                    ack_timeout=ack_timeout,
                    reply_id=reply_id,
                )
                if res.success or attempt >= max_attempts or not self._is_retriable_failure(res):
                    break
                logger.warning(
                    "Meshtastic chunk %d/%d not delivered (attempt %d/%d): %s — retrying in %.1fs",
                    idx + 1,
                    len(chunks),
                    attempt,
                    max_attempts,
                    res.error,
                    retry_backoff,
                )
                await asyncio.sleep(retry_backoff)

            if res.raw_response is not None:
                res.raw_response["attempts"] = attempt
                raw_chunks.append(res.raw_response)
            if not res.success:
                logger.error(
                    "Meshtastic chunk %d/%d failed after %d attempt(s): %s",
                    idx + 1,
                    len(chunks),
                    attempt,
                    res.error,
                )
                return SendResult(
                    success=False,
                    message_id=last_msg_id,
                    error=f"chunk {idx + 1}/{len(chunks)} failed after {attempt} attempt(s): {res.error}",
                    raw_response={"chunks": raw_chunks, "ack_waited": wait_for_ack},
                    continuation_message_ids=tuple(sent_ids[1:]) if len(sent_ids) > 1 else (),
                )
            if attempt > 1:
                logger.info(
                    "Meshtastic chunk %d/%d delivered on attempt %d/%d",
                    idx + 1,
                    len(chunks),
                    attempt,
                    max_attempts,
                )
            if res.message_id:
                sent_ids.append(res.message_id)
                last_msg_id = res.message_id

        return SendResult(
            success=True,
            message_id=last_msg_id,
            raw_response={"chunks": raw_chunks, "ack_waited": wait_for_ack},
            continuation_message_ids=tuple(sent_ids[1:]) if len(sent_ids) > 1 else (),
        )

    def _chunk_message(self, content: str) -> list[str]:
        return chunking.chunk_message(content)

    def _split_utf8(self, text: str, limit: int) -> list[str]:
        return chunking.split_utf8(text, limit)

    def _extract_packet_id(self, pkt: Any) -> str | None:
        """Return a Meshtastic packet ID from object or dict packet shapes."""
        pkt_id = getattr(pkt, "id", None)
        if pkt_id is None and isinstance(pkt, dict):
            pkt_id = pkt.get("id")
        return str(pkt_id) if pkt_id is not None else None

    @staticmethod
    def _parse_reply_id(reply_to: str | None) -> int | None:
        """Coerce a Hermes reply_to (prior packet id string) to a Meshtastic int replyId.

        Returns None for absent/non-integer ids (e.g. synthetic "queued" markers),
        so sendText is only threaded onto a genuine prior packet.
        """
        if not reply_to:
            return None
        try:
            return int(reply_to)
        except (TypeError, ValueError):
            return None

    def _queue_outbound_chunk(self, chat_id: str, chunk: str) -> SendResult:
        """Enqueue a chunk while disconnected (bounded, oldest-first eviction)."""
        with self._queue_lock:
            if len(self._outbound_queue) >= 100:
                self._outbound_queue.pop(0)
            self._outbound_queue.append(
                {"chat_id": chat_id, "content": chunk, "timestamp": time.time()}
            )
        logger.info("Outbound connection down. Message successfully queued.")
        return SendResult(success=True, message_id="queued")

    def _send_text_serialized(
        self,
        *,
        lifecycle_id: int,
        dest: str,
        content: str,
        parts: list[str],
        reply_id: int | None,
        ack_callback: Callable[..., Any],
    ) -> tuple[str | None, Any, str]:
        """Select an interface and call sendText on the single transport worker.

        Returns ``(error, packet, dest)``. ``error`` is a short machine token:
        ``no_iface``, ``no_pubkey``, or None on success. The worker serializes
        concurrent agent-session sends and
        platform reconnect/close against Meshtastic's unsynchronized packet-id /
        response-handler / TX-queue state.
        """
        with self._iface_lock:
            if lifecycle_id != self._lifecycle_id or not self._running:
                return "no_iface", None, dest
            ifaces = list(self._interfaces.values())
            if not ifaces:
                return "no_iface", None, dest

        iface = ifaces[0]
        if dest.startswith("!"):
            node_info = None
            for current_iface in ifaces:
                nodes = getattr(current_iface, "nodes", None) or {}
                # Exact key first, then case-insensitive scan of the node DB.
                if dest in nodes:
                    iface = current_iface
                    node_info = nodes[dest]
                    break
                dest_bare = dest.lstrip("!")
                for nid, ninfo in nodes.items():
                    if str(nid).lower().lstrip("!") == dest_bare:
                        iface = current_iface
                        node_info = ninfo
                        dest = str(nid)  # use the library's key form for sendText
                        break
                if node_info is not None:
                    break
            if node_info is not None and not node_info.get("user", {}).get("publicKey"):
                return "no_pubkey", None, dest
            pkt = iface.sendText(
                text=content,
                destinationId=dest,
                wantAck=True,
                onResponse=ack_callback,
                replyId=reply_id,
            )
            return None, pkt, dest

        channel_index = 0
        channel_name_or_index = parts[2] if len(parts) > 2 else "0"
        if channel_name_or_index.isdigit():
            channel_index = int(channel_name_or_index)
        else:
            for current_iface in ifaces:
                if hasattr(current_iface, "localNode") and hasattr(
                    current_iface.localNode, "channels"
                ):
                    for ch in current_iface.localNode.channels:
                        ch_name = self._channel_field(ch, "name")
                        if ch_name and ch_name.lower() == channel_name_or_index.lower():
                            iface = current_iface
                            channel_index = self._channel_field(ch, "index") or 0
                            break
        pkt = iface.sendText(
            text=content,
            channelIndex=channel_index,
            wantAck=True,
            onResponse=ack_callback,
            replyId=reply_id,
        )
        return None, pkt, dest

    async def _send_chunk(
        self,
        chat_id: str,
        chunk: str,
        allow_queueing: bool = True,
        *,
        wait_for_ack: bool = False,
        ack_timeout: float = 0.0,
        reply_id: int | None = None,
    ) -> SendResult:
        """Helper to send a single wrapped chunk, queueing it on failure/disconnect."""
        # Fast path under _iface_lock (map presence only). _send_immediate
        # re-checks on the serialized transport worker so a disconnect
        # between these cannot send on a closed interface without surfacing
        # no_iface for queueing.
        if not self._has_interfaces():
            if wait_for_ack:
                return SendResult(
                    success=False, error="No active interfaces connected; cannot wait for ACK"
                )
            if not allow_queueing:
                return SendResult(
                    success=False, error="No active interfaces connected and queueing disabled"
                )
            return self._queue_outbound_chunk(chat_id, chunk)

        res = await self._send_immediate(
            chat_id,
            chunk,
            wait_for_ack=wait_for_ack,
            ack_timeout=ack_timeout,
            reply_id=reply_id,
        )
        # Race: interface dropped after the fast-path check but before locked send.
        if (
            not res.success
            and res.error == "No active interfaces connected"
            and not wait_for_ack
            and allow_queueing
        ):
            return self._queue_outbound_chunk(chat_id, chunk)
        return res

    async def _send_immediate(
        self,
        chat_id: str,
        content: str,
        *,
        wait_for_ack: bool = False,
        ack_timeout: float = 0.0,
        reply_id: int | None = None,
    ) -> SendResult:
        """Dispatch one text chunk immediately to the interface."""
        send_token: object | None = None
        try:
            parts = chat_id.split(":", 2)
            if len(parts) < 2:
                return SendResult(success=False, error="Invalid chat_id format")

            dest = parts[1]
            # DM destinations are ``!``-prefixed node ids — canonicalize case so
            # node-DB lookup matches the library's lowercase keys.
            if dest.startswith("!"):
                dest = self._normalize_node_id(dest) or dest

            send_token = object()
            with self._lifecycle_lock:
                executor = self._transport_executor
                lifecycle_id = self._lifecycle_id
            if executor is None:
                return SendResult(success=False, error="No active interfaces connected")
            with self._ack_lock:
                self._ack_inflight_tokens[send_token] = lifecycle_id
            ack_callback = self._make_ack_callback_for_send(dest, content, send_token, lifecycle_id)
            try:
                err, pkt, dest = await asyncio.wrap_future(
                    executor.submit(
                        lambda: self._send_text_serialized(
                            lifecycle_id=lifecycle_id,
                            dest=dest,
                            content=content,
                            parts=parts,
                            reply_id=reply_id,
                            ack_callback=ack_callback,
                        )
                    )
                )
            except RuntimeError as exc:
                if "cannot schedule new futures after shutdown" in str(exc).lower():
                    with self._ack_lock:
                        self._ack_inflight_tokens.pop(send_token, None)
                        self._early_ack_packets.pop(send_token, None)
                    return SendResult(success=False, error="No active interfaces connected")
                raise
            with self._lifecycle_lock:
                stale_lifecycle = lifecycle_id != self._lifecycle_id or not self._running
            # Inspect definitive pre-send failures before lifecycle turnover.
            # In particular, a stale-generation worker returns no_iface before
            # sendText, which lets _send_chunk safely queue a non-ACK message.
            if err == "no_iface":
                with self._ack_lock:
                    self._ack_inflight_tokens.pop(send_token, None)
                    self._early_ack_packets.pop(send_token, None)
                return SendResult(success=False, error="No active interfaces connected")
            if err == "no_pubkey":
                with self._ack_lock:
                    self._ack_inflight_tokens.pop(send_token, None)
                    self._early_ack_packets.pop(send_token, None)
                return SendResult(
                    success=False,
                    error=f"Target node {dest} has no public key; direct message cannot be encrypted",
                )
            if stale_lifecycle:
                with self._ack_lock:
                    self._ack_inflight_tokens.pop(send_token, None)
                    self._early_ack_packets.pop(send_token, None)
                pkt_id = self._extract_packet_id(pkt)
                # Note: deliberately NOT stored in _pending_acks/_ack_responses.
                # A stale-lifecycle send must not pollute the new lifecycle's ACK
                # bookkeeping (an old worker returning after reconnect cannot
                # enter new ACK state). The outcome is surfaced only via this
                # SendResult's raw_response.
                ack_record = {
                    "dest": dest,
                    "bytes": len(content.encode("utf-8")),
                    "status": AckStatus.TIMEOUT,
                    "error_reason": "DISCONNECTED",
                    "response_at": time.time(),
                }
                error = (
                    f"Meshtastic disconnected while waiting for ACK on packet {pkt_id}"
                    if wait_for_ack and pkt_id
                    else "Meshtastic disconnected while transport send was in progress"
                )
                return SendResult(
                    success=False,
                    message_id=pkt_id,
                    error=error,
                    raw_response={
                        "packet_id": pkt_id,
                        "dest": dest,
                        "ack_requested": True,
                        "ack_waited": wait_for_ack,
                        "ack_timeout": ack_timeout if wait_for_ack else None,
                        "ack": ack_record,
                    },
                )
            pkt_id = self._extract_packet_id(pkt)
            ack_future = self._track_pending_ack(
                pkt_id,
                dest,
                content,
                create_future=wait_for_ack,
                send_token=send_token,
            )
            with self._ack_lock:
                self._ack_inflight_tokens.pop(send_token, None)
                early_ack = self._early_ack_packets.pop(send_token, None)
            if early_ack is not None:
                early_packet, early_dest, early_content, early_lifecycle = early_ack
                self._record_ack_response(
                    early_packet,
                    early_dest,
                    early_content,
                    send_token=send_token,
                    lifecycle_id=early_lifecycle,
                )
            logger.info(
                "Meshtastic chunk queued: dest=%s packet_id=%s bytes=%d text=%r",
                dest,
                pkt_id,
                len(content.encode("utf-8")),
                content[:80],
            )
            raw_response = {
                "packet_id": pkt_id,
                "dest": dest,
                "ack_requested": True,
                "ack_waited": wait_for_ack,
                "ack_timeout": ack_timeout if wait_for_ack else None,
                "ack": self.get_ack_status(pkt_id) if pkt_id else None,
            }

            if wait_for_ack:
                if not pkt_id or not ack_future:
                    return SendResult(
                        success=False,
                        message_id=pkt_id,
                        error="Cannot wait for ACK without a packet id",
                        raw_response=raw_response,
                    )
                ack_record = await self._wait_for_ack(pkt_id, ack_future, ack_timeout)
                raw_response["ack"] = ack_record
                status = ack_record.get("status")
                if status == AckStatus.ACK:
                    return SendResult(success=True, message_id=pkt_id, raw_response=raw_response)
                if status == AckStatus.NAK:
                    reason = ack_record.get("error_reason") or "unknown"
                    return SendResult(
                        success=False,
                        message_id=pkt_id,
                        error=f"Meshtastic NAK for packet {pkt_id}: {reason}",
                        raw_response=raw_response,
                    )
                if status == AckStatus.IMPLICIT_ACK:
                    return SendResult(
                        success=False,
                        message_id=pkt_id,
                        error=(
                            f"Meshtastic implicit ACK only for packet {pkt_id} "
                            f"(relayed by {ack_record.get('ack_from')}; destination not confirmed)"
                        ),
                        raw_response=raw_response,
                    )
                # TIMEOUT (including DISCONNECTED from _fail_pending_acks).
                err_reason = ack_record.get("error_reason")
                if err_reason == "DISCONNECTED":
                    return SendResult(
                        success=False,
                        message_id=pkt_id,
                        error=f"Meshtastic disconnected while waiting for ACK on packet {pkt_id}",
                        raw_response=raw_response,
                    )
                return SendResult(
                    success=False,
                    message_id=pkt_id,
                    error=f"Meshtastic ACK timeout for packet {pkt_id}",
                    raw_response=raw_response,
                )

            return SendResult(success=bool(pkt), message_id=pkt_id, raw_response=raw_response)

        except Exception as e:
            logger.error(f"Failed to deliver message immediately: {e}", exc_info=True)
            return SendResult(success=False, error=str(e))
        finally:
            if send_token is not None:
                with self._ack_lock:
                    self._ack_inflight_tokens.pop(send_token, None)
                    self._early_ack_packets.pop(send_token, None)

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
        metadata: dict[str, Any] | None = None,
        **kwargs,
    ) -> SendResult:
        """Meshtastic has no edit primitive.

        Do NOT emulate edits by sending each progressive update: that floods LoRa
        and causes partial long-answer delivery. Returning unsupported lets the
        gateway fall back to a single final send(), which this adapter chunks.
        """
        del chat_id, message_id, content, finalize, metadata, kwargs
        return SendResult(success=False, error="Meshtastic does not support editing")

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        """Fetch chat details."""
        parts = chat_id.split(":", 2)
        dest = parts[1] if len(parts) > 1 else ""

        if dest.startswith("!"):
            # DM
            name = dest
            ifaces = self.get_interfaces()
            for iface in ifaces:
                if hasattr(iface, "nodes") and dest in iface.nodes:
                    user = iface.nodes[dest].get("user", {})
                    name = user.get("longName") or user.get("shortName") or dest
                    break
            return {"name": name, "type": "dm"}
        else:
            # Channel
            channel_name = parts[2] if len(parts) > 2 else "0"
            return {"name": f"LoRa Channel {channel_name}", "type": "group"}


def _env_enablement() -> dict | None:
    """Helper to register and seed config extra from environment."""
    port = os.getenv("MESHTASTIC_SERIAL_PORT")
    tcp_host = os.getenv("MESHTASTIC_TCP_HOST")
    # Enable the platform when either transport is configured.
    if not port and not tcp_host:
        return None

    return {
        "serial_port": port,
        # ``or`` (not the getenv default) so a blank ``VAR=`` in .env still
        # falls back to the default instead of raising on ``int("")``.
        "baud_rate": int(os.getenv("MESHTASTIC_BAUD_RATE") or 115200),
        "tcp_host": tcp_host or "",
        "tcp_port": int(os.getenv("MESHTASTIC_TCP_PORT") or DEFAULT_TCP_PORT),
        "allowed_nodes": os.getenv("MESHTASTIC_ALLOWED_NODES")
        or os.getenv("MESHTASTIC_ALLOWED_USERS", ""),
        "allow_all_users": os.getenv("MESHTASTIC_ALLOW_ALL_USERS", "").lower()
        in ("1", "true", "yes"),
        "home_channel": os.getenv("MESHTASTIC_HOME_CHANNEL", ""),
    }


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: str | None = None,
    media_files: list[str] | None = None,
    force_document: bool = False,
) -> dict[str, Any]:
    """Standalone cron ephemeral delivery sender support."""
    try:
        # Create an instance of MeshtasticAdapter
        adapter = MeshtasticAdapter(pconfig)

        # Connect to establish the interface(s)
        await adapter.connect()

        # Wait for the connection task to run and register the interface
        success = False
        error = None
        for _ in range(20):
            if adapter.get_interfaces():
                break
            await asyncio.sleep(0.1)

        try:
            res = await adapter.send(chat_id=chat_id, content=message, allow_queueing=False)
            success = res.success
            error = res.error
        finally:
            await adapter.disconnect()

        if success:
            return {"success": True}
        else:
            return {"error": error or "Failed to send message"}
    except Exception as e:
        logger.error(f"Standalone send failure: {e}")
        return {"error": str(e)}


def register(ctx):
    """Entry point: called by the Hermes plugin loader."""
    ctx.register_platform(
        name="meshtastic",
        label="Meshtastic",
        adapter_factory=lambda cfg: MeshtasticAdapter(cfg),
        check_fn=lambda: True,  # Fallback to mock logic guarantees loading
        # No strictly-required env var: the adapter connects over serial (auto
        # discovery) OR TCP (MESHTASTIC_TCP_HOST). required_env only drives setup
        # UI display, and listing one transport's var would mislabel the other as
        # "not configured".
        required_env=[],
        env_enablement_fn=_env_enablement,
        # Declare the allowlist env vars so the gateway's own _is_user_authorized
        # layer integrates with them (defense-in-depth + setup-UI visibility).
        # The legacy MESHTASTIC_ALLOWED_USERS alias is still read adapter-locally.
        allowed_users_env="MESHTASTIC_ALLOWED_NODES",
        allow_all_env="MESHTASTIC_ALLOW_ALL_USERS",
        cron_deliver_env_var="MESHTASTIC_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        max_message_length=233,
        emoji="📡",
        pii_safe=True,
        platform_hint=(
            "You are chatting with the user over the Meshtastic LoRa mesh network. "
            "Only the message TRANSPORT is constrained: replies are split into ~170-byte "
            "LoRa-safe chunks, so keep answers concise and avoid filler. Your capabilities "
            "are NOT limited — you retain all your normal tools, including web search and "
            "browsing (the gateway host has internet), code/file tools, and the mesh_* tools "
            "for the local radio network. When asked for research, live data, or current "
            "events, use web search and browse normally; the LoRa link only affects how the "
            "final answer is delivered, never whether you can look things up."
        ),
    )
