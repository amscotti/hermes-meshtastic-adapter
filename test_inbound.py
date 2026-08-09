"""
Receive-path tests for the Meshtastic platform adapter.

Two layers: (1) the pubsub-to-Hermes inbound integration path — packet
normalization, DM vs broadcast vs channel chat_id routing, the authz gate,
self-echo filtering, node freshness overlay, and telemetry/position/signal
persistence into telemetry_db — and (2) direct unit tests for the receive-stage
pipeline extracted into inbound.py. Layer (1) was extracted from
test_meshtastic.py.
"""

import asyncio
import json
import os
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from contextlib import closing
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

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
from adapter import MeshtasticAdapter
from inbound import (
    InboundProcessor,
    _packet_fingerprint,
    build_packet_context,
    canonicalize_to_id,
    channel_field,
    classify_portnum,
    enqueue_incoming,
    event_timestamp,
    extract_text,
    interface_node_id,
    is_authorized_node,
    is_broadcast_dest,
    log_position_packet,
    log_telemetry_packet,
    normalize_packet,
    resolve_channel_name,
    resolve_packet_id,
    resolve_sender_name,
)
from node_freshness import NodeFreshness
from telemetry_db import get_position_history, get_telemetry_history, init_db

handle_mesh_list_nodes = meshtastic_tools.handle_mesh_list_nodes
handle_mesh_signal_quality = meshtastic_tools.handle_mesh_signal_quality
handle_mesh_telemetry = meshtastic_tools.handle_mesh_telemetry
handle_mesh_telemetry_history = meshtastic_tools.handle_mesh_telemetry_history


def _unlink_db_files(path: str) -> None:
    """Unlink a SQLite DB file plus its WAL/SHM sidecars.

    ``init_db`` enables WAL mode, creating ``-wal``/``-shm`` sidecars that a
    plain ``os.unlink`` of the main ``.db`` leaves behind in ``$TMPDIR`` to
    accumulate across a long CI run. Best-effort: swallow missing-file errors.
    """
    for candidate in (path, f"{path}-wal", f"{path}-shm"):
        try:
            os.unlink(candidate)
        except OSError:
            pass


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


def _backdate_signal(node_id: str, when: float) -> None:
    """Age a node's signal rows, to exercise the direct-range expiry window."""
    with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
        conn.execute("UPDATE signal_quality SET timestamp = ? WHERE node_id = ?", (when, node_id))
        conn.commit()


class TestReceivePath(unittest.IsolatedAsyncioTestCase):
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
        _unlink_db_files(self._tmp_db.name)

    async def _poll_handle_message_calls(self, expected: int, timeout: float = 1.0) -> None:
        """Wait until handle_message has been called ``expected`` times.

        handle_message runs via ``asyncio.create_task`` on the platform loop, so
        a fixed sleep does not guarantee the spawned task ran on a loaded CI
        runner — poll with a deadline instead of asserting against a sleep window.
        """
        deadline = time.monotonic() + timeout
        while self.adapter.handle_message.call_count < expected:
            if time.monotonic() >= deadline:
                break
            await asyncio.sleep(0.01)
        self.assertEqual(self.adapter.handle_message.call_count, expected)

    async def _assert_handle_message_never_called(self, timeout: float = 0.3) -> None:
        """Negative counterpart to ``_poll_handle_message_calls``.

        ``_on_receive`` bridges an accepted text by scheduling
        ``handle_message`` via ``asyncio.create_task``; a fixed ``sleep`` before
        ``assert_not_called`` can **false-pass** on a loaded CI runner if that
        task has not yet been scheduled/run. Yield the loop for a short window so
        any spawned task gets to run, then assert it was never called — making a
        regression that bridged the message deterministically fail.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        self.adapter.handle_message.assert_not_called()

    async def test_inbound_dm_scoping(self):
        """Test private Direct Messages create isolated DM sessions."""
        # Simulated Direct Message Packet
        packet = {
            "fromId": "!ab12cd34",
            "toId": "!da1b1613",
            "channel": 0,
            "decoded": {
                "portnum": "TEXT_MESSAGE_APP",
                "payload": b"Hello Hermes, this is a private message.",
            },
            "rxSnr": 7.5,
            "rxRssi": -95,
            "id": 12345,
        }

        # Trigger inbound handler
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])

        await self._poll_handle_message_calls(1)

        # Verify event creation & gateway dispatch
        event = self.adapter.handle_message.call_args[0][0]

        self.assertIn("Hello Hermes, this is a private message.", event.text)
        self.assertIn("rx_snr: 7.5 dB", event.channel_context)
        self.assertIn("rx_rssi: -95 dBm", event.channel_context)
        self.assertEqual(event.source.chat_id, "meshtastic:!ab12cd34")
        self.assertEqual(event.source.chat_type, "dm")
        self.assertEqual(event.source.user_id, "!ab12cd34")

    async def test_inbound_chunk_fragments_bridge_and_log_diagnostic(self):
        """[i/n] fragments are bridged per packet (reassembly is the agent's job,
        by design) but each arrival is surfaced with a diagnostic log."""
        iface = self.adapter.get_interfaces()[0]
        parts = ["[1/3] part one", "[2/3] part two", "[3/3] part three"]
        with self.assertLogs("inbound", level="INFO") as logs:
            for i, text in enumerate(parts):
                packet = {
                    "fromId": "!ab12cd34",
                    "toId": "!da1b1613",
                    "channel": 0,
                    "decoded": {"portnum": "TEXT_MESSAGE_APP", "payload": text.encode()},
                    "rxSnr": 7.5,
                    "rxRssi": -95,
                    "id": 3000 + i,
                }
                self.adapter._on_receive(packet, iface)
                await asyncio.sleep(0.02)
        await self._poll_handle_message_calls(3)
        self.assertTrue(any("[i/n]" in m for m in logs.output))
        self.assertEqual(self.adapter.handle_message.call_args[0][0].text, parts[-1])

    async def test_inbound_timestamp_from_rxtime(self):
        """MessageEvent.timestamp mirrors the packet's rxTime, not loop-drain time."""
        fixed = 1_700_000_000
        packet = {
            "fromId": "!ab12cd34",
            "toId": "!da1b1613",
            "rxTime": fixed,
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "payload": b"timed packet"},
            "id": 12345,
        }
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await self._poll_handle_message_calls(1)

        event = self.adapter.handle_message.call_args[0][0]
        self.assertEqual(int(event.timestamp.timestamp()), fixed)

    async def test_inbound_garbage_rxtime_still_delivers(self):
        """A skewed/garbage rxTime must never drop the message (falls back to now)."""
        packet = {
            "fromId": "!ab12cd34",
            "toId": "!da1b1613",
            "rxTime": 99_999_999_999_999,  # would make fromtimestamp raise (year overflow)
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "payload": b"still here"},
            "id": 12346,
        }
        before = time.time()
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await self._poll_handle_message_calls(1)

        event = self.adapter.handle_message.call_args[0][0]
        self.assertGreaterEqual(event.timestamp.timestamp(), before - 1)  # fallback: now()

    async def test_inbound_packet_id_zero_not_treated_as_missing(self):
        """A valid (if unusual) packet id of 0 must not fall through to rxTime."""
        packet = {
            "fromId": "!ab12cd34",
            "toId": "!da1b1613",
            "rxTime": 1_700_000_000,
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "payload": b"id zero"},
            "id": 0,
        }
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await self._poll_handle_message_calls(1)

        event = self.adapter.handle_message.call_args[0][0]
        self.assertEqual(event.message_id, "0")  # not "1700000000"

    async def test_inbound_channel_scoping(self):
        """Test broadcasts create shared channel sessions (when channels enabled)."""
        self.adapter.allow_channels = True  # channels are opt-in
        # Simulated Broadcast Packet
        packet = {
            "fromId": "!ab12cd34",
            "toId": "^all",
            "channel": 0,
            "decoded": {
                "portnum": "TEXT_MESSAGE_APP",
                "payload": b"Hello mesh, this is a broadcast channel update.",
            },
            "rxSnr": 6.2,
            "rxRssi": -101,
            "id": 67890,
        }

        # Trigger inbound handler
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])

        await self._poll_handle_message_calls(1)

        # Verify event scoping
        event = self.adapter.handle_message.call_args[0][0]

        self.assertIn("Hello mesh, this is a broadcast channel update.", event.text)
        self.assertIn("rx_snr: 6.2 dB", event.channel_context)
        self.assertIn("rx_rssi: -101 dBm", event.channel_context)
        self.assertEqual(event.source.chat_id, "meshtastic:channel:Primary")
        self.assertEqual(event.source.chat_type, "group")

    def test_channel_field_dict_and_protobuf(self):
        """_channel_field reads both dict channels (mock) and protobuf ones (hw)."""
        d = {"index": 2, "name": "Alpha"}
        self.assertEqual(self.adapter._channel_field(d, "index"), 2)
        self.assertEqual(self.adapter._channel_field(d, "name"), "Alpha")
        # Protobuf Channel: no .get(), name nested under .settings.
        pb = SimpleNamespace(index=3, settings=SimpleNamespace(name="Beta"))
        self.assertEqual(self.adapter._channel_field(pb, "index"), 3)
        self.assertEqual(self.adapter._channel_field(pb, "name"), "Beta")

    async def test_broadcast_scoping_with_protobuf_channels(self):
        """Broadcast scoping must not crash on protobuf channels (real hardware).

        The old ch.get() raised AttributeError on protobuf Channel objects, so
        channel messages crashed and never reached Hermes.
        """
        self.adapter.allow_channels = True  # channels are opt-in
        iface = self.adapter.get_interfaces()[0]
        iface.localNode.channels = [
            SimpleNamespace(index=0, settings=SimpleNamespace(name="Primary")),
        ]
        packet = {
            "fromId": "!ab12cd34",  # authorized
            "toId": "^all",
            "channel": 0,
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "payload": b"channel hello"},
            "id": 4242,
        }
        self.adapter._on_receive(packet, iface)
        await self._poll_handle_message_calls(1)

        event = self.adapter.handle_message.call_args[0][0]
        self.assertEqual(event.source.chat_id, "meshtastic:channel:Primary")

    async def test_channel_message_ignored_by_default(self):
        """By default the agent answers DMs only — channel messages are dropped."""
        self.assertFalse(self.adapter.allow_channels)  # default
        packet = {
            "fromId": "!ab12cd34",  # authorized node, but posting to the channel
            "toId": "^all",
            "channel": 0,
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "payload": b"hi channel"},
            "id": 5150,
        }
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await self._assert_handle_message_never_called()  # not bridged -> no public reply

    async def test_dm_still_answered_with_channels_off(self):
        """A DM is still handled when channels are disabled (the default)."""
        self.assertFalse(self.adapter.allow_channels)
        packet = {
            "fromId": "!ab12cd34",
            "toId": "!da1b1613",  # DM to the gateway node
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "payload": b"direct hi"},
            "id": 5151,
        }
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await self._poll_handle_message_calls(1)
        self.assertEqual(
            self.adapter.handle_message.call_args[0][0].source.chat_id, "meshtastic:!ab12cd34"
        )

    async def test_unauthorized_filter(self):
        """Verify unauthorized nodes are correctly filtered out."""
        # Packet from non-whitelisted node
        packet = {
            "fromId": "!bad55555",
            "toId": "!da1b1613",
            "decoded": {
                "portnum": "TEXT_MESSAGE_APP",
                "payload": b"Unauthorized prompt injection attempt.",
            },
        }
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await asyncio.sleep(0.05)
        # Verify handler was never called
        self.adapter.handle_message.assert_not_called()

    async def test_unauthorized_node_still_observed(self):
        """An unauthorized node is filtered from Hermes but still tracked (watch-only)."""
        packet = {
            "fromId": "!9e754610",  # not in the allowlist
            "toId": "^all",
            "rxTime": int(time.time()),
            "rxSnr": 6.0,
            "rxRssi": -70,
            "hopStart": 3,
            "hopLimit": 3,  # hop_count == 0 → direct
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "payload": b"watch me"},
        }
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await asyncio.sleep(0.02)

        self.adapter.handle_message.assert_not_called()  # still not bridged to Hermes
        obs = self.adapter.get_observed_node("!9e754610")
        self.assertGreater(obs.get("last_heard", 0), 0)  # but its freshness IS recorded
        self.assertEqual(obs.get("snr"), 6.0)

    async def test_observability_persisted_for_unauthorized_nodes(self):
        """Telemetry/position/signal are written to SQLite for EVERY heard node.

        The allowlist controls who may talk to the agent, not what the agent can
        see of the mesh. Gating these writes left the DB holding data for the one
        allowlisted node only, so the agent could report nothing current about
        any other node.
        """
        stranger = "!bad55555"  # deliberately not allowlisted
        self.assertFalse(self.adapter._is_authorized_node(stranger))
        iface = self.adapter.get_interfaces()[0]
        self.adapter._on_receive(
            {
                "fromId": stranger,
                "toId": "^all",
                "rxSnr": 5.5,
                "rxRssi": -95,
                "hopStart": 3,
                "hopLimit": 2,
                "decoded": {
                    "portnum": "TELEMETRY_APP",
                    "telemetry": {"deviceMetrics": {"batteryLevel": 77, "voltage": 4.01}},
                },
            },
            iface,
        )
        self.adapter._on_receive(
            {
                "fromId": stranger,
                "toId": "^all",
                "decoded": {
                    "portnum": "POSITION_APP",
                    "position": {"latitude": 55.75, "longitude": 37.61, "altitude": 150},
                },
            },
            iface,
        )
        await asyncio.sleep(0.15)
        self.assertTrue(telemetry_db.get_telemetry_history(stranger, limit=1))
        self.assertTrue(telemetry_db.get_position_history(stranger, limit=1))
        self.assertTrue(telemetry_db.get_signal_history(stranger, limit=1))
        # ...but its text still never reaches the agent.
        self.adapter.handle_message.assert_not_called()

    async def test_self_echo_skipped_before_auth_gate(self):
        """Our own node's packets drop silently, not as "Unauthorized" warnings.

        The local node is normally absent from the allowlist, so running the auth
        gate first logged every self-echo as unauthorized and left the echo
        filter unreachable.
        """
        iface = self.adapter.get_interfaces()[0]
        own_id = iface.getMyNodeId()  # !da1b1613
        # Production shape: the gateway's own node is NOT in the allowlist.
        self.adapter.allowed_nodes = {"!ab12cd34"}
        self.assertFalse(self.adapter._is_authorized_node(own_id))
        with self.assertNoLogs("adapter", level="WARNING"):
            self.adapter._on_receive(
                {
                    "fromId": own_id,
                    "toId": "!ab12cd34",
                    "decoded": {"portnum": "TEXT_MESSAGE_APP", "payload": b"echo of our reply"},
                    "id": 6001,
                },
                iface,
            )
        await self._assert_handle_message_never_called()

    async def test_mesh_list_nodes_prefers_fresh_last_heard(self):
        """mesh_list_nodes overlays observed last_heard over the stale library value."""
        fresher = int(time.time() - 10)  # newer than mock !ab12cd34's lastHeard (now-300)
        self.adapter._update_observed("!ab12cd34", fresher, None, None, None)
        res = json.loads(await handle_mesh_list_nodes({}))
        node = next(n for n in res["nodes"] if n["node_id"] == "!ab12cd34")
        self.assertEqual(
            node["last_heard"], time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(fresher))
        )

    async def test_telemetry_persistence(self):
        """Test real-time telemetry logging to SQLite."""
        packet = {
            "fromId": "!ab12cd34",
            "decoded": {
                "portnum": "TELEMETRY_APP",
                "telemetry": {
                    "deviceMetrics": {
                        "batteryLevel": 88,
                        "voltage": 4.05,
                        "uptimeSeconds": 3600,
                    },
                    "environmentMetrics": {
                        "temperature": 18.5,
                        "relativeHumidity": 60.1,
                        "barometricPressure": 1012.5,
                    },
                },
            },
        }

        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await asyncio.sleep(0.1)
        # Query persistent DB
        history = get_telemetry_history("!ab12cd34", limit=1)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["battery_level"], 88)
        self.assertEqual(history[0]["temperature"], 18.5)
        self.assertEqual(history[0]["humidity"], 60.1)
        self.assertEqual(history[0]["uptime"], 3600)

    async def test_telemetry_numeric_portnum_and_zero_metrics(self):
        """Numeric TELEMETRY_APP (67) and falsy metrics (0 / 0.0) must still log.

        Port 4 is NODEINFO_APP — must not be treated as telemetry. batteryLevel 0
        means external power on many devices and must not be dropped by `or`.
        """
        packet = {
            "fromId": "!ab12cd34",
            "decoded": {
                "portnum": 67,  # portnums_pb2.PortNum.TELEMETRY_APP
                "telemetry": {
                    "deviceMetrics": {
                        "batteryLevel": 0,
                        "voltage": 0.0,
                        "uptimeSeconds": 0,
                    },
                },
            },
        }
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await asyncio.sleep(0.1)

        history = get_telemetry_history("!ab12cd34", limit=1)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["battery_level"], 0)
        self.assertEqual(history[0]["voltage"], 0.0)
        self.assertEqual(history[0]["uptime"], 0)

        # NODEINFO_APP (4) must not be mis-classified as telemetry.
        before = len(get_telemetry_history("!ab12cd34", limit=10))
        self.adapter._on_receive(
            {
                "fromId": "!ab12cd34",
                "decoded": {
                    "portnum": 4,
                    "user": {"id": "!ab12cd34", "longName": "x"},
                },
            },
            self.adapter.get_interfaces()[0],
        )
        await asyncio.sleep(0.1)
        self.assertEqual(len(get_telemetry_history("!ab12cd34", limit=10)), before)

    async def test_zero_snr_is_preserved(self):
        """A direct packet with SNR 0.0 must not be treated as missing signal."""
        # Exercise the packet-path extraction (rxSnr=0 must not fall through to None).
        self.adapter.allow_all = True
        packet = {
            "fromId": "!dddd4444",
            "toId": "!da1b1613",
            "rxSnr": 0.0,
            "rxRssi": -100,
            "hopStart": 3,
            "hopLimit": 3,  # 0 hops away
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "payload": b"snr zero"},
            "id": 9001,
        }
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await asyncio.sleep(0.05)
        obs = self.adapter.get_observed_node("!dddd4444")
        self.assertEqual(obs.get("snr"), 0.0)
        self.assertEqual(obs.get("rssi"), -100)

    async def test_inbound_text_field_without_payload(self):
        """decoded.text alone is enough when payload bytes are absent."""
        packet = {
            "fromId": "!ab12cd34",
            "toId": "!da1b1613",
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "text": "hello via text field"},
            "id": 9002,
        }
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await self._poll_handle_message_calls(1)
        self.assertEqual(self.adapter.handle_message.call_args[0][0].text, "hello via text field")

    async def test_position_persistence(self):
        """Test position logging and coordinates scaling."""
        packet = {
            "fromId": "!ab12cd34",
            "decoded": {
                "portnum": "POSITION_APP",
                "position": {
                    "latitude": 426983000,  # Scaled 1e7
                    "longitude": -711234000,  # Scaled 1e7
                    "altitude": 120,
                },
            },
        }

        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await asyncio.sleep(0.1)
        # Query persistent DB
        history = get_position_history("!ab12cd34", limit=1)
        self.assertEqual(len(history), 1)
        self.assertAlmostEqual(history[0]["latitude"], 42.6983)
        self.assertAlmostEqual(history[0]["longitude"], -71.1234)
        self.assertEqual(history[0]["altitude"], 120)

    async def test_numeric_node_id_normalization(self):
        """Verify numeric Meshtastic node IDs normalize to !hex IDs for sessions."""
        packet = {
            "from": 0xAB12CD34,
            "toId": "!da1b1613",
            "decoded": {
                "portnum": "TEXT_MESSAGE_APP",
                "payload": b"numeric sender id",
            },
            "id": 24680,
        }

        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await self._poll_handle_message_calls(1)

        event = self.adapter.handle_message.call_args[0][0]
        self.assertEqual(event.source.chat_id, "meshtastic:!ab12cd34")
        self.assertEqual(event.source.user_id, "!ab12cd34")

    def test_schedule_on_loop_skips_when_loop_not_running(self):
        """Dropped schedules must not raise; return False for observability."""
        dead = asyncio.new_event_loop()
        self.addCleanup(dead.close)
        called = []

        ok = self.adapter._schedule_on_loop(dead, called.append, "x", what="unit-test skip")
        self.assertFalse(ok)
        self.assertEqual(called, [])

        ok_none = self.adapter._schedule_on_loop(None, called.append, "x", what="unit-test none")
        self.assertFalse(ok_none)

    def test_schedule_on_loop_swallows_closed_loop_race(self):
        """call_soon_threadsafe can raise if the loop closes after is_running()."""
        loop = MagicMock()
        loop.is_running.return_value = True
        loop.call_soon_threadsafe.side_effect = RuntimeError("Event loop is closed")
        ok = self.adapter._schedule_on_loop(loop, lambda: None, what="unit-test toctou")
        self.assertFalse(ok)

    def test_inbound_pubsub_always_targets_platform_loop(self):
        """Inbound enqueue must use self.loop (queue owner), never a send loop."""
        platform_loop = MagicMock()
        platform_loop.is_running.return_value = True
        self.adapter.loop = platform_loop
        self.adapter._incoming_queue = MagicMock()

        self.adapter._on_receive_pubsub({"id": 1}, interface=None)

        platform_loop.call_soon_threadsafe.assert_called_once()
        args = platform_loop.call_soon_threadsafe.call_args[0]
        # The bounded enqueue (not a raw put_nowait) runs on the platform loop,
        # and the queue is passed explicitly so a closed/replaced queue can't
        # crash the callback.
        self.assertEqual(args[0], enqueue_incoming)
        self.assertIs(args[1], self.adapter._incoming_queue)
        self.assertEqual(args[2], {"id": 1})
        self.assertIsNone(args[3])

    async def test_inbound_reply_id_mapped_to_event(self):
        """decoded.replyId surfaces as MessageEvent.reply_to_message_id."""
        packet = {
            "fromId": "!ab12cd34",
            "toId": "!da1b1613",
            "decoded": {
                "portnum": "TEXT_MESSAGE_APP",
                "payload": b"a threaded reply",
                "replyId": 7788,
            },
            "id": 9001,
        }
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await self._poll_handle_message_calls(1)
        event = self.adapter.handle_message.call_args[0][0]
        self.assertEqual(event.reply_to_message_id, "7788")

    async def test_list_nodes_falls_back_to_signal_history(self):
        """A node with no live/observed SNR gets its signal from the DB history."""
        iface = self.adapter.get_interfaces()[0]
        iface.nodes["!cc001122"] = {
            "num": 1,
            "user": {"id": "!cc001122", "longName": "Historic", "shortName": "HIS"},
        }
        telemetry_db.log_signal("!cc001122", snr=2.5, rssi=-110)

        res = json.loads(await handle_mesh_list_nodes({}))
        node = next(n for n in res["nodes"] if n["node_id"] == "!cc001122")
        self.assertEqual(node["snr"], 2.5)
        self.assertEqual(node["rssi"], -110)

    async def test_list_nodes_marks_relayed_signal_as_not_direct(self):
        """A relayed reading must not read as direct range — the original bug.

        Asked which nodes were in direct line of sight, the agent had no hop
        data in this payload and answered by listing everything with an RSSI,
        which included nodes 1-5 hops out.
        """
        iface = self.adapter.get_interfaces()[0]
        iface.nodes["!dd001122"] = {
            "num": 2,
            "user": {"id": "!dd001122", "longName": "Far Relayed", "shortName": "FAR"},
        }
        telemetry_db.log_signal("!dd001122", snr=6.0, rssi=-100, hop_count=3)

        res = json.loads(await handle_mesh_list_nodes({}))
        node = next(n for n in res["nodes"] if n["node_id"] == "!dd001122")
        self.assertEqual(node["hops_away"], 3)
        self.assertFalse(node["heard_directly"])
        self.assertEqual(node["signal_source"], "relayed")
        self.assertIsNone(node["last_direct_heard"])

    async def test_list_nodes_reports_direct_node_and_prefers_direct_signal(self):
        """A 0-hop node is flagged direct, and its signal comes from a direct packet."""
        iface = self.adapter.get_interfaces()[0]
        iface.nodes["!dd003344"] = {
            "num": 3,
            "user": {"id": "!dd003344", "longName": "Neighbour", "shortName": "NBR"},
        }
        # Heard directly first, then via a relay: the direct reading is the one
        # that describes this node's own link, regardless of which is newer.
        telemetry_db.log_signal("!dd003344", snr=4.0, rssi=-95, hop_count=0)
        telemetry_db.log_signal("!dd003344", snr=-2.0, rssi=-115, hop_count=2)

        res = json.loads(await handle_mesh_list_nodes({}))
        node = next(n for n in res["nodes"] if n["node_id"] == "!dd003344")
        self.assertEqual(node["signal_source"], "direct")
        self.assertEqual(node["snr"], 4.0)
        self.assertEqual(node["rssi"], -95)
        self.assertIsNotNone(node["last_direct_heard"])

    async def test_hops_survive_a_restart_via_persisted_history(self):
        """Hop data outlives the in-memory observations a restart clears."""
        iface = self.adapter.get_interfaces()[0]
        iface.nodes["!dd005566"] = {
            "num": 4,
            "user": {"id": "!dd005566", "longName": "Persisted", "shortName": "PST"},
        }
        telemetry_db.log_signal("!dd005566", snr=3.0, rssi=-99, hop_count=0)
        self.adapter._node_freshness._observed.clear()  # what a gateway restart leaves behind

        res = json.loads(await handle_mesh_list_nodes({}))
        node = next(n for n in res["nodes"] if n["node_id"] == "!dd005566")
        self.assertEqual(node["hops_away"], 0)
        self.assertTrue(node["heard_directly"])

    async def test_unknown_hops_are_not_claimed_as_direct(self):
        """No hop information anywhere means unknown, never 'direct'."""
        iface = self.adapter.get_interfaces()[0]
        iface.nodes["!dd007788"] = {
            "num": 5,
            "user": {"id": "!dd007788", "longName": "Unknown Hops", "shortName": "UNK"},
            "snr": 5.0,  # library node DB reading, origin unknown
        }

        res = json.loads(await handle_mesh_list_nodes({}))
        node = next(n for n in res["nodes"] if n["node_id"] == "!dd007788")
        self.assertIsNone(node["hops_away"])
        self.assertFalse(node["heard_directly"])
        self.assertEqual(node["signal_source"], "unknown")

    async def test_stale_direct_reception_expires(self):
        """A node heard directly weeks ago is no longer 'in direct range'.

        Signal history is kept for 30 days, so without a window a node that has
        since moved or gone quiet would report as a neighbour forever.
        """
        iface = self.adapter.get_interfaces()[0]
        iface.nodes["!ee001122"] = {
            "num": 7,
            "user": {"id": "!ee001122", "longName": "Long Gone", "shortName": "GON"},
        }
        telemetry_db.log_signal("!ee001122", snr=5.0, rssi=-90, hop_count=0)
        _backdate_signal("!ee001122", time.time() - 20 * 86400)

        res = json.loads(await handle_mesh_list_nodes({}))
        node = next(n for n in res["nodes"] if n["node_id"] == "!ee001122")
        self.assertFalse(node["heard_directly"])
        # The evidence is still reported — it just no longer counts as current.
        self.assertIsNotNone(node["last_direct_heard"])
        self.assertGreater(node["last_direct_heard_age_hours"], 24)

    async def test_recent_direct_reception_still_counts(self):
        """Just inside the window, a direct reception is still direct range."""
        iface = self.adapter.get_interfaces()[0]
        iface.nodes["!ee003344"] = {
            "num": 8,
            "user": {"id": "!ee003344", "longName": "Recent", "shortName": "RCT"},
        }
        telemetry_db.log_signal("!ee003344", snr=5.0, rssi=-90, hop_count=0)
        _backdate_signal("!ee003344", time.time() - 6 * 3600)

        res = json.loads(await handle_mesh_list_nodes({}))
        node = next(n for n in res["nodes"] if n["node_id"] == "!ee003344")
        self.assertTrue(node["heard_directly"])

    async def test_signal_quality_reports_hops_per_trend_sample(self):
        """The trend must say which samples were direct — mixing links reads as noise."""
        iface = self.adapter.get_interfaces()[0]
        iface.nodes["!dd009900"] = {
            "num": 6,
            "user": {"id": "!dd009900", "longName": "Trended", "shortName": "TRD"},
        }
        telemetry_db.log_signal("!dd009900", snr=5.0, rssi=-90, hop_count=0)
        telemetry_db.log_signal("!dd009900", snr=-1.0, rssi=-118, hop_count=4)

        res = json.loads(await handle_mesh_signal_quality({"node_id": "!dd009900"}))
        self.assertEqual(res["current"]["signal_source"], "direct")
        # Still in direct range even though the newest packet came via 4 relays.
        self.assertTrue(res["current"]["heard_directly"])
        self.assertEqual(res["current"]["hops_away"], 4)
        self.assertEqual({s["hops_away"] for s in res["trend_history"]}, {0, 4})

    async def test_list_nodes_dedupes_across_interfaces(self):
        """The same node seen on two interfaces appears once."""
        iface = self.adapter.get_interfaces()[0]
        self.adapter._interfaces["second_port"] = iface  # same node DB twice
        try:
            res = json.loads(await handle_mesh_list_nodes({}))
            ids = [n["node_id"] for n in res["nodes"]]
            self.assertEqual(len(ids), len(set(ids)))
        finally:
            self.adapter._interfaces.pop("second_port", None)

    async def test_signal_quality_history_fallback_and_no_data(self):
        """signal_quality falls back to DB history; errors when nothing is known."""
        iface = self.adapter.get_interfaces()[0]
        iface.nodes["!cc001122"] = {
            "num": 2,
            "user": {"id": "!cc001122", "longName": "Historic", "shortName": "HIS"},
        }
        # No live snr, no history -> explicit no-readings error.
        result = json.loads(await handle_mesh_signal_quality({"node_id": "!cc001122"}))
        self.assertIn("No signal quality readings", result["error"])
        # With history -> falls back to the persisted reading and builds a trend.
        telemetry_db.log_signal("!cc001122", snr=1.5, rssi=-115)
        result = json.loads(await handle_mesh_signal_quality({"node_id": "!cc001122"}))
        self.assertEqual(result["current"]["snr"], 1.5)
        self.assertEqual(len(result["trend_history"]), 1)

    async def test_telemetry_history_fallback_and_no_data(self):
        """mesh_telemetry uses DB history when node metrics are absent; errors when neither."""
        iface = self.adapter.get_interfaces()[0]
        iface.nodes["!cc001122"] = {
            "num": 3,
            "user": {"id": "!cc001122", "longName": "Historic", "shortName": "HIS"},
        }
        # No metrics anywhere -> error.
        result = json.loads(await handle_mesh_telemetry({"node_id": "!cc001122"}))
        self.assertIn("No telemetry data", result["error"])
        # Persisted telemetry -> served from the DB fallback.
        telemetry_db.log_telemetry("!cc001122", battery_level=77, temperature=19.5)
        result = json.loads(await handle_mesh_telemetry({"node_id": "!cc001122"}))
        self.assertEqual(result["battery_level"], 77)
        self.assertEqual(result["temperature"], 19.5)

    async def test_history_window_selects_by_time_not_row_count(self):
        """since_hours asks for a period; rows outside it are excluded."""
        now = time.time()
        for age_hours, lat in ((1, 55.1), (10, 55.2), (100, 55.3)):
            telemetry_db.log_position("!ab12cd34", latitude=lat, longitude=61.0, altitude=1)
            with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
                conn.execute(
                    "UPDATE positions SET timestamp = ? WHERE latitude = ?",
                    (now - age_hours * 3600, lat),
                )
                conn.commit()

        res = json.loads(
            await handle_mesh_telemetry_history(
                {"node_id": "!ab12cd34", "metric_type": "positions", "since_hours": 24}
            )
        )
        lats = {h["latitude"] for h in res["history"]}
        self.assertEqual(lats, {55.1, 55.2})  # the 100h-old fix is outside the window
        self.assertEqual(res["returned"], 2)
        self.assertFalse(res["truncated"])
        self.assertIsNotNone(res["oldest_returned"])

    async def test_history_window_reports_truncation(self):
        """A window denser than the cap must say so, not look complete."""
        for i in range(5):
            telemetry_db.log_position(
                "!ab12cd34", latitude=40.0 + i / 100, longitude=61.0, altitude=1
            )

        res = json.loads(
            await handle_mesh_telemetry_history(
                {"node_id": "!ab12cd34", "metric_type": "positions", "since_hours": 24, "limit": 3}
            )
        )
        self.assertEqual(res["returned"], 3)
        self.assertTrue(res["truncated"])

    async def test_history_window_exact_limit_is_not_truncated(self):
        """Exactly `limit` rows in a window is complete, not cut."""
        for lat in (40.0, 40.01, 40.02):
            telemetry_db.log_position("!ab12cd34", latitude=lat, longitude=61.0, altitude=1)
        args = {"node_id": "!ab12cd34", "metric_type": "positions", "since_hours": 24}
        args["limit"] = 3
        res = json.loads(await handle_mesh_telemetry_history(args))
        self.assertEqual(res["returned"], 3)
        self.assertFalse(res["truncated"])

    async def test_history_window_rejects_nonsense_and_caps_range(self):
        """A bad since_hours errors out; an absurd one clamps to the retention period."""
        res = json.loads(
            await handle_mesh_telemetry_history({"node_id": "!ab12cd34", "since_hours": "soon"})
        )
        self.assertIn("error", res)
        res = json.loads(
            await handle_mesh_telemetry_history({"node_id": "!ab12cd34", "since_hours": -5})
        )
        self.assertIn("error", res)

        telemetry_db.log_signal("!ab12cd34", snr=1.0, rssi=-90)
        res = json.loads(
            await handle_mesh_telemetry_history(
                {
                    "node_id": "!ab12cd34",
                    "metric_type": "signal_quality",
                    "since_hours": 99999,  # far beyond retention
                }
            )
        )
        self.assertEqual(res["returned"], 1)  # clamped, not rejected

    async def test_telemetry_history_metric_types_and_limits(self):
        """telemetry_history serves all metric types, rejects bad ones, clamps limits."""
        telemetry_db.log_position("!ab12cd34", latitude=42.0, longitude=-71.0, altitude=10.0)
        telemetry_db.log_signal("!ab12cd34", snr=4.0, rssi=-98)

        res = json.loads(
            await handle_mesh_telemetry_history(
                {"node_id": "!ab12cd34", "metric_type": "positions"}
            )
        )
        self.assertEqual(res["metric_type"], "positions")
        self.assertEqual(len(res["history"]), 1)
        self.assertIn("time", res["history"][0])  # formatted timestamp added

        res = json.loads(
            await handle_mesh_telemetry_history(
                {"node_id": "!ab12cd34", "metric_type": "signal_quality", "limit": "not-a-number"}
            )
        )
        # A non-numeric limit errors like since_hours does (C11) instead of
        # silently degrading to the default.
        self.assertIn("Parameter 'limit' must be a number.", res["error"])

        res = json.loads(
            await handle_mesh_telemetry_history({"node_id": "!ab12cd34", "metric_type": "bogus"})
        )
        self.assertIn("Invalid metric_type", res["error"])

    async def test_inbound_uppercase_from_id_normalized(self):
        """Uppercase fromId is lowercased so Hermes allowlist exact-match works."""
        packet = {
            "fromId": "!AB12CD34",  # same node as allowlist !ab12cd34
            "toId": "!da1b1613",
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "payload": b"case fold"},
            "id": 9100,
        }
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await self._poll_handle_message_calls(1)
        event = self.adapter.handle_message.call_args[0][0]
        self.assertEqual(event.source.user_id, "!ab12cd34")
        self.assertEqual(event.source.chat_id, "meshtastic:!ab12cd34")


class _RecordingFreshness:
    """FreshnessStore fake that records update() calls for assertions."""

    def __init__(self):
        self.calls = []

    def update(self, node_id, rx_time, snr, rssi, hop_count):
        self.calls.append((node_id, rx_time, snr, rssi, hop_count))


class TestInboundStages(unittest.TestCase):
    """Direct unit tests for the receive-stage pipeline in inbound.py.

    These call the pure stages / InboundProcessor with hand-built packets — no
    adapter, no interface, no DB. Writers are recording fakes; the authz /
    broadcast / classification decisions are asserted from the result.
    """

    _TEXT = {"portnum": "TEXT_MESSAGE_APP", "payload": b"hello"}

    @staticmethod
    def _normalize_id(node_id):
        return MeshtasticAdapter._normalize_node_id(node_id)

    def _processor(self, **overrides):
        defaults = {
            "normalize_id": self._normalize_id,
            "freshness": _RecordingFreshness(),
            "allow_all": lambda: False,
            "allowed_nodes": lambda: set(),
        }
        defaults.update(overrides)
        return InboundProcessor(**defaults)

    # -- normalization ----------------------------------------------------

    def test_normalize_maps_from_id_and_to_id(self):
        norm = normalize_packet({"fromId": "!ab12cd34", "toId": "!da1b1613"}, self._normalize_id)
        self.assertEqual(norm.sender, "!ab12cd34")
        self.assertEqual(norm.to_id, "!da1b1613")
        self.assertEqual(norm.channel_index, 0)  # default

    def test_normalize_present_none_channel_becomes_default(self):
        """A present-but-None channel must fall back to the documented default 0.

        ``_packet_get`` preserves a present-None for dicts; without coercion
        that None renders as the literal "None" in the context and matches the
        first index-less channel in resolve_channel_name.
        """
        norm = normalize_packet({"fromId": "!ab12cd34", "channel": None}, self._normalize_id)
        self.assertEqual(norm.channel_index, 0)
        # ...and resolve_channel_name does not match an index-less channel on None.
        iface = SimpleNamespace(
            localNode=SimpleNamespace(channels=[{"index": None, "name": "Indexless"}])
        )
        self.assertEqual(resolve_channel_name(iface, norm.channel_index), "0")

    def test_normalize_accepts_numeric_from(self):
        norm = normalize_packet({"from": 0xAB12CD34}, self._normalize_id)
        self.assertEqual(norm.sender, "!ab12cd34")

    def test_normalize_missing_sender_is_none(self):
        norm = normalize_packet({"toId": "!da1b1613"}, self._normalize_id)
        self.assertIsNone(norm.sender)

    def test_normalize_rx_links_preferred_over_plain(self):
        norm = normalize_packet(
            {"rxSnr": 7.5, "snr": 1.0, "rxRssi": -95, "rssi": -60}, self._normalize_id
        )
        self.assertEqual(norm.snr, 7.5)
        self.assertEqual(norm.rssi, -95)

    def test_normalize_keeps_zero_snr(self):
        norm = normalize_packet({"rxSnr": 0.0, "rxRssi": -100}, self._normalize_id)
        self.assertEqual(norm.snr, 0.0)
        self.assertEqual(norm.rssi, -100)

    def test_normalize_falls_back_to_plain_links(self):
        norm = normalize_packet({"snr": -2.5}, self._normalize_id)
        self.assertEqual(norm.snr, -2.5)
        self.assertIsNone(norm.rssi)

    def test_normalize_hop_count_from_start_and_limit(self):
        norm = normalize_packet({"hopStart": 3, "hopLimit": 1}, self._normalize_id)
        self.assertEqual(norm.hop_count, 2)
        self.assertIsNone(normalize_packet({"hopStart": 3}, self._normalize_id).hop_count)

    def test_normalize_hop_count_coerces_strings_and_guards_garbage(self):
        """Non-int hop fields must not raise (log-amplification DoS guard)."""
        self.assertEqual(
            normalize_packet({"hopStart": "3", "hopLimit": "1"}, self._normalize_id).hop_count, 2
        )
        self.assertEqual(
            normalize_packet({"hopStart": 3, "hopLimit": "1"}, self._normalize_id).hop_count, 2
        )
        bad = normalize_packet({"hopStart": "x", "hopLimit": 1}, self._normalize_id)
        self.assertIsNone(bad.hop_count)

    def test_normalize_protobuf_like_packet(self):
        pb = SimpleNamespace(
            fromId="!ab12cd34",
            rxSnr=6.0,
            hopStart=3,
            hopLimit=2,
            decoded={"portnum": "TEXT_MESSAGE_APP"},
        )
        norm = normalize_packet(pb, self._normalize_id)
        self.assertEqual(norm.sender, "!ab12cd34")
        self.assertEqual(norm.snr, 6.0)
        self.assertEqual(norm.hop_count, 1)
        self.assertEqual(norm.portnum, "TEXT_MESSAGE_APP")

    def test_normalize_non_dict_decoded_becomes_empty(self):
        norm = normalize_packet({"fromId": "!ab12cd34", "decoded": None}, self._normalize_id)
        self.assertEqual(norm.decoded, {})
        self.assertIsNone(norm.portnum)

    def test_normalize_bounds_oversized_sender(self):
        """A hostile unbounded fromId string is capped before reaching writers.

        ``_normalize_node_id`` lowercases non-canonical shapes verbatim, so a
        very long fromId would flow unbounded into telemetry/signal rows.
        Defense-in-depth: normalize_packet caps it.
        """
        long_id = "x" * 5000
        norm = normalize_packet({"fromId": long_id}, self._normalize_id)
        self.assertIsNotNone(norm.sender)
        self.assertLessEqual(len(norm.sender), 32)
        self.assertEqual(norm.sender, "x" * 32)
        # A canonical 9-char node id is untouched.
        self.assertEqual(
            normalize_packet({"fromId": "!ab12cd34"}, self._normalize_id).sender, "!ab12cd34"
        )

    # -- text extraction --------------------------------------------------

    def test_extract_text_payload_bytes_preferred(self):
        self.assertEqual(extract_text({"payload": b"hi"}), "hi")
        self.assertEqual(extract_text({"payload": "string payload"}), "string payload")
        self.assertEqual(extract_text({"text": "via field"}), "via field")

    def test_extract_text_replaces_invalid_utf8(self):
        self.assertEqual(extract_text({"payload": b"\xff\xfe"}), "\ufffd\ufffd")

    def test_extract_text_none_when_both_absent(self):
        self.assertIsNone(extract_text({}))

    def test_extract_text_rejects_repr_garbage(self):
        """Non-bytes payload / non-str text must drop, not bridge a Python repr."""
        self.assertIsNone(extract_text({"payload": {"a": 1}}))
        self.assertIsNone(extract_text({"payload": [1, 2]}))
        self.assertIsNone(extract_text({"text": 123}))
        self.assertIsNone(extract_text({"text": None}))

    # -- classification ---------------------------------------------------

    def test_classify_portnum_routes_telemetry_position_text(self):
        self.assertEqual(classify_portnum("TELEMETRY_APP"), "telemetry")
        self.assertEqual(classify_portnum(67), "telemetry")
        self.assertEqual(classify_portnum("POSITION_APP"), "position")
        self.assertEqual(classify_portnum(3), "position")
        self.assertEqual(classify_portnum("TEXT_MESSAGE_APP"), "text")
        self.assertEqual(classify_portnum(1), "text")
        self.assertEqual(classify_portnum(4), "other")  # NODEINFO_APP
        self.assertEqual(classify_portnum(None), "other")

    def test_authz_pre_check_decisions(self):
        # The adapter's __init__ expands the allowlist to bang / no-bang forms,
        # so match that shape here.
        allowed = {"!ab12cd34", "ab12cd34", "da1b1613", "!da1b1613"}
        self.assertTrue(is_authorized_node("!ab12cd34", allow_all=False, allowed_nodes=allowed))
        self.assertTrue(is_authorized_node("AB12CD34", allow_all=False, allowed_nodes=allowed))
        self.assertTrue(is_authorized_node("da1b1613", allow_all=False, allowed_nodes=allowed))
        self.assertFalse(is_authorized_node("!bad55555", allow_all=False, allowed_nodes=allowed))
        self.assertTrue(is_authorized_node("!whoever", allow_all=True, allowed_nodes=allowed))

    def test_authz_rejects_multi_bang_ids(self):
        """\"!!<allowed-id>\" is malformed — a single-bang strip must not pass it."""
        allowed = {"!ab12cd34", "ab12cd34"}
        self.assertFalse(is_authorized_node("!!ab12cd34", allow_all=False, allowed_nodes=allowed))
        self.assertFalse(is_authorized_node("!!!ab12cd34", allow_all=False, allowed_nodes=allowed))

    def test_broadcast_dest_detection(self):
        self.assertTrue(is_broadcast_dest(4294967295))
        self.assertTrue(is_broadcast_dest(0xFFFFFFFF))
        for form in ("^all", "broadcast", "4294967295", "0xffffffff", "ffffffff", "!ffffffff"):
            self.assertTrue(is_broadcast_dest(form), form)
        self.assertTrue(is_broadcast_dest("  ^ALL  "))
        self.assertFalse(is_broadcast_dest("!ab12cd34"))
        self.assertFalse(is_broadcast_dest("!da1b1613"))
        self.assertFalse(is_broadcast_dest(None))

    def test_canonicalize_to_id(self):
        # Numeric DM dest → !<8hex>.
        self.assertEqual(canonicalize_to_id(0xDA1B1613), ("!da1b1613", False))
        # Int / string broadcast dests collapse to ^all.
        self.assertEqual(canonicalize_to_id(0xFFFFFFFF), ("^all", True))
        self.assertEqual(canonicalize_to_id("4294967295"), ("^all", True))
        self.assertEqual(canonicalize_to_id("broadcast"), ("^all", True))
        # Out-of-range int stays as-is (never mints a malformed !-id).
        for bad in (-5, 2**32):
            self.assertEqual(canonicalize_to_id(bad), (bad, False))
        # String DM dest passes through unchanged.
        self.assertEqual(canonicalize_to_id("!da1b1613"), ("!da1b1613", False))
        # bool (bool ⊂ int) must NOT format as a node id.
        for bad in (True, False):
            out, is_bcast = canonicalize_to_id(bad)
            self.assertNotIn("!", str(out))
            self.assertNotEqual(out, "!00000001")
            self.assertFalse(is_bcast)

    # -- message id / timestamp -------------------------------------------

    def test_resolve_packet_id_zero_is_not_treated_as_missing(self):
        self.assertEqual(resolve_packet_id({"id": 0, "rxTime": 1_700_000_000}), "0")
        self.assertEqual(resolve_packet_id({"id": 123}), "123")
        self.assertEqual(resolve_packet_id({"rxTime": 1_700_000_000}), "1700000000")
        self.assertNotEqual(resolve_packet_id({}), "")  # wall-clock fallback

    def test_resolve_packet_id_dedups_id_less_retransmissions(self):
        """Byte-identical id-less, rxTime-less packets get the same message id."""
        pkt = {"fromId": "!ab12cd34", "decoded": {"portnum": "TEXT_MESSAGE_APP", "payload": b"hi"}}
        other = {
            "fromId": "!ab12cd34",
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "payload": b"yo"},
        }
        self.assertEqual(resolve_packet_id(pkt), resolve_packet_id(pkt))
        self.assertNotEqual(resolve_packet_id(pkt), resolve_packet_id(other))
        self.assertTrue(resolve_packet_id(pkt))

    def test_packet_fingerprint_falls_back_on_unsafe_dict_and_non_dict(self):
        """_packet_fingerprint survives non-serializable / non-dict packets.

        A circular dict makes json.dumps raise (ValueError), and a non-dict
        packet skips JSON entirely — both fall back to a stable repr hash.
        """
        circular: dict = {"decoded": {"portnum": "TEXT_MESSAGE_APP"}}
        circular["self"] = circular
        self.assertTrue(_packet_fingerprint(circular))
        # Deterministic across calls, distinct across packets.
        self.assertEqual(_packet_fingerprint(circular), _packet_fingerprint(circular))
        self.assertNotEqual(
            _packet_fingerprint(circular),
            _packet_fingerprint({"decoded": {"portnum": "POSITION_APP"}}),
        )
        # Non-dict packet shape (protobuf-like SimpleNamespace).
        ns_pkt = SimpleNamespace(decoded={"portnum": "TEXT_MESSAGE_APP"}, fromId="!ab12cd34")
        fp = _packet_fingerprint(ns_pkt)
        self.assertTrue(fp)
        self.assertEqual(_packet_fingerprint(ns_pkt), fp)
        # A non-bytes leaf falls through json.dumps' default hook to repr.
        leaf = object()
        obj_leaf = _packet_fingerprint({"decoded": {"unserializable": leaf}})
        self.assertTrue(obj_leaf)
        self.assertEqual(obj_leaf, _packet_fingerprint({"decoded": {"unserializable": leaf}}))

    def test_event_timestamp_uses_rxtime_and_falls_back_on_garbage(self):
        ts = event_timestamp({"rxTime": 1_700_000_000})
        self.assertEqual(int(ts.timestamp()), 1_700_000_000)
        fallback = event_timestamp({"rxTime": 99_999_999_999_999})  # year overflow
        self.assertLess(abs(fallback.timestamp() - time.time()), 5)
        self.assertLess(abs(event_timestamp({}).timestamp() - time.time()), 5)

    def test_event_timestamp_clamps_far_future_rxtime(self):
        """A valid-but-far-future rxTime must not become the event timestamp."""
        future = time.time() + 1e6
        ts = event_timestamp({"rxTime": future})
        self.assertLessEqual(ts.timestamp(), time.time())

    def test_event_timestamp_clamps_negative_rxtime(self):
        """A valid-but-negative rxTime must not yield a 1969-era event timestamp.

        ``datetime.fromtimestamp(-1)`` parses cleanly, so without a floor a
        truthy negative rxTime would slip past the far-future clamp and plant
        the message at the front of session-history ordering.
        """
        for bad in (-1, -1e6, -1_700_000_000):
            ts = event_timestamp({"rxTime": bad})
            self.assertLess(abs(ts.timestamp() - time.time()), 5, bad)
            # And never a pre-2020 timestamp.
            self.assertGreaterEqual(ts.timestamp(), 1_577_836_800)  # 2020-01-01

    # -- channel / sender name helpers ------------------------------------

    def test_channel_field_dict_and_protobuf(self):
        d = {"index": 2, "name": "Alpha"}
        self.assertEqual(channel_field(d, "index"), 2)
        self.assertEqual(channel_field(d, "name"), "Alpha")
        pb = SimpleNamespace(index=3, settings=SimpleNamespace(name="Beta"))
        self.assertEqual(channel_field(pb, "index"), 3)
        self.assertEqual(channel_field(pb, "name"), "Beta")

    def test_resolve_channel_name_dict_and_protobuf_channels(self):
        iface = SimpleNamespace(
            localNode=SimpleNamespace(
                channels=[
                    SimpleNamespace(index=0, settings=SimpleNamespace(name="Primary")),
                    {"index": 1, "name": "Work"},
                ]
            )
        )
        self.assertEqual(resolve_channel_name(iface, 0), "Primary")
        self.assertEqual(resolve_channel_name(iface, 1), "Work")
        self.assertEqual(resolve_channel_name(iface, 2), "2")  # unknown index
        self.assertEqual(resolve_channel_name(None, 3), "3")  # no interface

    def test_resolve_channel_name_protobuf_channel_unset_index(self):
        """A protobuf Channel with an unset index (proto3 default 0) resolves by name."""
        pb = SimpleNamespace(index=0, settings=SimpleNamespace(name="Primary"))
        iface = SimpleNamespace(localNode=SimpleNamespace(channels=[pb]))
        self.assertEqual(resolve_channel_name(iface, 0), "Primary")
        self.assertEqual(resolve_channel_name(iface, 1), "1")

    def test_enqueue_incoming_sheds_oldest_when_full(self):
        """A full inbound queue sheds the OLDEST packet, never exceeding its bound."""
        q = asyncio.Queue(maxsize=2)
        q.put_nowait(("oldest", None))
        q.put_nowait(("old", None))
        enqueue_incoming(q, "new", None)
        self.assertEqual(q.qsize(), 2)
        drained = []
        while not q.empty():
            drained.append(q.get_nowait()[0])
        self.assertEqual(drained, ["old", "new"])

    def test_resolve_sender_name(self):
        iface = SimpleNamespace(
            nodes={"!ab12cd34": {"user": {"longName": "Long", "shortName": "S"}}}
        )
        self.assertEqual(resolve_sender_name(iface, "!ab12cd34"), "Long")
        self.assertEqual(resolve_sender_name(SimpleNamespace(nodes={}), "!ab12cd34"), "!ab12cd34")
        self.assertEqual(resolve_sender_name(None, "!ab12cd34"), "!ab12cd34")

    def test_build_packet_context_marks_public_key_presence(self):
        ctx = build_packet_context(
            {"id": 7, "rxTime": 1_700_000_000, "publicKey": None, "nextHop": 123},
            sender="!ab12cd34",
            sender_name="Long",
            to_id="!da1b1613",
            chat_id="meshtastic:!ab12cd34",
            chat_type="dm",
            channel_index=0,
            snr=7.5,
            rssi=-95,
            hop_count=1,
            hop_limit=2,
            hop_start=3,
        )
        self.assertIn("rx_snr: 7.5 dB", ctx)
        self.assertIn("rx_rssi: -95 dBm", ctx)
        self.assertIn("hop_count: 1", ctx)
        self.assertIn("publicKey: absent", ctx)
        self.assertIn("id: 7", ctx)
        present = build_packet_context(
            {"publicKey": b"\x01"},
            sender="!a",
            sender_name="a",
            to_id="!b",
            chat_id="c",
            chat_type="dm",
            channel_index=0,
            snr=None,
            rssi=None,
            hop_count=None,
            hop_limit=None,
            hop_start=None,
        )
        self.assertIn("publicKey: present", present)

    def test_build_packet_context_coerces_non_numeric_links(self):
        """A non-numeric / NaN snr/rssi must drop, not render as ``nan`` / repr.

        Mirrors the finite-numeric coercion applied at every persistence
        boundary; the context block is agent-visible (post-auth, display-only).
        """
        for bad in (float("nan"), float("inf"), "not-a-number", True, None):
            ctx = build_packet_context(
                {},
                sender="!a",
                sender_name="a",
                to_id="!b",
                chat_id="c",
                chat_type="dm",
                channel_index=0,
                snr=bad,
                rssi=bad,
                hop_count=bad,
                hop_limit=bad,
                hop_start=bad,
            )
            self.assertNotIn("nan", ctx.lower(), bad)
            self.assertNotIn("not-a-number", ctx, bad)
            self.assertNotIn("rx_snr", ctx, bad)
            self.assertNotIn("rx_rssi", ctx, bad)
            self.assertNotIn("hop_count", ctx, bad)

    # -- pipeline routing (InboundProcessor) ------------------------------

    def test_process_missing_sender_skipped(self):
        res = self._processor().process({"toId": "!da1b1613"})
        self.assertEqual(res.kind, "skipped")
        self.assertTrue(res.dropped)
        self.assertIsNone(res.sender)

    def test_process_records_freshness_for_every_packet(self):
        store = _RecordingFreshness()
        proc = self._processor(freshness=store)
        proc.process({"fromId": "!ab12cd34", "rxTime": 1234, "rxSnr": 5.0, "rxRssi": -90})
        self.assertEqual(store.calls, [("!ab12cd34", 1234, 5.0, -90, None)])

    def test_process_garbage_rxtime_does_not_crash_freshness(self):
        store = NodeFreshness()
        proc = self._processor(freshness=store)
        proc.process(
            {
                "fromId": "!ab12cd34",
                "rxTime": "garbage",
                "rxSnr": 1.0,
                "rxRssi": -90,
                "hopStart": 3,
                "hopLimit": 3,  # direct -> link metrics describe this node
            }
        )
        obs = store.get("!ab12cd34")
        self.assertLessEqual(abs(obs["last_heard"] - time.time()), 5)
        self.assertEqual(obs["snr"], 1.0)

    def test_process_routes_telemetry_to_writer_and_drops(self):
        telemetry_calls = []
        proc = self._processor(write_telemetry=lambda nid, d: telemetry_calls.append((nid, d)))
        res = proc.process(
            {
                "fromId": "!ab12cd34",
                "decoded": {"portnum": "TELEMETRY_APP", "telemetry": {"deviceMetrics": {}}},
            }
        )
        self.assertEqual(res.kind, "telemetry")
        self.assertTrue(res.dropped)
        self.assertEqual(len(telemetry_calls), 1)
        self.assertEqual(telemetry_calls[0][0], "!ab12cd34")

    def test_process_routes_position_and_writes_signal_before(self):
        signal_calls, position_calls = [], []
        proc = self._processor(
            write_signal=lambda nid, snr, rssi, hops: signal_calls.append((nid, snr, rssi, hops)),
            write_position=lambda nid, d: position_calls.append(nid),
        )
        res = proc.process(
            {
                "fromId": "!ab12cd34",
                "rxSnr": 6.0,
                "hopStart": 3,
                "hopLimit": 3,
                "decoded": {"portnum": 3, "position": {"latitude": 42.0, "longitude": 42.0}},
            }
        )
        self.assertEqual(res.kind, "position")
        self.assertTrue(res.dropped)
        self.assertEqual(signal_calls, [("!ab12cd34", 6.0, None, 0)])
        self.assertEqual(position_calls, ["!ab12cd34"])

    def test_process_signal_writer_skipped_when_no_links(self):
        signal_calls = []
        proc = self._processor(write_signal=lambda nid, snr, rssi, hops: signal_calls.append(nid))
        proc.process({"fromId": "!ab12cd34", "decoded": dict(self._TEXT)})
        self.assertEqual(signal_calls, [])

    def test_process_coerces_hostile_signal_fields(self):
        """Junk envelope signal fields are coerced to None before the writer."""
        signal_calls = []
        proc = self._processor(
            write_signal=lambda nid, snr, rssi, hops: signal_calls.append((nid, snr, rssi, hops))
        )
        proc.process(
            {
                "fromId": "!ab12cd34",
                "rxSnr": "garbage",
                "rxRssi": "-9999999",
                "hopStart": "not-an-int",
                "decoded": {"portnum": "TELEMETRY_APP"},
            }
        )
        self.assertEqual(signal_calls, [("!ab12cd34", None, -9999999.0, None)])

        signal_calls.clear()
        proc.process(
            {
                "fromId": "!ab12cd34",
                "snr": float("nan"),
                "rssi": 2.0,
                "decoded": {"portnum": "TELEMETRY_APP"},
            }
        )
        self.assertEqual(signal_calls, [("!ab12cd34", None, 2.0, None)])

    def test_process_coerces_huge_int_signal_fields(self):
        """A hostile huge-int signal field (10**400) overflows float() with
        OverflowError (not ValueError); the inbound coerce path must drop it to
        None instead of crashing InboundProcessor.process() (which would drop the
        whole packet / drain). Mirrors node_freshness._coerce_float. A valid
        sibling field is kept so the writer still fires and the drop is visible."""
        signal_calls = []
        proc = self._processor(
            write_signal=lambda nid, snr, rssi, hops: signal_calls.append((nid, snr, rssi, hops))
        )
        proc.process(
            {
                "fromId": "!ab12cd34",
                "rxSnr": 10**400,  # huge-int -> None (must not raise)
                "rxRssi": -90,  # valid -> kept, so the writer fires
                "decoded": {"portnum": "TELEMETRY_APP"},
            }
        )
        # The huge-int rxSnr dropped to None; the valid rssi survived; no raise.
        self.assertEqual(signal_calls, [("!ab12cd34", None, -90.0, None)])

    def test_coerce_helpers_reject_huge_int(self):
        """inbound._coerce_float and _finite_or_none must drop a hostile
        huge-int value (10**400 overflows float() with OverflowError, not
        ValueError) instead of raising on the inbound path. Mirrors
        node_freshness._coerce_float / chunking._effective_chunk_bytes."""
        from inbound import _coerce_float, _finite_or_none

        self.assertIsNone(_coerce_float(10**400))
        self.assertIsNone(_coerce_float(-(10**400)))
        self.assertEqual(_coerce_float(10**18), 1e18)
        # _finite_or_none preserves ints as-is, but a huge-int *string*
        # ("1e400") goes through float() and must not crash build_packet_context.
        self.assertIsNone(_finite_or_none("1e400"))
        self.assertIsNone(_finite_or_none("1" + "0" * 400))

    def test_process_self_echo_dropped_but_still_observed(self):
        store = _RecordingFreshness()
        telemetry_calls = []
        proc = self._processor(
            freshness=store,
            write_telemetry=lambda nid, d: telemetry_calls.append(nid),
        )
        res = proc.process(
            {"fromId": "!ab12cd34", "decoded": {"portnum": "TELEMETRY_APP"}},
            my_node_id="!ab12cd34",
        )
        self.assertEqual(res.kind, "echo")
        self.assertTrue(res.dropped)
        self.assertEqual(len(store.calls), 1)  # freshness recorded before echo filter
        self.assertEqual(telemetry_calls, [])  # routing never reached

    def test_process_unauthorized_text_returns_verdict_not_dropped(self):
        proc = self._processor(allowed_nodes=lambda: {"!ab12cd34"})
        res = proc.process({"fromId": "!bad55555", "decoded": dict(self._TEXT)})
        self.assertEqual(res.kind, "unauthorized")
        self.assertFalse(res.dropped)  # adapter logs the "skipped" warning here
        self.assertFalse(res.authorized)
        self.assertIsNone(res.text)

    def test_process_authorized_text_extracts_payload(self):
        proc = self._processor(allowed_nodes=lambda: {"!ab12cd34"})
        res = proc.process({"fromId": "!ab12cd34", "decoded": dict(self._TEXT)})
        self.assertEqual(res.kind, "text")
        self.assertFalse(res.dropped)
        self.assertTrue(res.authorized)
        self.assertEqual(res.text, "hello")

    def test_process_allow_all_opens_the_gate(self):
        proc = self._processor(allow_all=lambda: True)
        res = proc.process({"fromId": "!whoever", "decoded": dict(self._TEXT)})
        self.assertTrue(res.authorized)
        self.assertEqual(res.text, "hello")

    def test_process_text_without_payload_drops(self):
        proc = self._processor(allowed_nodes=lambda: {"!ab12cd34"})
        res = proc.process({"fromId": "!ab12cd34", "decoded": {"portnum": "TEXT_MESSAGE_APP"}})
        self.assertEqual(res.kind, "no_text")
        self.assertTrue(res.dropped)

    def test_process_non_text_portnum_drops_silently(self):
        proc = self._processor()
        res = proc.process({"fromId": "!ab12cd34", "decoded": {"portnum": 4}})
        self.assertEqual(res.kind, "other")
        self.assertTrue(res.dropped)

    def test_process_classifies_broadcast_to_id(self):
        proc = self._processor(allowed_nodes=lambda: {"!ab12cd34"})
        res = proc.process(
            {"fromId": "!ab12cd34", "toId": "^all", "channel": 0, "decoded": dict(self._TEXT)}
        )
        self.assertTrue(res.is_broadcast)
        self.assertEqual(res.to_id, "^all")

    def test_process_normalizes_numeric_dm_to_id(self):
        proc = self._processor(allowed_nodes=lambda: {"!ab12cd34"})
        res = proc.process({"fromId": "!ab12cd34", "to": 0xDA1B1613, "decoded": dict(self._TEXT)})
        self.assertFalse(res.is_broadcast)
        self.assertEqual(res.to_id, "!da1b1613")

    def test_process_out_of_range_to_id_not_formatted(self):
        """A negative/out-of-range numeric to_id must not mint a malformed !-id."""
        for bad in (-5, 2**32):
            proc = self._processor(allowed_nodes=lambda: {"!ab12cd34"})
            res = proc.process({"fromId": "!ab12cd34", "to": bad, "decoded": dict(self._TEXT)})
            self.assertFalse(res.is_broadcast)
            self.assertEqual(res.to_id, bad)
            self.assertNotIn("!", str(res.to_id))

    def test_process_bool_to_id_not_formatted_as_node_id(self):
        """A bool to_id (bool ⊂ int) must not format as !00000001 / !00000000.

        ``isinstance(True, int)`` is True, so without the bool exclusion a
        malformed envelope value would mint a valid-looking but wrong node id.
        """
        for bad in (True, False):
            proc = self._processor(allowed_nodes=lambda: {"!ab12cd34"})
            res = proc.process({"fromId": "!ab12cd34", "to": bad, "decoded": dict(self._TEXT)})
            self.assertFalse(res.is_broadcast)
            self.assertNotIn("!", str(res.to_id))
            self.assertNotEqual(res.to_id, "!00000001")
            self.assertNotEqual(res.to_id, "!00000000")

    def test_process_canonicalizes_string_broadcast_to_id(self):
        """String broadcast forms canonicalize to \"^all\" alongside the int."""
        proc = self._processor(allowed_nodes=lambda: {"!ab12cd34"})
        res = proc.process({"fromId": "!ab12cd34", "to": "4294967295", "decoded": dict(self._TEXT)})
        self.assertTrue(res.is_broadcast)
        self.assertEqual(res.to_id, "^all")

    def test_process_canonicalizes_int_broadcast_to_id(self):
        """An int broadcast dest (0xFFFFFFFF) canonicalizes to \"^all\" through
        process(), matching the string form — the int leg of the canonicalizer."""
        proc = self._processor(allowed_nodes=lambda: {"!ab12cd34"})
        res = proc.process({"fromId": "!ab12cd34", "to": 0xFFFFFFFF, "decoded": dict(self._TEXT)})
        self.assertTrue(res.is_broadcast)
        self.assertEqual(res.to_id, "^all")

    # -- telemetry/position persistence stages ----------------------------

    def test_log_telemetry_packet_extracts_and_preserves_zero(self):
        with patch("inbound.telemetry_db.log_telemetry") as log:
            log_telemetry_packet(
                "!ab12cd34",
                {
                    "telemetry": {
                        "deviceMetrics": {"batteryLevel": 0, "voltage": 0.0},
                        "environmentMetrics": {"temperature": 18.5, "relativeHumidity": 60.1},
                    }
                },
            )
            log.assert_called_once()
            self.assertEqual(log.call_args.kwargs["battery_level"], 0)
            self.assertEqual(log.call_args.kwargs["voltage"], 0.0)
            self.assertEqual(log.call_args.kwargs["temperature"], 18.5)
            self.assertEqual(log.call_args.kwargs["humidity"], 60.1)

    def test_log_telemetry_packet_flat_decoded_fallback(self):
        with patch("inbound.telemetry_db.log_telemetry") as log:
            log_telemetry_packet("!ab12cd34", {"batteryLevel": 44, "uptime": 99})
            self.assertEqual(log.call_args.kwargs["battery_level"], 44)
            self.assertEqual(log.call_args.kwargs["uptime"], 99)

    def test_log_telemetry_packet_no_metrics_skips_write(self):
        with patch("inbound.telemetry_db.log_telemetry") as log:
            log_telemetry_packet("!ab12cd34", {"telemetry": {}})
            log.assert_not_called()

    def test_log_telemetry_packet_drops_non_numeric_and_nan_fields(self):
        """Garbage/NaN fields sanitize to None; valid string numerics coerce."""
        with patch("inbound.telemetry_db.log_telemetry") as log:
            log_telemetry_packet(
                "!ab12cd34",
                {
                    "telemetry": {
                        "deviceMetrics": {
                            "batteryLevel": "garbage",
                            "voltage": "4.01",
                            "uptimeSeconds": float("nan"),
                        },
                        "environmentMetrics": {
                            "temperature": float("nan"),
                            "relativeHumidity": 60.0,
                        },
                    }
                },
            )
            log.assert_called_once()
            self.assertIsNone(log.call_args.kwargs["battery_level"])
            self.assertEqual(log.call_args.kwargs["voltage"], 4.01)
            self.assertIsNone(log.call_args.kwargs["uptime"])
            self.assertIsNone(log.call_args.kwargs["temperature"])
            self.assertEqual(log.call_args.kwargs["humidity"], 60.0)

    def test_log_telemetry_packet_all_garbage_skips_write(self):
        with patch("inbound.telemetry_db.log_telemetry") as log:
            log_telemetry_packet(
                "!ab12cd34",
                {
                    "telemetry": {
                        "deviceMetrics": {"batteryLevel": "x", "voltage": "y"},
                        "environmentMetrics": {"temperature": float("inf")},
                    }
                },
            )
            log.assert_not_called()

    def test_log_position_packet_scales_protobuf_coordinates(self):
        with patch("inbound.telemetry_db.log_position") as log:
            log_position_packet(
                "!ab12cd34",
                {"position": {"latitude": 426983000, "longitude": -711234000, "altitude": 120}},
            )
            log.assert_called_once()
            self.assertEqual(log.call_args.kwargs["latitude"], 42.6983)
            self.assertEqual(log.call_args.kwargs["longitude"], -71.1234)
            self.assertEqual(log.call_args.kwargs["altitude"], 120)

    def test_log_position_packet_scales_camel_case_hardware_shape(self):
        """Real hardware delivers MessageToDict(Position(...)): camelCase
        latitudeI/longitudeI keys — the same 1e7 scale must be undone."""
        with patch("inbound.telemetry_db.log_position") as log:
            log_position_packet(
                "!ab12cd34",
                {"position": {"latitudeI": 426983000, "longitudeI": -711234000, "altitude": 120}},
            )
            log.assert_called_once()
            self.assertEqual(log.call_args.kwargs["latitude"], 42.6983)
            self.assertEqual(log.call_args.kwargs["longitude"], -71.1234)
            self.assertEqual(log.call_args.kwargs["altitude"], 120)

    def test_log_position_packet_keeps_decimal_coordinates(self):
        with patch("inbound.telemetry_db.log_position") as log:
            log_position_packet("!ab12cd34", {"position": {"latitude": 55.75, "longitude": 37.61}})
            self.assertEqual(log.call_args.kwargs["latitude"], 55.75)
            self.assertEqual(log.call_args.kwargs["longitude"], 37.61)

    def test_log_position_packet_missing_coords_skips_write(self):
        with patch("inbound.telemetry_db.log_position") as log:
            log_position_packet("!ab12cd34", {"position": {"altitude": 10}})
            log.assert_not_called()

    def test_log_position_packet_drops_non_numeric_coords(self):
        """A non-numeric coordinate must not raise and must not persist garbage."""
        with patch("inbound.telemetry_db.log_position") as log:
            log_position_packet(
                "!ab12cd34", {"position": {"latitude": "north", "longitude": -71.0}}
            )
            log.assert_not_called()

    def test_log_position_packet_clamps_absurd_coordinates(self):
        """Far-out-of-range coordinates clamp to valid lat/lon bounds."""
        with patch("inbound.telemetry_db.log_position") as log:
            log_position_packet("!ab12cd34", {"position": {"latitude": 1e30, "longitude": -1e30}})
            log.assert_called_once()
            self.assertEqual(log.call_args.kwargs["latitude"], 90.0)
            self.assertEqual(log.call_args.kwargs["longitude"], -180.0)


class TestInterfaceNodeId(unittest.TestCase):
    """Local-node-id fallback chain (moved from the adapter; the adapter
    delegate keeps the same behavior, pinned here directly)."""

    _normalize_id = staticmethod(MeshtasticAdapter._normalize_node_id)

    def test_dict_myinfo_my_node_num(self):
        iface = SimpleNamespace(myInfo={"my_node_num": 0xAB12CD34})
        self.assertEqual(interface_node_id(iface, normalize_id=self._normalize_id), "!ab12cd34")

    def test_get_my_node_info_prefers_user_id(self):
        iface = MagicMock()
        iface.getMyNodeInfo.return_value = {
            "num": 0xDA1B1613,
            "user": {"id": "!DA1B1613"},
        }
        self.assertEqual(interface_node_id(iface, normalize_id=self._normalize_id), "!da1b1613")

    def test_get_my_node_info_falls_back_to_num(self):
        iface = MagicMock()
        iface.getMyNodeInfo.return_value = {"num": 0xDA1B1613, "user": {}}
        self.assertEqual(interface_node_id(iface, normalize_id=self._normalize_id), "!da1b1613")

    def test_get_my_node_info_num_as_string(self):
        iface = MagicMock()
        iface.getMyNodeInfo.return_value = {"num": "2870136116", "user": {}}
        self.assertEqual(interface_node_id(iface, normalize_id=self._normalize_id), "!ab12cd34")

    def test_get_my_node_info_num_garbage_falls_through(self):
        iface = MagicMock()
        iface.getMyNodeInfo.return_value = {"num": "not-a-number", "user": {}}
        iface.myInfo = {"my_node_num": 0xAB12CD34}
        self.assertEqual(interface_node_id(iface, normalize_id=self._normalize_id), "!ab12cd34")

    def test_get_my_node_info_raising_falls_through(self):
        iface = MagicMock()
        iface.getMyNodeInfo.side_effect = RuntimeError("no info yet")
        iface.myInfo = {"my_node_num": 0xAB12CD34}
        self.assertEqual(interface_node_id(iface, normalize_id=self._normalize_id), "!ab12cd34")

    def test_protobuf_like_myinfo_attribute(self):
        iface = SimpleNamespace(myInfo=SimpleNamespace(my_node_num=0xAB12CD34))
        self.assertEqual(interface_node_id(iface, normalize_id=self._normalize_id), "!ab12cd34")

    def test_my_node_num_string_converts(self):
        iface = SimpleNamespace(myInfo=SimpleNamespace(my_node_num="2870136116"))
        self.assertEqual(interface_node_id(iface, normalize_id=self._normalize_id), "!ab12cd34")

    def test_my_node_num_bad_string_falls_to_get_my_node_id(self):
        iface = SimpleNamespace(
            myInfo=SimpleNamespace(my_node_num="not-a-number"),
            getMyNodeId=lambda: "!AB12CD34",
        )
        self.assertEqual(interface_node_id(iface, normalize_id=self._normalize_id), "!ab12cd34")

    def test_get_my_node_id_mock_fallback(self):
        iface = SimpleNamespace(getMyNodeId=lambda: "!AB12CD34")
        self.assertEqual(interface_node_id(iface, normalize_id=self._normalize_id), "!ab12cd34")

    def test_get_my_node_id_raising_returns_none(self):
        def boom():
            raise RuntimeError("boom")

        iface = SimpleNamespace(getMyNodeId=boom)
        self.assertIsNone(interface_node_id(iface, normalize_id=self._normalize_id))

    def test_no_handles_returns_none(self):
        self.assertIsNone(interface_node_id(object(), normalize_id=self._normalize_id))


class TestTelemetryDbGuards(unittest.TestCase):
    """telemetry_db robustness: serialized writers and the row-ceiling prune."""

    def setUp(self):
        self._tmp_db = tempfile.NamedTemporaryFile(delete=False)
        self._tmp_db.close()
        telemetry_db.DB_PATH = self._tmp_db.name
        init_db()
        self.addCleanup(self._cleanup)

    def _cleanup(self):
        _unlink_db_files(self._tmp_db.name)

    def test_concurrent_writers_do_not_lose_rows(self):
        """N parallel log_* calls under executor-slot concurrency lose nothing."""
        from telemetry_db import log_position, log_signal, log_telemetry

        errors: list[BaseException] = []

        def writer(i: int) -> None:
            try:
                for _ in range(5):
                    log_signal(f"!node{i:02x}", snr=1.0, rssi=-90)
                    log_telemetry(f"!node{i:02x}", battery_level=i, temperature=20.0)
                    log_position(f"!node{i:02x}", latitude=42.0, longitude=-71.0)
            except Exception as e:  # pragma: no cover - failure only
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(12)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])
        with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
            counts = {
                name: conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
                for name in ("telemetry", "positions", "signal_quality")
            }
        self.assertEqual(counts, {"telemetry": 60, "positions": 60, "signal_quality": 60})

    def test_row_ceiling_prunes_without_age_cutoff(self):
        """A table past its row ceiling is pruned immediately (throttle bypass)."""
        from telemetry_db import log_position, log_signal, log_telemetry

        with patch.dict(os.environ, {"MESHTASTIC_TELEMETRY_MAX_ROWS": "5"}):
            telemetry_db._last_prune_monotonic = None
            for i in range(10):
                log_signal("!ab12cd34", snr=float(i), rssi=-90)
                log_position("!ab12cd34", latitude=40.0 + i / 10, longitude=-71.0)
                log_telemetry("!ab12cd34", battery_level=i)

            with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
                counts = {
                    name: conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
                    for name in ("telemetry", "positions", "signal_quality")
                }
        # Each table was flooded past the ceiling and pruned back to it.
        for table, n in counts.items():
            self.assertLessEqual(n, 5, table)
        # The newest rows survive (kept by timestamp), not the oldest.
        with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
            top_signal = conn.execute(
                "SELECT snr FROM signal_quality ORDER BY timestamp DESC LIMIT 1"
            ).fetchone()[0]
        self.assertEqual(top_signal, 9.0)

    def test_maybe_prune_skips_count_scan_when_throttled(self):
        """The flood-bypass COUNT(*) is not run on a throttled, under-ceiling write."""
        telemetry_db._row_estimates.clear()
        telemetry_db._last_prune_monotonic = time.monotonic()  # just pruned
        with patch.object(telemetry_db, "_any_table_over_ceiling") as count_check:
            telemetry_db.maybe_prune()
        count_check.assert_not_called()

    def test_retention_prunes_on_idle_db(self):
        """Age-based retention fires from init_db even with no log_* writes."""
        old_ts = time.time() - 40 * 86400
        with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
            conn.execute(
                "INSERT INTO signal_quality (node_id, timestamp, snr, rssi) VALUES (?, ?, ?, ?)",
                ("!old", old_ts, 1.0, -100),
            )
            conn.commit()
        telemetry_db._last_prune_monotonic = None
        with patch.dict(os.environ, {"MESHTASTIC_TELEMETRY_RETENTION_DAYS": "30"}):
            init_db()
        with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
            remaining = conn.execute(
                "SELECT COUNT(*) FROM signal_quality WHERE node_id = '!old'"
            ).fetchone()[0]
        self.assertEqual(remaining, 0)

    def test_init_db_fails_fast_on_unwritable_path(self):
        """init_db re-raises so a broken DB path surfaces at construction."""
        with tempfile.NamedTemporaryFile() as blocker:
            old = telemetry_db.DB_PATH
            telemetry_db.DB_PATH = os.path.join(blocker.name, "nested.db")
            try:
                with self.assertRaises(OSError):
                    init_db()
            finally:
                telemetry_db.DB_PATH = old

    def test_history_limit_clamped_to_positive(self):
        """Non-positive limits never request an unbounded read."""
        for i in range(10):
            telemetry_db.log_signal("!aa001122", snr=float(i), rssi=-90)
        self.assertEqual(len(telemetry_db.get_signal_history("!aa001122", limit=-1)), 1)
        self.assertEqual(len(telemetry_db.get_signal_history("!aa001122", limit=0)), 1)
        self.assertEqual(len(telemetry_db.get_signal_history("!aa001122", limit=10)), 10)

    def test_latest_signal_tiebreak_prefers_newest_id(self):
        """Same-timestamp rows resolve to the highest id, deterministically."""
        ts = time.time()
        with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
            conn.execute(
                "INSERT INTO signal_quality (node_id, timestamp, snr, rssi, hop_count)"
                " VALUES (?, ?, ?, ?, ?)",
                ("!aa001122", ts, 1.0, -90, 1),
            )
            conn.execute(
                "INSERT INTO signal_quality (node_id, timestamp, snr, rssi, hop_count)"
                " VALUES (?, ?, ?, ?, ?)",
                ("!aa001122", ts, 2.0, -91, 0),
            )
            conn.commit()
        latest = telemetry_db.get_latest_signal_by_node(direct_only=False)["!aa001122"]
        self.assertEqual(latest["snr"], 2.0)  # higher id wins the tie
        direct = telemetry_db.get_latest_signal_by_node(direct_only=True)["!aa001122"]
        self.assertEqual(direct["snr"], 2.0)

    def test_direct_only_excludes_null_hop_rows(self):
        """A node whose readings all lack hop info must not appear in the direct map."""
        telemetry_db.log_signal("!bb001122", snr=3.0, rssi=-99)
        self.assertNotIn("!bb001122", telemetry_db.get_latest_signal_by_node(direct_only=True))
        self.assertIn("!bb001122", telemetry_db.get_latest_signal_by_node(direct_only=False))

    def test_get_latest_direct_signal_single_node_counterpart(self):
        """The single-node getter returns the same row the all-nodes direct map
        holds for that node — a bounded index lookup, not the full-table window
        scan — and excludes non-direct (relayed) and NULL-hop rows."""
        ts = time.time()
        with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
            # A relayed reading (hop_count=1) and a NULL-hop reading for the
            # target node, plus a direct reading for a different node.
            conn.execute(
                "INSERT INTO signal_quality (node_id, timestamp, snr, rssi, hop_count)"
                " VALUES (?, ?, ?, ?, ?)",
                ("!aa001122", ts - 100, 1.0, -90, 1),
            )
            conn.execute(
                "INSERT INTO signal_quality (node_id, timestamp, snr, rssi) VALUES (?, ?, ?, ?)",
                ("!aa001122", ts - 50, 2.0, -91),
            )
            conn.execute(
                "INSERT INTO signal_quality (node_id, timestamp, snr, rssi, hop_count)"
                " VALUES (?, ?, ?, ?, ?)",
                ("!aa001122", ts, 9.0, -60, 0),
            )
            conn.execute(
                "INSERT INTO signal_quality (node_id, timestamp, snr, rssi, hop_count)"
                " VALUES (?, ?, ?, ?, ?)",
                ("!dd001122", ts, 7.0, -70, 0),
            )
            conn.commit()
        direct_map = telemetry_db.get_latest_signal_by_node(direct_only=True)
        self.assertEqual(direct_map["!aa001122"]["snr"], 9.0)
        # Single-node getter agrees with the all-nodes direct map for that node.
        single = telemetry_db.get_latest_direct_signal("!aa001122")
        self.assertIsNotNone(single)
        self.assertEqual(single, direct_map["!aa001122"])
        self.assertEqual(single["snr"], 9.0)
        self.assertEqual(single["hop_count"], 0)
        # A node with no direct reading returns None.
        self.assertIsNone(telemetry_db.get_latest_direct_signal("!bb001122"))

    def test_get_latest_direct_signal_degrades_to_none_on_read_error(self):
        """A failed read degrades to None, not a crash."""
        with patch.object(
            telemetry_db.sqlite3, "connect", side_effect=sqlite3.OperationalError("disk I/O error")
        ):
            self.assertIsNone(telemetry_db.get_latest_direct_signal("!zz001122"))

    def test_history_since_inclusive_at_boundary(self):
        """The since window includes a row at exactly the boundary (>= semantics)."""
        ts = time.time()
        with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
            conn.execute(
                "INSERT INTO signal_quality (node_id, timestamp, snr, rssi) VALUES (?, ?, ?, ?)",
                ("!cc001122", ts, 1.0, -90),
            )
            conn.commit()
        rows = telemetry_db.get_signal_history("!cc001122", limit=10, since=ts)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["timestamp"], ts)

    def test_read_retries_transient_busy(self):
        """A transient 'database is locked' is retried, not read as no data."""
        state = {"attempts": 0}

        def flaky_run():
            state["attempts"] += 1
            if state["attempts"] < 3:
                raise sqlite3.OperationalError("database is locked")
            return [{"ok": True}]

        result = telemetry_db._read_guarded("test read", [], flaky_run)
        self.assertEqual(result, [{"ok": True}])
        self.assertEqual(state["attempts"], 3)

    def test_read_busy_exhaustion_returns_empty_not_raises(self):
        """A persistently busy DB degrades to an empty result, never a crash."""

        def stuck_run():
            raise sqlite3.OperationalError("database is locked")

        self.assertEqual(telemetry_db._read_guarded("test read", [], stuck_run), [])

    def test_prune_signals_failure_when_ceiling_delete_raises(self):
        """A mid-prune failure rolls back and signals failure (-1), not a partial count."""
        now = time.time()
        with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
            conn.execute(
                "INSERT INTO signal_quality (node_id, timestamp, snr, rssi) VALUES (?, ?, ?, ?)",
                ("!pp001122", now - 10 * 86400, 1.0, -90),
            )
            conn.commit()

        def boom(cursor):
            raise sqlite3.OperationalError("database is locked")

        with patch.object(telemetry_db, "_delete_over_ceiling", side_effect=boom):
            deleted = telemetry_db.prune(5.0)
        self.assertEqual(deleted, telemetry_db._PRUNE_FAILED)
        with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
            remaining = conn.execute(
                "SELECT COUNT(*) FROM signal_quality WHERE node_id = '!pp001122'"
            ).fetchone()[0]
        self.assertEqual(remaining, 1)  # uncommitted DELETE rolled back

    def test_any_table_over_ceiling_degrades_on_db_error(self):
        """A failed ceiling check reports 'not over' instead of crashing the write path."""
        with patch.object(
            telemetry_db.sqlite3,
            "connect",
            side_effect=sqlite3.OperationalError("database is locked"),
        ):
            self.assertFalse(telemetry_db._any_table_over_ceiling())

    def test_any_table_over_ceiling_disabled_when_ceiling_zero(self):
        """A zero ceiling disables both the real check and the estimate check."""
        with patch.dict(os.environ, {"MESHTASTIC_TELEMETRY_MAX_ROWS": "0"}):
            self.assertFalse(telemetry_db._any_table_over_ceiling())
            self.assertFalse(telemetry_db._estimate_over_ceiling())
            with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
                self.assertEqual(telemetry_db._delete_over_ceiling(conn.cursor()), 0)

    def test_env_fallbacks_on_bad_input(self):
        """Invalid env values fall back to the module defaults (or clamp)."""
        with patch.dict(os.environ, {"MESHTASTIC_TELEMETRY_RETENTION_DAYS": "junk"}):
            self.assertEqual(telemetry_db._retention_days(), telemetry_db.DEFAULT_RETENTION_DAYS)
        with patch.dict(os.environ, {"MESHTASTIC_TELEMETRY_MAX_ROWS": "junk"}):
            self.assertEqual(
                telemetry_db._max_rows_per_table(), telemetry_db.DEFAULT_MAX_ROWS_PER_TABLE
            )
        with patch.dict(os.environ, {"MESHTASTIC_TELEMETRY_MAX_ROWS": "-5"}):
            self.assertEqual(telemetry_db._max_rows_per_table(), 0)

    def test_history_query_degrades_to_empty_on_read_error(self):
        """A non-busy read failure degrades to an empty result, not a crash."""
        with patch.object(
            telemetry_db.sqlite3, "connect", side_effect=sqlite3.OperationalError("disk I/O error")
        ):
            self.assertEqual(telemetry_db.get_telemetry_history("!zz001122", limit=1), [])

    def test_latest_signal_degrades_to_empty_on_read_error(self):
        """A non-busy read failure degrades to an empty map, not a crash."""
        with patch.object(
            telemetry_db.sqlite3, "connect", side_effect=sqlite3.OperationalError("disk I/O error")
        ):
            self.assertEqual(telemetry_db.get_latest_signal_by_node(), {})

    def test_log_write_failures_are_logged_not_crashed(self):
        """A per-write failure is logged, never raised out of the log_* helper."""
        with patch.object(
            telemetry_db.sqlite3, "connect", side_effect=sqlite3.OperationalError("disk I/O error")
        ):
            telemetry_db.log_telemetry("!zz001122", battery_level=1)
            telemetry_db.log_position("!zz001122", latitude=42.0, longitude=-71.0)
            telemetry_db.log_signal("!zz001122", snr=1.0, rssi=-90)

    def test_read_guarded_degrades_on_non_sqlite_error(self):
        """A non-busy runtime failure in a read degrades to empty, not a crash."""

        def run():
            raise RuntimeError("boom")

        self.assertEqual(telemetry_db._read_guarded("test read", [], run), [])

    def test_read_guarded_empty_retry_loop_returns_empty(self):
        """With zero retry attempts the guarded read returns the empty default."""
        with patch.object(telemetry_db, "_READ_RETRY_ATTEMPTS", 0):
            result = telemetry_db._read_guarded("test read", [], lambda: [{"ok": True}])
        self.assertEqual(result, [])

    def test_maybe_prune_stale_estimate_does_not_prune(self):
        """A stale-high estimate is re-checked against real counts and skips pruning."""
        telemetry_db._row_estimates["telemetry"] = 1_000_000
        telemetry_db._last_prune_monotonic = time.monotonic()  # throttled
        with patch.object(telemetry_db, "prune") as mock_prune:
            telemetry_db.maybe_prune()
        mock_prune.assert_not_called()

    def test_log_signal_coerces_hostile_values(self):
        """log_signal drops non-numeric strings / NaN at the storage boundary."""
        telemetry_db.log_signal("!zz001122", snr="garbage", rssi="-9999999", hop_count="nope")
        telemetry_db.log_signal("!zz002233", snr=float("nan"), rssi=float("inf"))
        with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
            rows = {
                nid: (snr, rssi, hops)
                for nid, snr, rssi, hops in conn.execute(
                    "SELECT node_id, snr, rssi, hop_count FROM signal_quality"
                )
            }
        self.assertEqual(rows["!zz001122"], (None, -9999999.0, None))
        self.assertEqual(rows["!zz002233"], (None, None, None))

    def test_log_telemetry_coerces_hostile_values(self):
        """log_telemetry drops non-numeric strings / NaN / huge-int at the
        storage boundary (mirrors test_log_signal_coerces_hostile_values; the
        inbound tests mock telemetry_db.log_telemetry so the real DB-boundary
        coercion (_finite_float/_finite_int) is never exercised for telemetry)."""
        from telemetry_db import log_telemetry

        log_telemetry(
            "!tt001122",
            battery_level="full",  # non-numeric -> NULL
            voltage=10**400,  # huge-int overflows float() -> NULL
            temperature=float("nan"),  # NaN -> NULL
            humidity=float("inf"),  # inf -> NULL
            pressure=1013.25,  # finite -> kept
            uptime=True,  # bool -> NULL
        )
        with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
            row = conn.execute(
                "SELECT battery_level, voltage, temperature, humidity, pressure, uptime "
                "FROM telemetry WHERE node_id = ?",
                ("!tt001122",),
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertIsNone(row[0])  # battery_level
        self.assertIsNone(row[1])  # voltage (huge-int dropped)
        self.assertIsNone(row[2])  # temperature (NaN dropped)
        self.assertIsNone(row[3])  # humidity (inf dropped)
        self.assertEqual(row[4], 1013.25)  # finite sibling kept
        self.assertIsNone(row[5])  # uptime (bool rejected)

    def test_log_position_coerces_hostile_values(self):
        """log_position drops hostile coordinates at the storage boundary
        (mirrors test_log_signal_coerces_hostile_values for the position table)."""
        from telemetry_db import log_position

        log_position(
            "!pp001122",
            latitude=float("inf"),  # inf -> NULL
            longitude=-(10**400),  # huge-int -> NULL
            altitude="high",  # non-numeric -> NULL
        )
        with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
            row = conn.execute(
                "SELECT latitude, longitude, altitude FROM positions WHERE node_id = ?",
                ("!pp001122",),
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertIsNone(row[0])  # inf latitude dropped
        self.assertIsNone(row[1])  # huge-int longitude dropped
        self.assertIsNone(row[2])  # altitude dropped

    def test_log_signal_survives_sql_metacharacter_node_id(self):
        """A SQL-metacharacter node id must be stored parameterized (never
        interpolated) so it cannot inject / drop a table. _normalize_node_id
        passes non-canonical shapes through verbatim, so this guards against a
        future refactor to f-string interpolation going undetected."""
        hostile = "!x'; DROP TABLE telemetry;--"
        telemetry_db.log_signal(hostile, snr=2.0, rssi=-90, hop_count=1)
        with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
            # The telemetry table still exists (no DROP executed).
            tables = {
                name
                for (name,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            self.assertIn("telemetry", tables)
            self.assertIn("signal_quality", tables)
            # Only the literal hostile id matches; round-trips verbatim.
            row = conn.execute(
                "SELECT node_id, snr FROM signal_quality WHERE node_id = ?",
                (hostile,),
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], hostile)
        self.assertEqual(row[1], 2.0)

    def test_prune_throttle_survives_backward_wall_clock_step(self):
        """A backward wall-clock step cannot stall pruning (monotonic throttle)."""
        two_hours = 2 * 3600.0
        past_mono = time.monotonic() - two_hours
        telemetry_db._last_prune_monotonic = past_mono  # interval long expired
        with (
            patch.object(telemetry_db, "_estimate_over_ceiling", return_value=False),
            patch.object(telemetry_db, "prune") as mock_prune,
            patch("time.time", return_value=past_mono - 1),  # wall clock stepped back
        ):
            telemetry_db.maybe_prune()
        mock_prune.assert_called_once()
