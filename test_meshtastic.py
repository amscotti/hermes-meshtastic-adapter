"""
Unit and Integration Test Suite for Meshtastic Platform Adapter.
"""

import asyncio
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from concurrent.futures import Future as ConcurrentFuture
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

# Load local tools.py dynamically to prevent name collision with Hermes core tools package
tools_spec = importlib.util.spec_from_file_location(
    "meshtastic_tools", os.path.join(os.path.dirname(os.path.abspath(__file__)), "tools.py")
)
meshtastic_tools = importlib.util.module_from_spec(tools_spec)
sys.modules["meshtastic_tools"] = meshtastic_tools
tools_spec.loader.exec_module(meshtastic_tools)

import telemetry_db
from adapter import (
    HAS_MESHTASTIC,
    AckStatus,
    MeshtasticAdapter,
    MockSerialInterface,
    _DaemonTransportExecutor,
    _env_enablement,
    _standalone_send,
)
from telemetry_db import get_position_history, get_telemetry_history, init_db

handle_mesh_list_nodes = meshtastic_tools.handle_mesh_list_nodes
handle_mesh_node_info = meshtastic_tools.handle_mesh_node_info
handle_mesh_signal_quality = meshtastic_tools.handle_mesh_signal_quality
handle_mesh_send_dm = meshtastic_tools.handle_mesh_send_dm
handle_mesh_send_broadcast = meshtastic_tools.handle_mesh_send_broadcast
handle_mesh_telemetry = meshtastic_tools.handle_mesh_telemetry
handle_mesh_telemetry_history = meshtastic_tools.handle_mesh_telemetry_history


class TestMeshtasticPlatform(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._env_patcher = patch.dict(
            os.environ,
            {
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
            },
        )
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

    def test_mock_connection(self):
        """Verify mock interface connects successfully."""
        interfaces = self.adapter.get_interfaces()
        self.assertEqual(len(interfaces), 1)
        self.assertIsInstance(interfaces[0], MockSerialInterface)
        self.assertEqual(interfaces[0].getMyNodeId(), "!da1b1613")

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

        # Give asyncio loop a tick to process
        await asyncio.sleep(0.05)

        # Verify event creation & gateway dispatch
        self.adapter.handle_message.assert_called_once()
        event = self.adapter.handle_message.call_args[0][0]

        self.assertIn("Hello Hermes, this is a private message.", event.text)
        self.assertIn("rx_snr: 7.5 dB", event.channel_context)
        self.assertIn("rx_rssi: -95 dBm", event.channel_context)
        self.assertEqual(event.source.chat_id, "meshtastic:!ab12cd34")
        self.assertEqual(event.source.chat_type, "dm")
        self.assertEqual(event.source.user_id, "!ab12cd34")

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
        await asyncio.sleep(0.05)

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
        await asyncio.sleep(0.05)

        self.adapter.handle_message.assert_called_once()  # message delivered, not dropped
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
        await asyncio.sleep(0.05)

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

        # Give asyncio loop a tick
        await asyncio.sleep(0.05)

        # Verify event scoping
        self.adapter.handle_message.assert_called_once()
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
        await asyncio.sleep(0.05)

        self.adapter.handle_message.assert_called_once()  # no crash, message bridged
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
        await asyncio.sleep(0.05)
        self.adapter.handle_message.assert_not_called()  # not bridged -> no public reply

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
        await asyncio.sleep(0.05)
        self.adapter.handle_message.assert_called_once()
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

    def test_update_observed_last_heard_and_direct_signal(self):
        """last_heard tracks rx_time; snr/rssi only from direct (0-hop) packets."""
        self.adapter._update_observed("!aaaa1111", 1_700_000_000, 5.0, -80, 0)
        obs = self.adapter.get_observed_node("!aaaa1111")
        self.assertEqual(obs["last_heard"], 1_700_000_000)
        self.assertEqual(obs["snr"], 5.0)
        self.assertEqual(obs["rssi"], -80)
        self.assertEqual(obs["hops_away"], 0)

    def test_update_observed_relayed_packet_skips_signal(self):
        """A relayed (hop>0) packet bumps last_heard but not snr/rssi."""
        self.adapter._update_observed("!bbbb2222", None, 3.0, -90, 2)
        obs = self.adapter.get_observed_node("!bbbb2222")
        self.assertGreater(obs["last_heard"], 0)
        self.assertEqual(obs["hops_away"], 2)
        self.assertNotIn("snr", obs)  # relay metrics belong to the last hop
        self.assertNotIn("rssi", obs)

    def test_update_observed_future_rxtime_clamped(self):
        """A future rx_time (clock skew) is clamped to now."""
        self.adapter._update_observed("!cccc3333", time.time() + 10_000, None, None, None)
        self.assertLessEqual(
            self.adapter.get_observed_node("!cccc3333")["last_heard"], time.time() + 1
        )

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

    async def test_mesh_list_nodes_prefers_fresh_last_heard(self):
        """mesh_list_nodes overlays observed last_heard over the stale library value."""
        fresher = int(time.time() - 10)  # newer than mock !ab12cd34's lastHeard (now-300)
        self.adapter._update_observed("!ab12cd34", fresher, None, None, None)
        res = json.loads(await handle_mesh_list_nodes({}))
        node = next(n for n in res["nodes"] if n["node_id"] == "!ab12cd34")
        self.assertEqual(
            node["last_heard"], time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(fresher))
        )

    def test_observed_overlay_is_size_bounded(self):
        """The observed overlay evicts the stalest entry past its cap."""
        self.adapter.OBSERVED_NODE_LIMIT = 3
        for i in range(10):
            self.adapter._update_observed(f"!n{i:07d}", 1_700_000_000 + i, None, None, None)
        self.assertLessEqual(len(self.adapter._node_observed), 3)
        self.assertIn("!n0000009", self.adapter._node_observed)  # newest kept
        self.assertNotIn("!n0000000", self.adapter._node_observed)  # stalest evicted

    async def test_payload_splitting_on_send(self):
        """Verify outbound messages above the 233-byte ceiling are split."""
        long_message = "A" * 300  # ASCII bytes exceed the 233-byte ceiling.

        # Mock low-level sendText
        iface = self.adapter.get_interfaces()[0]
        iface.sendText = MagicMock(return_value=True)

        # Send
        res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content=long_message)

        self.assertTrue(res.success)
        # Should split into multiple LoRa-safe numbered chunks.
        self.assertGreater(iface.sendText.call_count, 1)
        calls = iface.sendText.call_args_list
        for call in calls:
            self.assertLessEqual(
                len(call[1]["text"].encode("utf-8")), self.adapter.MAX_MESSAGE_LENGTH
            )
        self.assertTrue(calls[0][1]["text"].startswith("[1/"))

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
        await asyncio.sleep(0.05)
        self.adapter.handle_message.assert_called_once()
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

    async def test_tool_handlers(self):
        """Test executing tool handlers retrieve data correctly."""
        # 1. Test listing nodes
        res_list = await handle_mesh_list_nodes({})
        self.assertIn("Phoenix HQ", res_list)
        self.assertIn("Park Sensor Node", res_list)

        # 2. Test node info query
        res_info = await handle_mesh_node_info({"node_id": "PARK"})
        self.assertIn("SENSECAP_T1000", res_info)

        # 3. Test sending broadcast tool
        res_send = await handle_mesh_send_broadcast({"message": "Emergency alert!"})
        self.assertIn('"success": true', res_send)

    async def test_tool_handlers_accept_task_id_kwarg(self):
        """Hermes invokes tool handlers with extra kwargs (e.g. task_id)."""
        res_list = await handle_mesh_list_nodes({}, task_id="t-1")
        self.assertIn("Phoenix HQ", res_list)
        res_info = await handle_mesh_node_info({"node_id": "PARK"}, task_id="t-1")
        self.assertIn("SENSECAP_T1000", res_info)
        res_sig = await handle_mesh_signal_quality({"node_id": "!da1b1613"}, task_id="t-1")
        self.assertIn("quality", res_sig)
        res_tel = await handle_mesh_telemetry({"node_id": "PARK"}, task_id="t-1")
        self.assertIn("temperature", res_tel)
        res_hist = await handle_mesh_telemetry_history({"node_id": "PARK"}, task_id="t-1")
        self.assertIn("history", res_hist)
        res_dm = await handle_mesh_send_dm({"node_id": "PARK", "message": "hi"}, task_id="t-1")
        self.assertIn("success", res_dm)
        res_bc = await handle_mesh_send_broadcast({"message": "hi"}, task_id="t-1")
        self.assertIn("success", res_bc)

    async def test_standalone_send(self):
        """Test that cron standalone ephemeral send routes through adapter.send."""
        res = await _standalone_send(
            self.config, "meshtastic:!ab12cd34", "Cron standalone message check"
        )
        self.assertTrue(res.get("success"))

    async def test_utf8_chunking(self):
        """Verify that chunking measures UTF-8 bytes and safely splits multi-byte characters."""
        # The emoji is four UTF-8 bytes; 60 of them (240 bytes) exceed the default
        # 170-byte budget and the 233-byte protocol ceiling.
        long_emoji_msg = "💩" * 60

        iface = self.adapter.get_interfaces()[0]
        iface.sendText = MagicMock(return_value=True)

        res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content=long_emoji_msg)
        self.assertTrue(res.success)

        # Each chunk must be UTF-8 byte safe, including numbering prefixes.
        self.assertGreater(iface.sendText.call_count, 1)
        calls = iface.sendText.call_args_list
        for call in calls:
            self.assertLessEqual(
                len(call[1]["text"].encode("utf-8")), self.adapter.MAX_MESSAGE_LENGTH
            )
        reconstructed = "".join(call[1]["text"].split("] ", 1)[1] for call in calls)
        self.assertEqual(reconstructed, long_emoji_msg)

    def test_mixed_ascii_emoji_chunk_reconstruction(self):
        """Verify mixed ASCII and emoji chunks reconstruct without dropping spaces."""
        message = ("status update " * 30) + ("💩" * 40) + " final words"
        chunks = self.adapter._chunk_message(message)

        self.assertGreater(len(chunks), 1)
        for chunk in chunks:
            self.assertLessEqual(len(chunk.encode("utf-8")), self.adapter.MAX_MESSAGE_LENGTH)

        reconstructed = "".join(chunk.split("] ", 1)[1] for chunk in chunks)
        self.assertEqual(reconstructed, message)

    def test_default_chunk_budget_is_conservative(self):
        """With no override, chunks stay within the conservative default budget.

        The raw protocol ceiling is 233 bytes, but that leaves no room for
        encrypted-DM (PKI) overhead — the radio NAKs oversized DM chunks with
        TOO_LARGE — so the default must be lower.
        """
        self.assertEqual(self.adapter.DEFAULT_CHUNK_BYTES, 170)
        self.assertEqual(self.adapter.MAX_MESSAGE_LENGTH, 233)
        # setUp leaves MESHTASTIC_CHUNK_BYTES blank → default budget applies.
        chunks = self.adapter._chunk_message("A" * 400)
        self.assertGreater(len(chunks), 1)
        for chunk in chunks:
            self.assertLessEqual(len(chunk.encode("utf-8")), self.adapter.DEFAULT_CHUNK_BYTES)

    def test_declares_native_chunking(self):
        """The adapter chunks in send(), so the gateway must not truncate payloads."""
        self.assertTrue(self.adapter.splits_long_messages)

    def test_keepalive_tcp_socket(self):
        """TCP liveness follows the socket handle (None == dropped)."""
        self.assertTrue(self.adapter._interface_is_alive(SimpleNamespace(socket=object())))
        self.assertFalse(self.adapter._interface_is_alive(SimpleNamespace(socket=None)))

    def test_keepalive_serial_stream(self):
        """Serial liveness follows the pyserial stream's is_open."""
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

    async def test_send_without_queueing_fails_when_disconnected(self):
        """Verify cron-style sends do not silently queue on disconnected adapters."""
        adapter = MeshtasticAdapter(self.config)

        res = await adapter.send(
            chat_id="meshtastic:!ab12cd34",
            content="cron should fail loudly when disconnected",
            allow_queueing=False,
        )

        self.assertFalse(res.success)
        self.assertIn("queueing disabled", res.error)
        self.assertEqual(adapter._outbound_queue, [])

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
        await asyncio.sleep(0.05)

        self.adapter.handle_message.assert_called_once()
        event = self.adapter.handle_message.call_args[0][0]
        self.assertEqual(event.source.chat_id, "meshtastic:!ab12cd34")
        self.assertEqual(event.source.user_id, "!ab12cd34")

    def test_local_node_id_from_dict_myinfo(self):
        """Verify local node ID extraction handles dict-shaped myInfo."""
        iface = SimpleNamespace(myInfo={"my_node_num": 0xAB12CD34})

        self.assertEqual(self.adapter._get_interface_node_id(iface), "!ab12cd34")

    def test_temp_db_isolation(self):
        """Verify tests point telemetry writes at a temporary DB, not the live Hermes DB."""
        self.assertEqual(telemetry_db.DB_PATH, self._tmp_db.name)
        self.assertNotIn(".hermes/meshtastic_telemetry.db", telemetry_db.DB_PATH)

    def test_prune_deletes_only_old_rows(self):
        """prune() removes rows older than the cutoff and keeps recent ones."""
        import sqlite3
        from contextlib import closing

        from telemetry_db import prune

        now = time.time()
        with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
            # One old (10 days ago) and one fresh signal row.
            conn.execute(
                "INSERT INTO signal_quality (node_id, timestamp, snr, rssi) VALUES (?, ?, ?, ?)",
                ("!old", now - 10 * 86400, 1.0, -100),
            )
            conn.execute(
                "INSERT INTO signal_quality (node_id, timestamp, snr, rssi) VALUES (?, ?, ?, ?)",
                ("!new", now, 5.0, -90),
            )
            conn.commit()

        deleted = prune(5.0)  # cutoff: 5 days
        self.assertEqual(deleted, 1)

        with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
            remaining = {r[0] for r in conn.execute("SELECT node_id FROM signal_quality")}
        self.assertNotIn("!old", remaining)
        self.assertIn("!new", remaining)

    def test_prune_disabled_when_retention_zero(self):
        """prune(0) is a no-op (retention disabled)."""
        from telemetry_db import prune

        self.assertEqual(prune(0.0), 0)

    def test_maybe_prune_throttles_and_respects_env(self):
        """maybe_prune runs at most once per interval and honors the env var."""
        import telemetry_db as tdb

        tdb._last_prune_epoch = time.time()  # just ran -> throttled
        with patch.object(tdb, "prune") as mock_prune:
            tdb.maybe_prune()
        mock_prune.assert_not_called()  # throttled within the interval

        tdb._last_prune_epoch = 0.0  # force a run
        with patch.dict(os.environ, {"MESHTASTIC_TELEMETRY_RETENTION_DAYS": "7"}):
            with patch.object(tdb, "prune") as mock_prune:
                tdb.maybe_prune()
        mock_prune.assert_called_once_with(7.0)

    async def test_send_result_uses_packet_id(self):
        """Verify SendResult exposes the packet id returned by sendText."""
        iface = self.adapter.get_interfaces()[0]
        iface.sendText = MagicMock(return_value=SimpleNamespace(id=98765))

        res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content="packet id check")

        self.assertTrue(res.success)
        self.assertEqual(res.message_id, "98765")

    async def test_wait_for_ack_success(self):
        """Verify ACK callbacks can be awaited and exposed in SendResult."""
        iface = self.adapter.get_interfaces()[0]

        def send_text(text, destinationId=None, wantAck=False, onResponse=None, **kwargs):
            self.assertTrue(wantAck)
            self.assertIsNotNone(onResponse)
            onResponse({"decoded": {"requestId": 123456, "routing": {"errorReason": "NONE"}}})
            return SimpleNamespace(id=123456)

        iface.sendText = MagicMock(side_effect=send_text)

        with patch.dict(os.environ, {"MESHTASTIC_ACK_TIMEOUT": "1"}):
            res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content="ack check")

        self.assertTrue(res.success)
        self.assertEqual(res.message_id, "123456")
        self.assertEqual(res.raw_response["chunks"][0]["ack"]["status"], AckStatus.ACK)
        self.assertEqual(self.adapter.get_ack_status("123456")["status"], AckStatus.ACK)

    async def test_wait_for_nak_fails_send(self):
        """Verify NAK callbacks fail the send when ACK waiting is enabled."""
        iface = self.adapter.get_interfaces()[0]

        def send_text(text, destinationId=None, wantAck=False, onResponse=None, **kwargs):
            onResponse({"decoded": {"requestId": 222333, "routing": {"errorReason": "NO_ROUTE"}}})
            return SimpleNamespace(id=222333)

        iface.sendText = MagicMock(side_effect=send_text)

        with patch.dict(os.environ, {"MESHTASTIC_ACK_TIMEOUT": "1"}):
            res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content="nak check")

        self.assertFalse(res.success)
        self.assertIn("Meshtastic NAK", res.error)
        self.assertEqual(res.raw_response["chunks"][0]["ack"]["status"], AckStatus.NAK)
        self.assertEqual(res.raw_response["chunks"][0]["ack"]["error_reason"], "NO_ROUTE")

    async def test_wait_for_ack_timeout_fails_send(self):
        """Verify missing ACK/NACK fails after the configured timeout."""
        iface = self.adapter.get_interfaces()[0]
        iface.sendText = MagicMock(return_value=SimpleNamespace(id=333444))

        with patch.dict(os.environ, {"MESHTASTIC_ACK_TIMEOUT": "0.01"}):
            res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content="timeout check")

        self.assertFalse(res.success)
        self.assertIn("ACK timeout", res.error)
        self.assertEqual(res.raw_response["chunks"][0]["ack"]["status"], AckStatus.TIMEOUT)

    async def test_ack_arrives_while_waiting_from_background_thread(self):
        """A real ACK after the waiter is pending resolves from a non-loop thread.

        Existing tests fire onResponse inside sendText (before the future exists),
        so the early-response path in _track_pending_ack resolves immediately.
        This covers concurrent.futures set_result from a pubsub-like thread.
        """
        iface = self.adapter.get_interfaces()[0]
        captured: dict = {}
        pkt_id = 94001

        def send_text(text, destinationId=None, wantAck=False, onResponse=None, **kwargs):
            # Do NOT call onResponse here — leave the waiter open.
            captured["onResponse"] = onResponse
            return SimpleNamespace(id=pkt_id)

        iface.sendText = MagicMock(side_effect=send_text)

        async def deliver_ack_after_waiter_registered():
            # Poll until _track_pending_ack has registered the future.
            for _ in range(200):
                with self.adapter._ack_lock:
                    fut = self.adapter._ack_futures.get(str(pkt_id))
                if fut is not None and not fut.done():
                    break
                await asyncio.sleep(0.01)
            else:
                self.fail("ACK future was never registered for the waiting send")

            def fire_from_background_thread():
                cb = captured.get("onResponse")
                self.assertIsNotNone(cb)
                cb(
                    {
                        "fromId": "!ab12cd34",  # real ACK from destination
                        "decoded": {
                            "requestId": pkt_id,
                            "routing": {"errorReason": "NONE"},
                        },
                    }
                )

            # Fire from a non-loop thread (same as meshtastic pubsub / radio).
            await asyncio.to_thread(fire_from_background_thread)

        with patch.dict(os.environ, {"MESHTASTIC_ACK_TIMEOUT": "2"}):
            deliver_task = asyncio.create_task(deliver_ack_after_waiter_registered())
            res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content="late real ack")
            await deliver_task

        self.assertTrue(res.success)
        self.assertEqual(res.message_id, str(pkt_id))
        self.assertEqual(res.raw_response["chunks"][0]["ack"]["status"], AckStatus.ACK)

    async def test_retry_resends_transient_nak_until_ack(self):
        """A transient NAK is re-sent; delivery succeeds on a later attempt."""
        iface = self.adapter.get_interfaces()[0]
        calls = {"n": 0}

        def send_text(text, destinationId=None, wantAck=False, onResponse=None, **kwargs):
            calls["n"] += 1
            pid = 5000 + calls["n"]
            reason = "NONE" if calls["n"] >= 2 else "NO_ROUTE"  # NAK once, then ACK
            onResponse({"decoded": {"requestId": pid, "routing": {"errorReason": reason}}})
            return SimpleNamespace(id=pid)

        iface.sendText = MagicMock(side_effect=send_text)

        with patch.dict(os.environ, {"MESHTASTIC_SEND_RETRIES": "2"}):
            res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content="retry me")

        self.assertTrue(res.success)
        self.assertEqual(iface.sendText.call_count, 2)
        self.assertEqual(res.raw_response["chunks"][0]["attempts"], 2)

    async def test_retry_gives_up_after_max_attempts(self):
        """Persistent transient failure fails after retries+1 attempts."""
        iface = self.adapter.get_interfaces()[0]
        calls = {"n": 0}

        def send_text(text, destinationId=None, wantAck=False, onResponse=None, **kwargs):
            calls["n"] += 1
            pid = 6000 + calls["n"]
            onResponse({"decoded": {"requestId": pid, "routing": {"errorReason": "NO_ROUTE"}}})
            return SimpleNamespace(id=pid)

        iface.sendText = MagicMock(side_effect=send_text)

        with patch.dict(os.environ, {"MESHTASTIC_SEND_RETRIES": "2"}):
            res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content="never lands")

        self.assertFalse(res.success)
        self.assertEqual(iface.sendText.call_count, 3)  # 1 + 2 retries
        self.assertIn("after 3 attempt", res.error)

    async def test_permanent_nak_not_retried(self):
        """A permanent NAK (e.g. TOO_LARGE) is never re-sent, even with retries on."""
        iface = self.adapter.get_interfaces()[0]

        def send_text(text, destinationId=None, wantAck=False, onResponse=None, **kwargs):
            onResponse({"decoded": {"requestId": 7001, "routing": {"errorReason": "TOO_LARGE"}}})
            return SimpleNamespace(id=7001)

        iface.sendText = MagicMock(side_effect=send_text)

        with patch.dict(os.environ, {"MESHTASTIC_SEND_RETRIES": "3"}):
            res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content="too big")

        self.assertFalse(res.success)
        self.assertEqual(iface.sendText.call_count, 1)  # not retried

    async def test_broadcast_not_retried(self):
        """Broadcasts have no per-recipient ACK, so retry never applies to them."""
        iface = self.adapter.get_interfaces()[0]
        iface.sendText = MagicMock(return_value=SimpleNamespace(id=8001))  # no ACK -> timeout

        with patch.dict(
            os.environ, {"MESHTASTIC_SEND_RETRIES": "3", "MESHTASTIC_ACK_TIMEOUT": "0.01"}
        ):
            res = await self.adapter.send(chat_id="meshtastic:channel:0", content="broadcast")

        self.assertFalse(res.success)
        self.assertEqual(iface.sendText.call_count, 1)  # single attempt, no retry

    def test_is_retriable_failure_classification(self):
        """Only ACK-observed transient failures are retriable."""
        from gateway.platforms.base import SendResult

        def r(ack):
            return SendResult(success=False, raw_response={"ack": ack} if ack else None)

        self.assertTrue(self.adapter._is_retriable_failure(r({"status": AckStatus.TIMEOUT})))
        self.assertTrue(
            self.adapter._is_retriable_failure(
                r({"status": AckStatus.NAK, "error_reason": "NO_ROUTE"})
            )
        )
        self.assertFalse(
            self.adapter._is_retriable_failure(
                r({"status": AckStatus.NAK, "error_reason": "TOO_LARGE"})
            )
        )
        # PKI / auth failures are permanent — re-sending can't fix a key problem.
        for reason in (
            "PKI_FAILED",
            "PKI_UNKNOWN_PUBKEY",
            "PKI_SEND_FAIL_PUBLIC_KEY",
            "ADMIN_PUBLIC_KEY_UNAUTHORIZED",
            "NOT_AUTHORIZED",
            "DUTY_CYCLE_LIMIT",
            "RATE_LIMIT_EXCEEDED",
        ):
            self.assertFalse(
                self.adapter._is_retriable_failure(
                    r({"status": AckStatus.NAK, "error_reason": reason})
                ),
                f"{reason} should be permanent",
            )
        self.assertFalse(self.adapter._is_retriable_failure(r({"status": AckStatus.ACK})))
        self.assertTrue(self.adapter._is_retriable_failure(r({"status": AckStatus.IMPLICIT_ACK})))
        # Plain strings still match (StrEnum + public JSON surface).
        self.assertTrue(self.adapter._is_retriable_failure(r({"status": "timeout"})))
        self.assertFalse(self.adapter._is_retriable_failure(r(None)))  # pre-send error
        # Disconnect-settled waiters must not spin retries against a closed radio.
        self.assertFalse(
            self.adapter._is_retriable_failure(
                r({"status": AckStatus.TIMEOUT, "error_reason": "DISCONNECTED"})
            )
        )
        # Adapter-internal collision NAK: the chunk was already transmitted, so
        # retrying would duplicate it on-air.
        self.assertFalse(
            self.adapter._is_retriable_failure(
                r({"status": AckStatus.NAK, "error_reason": "DUPLICATE_PACKET_ID"})
            )
        )

    async def test_real_ack_from_destination_is_delivery(self):
        """A routing ACK whose sender IS the destination confirms real delivery."""
        iface = self.adapter.get_interfaces()[0]

        def send_text(text, destinationId=None, wantAck=False, onResponse=None, **kwargs):
            onResponse(
                {
                    "fromId": "!ab12cd34",  # ACK came from the destination itself
                    "decoded": {"requestId": 91001, "routing": {"errorReason": "NONE"}},
                }
            )
            return SimpleNamespace(id=91001)

        iface.sendText = MagicMock(side_effect=send_text)
        with patch.dict(os.environ, {"MESHTASTIC_ACK_TIMEOUT": "1"}):
            res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content="real ack")

        self.assertTrue(res.success)
        self.assertEqual(res.raw_response["chunks"][0]["ack"]["status"], AckStatus.ACK)
        # Wire/JSON surface stays the plain string value.
        self.assertEqual(str(res.raw_response["chunks"][0]["ack"]["status"]), "ack")

    async def test_ack_future_binds_to_awaiting_loop_not_self_loop(self):
        """wrap_future is awaitable on the send loop even when adapter.loop differs.

        concurrent.futures storage is loop-independent; asyncio.wrap_future
        attaches the awaitable view to the running loop so wait_for works.
        """
        other_loop = asyncio.new_event_loop()
        self.addCleanup(other_loop.close)
        self.adapter.loop = other_loop  # pretend connect() ran on a different loop

        cf = self.adapter._track_pending_ack("77007", "!ab12cd34", "hi", create_future=True)
        self.assertIsNotNone(cf)
        # Storage has no event-loop affinity. The awaitable view binds here.
        fut = asyncio.wrap_future(cf)
        self.assertIs(fut.get_loop(), asyncio.get_running_loop())
        self.assertIsNot(fut.get_loop(), other_loop)
        self.adapter._set_ack_future_result(cf, {"status": AckStatus.ACK})
        record = await fut
        self.assertEqual(record["status"], AckStatus.ACK)

    async def test_ack_resolution_from_background_without_target_loop(self):
        """_record_ack_response settles concurrent.futures from any thread.

        Even when adapter.loop is a different (non-running) loop, a late pubsub
        ACK must complete the waiter without needing that loop.
        """
        platform_loop = asyncio.new_event_loop()
        self.addCleanup(platform_loop.close)
        self.adapter.loop = platform_loop  # connect() loop ≠ send loop

        dest = "!ab12cd34"
        pkt_id = 77008
        cf = self.adapter._track_pending_ack(str(pkt_id), dest, "hi", create_future=True)
        self.assertIsNotNone(cf)

        def fire_ack_from_background():
            self.adapter._record_ack_response(
                {
                    "fromId": dest,
                    "decoded": {
                        "requestId": pkt_id,
                        "routing": {"errorReason": "NONE"},
                    },
                },
                dest,
                "hi",
            )

        with patch.object(platform_loop, "call_soon_threadsafe") as platform_ts:
            await asyncio.to_thread(fire_ack_from_background)
            record = await asyncio.wait_for(asyncio.wrap_future(cf), timeout=1.0)

        self.assertEqual(record["status"], AckStatus.ACK)
        platform_ts.assert_not_called()

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
        self.assertIs(args[0], self.adapter._incoming_queue.put_nowait)

    async def test_cross_loop_send_logs_once(self):
        """First ACK future on a non-platform loop logs; later ones stay quiet."""
        other_loop = asyncio.new_event_loop()
        self.addCleanup(other_loop.close)
        self.adapter.loop = other_loop
        self.adapter._cross_loop_send_logged = False

        with self.assertLogs("adapter", level="INFO") as cm:
            self.adapter._track_pending_ack("1", "!ab12cd34", "a", create_future=True)
            self.adapter._track_pending_ack("2", "!ab12cd34", "b", create_future=True)

        cross = [line for line in cm.output if "different event loop" in line]
        self.assertEqual(len(cross), 1)
        self.assertTrue(self.adapter._cross_loop_send_logged)

    async def test_disconnect_settles_pending_ack_waiters(self):
        """disconnect() must unblock ACK waiters instead of leaving them until timeout."""
        cf = self.adapter._track_pending_ack("99001", "!ab12cd34", "hi", create_future=True)
        self.assertIsNotNone(cf)
        self.assertFalse(cf.done())

        await self.adapter.disconnect()

        record = await asyncio.wait_for(asyncio.wrap_future(cf), timeout=1.0)
        self.assertEqual(record["status"], AckStatus.TIMEOUT)
        self.assertEqual(record["error_reason"], "DISCONNECTED")
        with self.adapter._ack_lock:
            self.assertNotIn("99001", self.adapter._ack_futures)

    async def test_send_text_serialized_on_transport_worker(self):
        """Concurrent sends must not interleave Meshtastic sendText calls."""
        iface = self.adapter.get_interfaces()[0]
        active = {"n": 0, "max": 0}
        gate = threading.Lock()
        ids = {"n": 0}

        def send_text(text, destinationId=None, wantAck=False, onResponse=None, **kwargs):
            with gate:
                active["n"] += 1
                active["max"] = max(active["max"], active["n"])
            time.sleep(0.05)
            with gate:
                active["n"] -= 1
                ids["n"] += 1
                pid = 88000 + ids["n"]
            if onResponse:
                onResponse(
                    {
                        "fromId": destinationId or "!ab12cd34",
                        "decoded": {"requestId": pid, "routing": {"errorReason": "NONE"}},
                    }
                )
            return SimpleNamespace(id=pid)

        iface.sendText = send_text
        with patch.dict(os.environ, {"MESHTASTIC_ACK_TIMEOUT": "1"}):
            results = await asyncio.gather(
                self.adapter.send(chat_id="meshtastic:!ab12cd34", content="one"),
                self.adapter.send(chat_id="meshtastic:!ab12cd34", content="two"),
                self.adapter.send(chat_id="meshtastic:!ab12cd34", content="three"),
            )
        self.assertTrue(all(r.success for r in results))
        self.assertEqual(active["max"], 1)

    async def test_disconnect_during_send_does_not_block_loop_or_leave_ack_waiter(self):
        """Slow sendText must not block the loop; late ACK registration settles."""
        iface = self.adapter.get_interfaces()[0]
        started = threading.Event()
        release = threading.Event()

        def send_text(text, destinationId=None, **kwargs):
            started.set()
            if not release.wait(timeout=2):
                raise TimeoutError("test did not release sendText")
            return SimpleNamespace(id=99101)

        iface.sendText = send_text
        send_task = asyncio.create_task(
            self.adapter.send(
                chat_id="meshtastic:!ab12cd34",
                content="blocked send",
                metadata={"meshtastic_ack_timeout": 30},
            )
        )
        self.assertTrue(await asyncio.to_thread(started.wait, 1))

        disconnect_task = asyncio.create_task(self.adapter.disconnect())
        # If disconnect acquired a contended threading lock on this loop, this
        # sleep and assertion could not run until sendText completed.
        await asyncio.sleep(0.05)
        self.assertFalse(disconnect_task.done())

        release.set()
        result = await asyncio.wait_for(send_task, timeout=1)
        await asyncio.wait_for(disconnect_task, timeout=1)
        self.assertFalse(result.success)
        self.assertIn("disconnected while waiting for ACK", result.error)
        self.assertEqual(result.raw_response["chunks"][0]["ack"]["error_reason"], "DISCONNECTED")
        with self.adapter._ack_lock:
            self.assertNotIn("99101", self.adapter._ack_futures)

    async def test_disconnect_makes_implicit_ack_terminal(self):
        """An unresolved relay ACK becomes DISCONNECTED, not retriable implicit."""
        dest = "!ab12cd34"
        pkt_id = "99102"
        cf = self.adapter._track_pending_ack(pkt_id, dest, "hi", create_future=True)
        self.adapter._record_ack_response(
            {
                "fromId": "!9e77edec",
                "decoded": {"requestId": int(pkt_id), "routing": {"errorReason": "NONE"}},
            },
            dest,
            "hi",
        )
        self.assertFalse(cf.done())

        await self.adapter.disconnect()
        record = await asyncio.wait_for(asyncio.wrap_future(cf), timeout=1)
        self.assertEqual(record["status"], AckStatus.TIMEOUT)
        self.assertEqual(record["error_reason"], "DISCONNECTED")

    async def test_pubsub_real_ack_upgrades_one_shot_implicit_callback(self):
        """A real routing ACK from pubsub upgrades an earlier relay callback."""
        dest = "!ab12cd34"
        pkt_id = "99103"
        cf = self.adapter._track_pending_ack(pkt_id, dest, "hi", create_future=True)
        self.adapter._record_ack_response(
            {
                "fromId": "!9e77edec",
                "decoded": {"requestId": int(pkt_id), "routing": {"errorReason": "NONE"}},
            },
            dest,
            "hi",
        )
        self.assertFalse(cf.done())

        self.adapter._on_receive(
            {
                "fromId": dest,
                "decoded": {"requestId": int(pkt_id), "routing": {"errorReason": "NONE"}},
            }
        )
        record = await asyncio.wait_for(asyncio.wrap_future(cf), timeout=1)
        self.assertEqual(record["status"], AckStatus.ACK)
        self.assertEqual(record["ack_from"], dest)

    async def test_pubsub_ack_does_not_resolve_pending_waiter(self):
        """A pubsub routing ACK must not resolve a still-PENDING waiter.

        The pubsub fallback exists only to upgrade IMPLICIT_ACK (relay) records
        after the one-shot onAckNak callback has been consumed. A PENDING waiter
        must keep waiting for its own callback/timeout — matching a pubsub
        packet to it would risk misattributing a reused packet id.
        """
        dest = "!ab12cd34"
        pkt_id = "99104"
        cf = self.adapter._track_pending_ack(pkt_id, dest, "hi", create_future=True)
        self.assertFalse(cf.done())
        with self.adapter._ack_lock:
            self.assertEqual(self.adapter._pending_acks[pkt_id]["status"], AckStatus.PENDING)

        handled = self.adapter._maybe_record_pubsub_ack(
            {
                "fromId": dest,
                "decoded": {"requestId": int(pkt_id), "routing": {"errorReason": "NONE"}},
            }
        )

        self.assertFalse(handled)
        self.assertFalse(cf.done())
        with self.adapter._ack_lock:
            self.assertEqual(self.adapter._pending_acks[pkt_id]["status"], AckStatus.PENDING)

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

    async def test_transport_submit_shutdown_race_never_strands_future(self):
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

    async def test_cancelled_open_timeout_expired_preserves_cancelled_error(self):
        """A hung open beyond the cancel wait still re-raises CancelledError."""
        started = threading.Event()

        def open_hangs(*_args):
            started.set()
            time.sleep(0.2)
            return SimpleNamespace(close=MagicMock())

        self.adapter._open_and_register_interface = MagicMock(side_effect=open_hangs)
        task = asyncio.create_task(
            self.adapter._reconnect_loop("slow_port", self.adapter._lifecycle_id)
        )
        self.assertTrue(await asyncio.to_thread(started.wait, 1))
        with patch.dict(os.environ, {"MESHTASTIC_OPEN_CANCEL_TIMEOUT": "0.02"}):
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

    async def test_cancelled_open_zero_timeout_abandons_wait_immediately(self):
        """MESHTASTIC_OPEN_CANCEL_TIMEOUT=0 skips the cleanup wait entirely."""
        started = threading.Event()

        def open_hangs(*_args):
            started.set()
            time.sleep(0.2)
            return SimpleNamespace(close=MagicMock())

        self.adapter._open_and_register_interface = MagicMock(side_effect=open_hangs)
        task = asyncio.create_task(
            self.adapter._reconnect_loop("slow_port", self.adapter._lifecycle_id)
        )
        self.assertTrue(await asyncio.to_thread(started.wait, 1))
        with patch.dict(os.environ, {"MESHTASTIC_OPEN_CANCEL_TIMEOUT": "0"}):
            start = time.monotonic()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertLess(time.monotonic() - start, 0.15)

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

    async def test_stale_transport_job_cannot_send_after_reconnect(self):
        """A queued old-lifecycle send cannot use a new lifecycle interface."""
        old_lifecycle = self.adapter._lifecycle_id
        await self.adapter.disconnect()
        self.assertTrue(await self.adapter.connect())
        for _ in range(100):
            if self.adapter.get_interfaces():
                break
            await asyncio.sleep(0.01)
        iface = self.adapter.get_interfaces()[0]
        iface.sendText = MagicMock(return_value=SimpleNamespace(id=1))

        err, packet, _ = self.adapter._send_text_serialized(
            lifecycle_id=old_lifecycle,
            dest="!ab12cd34",
            content="stale",
            parts=["meshtastic", "!ab12cd34"],
            reply_id=None,
            ack_callback=lambda packet: None,
        )
        self.assertEqual(err, "no_iface")
        self.assertIsNone(packet)
        iface.sendText.assert_not_called()

    async def test_old_send_completion_cannot_register_ack_after_reconnect(self):
        """An old worker returning after reconnect cannot enter new ACK state."""
        iface = self.adapter.get_interfaces()[0]
        started = threading.Event()
        release = threading.Event()

        def send_text(**kwargs):
            kwargs["onResponse"](
                {
                    "fromId": "!ab12cd34",
                    "decoded": {
                        "requestId": 99301,
                        "routing": {"errorReason": "NONE"},
                    },
                }
            )
            started.set()
            release.wait(timeout=2)
            return SimpleNamespace(id=99301)

        iface.sendText = send_text
        send_task = asyncio.create_task(
            self.adapter.send(
                chat_id="meshtastic:!ab12cd34",
                content="old lifecycle",
                metadata={"meshtastic_ack_timeout": 30},
            )
        )
        self.assertTrue(await asyncio.to_thread(started.wait, 1))
        with patch.dict(
            os.environ,
            {
                "MESHTASTIC_EXECUTOR_SHUTDOWN_TIMEOUT": "0",
                "MESHTASTIC_OPEN_CANCEL_TIMEOUT": "0",
            },
        ):
            await self.adapter.disconnect()
        self.assertTrue(await self.adapter.connect())
        release.set()
        result = await asyncio.wait_for(send_task, timeout=1)
        self.assertFalse(result.success)
        self.assertIn("disconnected while waiting for ACK", result.error)
        with self.adapter._ack_lock:
            self.assertNotIn("99301", self.adapter._ack_futures)
            self.assertNotIn("99301", self.adapter._pending_acks)

    async def test_cancelled_send_cleans_provisional_ack_state(self):
        """Cancelling while sendText runs cannot leak provisional token state."""
        iface = self.adapter.get_interfaces()[0]
        started = threading.Event()
        release = threading.Event()

        def send_text(**_kwargs):
            started.set()
            release.wait(timeout=2)
            return SimpleNamespace(id=99302)

        iface.sendText = send_text
        send_task = asyncio.create_task(
            self.adapter._send_immediate("meshtastic:!ab12cd34", "cancel inflight")
        )
        self.assertTrue(await asyncio.to_thread(started.wait, 1))
        send_task.cancel()
        await asyncio.gather(send_task, return_exceptions=True)
        release.set()

        with self.adapter._ack_lock:
            self.assertEqual(self.adapter._ack_inflight_tokens, {})
            self.assertEqual(self.adapter._early_ack_packets, {})

    async def test_ack_settle_race_does_not_raise(self):
        """Pubsub/disconnect racing set_result is harmless."""
        future = ConcurrentFuture()
        errors: list[BaseException] = []
        barrier = threading.Barrier(3)

        def settle(status):
            try:
                barrier.wait()
                self.adapter._set_ack_future_result(future, {"status": status})
            except BaseException as exc:
                errors.append(exc)

        threads = [
            threading.Thread(target=settle, args=(AckStatus.ACK,)),
            threading.Thread(target=settle, args=(AckStatus.TIMEOUT,)),
        ]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join(timeout=1)
        self.assertEqual(errors, [])
        self.assertIn(future.result()["status"], (AckStatus.ACK, AckStatus.TIMEOUT))

    async def test_duplicate_active_packet_id_rejects_new_waiter(self):
        """A second active waiter cannot replace the first for the same packet id."""
        first_token = object()
        second_token = object()
        first = self.adapter._track_pending_ack(
            "dup-1",
            "!ab12cd34",
            "first",
            create_future=True,
            send_token=first_token,
        )
        second = self.adapter._track_pending_ack(
            "dup-1",
            "!ab12cd34",
            "second",
            create_future=True,
            send_token=second_token,
        )
        self.assertNotIn("dup-1", self.adapter._ack_futures)
        self.assertEqual(first.result(timeout=0.1)["error_reason"], "DUPLICATE_PACKET_ID")
        result = second.result(timeout=0.1)
        self.assertEqual(result["status"], AckStatus.NAK)
        self.assertEqual(result["error_reason"], "DUPLICATE_PACKET_ID")
        for token in (first_token, second_token):
            self.adapter._record_ack_response(
                {
                    "fromId": "!ab12cd34",
                    "decoded": {
                        "requestId": "dup-1",
                        "routing": {"errorReason": "NONE"},
                    },
                },
                "!ab12cd34",
                "collision",
                send_token=token,
            )
        self.assertEqual(
            self.adapter.get_ack_status("dup-1")["error_reason"],
            "DUPLICATE_PACKET_ID",
        )

    async def test_nonwaiting_packet_id_collision_terminates_old_waiter(self):
        """A fire-and-forget collision cannot leave the old waiter for pubsub ACK."""
        old = self.adapter._track_pending_ack(
            "dup-nonwait", "!ab12cd34", "old", create_future=True, send_token=object()
        )
        self.adapter._track_pending_ack(
            "dup-nonwait", "!ab12cd34", "new", create_future=False, send_token=object()
        )
        self.assertTrue(old.done())
        self.assertEqual(old.result()["error_reason"], "DUPLICATE_PACKET_ID")
        with self.adapter._ack_lock:
            self.assertNotIn("dup-nonwait", self.adapter._ack_futures)

    async def test_nonwaiting_collision_new_token_owns_ack_observability(self):
        """After a fire-and-forget collision, the new send's ACK updates the record."""
        old_token = object()
        new_token = object()
        pkt_id = "777001"
        dest = "!ab12cd34"
        self.adapter._track_pending_ack(
            pkt_id, dest, "old", create_future=True, send_token=old_token
        )
        self.adapter._track_pending_ack(
            pkt_id, dest, "new", create_future=False, send_token=new_token
        )

        # A delayed callback from the OLD send is ignored as stale.
        self.adapter._record_ack_response(
            {
                "fromId": dest,
                "decoded": {"requestId": int(pkt_id), "routing": {"errorReason": "NONE"}},
            },
            dest,
            "old",
            send_token=old_token,
        )
        with self.adapter._ack_lock:
            self.assertEqual(self.adapter._pending_acks[pkt_id]["status"], AckStatus.NAK)
            self.assertEqual(
                self.adapter._pending_acks[pkt_id]["error_reason"], "DUPLICATE_PACKET_ID"
            )

        # The NEW send's real ACK upgrades the collision record to ACK.
        self.adapter._record_ack_response(
            {
                "fromId": dest,
                "decoded": {"requestId": int(pkt_id), "routing": {"errorReason": "NONE"}},
            },
            dest,
            "new",
            send_token=new_token,
        )
        with self.adapter._ack_lock:
            self.assertEqual(self.adapter._pending_acks[pkt_id]["status"], AckStatus.ACK)

    async def test_callback_during_sendtext_survives_token_adoption(self):
        """A synchronous callback for a reused id is replayed after token adoption."""
        pkt_id = "777002"
        dest = "!ab12cd34"
        old = self.adapter._track_pending_ack(
            pkt_id, dest, "old", create_future=True, send_token=object()
        )
        iface = self.adapter.get_interfaces()[0]

        def send_text(**kwargs):
            kwargs["onResponse"](
                {
                    "fromId": dest,
                    "decoded": {
                        "requestId": int(pkt_id),
                        "routing": {"errorReason": "NONE"},
                    },
                }
            )
            return SimpleNamespace(id=int(pkt_id))

        iface.sendText = send_text
        result = await self.adapter._send_immediate(f"meshtastic:{dest}", "new", wait_for_ack=False)

        self.assertTrue(result.success)
        self.assertEqual(old.result()["error_reason"], "DUPLICATE_PACKET_ID")
        self.assertEqual(self.adapter.get_ack_status(pkt_id)["status"], AckStatus.ACK)

    async def test_disconnect_preserves_definitive_ack_record(self):
        """A waiter already settled by a real ACK keeps ACK through disconnect."""
        pkt_id = "88001"
        dest = "!ab12cd34"
        cf = self.adapter._track_pending_ack(pkt_id, dest, "hi", create_future=True)
        self.adapter._record_ack_response(
            {
                "fromId": dest,
                "decoded": {"requestId": int(pkt_id), "routing": {"errorReason": "NONE"}},
            },
            dest,
            "hi",
        )
        self.assertEqual(cf.result()["status"], AckStatus.ACK)
        # Re-register so disconnect's sweep sees a waiter with a definitive record.
        with self.adapter._ack_lock:
            self.adapter._ack_futures[pkt_id] = cf

        await self.adapter.disconnect()

        with self.adapter._ack_lock:
            self.assertEqual(self.adapter._pending_acks[pkt_id]["status"], AckStatus.ACK)

    def test_stale_lifecycle_ack_callback_is_ignored(self):
        """A callback tagged with a dead lifecycle writes nothing."""
        with self.adapter._lifecycle_lock:
            stale = self.adapter._lifecycle_id + 1

        self.adapter._record_ack_response(
            {
                "fromId": "!ab12cd34",
                "decoded": {"requestId": 88002, "routing": {"errorReason": "NONE"}},
            },
            "!ab12cd34",
            "hi",
            send_token=object(),
            lifecycle_id=stale,
        )

        with self.adapter._ack_lock:
            self.assertNotIn("88002", self.adapter._pending_acks)
            self.assertNotIn("88002", self.adapter._ack_responses)

    def test_inflight_lifecycle_mismatch_callback_is_ignored(self):
        """A staged token from another lifecycle cannot commit an ACK record."""
        token = object()
        with self.adapter._lifecycle_lock:
            current = self.adapter._lifecycle_id
        with self.adapter._ack_lock:
            self.adapter._ack_inflight_tokens[token] = current + 1  # wrong generation

        self.adapter._record_ack_response(
            {
                "fromId": "!ab12cd34",
                "decoded": {"requestId": 88003, "routing": {"errorReason": "NONE"}},
            },
            "!ab12cd34",
            "hi",
            send_token=token,
            lifecycle_id=current,
        )

        with self.adapter._ack_lock:
            self.assertNotIn("88003", self.adapter._pending_acks)
            self.assertNotIn(token, self.adapter._early_ack_packets)

    def test_track_pending_ack_discards_response_from_older_token(self):
        """An early ACK from an older send must not pre-resolve the new waiter."""
        pkt_id = "88004"
        old_token = object()
        with self.adapter._ack_lock:
            self.adapter._ack_responses[pkt_id] = {
                "dest": "!ab12cd34",
                "status": AckStatus.ACK,
                "response_at": time.time(),
            }
            self.adapter._ack_response_tokens[pkt_id] = old_token

        cf = self.adapter._track_pending_ack(
            pkt_id, "!ab12cd34", "new", create_future=True, send_token=object()
        )

        self.assertFalse(cf.done())
        with self.adapter._ack_lock:
            self.assertEqual(self.adapter._pending_acks[pkt_id]["status"], AckStatus.PENDING)

    def test_make_ack_callback_wrapper_records_response(self):
        """The tokenless _make_ack_callback wrapper still records ACKs."""
        callback = self.adapter._make_ack_callback("!ab12cd34", "hi")
        self.assertEqual(callback.__name__, "onAckNak")
        callback(
            {
                "fromId": "!ab12cd34",
                "decoded": {"requestId": 88005, "routing": {"errorReason": "NONE"}},
            }
        )
        with self.adapter._ack_lock:
            self.assertEqual(self.adapter._pending_acks["88005"]["status"], AckStatus.ACK)

    async def test_sequential_packet_id_reuse_fails_safe(self):
        """Sequential reuse is rejected because wire ACK generations are ambiguous."""
        old_token = object()
        new_token = object()
        pkt_id = "dup-2"
        old = self.adapter._track_pending_ack(
            pkt_id, "!ab12cd34", "old", create_future=True, send_token=old_token
        )
        self.adapter._set_ack_future_result(old, {"status": AckStatus.TIMEOUT})
        with self.adapter._ack_lock:
            self.adapter._ack_futures.pop(pkt_id, None)
        new = self.adapter._track_pending_ack(
            pkt_id, "!ab12cd34", "new", create_future=True, send_token=new_token
        )
        self.assertTrue(new.done())
        self.assertEqual(new.result()["error_reason"], "DUPLICATE_PACKET_ID")
        self.assertEqual(self.adapter.get_ack_status(pkt_id)["error_reason"], "DUPLICATE_PACKET_ID")

    async def test_send_text_serialized_uses_node_db_key_forms(self):
        """DM node lookup: exact key wins; case-insensitive scan rewrites dest."""
        nodes = {
            "!ab12cd34": {"user": {"publicKey": b"k1"}},
            "!AA00BB11": {"user": {"publicKey": b"k2"}},
        }
        iface = self.adapter.get_interfaces()[0]
        iface.nodes = nodes
        iface.sendText = MagicMock(return_value=SimpleNamespace(id=4242))
        lifecycle_id = self.adapter._lifecycle_id

        def callback(packet):
            return None

        err, pkt, dest = self.adapter._send_text_serialized(
            lifecycle_id=lifecycle_id,
            dest="!ab12cd34",
            content="exact",
            parts=["meshtastic", "!ab12cd34"],
            reply_id=None,
            ack_callback=callback,
        )
        self.assertIsNone(err)
        self.assertEqual(dest, "!ab12cd34")
        self.assertEqual(pkt.id, 4242)

        err, pkt, dest = self.adapter._send_text_serialized(
            lifecycle_id=lifecycle_id,
            dest="!aa00bb11",  # DB stores uppercase key; library wants its own form
            content="scan",
            parts=["meshtastic", "!aa00bb11"],
            reply_id=None,
            ack_callback=callback,
        )
        self.assertIsNone(err)
        self.assertEqual(dest, "!AA00BB11")

    async def test_send_text_serialized_routes_to_iface_owning_node(self):
        """A DM goes out on whichever interface's node DB owns the destination."""
        iface_a = SimpleNamespace(nodes={}, sendText=MagicMock())
        iface_b = SimpleNamespace(
            nodes={"!ab12cd34": {"user": {"publicKey": b"k"}}},
            sendText=MagicMock(return_value=SimpleNamespace(id=7777)),
        )
        with self.adapter._iface_lock:
            self.adapter._interfaces.clear()
            self.adapter._interfaces["a"] = iface_a
            self.adapter._interfaces["b"] = iface_b

        err, pkt, _ = self.adapter._send_text_serialized(
            lifecycle_id=self.adapter._lifecycle_id,
            dest="!ab12cd34",
            content="route",
            parts=["meshtastic", "!ab12cd34"],
            reply_id=None,
            ack_callback=lambda packet: None,
        )

        self.assertIsNone(err)
        self.assertEqual(pkt.id, 7777)
        iface_b.sendText.assert_called_once()
        iface_a.sendText.assert_not_called()

    async def test_send_immediate_shutdown_executor_returns_no_iface(self):
        """Submitting to a shut-down executor surfaces as 'No active interfaces'."""
        dead = _DaemonTransportExecutor("meshtastic-test-dead-submit")
        dead.shutdown(wait=True)
        with patch.object(self.adapter, "_transport_executor", dead):
            result = await self.adapter._send_immediate("meshtastic:!ab12cd34", "late send")

        self.assertFalse(result.success)
        self.assertEqual(result.error, "No active interfaces connected")
        with self.adapter._ack_lock:
            self.assertEqual(self.adapter._ack_inflight_tokens, {})
            self.assertEqual(self.adapter._early_ack_packets, {})

    async def test_drain_loop_cancelled_with_failed_send_requeues(self):
        """Drain cancelled mid-send requeues when the send definitively failed."""
        from gateway.platforms.base import SendResult

        started = threading.Event()
        release = threading.Event()

        async def blocked_send(chat_id, content, **kwargs):
            started.set()
            await asyncio.to_thread(release.wait, 2)
            return SendResult(success=False, error="No active interfaces connected")

        with self.adapter._queue_lock:
            self.adapter._outbound_queue.append(
                {"chat_id": "meshtastic:!ab12cd34", "content": "drain me"}
            )
        drain = asyncio.create_task(self.adapter._drain_queue_loop(self.adapter._lifecycle_id))
        with patch.object(self.adapter, "_send_immediate", side_effect=blocked_send):
            self.assertTrue(await asyncio.to_thread(started.wait, 1))
            drain.cancel()
            release.set()
            await asyncio.gather(drain, return_exceptions=True)

        with self.adapter._queue_lock:
            self.assertEqual(len(self.adapter._outbound_queue), 1)
            self.assertEqual(self.adapter._outbound_queue[0]["content"], "drain me")

    async def test_drain_loop_cancelled_with_indeterminate_send_does_not_requeue(self):
        """Drain cancelled with the send unresolved must NOT requeue (dup risk)."""
        from gateway.platforms.base import SendResult

        started = threading.Event()

        async def blocked_send(chat_id, content, **kwargs):
            started.set()
            await asyncio.sleep(30)
            return SendResult(success=True, message_id="late")

        with self.adapter._queue_lock:
            self.adapter._outbound_queue.append(
                {"chat_id": "meshtastic:!ab12cd34", "content": "indeterminate"}
            )
        drain = asyncio.create_task(self.adapter._drain_queue_loop(self.adapter._lifecycle_id))
        with (
            patch.object(self.adapter, "_send_immediate", side_effect=blocked_send),
            patch.dict(os.environ, {"MESHTASTIC_EXECUTOR_SHUTDOWN_TIMEOUT": "0.02"}),
        ):
            self.assertTrue(await asyncio.to_thread(started.wait, 1))
            drain.cancel()
            await asyncio.gather(drain, return_exceptions=True)

        with self.adapter._queue_lock:
            self.assertEqual(self.adapter._outbound_queue, [])

    async def test_drain_loop_send_exception_requeues_item(self):
        """A send raising (not returning failure) also requeues the item."""
        calls = {"n": 0}

        async def raising_send(chat_id, content, **kwargs):
            calls["n"] += 1
            raise RuntimeError("boom")

        with self.adapter._queue_lock:
            self.adapter._outbound_queue.append(
                {"chat_id": "meshtastic:!ab12cd34", "content": "will raise"}
            )
        with (
            patch.object(self.adapter, "_send_immediate", side_effect=raising_send),
            patch.object(self.adapter, "_has_interfaces", return_value=True),
        ):
            drain = asyncio.create_task(self.adapter._drain_queue_loop(self.adapter._lifecycle_id))
            for _ in range(100):
                if calls["n"] >= 1:
                    break
                await asyncio.sleep(0.01)
            drain.cancel()
            await asyncio.gather(drain, return_exceptions=True)

        self.assertGreaterEqual(calls["n"], 1)
        with self.adapter._queue_lock:
            self.assertEqual(len(self.adapter._outbound_queue), 1)
            self.assertEqual(self.adapter._outbound_queue[0]["content"], "will raise")

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

    async def test_fail_pending_acks_synthesizes_record_for_orphan_waiter(self):
        """A waiter with no tracked record still settles as DISCONNECTED."""
        orphan = ConcurrentFuture()
        with self.adapter._ack_lock:
            self.adapter._ack_futures["orphan-1"] = orphan
            self.adapter._pending_acks.pop("orphan-1", None)

        self.adapter._fail_pending_acks(reason="DISCONNECTED")

        record = orphan.result(timeout=0.1)
        self.assertEqual(record["status"], AckStatus.TIMEOUT)
        self.assertEqual(record["error_reason"], "DISCONNECTED")
        with self.adapter._ack_lock:
            self.assertEqual(self.adapter._pending_acks["orphan-1"]["error_reason"], "DISCONNECTED")

    async def test_send_text_serialized_no_interfaces_returns_no_iface(self):
        """The worker re-check surfaces no_iface when the map is empty."""
        with self.adapter._iface_lock:
            self.adapter._interfaces.clear()

        err, pkt, _ = self.adapter._send_text_serialized(
            lifecycle_id=self.adapter._lifecycle_id,
            dest="!ab12cd34",
            content="x",
            parts=["meshtastic", "!ab12cd34"],
            reply_id=None,
            ack_callback=lambda packet: None,
        )
        self.assertEqual(err, "no_iface")
        self.assertIsNone(pkt)

    async def test_send_queues_when_iface_drops_on_worker(self):
        """If the serialized worker send sees no interfaces, non-ACK sends still queue."""
        # Fast-path still thinks we are connected; the worker send discovers the drop.
        with (
            patch.object(self.adapter, "_has_interfaces", return_value=True),
            patch.object(
                self.adapter,
                "_send_text_serialized",
                return_value=("no_iface", None, "!ab12cd34"),
            ),
        ):
            res = await self.adapter._send_chunk(
                "meshtastic:!ab12cd34",
                "queued after race",
                allow_queueing=True,
                wait_for_ack=False,
            )
        self.assertTrue(res.success)
        self.assertEqual(res.message_id, "queued")
        with self.adapter._queue_lock:
            self.assertTrue(
                any(item["content"] == "queued after race" for item in self.adapter._outbound_queue)
            )

    async def test_unsent_stale_lifecycle_job_is_queued(self):
        """A stale worker rejection before sendText must not drop the message."""

        def reject_as_stale(**_kwargs):
            with self.adapter._lifecycle_lock:
                self.adapter._lifecycle_id += 1
            return "no_iface", None, "!ab12cd34"

        with patch.object(self.adapter, "_send_text_serialized", side_effect=reject_as_stale):
            result = await self.adapter._send_chunk(
                "meshtastic:!ab12cd34",
                "queue stale unsent",
                allow_queueing=True,
                wait_for_ack=False,
            )

        self.assertTrue(result.success)
        self.assertEqual(result.message_id, "queued")
        with self.adapter._queue_lock:
            self.assertTrue(
                any(
                    item["content"] == "queue stale unsent" for item in self.adapter._outbound_queue
                )
            )

    async def test_implicit_ack_from_relay_is_not_delivery(self):
        """A routing ACK relayed by another node is implicit — not confirmed delivery."""
        iface = self.adapter.get_interfaces()[0]

        def send_text(text, destinationId=None, wantAck=False, onResponse=None, **kwargs):
            onResponse(
                {
                    "fromId": "!9e77edec",  # a RELAY, not the destination !ab12cd34
                    "decoded": {"requestId": 91002, "routing": {"errorReason": "NONE"}},
                }
            )
            return SimpleNamespace(id=91002)

        iface.sendText = MagicMock(side_effect=send_text)
        with patch.dict(os.environ, {"MESHTASTIC_ACK_TIMEOUT": "0.3"}):
            res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content="implicit ack")

        self.assertFalse(res.success)  # relay heard it, destination did not confirm
        self.assertEqual(res.raw_response["chunks"][0]["ack"]["status"], AckStatus.IMPLICIT_ACK)
        self.assertIn("implicit ACK only", res.error or "")

    async def test_implicit_ack_retries_until_real_ack(self):
        """With retries on, an implicit-only ACK is re-sent; a later real ACK delivers."""
        iface = self.adapter.get_interfaces()[0]
        calls = {"n": 0}

        def send_text(text, destinationId=None, wantAck=False, onResponse=None, **kwargs):
            calls["n"] += 1
            pid = 92000 + calls["n"]
            # Attempt 1 gets only a relay (implicit) ACK; attempt 2 the real one.
            ack_from = "!9e77edec" if calls["n"] == 1 else "!ab12cd34"
            onResponse(
                {
                    "fromId": ack_from,
                    "decoded": {"requestId": pid, "routing": {"errorReason": "NONE"}},
                }
            )
            return SimpleNamespace(id=pid)

        iface.sendText = MagicMock(side_effect=send_text)
        with patch.dict(
            os.environ, {"MESHTASTIC_SEND_RETRIES": "2", "MESHTASTIC_ACK_TIMEOUT": "0.3"}
        ):
            res = await self.adapter.send(
                chat_id="meshtastic:!ab12cd34", content="retry on implicit"
            )

        self.assertTrue(res.success)
        self.assertEqual(iface.sendText.call_count, 2)  # implicit -> retry -> real ack
        self.assertEqual(res.raw_response["chunks"][0]["attempts"], 2)

    def test_retry_backoff_defensive_parsing(self):
        """_retry_backoff falls back to the default on non-numeric input."""
        with patch.dict(os.environ, {"MESHTASTIC_RETRY_BACKOFF": "2.5"}):
            self.assertEqual(self.adapter._retry_backoff(), 2.5)
        with patch.dict(os.environ, {"MESHTASTIC_RETRY_BACKOFF": "garbage"}):
            self.assertEqual(self.adapter._retry_backoff(), 5.0)  # default, no crash
        with patch.dict(os.environ, {"MESHTASTIC_RETRY_BACKOFF": ""}):
            self.assertEqual(self.adapter._retry_backoff(), 5.0)

    async def test_get_chat_info_dm_resolves_name(self):
        """get_chat_info returns the long name for a known DM node."""
        info = await self.adapter.get_chat_info("meshtastic:!ab12cd34")
        self.assertEqual(info["type"], "dm")
        self.assertEqual(info["name"], "Park Sensor Node")

    async def test_get_chat_info_dm_unknown_falls_back_to_id(self):
        """An unknown DM node falls back to its raw id as the name."""
        info = await self.adapter.get_chat_info("meshtastic:!deadbeef")
        self.assertEqual(info["type"], "dm")
        self.assertEqual(info["name"], "!deadbeef")

    async def test_get_chat_info_channel(self):
        """get_chat_info reports a channel as a group chat."""
        info = await self.adapter.get_chat_info("meshtastic:channel:Primary")
        self.assertEqual(info["type"], "group")
        self.assertIn("Primary", info["name"])

    def test_dm_policy_reflects_access_mode(self):
        """_dm_policy mirrors the active access mode for the gateway trust path."""
        # Default fixture: allowed_nodes set, allow_all False -> allowlist policy.
        self.assertTrue(self.adapter.enforces_own_access_policy)
        self.assertEqual(self.adapter._dm_policy, "allowlist")
        # Channel broadcasts pass the same intake gate -> same policy.
        self.assertEqual(self.adapter._group_policy, "allowlist")
        # allow_all flips to "open" (adapter forwards everyone).
        self.adapter.allow_all = True
        self.assertEqual(self.adapter._dm_policy, "open")
        self.assertEqual(self.adapter._group_policy, "open")
        # No allowlist + not allow_all -> "open" (adapter default-denies at intake,
        # so the gateway never sees this traffic).
        self.adapter.allow_all = False
        self.adapter.allowed_nodes = set()
        self.assertEqual(self.adapter._dm_policy, "open")

    def test_tool_event_chrome_suppressed(self):
        """format_tool_event returns None so tool progress never hits LoRa."""
        self.assertIsNone(self.adapter.format_tool_event(SimpleNamespace()))

    def test_extract_packet_id_object_and_dict_shapes(self):
        """_extract_packet_id reads id from protobuf objects and dict packets."""
        self.assertEqual(self.adapter._extract_packet_id(SimpleNamespace(id=42)), "42")
        self.assertEqual(self.adapter._extract_packet_id({"id": 99}), "99")
        self.assertIsNone(self.adapter._extract_packet_id(SimpleNamespace()))
        self.assertIsNone(self.adapter._extract_packet_id({}))

    def test_parse_reply_id_coerces_valid_int_only(self):
        """_parse_reply_id returns an int only for genuine packet-id strings."""
        self.assertEqual(self.adapter._parse_reply_id("12345"), 12345)
        self.assertIsNone(self.adapter._parse_reply_id(None))
        self.assertIsNone(self.adapter._parse_reply_id("queued"))  # synthetic marker
        self.assertIsNone(self.adapter._parse_reply_id("not-a-number"))

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

    async def test_outbound_send_threads_reply_id(self):
        """A valid reply_to is forwarded to sendText as replyId."""
        iface = self.adapter.get_interfaces()[0]
        iface.sendText = MagicMock(return_value=SimpleNamespace(id=555))
        await self.adapter.send(
            chat_id="meshtastic:!ab12cd34", content="reply body", reply_to="4242"
        )
        self.assertEqual(iface.sendText.call_args.kwargs["replyId"], 4242)

    async def test_outbound_send_no_reply_id_when_absent(self):
        """When reply_to is absent, sendText gets replyId=None (no threading)."""
        iface = self.adapter.get_interfaces()[0]
        iface.sendText = MagicMock(return_value=SimpleNamespace(id=556))
        await self.adapter.send(chat_id="meshtastic:!ab12cd34", content="plain")
        self.assertIsNone(iface.sendText.call_args.kwargs["replyId"])

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
        await asyncio.sleep(0.05)
        event = self.adapter.handle_message.call_args[0][0]
        self.assertEqual(event.reply_to_message_id, "7788")

    def test_chunk_bytes_clamped_to_protocol_ceiling(self):
        """MESHTASTIC_CHUNK_BYTES above the 233-byte ceiling is clamped down."""
        # A single-chunk payload (<= default 170) is unaffected by the override.
        with patch.dict(os.environ, {"MESHTASTIC_CHUNK_BYTES": "500"}):
            chunks = self.adapter._chunk_message("short message")
        self.assertEqual(chunks, ["short message"])
        # A long payload over 233 bytes must still split — never a single 500-byte chunk.
        long = "y" * 400
        with patch.dict(os.environ, {"MESHTASTIC_CHUNK_BYTES": "500"}):
            chunks = self.adapter._chunk_message(long)
        self.assertGreater(len(chunks), 1)
        for c in chunks:
            self.assertLessEqual(len(c.encode("utf-8")), self.adapter.MAX_MESSAGE_LENGTH)

    def test_chunk_bytes_garbage_falls_back_to_default(self):
        """A non-numeric MESHTASTIC_CHUNK_BYTES falls back to the default, not crash."""
        long = "z" * 400  # exceeds the 170 default, so it must still split
        with patch.dict(os.environ, {"MESHTASTIC_CHUNK_BYTES": "not-a-number"}):
            chunks = self.adapter._chunk_message(long)
        self.assertGreater(len(chunks), 1)
        for c in chunks:
            self.assertLessEqual(len(c.encode("utf-8")), self.adapter.DEFAULT_CHUNK_BYTES)

    def test_split_utf8_handles_no_whitespace_and_multibyte(self):
        """_split_utf8 splits long runs without spaces and respects UTF-8 boundaries."""
        # No whitespace: must still split by byte budget (char_idx<=0 path never trips).
        no_ws = "x" * 500
        parts = self.adapter._split_utf8(no_ws, 50)
        self.assertTrue(len(parts) > 1)
        self.assertEqual("".join(parts), no_ws)
        # Multi-byte: a split point must never land inside a UTF-8 character.
        multibyte = "日本語" * 50  # 3 bytes/char
        parts = self.adapter._split_utf8(multibyte, 20)
        self.assertEqual("".join(parts), multibyte)
        for p in parts:
            p.encode("utf-8")  # each part is valid UTF-8 on its own

    async def test_outbound_queue_evicts_oldest_when_disconnected(self):
        """With no interfaces, sends queue (bounded at 100) and evict oldest-first."""
        self.adapter._interfaces.clear()
        for i in range(102):
            res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content=f"m{i}")
            self.assertTrue(res.success)
            self.assertEqual(res.message_id, "queued")
        with self.adapter._queue_lock:
            self.assertLessEqual(len(self.adapter._outbound_queue), 100)
            # First two (m0, m1) evicted; m2 is now the oldest retained.
            self.assertEqual(self.adapter._outbound_queue[0]["content"], "m2")

    async def test_named_channel_send_resolves_index(self):
        """Sending to a named channel resolves its channel index from localNode."""
        iface = self.adapter.get_interfaces()[0]
        iface.sendText = MagicMock(return_value=SimpleNamespace(id=4242))

        res = await self.adapter.send(chat_id="meshtastic:channel:Primary", content="hi")

        self.assertTrue(res.success)
        iface.sendText.assert_called_once()
        self.assertEqual(iface.sendText.call_args.kwargs["channelIndex"], 0)

    async def test_send_errors_known_dm_without_public_key(self):
        """Verify direct sends fail hard when node info shows no public key."""
        iface = self.adapter.get_interfaces()[0]
        iface.nodes["!ab12cd34"]["user"]["publicKey"] = ""
        iface.sendText = MagicMock(return_value=SimpleNamespace(id=1))

        res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content="should not send")

        self.assertFalse(res.success)
        self.assertIn("no public key", res.error)
        iface.sendText.assert_not_called()

    async def test_mesh_send_dm_errors_without_public_key(self):
        """Verify the DM tool returns a hard error for missing node public keys."""
        iface = self.adapter.get_interfaces()[0]
        iface.nodes["!ab12cd34"]["user"]["publicKey"] = ""
        iface.sendText = MagicMock(return_value=SimpleNamespace(id=1))

        result = json.loads(await handle_mesh_send_dm({"node_id": "PARK", "message": "hello"}))

        self.assertFalse(result["success"])
        self.assertIn("public key", result["error"])
        iface.sendText.assert_not_called()

    async def test_all_tools_error_when_no_adapter(self):
        """Every mesh_* handler returns a JSON error when no adapter is active."""
        meshtastic_tools.set_adapter(None)
        try:
            calls = [
                handle_mesh_list_nodes({}),
                handle_mesh_node_info({"node_id": "!ab12cd34"}),
                handle_mesh_signal_quality({"node_id": "!ab12cd34"}),
                handle_mesh_send_dm({"node_id": "!ab12cd34", "message": "hi"}),
                handle_mesh_send_broadcast({"message": "hi"}),
                handle_mesh_telemetry({"node_id": "!ab12cd34"}),
                handle_mesh_telemetry_history({"node_id": "!ab12cd34"}),
            ]
            for coro in calls:
                result = json.loads(await coro)
                self.assertIn("error", result)
                self.assertIn("not connected", result["error"])
        finally:
            meshtastic_tools.set_adapter(self.adapter)

    async def test_tools_error_on_missing_required_params(self):
        """Handlers reject calls with missing required parameters."""
        for coro in (
            handle_mesh_node_info({}),
            handle_mesh_signal_quality({}),
            handle_mesh_send_dm({"node_id": "!ab12cd34"}),  # no message
            handle_mesh_send_dm({"message": "hi"}),  # no node_id
            handle_mesh_send_broadcast({}),
            handle_mesh_telemetry({}),
            handle_mesh_telemetry_history({}),
        ):
            result = json.loads(await coro)
            self.assertIn("error", result)
            self.assertIn("required", result["error"])

    async def test_tools_error_on_unresolved_node(self):
        """node_info and send_dm surface a clear error for unknown nodes."""
        result = json.loads(await handle_mesh_node_info({"node_id": "!deadbeef"}))
        self.assertIn("not found", result["error"])
        result = json.loads(await handle_mesh_send_dm({"node_id": "!deadbeef", "message": "x"}))
        self.assertIn("could not be resolved", result["error"])

    def test_resolve_node_lookup_paths(self):
        """resolve_node matches by id, name, numeric num — and misses cleanly."""
        resolve_node = meshtastic_tools.resolve_node
        # Empty query.
        self.assertEqual(resolve_node("", self.adapter), (None, None))
        # Numeric node-num lookup (mock PARK node num).
        _, info = resolve_node("2870135092", self.adapter)
        self.assertEqual(info["user"]["id"], "!ab12cd34")
        # Miss returns (None, None).
        self.assertEqual(resolve_node("no-such-node", self.adapter), (None, None))

    def test_assess_signal_quality_bands(self):
        """assess_signal_quality covers every SNR band."""
        assess = meshtastic_tools.assess_signal_quality
        self.assertEqual(assess(None), "Unknown")
        self.assertEqual(assess(9.0), "Excellent")
        self.assertEqual(assess(5.0), "Good")
        self.assertEqual(assess(0.0), "Fair")
        self.assertEqual(assess(-10.0), "Poor")
        self.assertEqual(assess(-20.0), "No signal")

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
        self.assertEqual(res["metric_type"], "signal_quality")  # bad limit falls back to 10
        self.assertEqual(len(res["history"]), 1)

        res = json.loads(
            await handle_mesh_telemetry_history({"node_id": "!ab12cd34", "metric_type": "bogus"})
        )
        self.assertIn("Invalid metric_type", res["error"])

    def test_ack_history_is_bounded(self):
        """Verify ACK bookkeeping does not grow without bound."""
        self.adapter.ACK_RECORD_LIMIT = 5

        for i in range(50):
            token = object()
            self.adapter._track_pending_ack(str(i), "!ab12cd34", "x", send_token=token)
            self.adapter._record_ack_response(
                {
                    "fromId": "!ab12cd34",
                    "decoded": {
                        "requestId": i,
                        "routing": {"errorReason": "NONE"},
                    },
                },
                "!ab12cd34",
                "x",
                send_token=token,
            )

        self.assertLessEqual(len(self.adapter._pending_acks), 5)
        self.assertLessEqual(len(self.adapter._ack_responses), 5)
        self.assertLessEqual(len(self.adapter._ack_tokens), 5)
        self.assertLessEqual(len(self.adapter._ack_response_tokens), 5)
        # The most recent packet id is always retained.
        self.assertIn("49", self.adapter._pending_acks)

    async def test_edit_message_unsupported_does_not_send(self):
        """Verify edit updates are rejected instead of spamming LoRa progress messages."""
        iface = self.adapter.get_interfaces()[0]
        iface.sendText = MagicMock(return_value=SimpleNamespace(id=1))

        res = await self.adapter.edit_message(
            chat_id="meshtastic:!ab12cd34",
            message_id="existing",
            content="partial update",
        )

        self.assertFalse(res.success)
        self.assertIn("does not support editing", res.error)
        iface.sendText.assert_not_called()

    def test_env_parsing_prefers_allowed_nodes_alias(self):
        """Verify preferred MESHTASTIC_ALLOWED_NODES wins over legacy USERS alias."""
        with patch.dict(
            os.environ,
            {
                "MESHTASTIC_SERIAL_PORT": "mock_port",
                "MESHTASTIC_BAUD_RATE": "57600",
                "MESHTASTIC_ALLOWED_NODES": "ab12cd34",
                "MESHTASTIC_ALLOWED_USERS": "bad55555",
                "MESHTASTIC_ALLOW_ALL_USERS": "true",
                "MESHTASTIC_HOME_CHANNEL": "meshtastic:channel:0",
            },
        ):
            # Seed-from-env before the adapter expands the allowlist env for Hermes.
            env_config = _env_enablement()
            self.assertEqual(env_config["allowed_nodes"], "ab12cd34")

            config = MagicMock()
            config.extra = {}
            adapter = MeshtasticAdapter(config)
            # Hermes gateway exact-matches the env allowlist — expansion must
            # include both bang and bare forms so intake and gateway agree.
            expanded = os.environ["MESHTASTIC_ALLOWED_NODES"]
            self.assertIn("ab12cd34", expanded)
            self.assertIn("!ab12cd34", expanded)

        self.assertEqual(adapter.serial_port, "mock_port")
        self.assertEqual(adapter.baud_rate, 57600)
        self.assertTrue(adapter.allow_all)
        self.assertIn("ab12cd34", adapter.allowed_nodes)
        self.assertIn("!ab12cd34", adapter.allowed_nodes)
        self.assertNotIn("bad55555", adapter.allowed_nodes)

    def test_normalize_node_id_forms(self):
        """_normalize_node_id produces stable ! + lowercase 8-hex ids."""
        norm = MeshtasticAdapter._normalize_node_id
        self.assertEqual(norm(0xAB12CD34), "!ab12cd34")
        self.assertEqual(norm("!AB12CD34"), "!ab12cd34")
        self.assertEqual(norm("ab12cd34"), "!ab12cd34")
        self.assertEqual(norm("  !Da1b1613  "), "!da1b1613")
        self.assertIsNone(norm(None))
        self.assertIsNone(norm(""))
        # Non-hex labels are lowercased as-is (not forced into !hex form).
        self.assertEqual(norm("PARK"), "park")
        # bool is a subclass of int — must not become !00000001 / !00000000.
        self.assertEqual(norm(True), "true")
        self.assertEqual(norm(False), "false")

    async def test_wait_for_ack_timeout_does_not_overwrite_concurrent_ack(self):
        """Timeout must not stamp TIMEOUT over a real ACK that landed in the race window."""
        self.adapter.loop = asyncio.get_running_loop()
        pkt_id = "race-ack-1"
        cf = ConcurrentFuture()
        with self.adapter._ack_lock:
            self.adapter._pending_acks[pkt_id] = {
                "status": AckStatus.PENDING,
                "dest": "!ab12cd34",
            }
            self.adapter._ack_futures[pkt_id] = cf

        async def inject_ack_while_waiting():
            # Land a real ACK under the lock without resolving the future, so
            # wait_for still times out and the except path must preserve ACK.
            await asyncio.sleep(0.05)
            with self.adapter._ack_lock:
                rec = self.adapter._pending_acks[pkt_id]
                rec["status"] = AckStatus.ACK
                rec["error_reason"] = None

        injector = asyncio.create_task(inject_ack_while_waiting())
        record = await self.adapter._wait_for_ack(pkt_id, cf, 0.15)
        await injector

        self.assertEqual(record["status"], AckStatus.ACK)
        self.assertNotEqual(record.get("error_reason"), "ACK_TIMEOUT")

    async def test_wait_for_ack_timeout_stamps_pending_only(self):
        """A still-pending wait correctly becomes TIMEOUT."""
        self.adapter.loop = asyncio.get_running_loop()
        pkt_id = "race-timeout-1"
        cf = ConcurrentFuture()
        with self.adapter._ack_lock:
            self.adapter._pending_acks[pkt_id] = {
                "status": AckStatus.PENDING,
                "dest": "!ab12cd34",
            }
            self.adapter._ack_futures[pkt_id] = cf

        record = await self.adapter._wait_for_ack(pkt_id, cf, 0.05)
        self.assertEqual(record["status"], AckStatus.TIMEOUT)
        self.assertEqual(record["error_reason"], "ACK_TIMEOUT")

    def test_record_ack_does_not_downgrade_real_ack_to_implicit(self):
        """A later relay implicit ACK must not overwrite a real destination ACK."""
        dest = "!ab12cd34"
        # Real ACK from destination first.
        self.adapter._record_ack_response(
            {
                "fromId": dest,
                "decoded": {"requestId": 81001, "routing": {"errorReason": "NONE"}},
            },
            dest,
            "hi",
        )
        self.assertEqual(self.adapter.get_ack_status("81001")["status"], AckStatus.ACK)

        # Later implicit from a relay — keep the definitive result.
        self.adapter._record_ack_response(
            {
                "fromId": "!9e77edec",
                "decoded": {"requestId": 81001, "routing": {"errorReason": "NONE"}},
            },
            dest,
            "hi",
        )
        self.assertEqual(self.adapter.get_ack_status("81001")["status"], AckStatus.ACK)

    def test_record_ack_upgrades_implicit_to_real(self):
        """A real destination ACK after an implicit relay ACK upgrades status."""
        dest = "!ab12cd34"
        self.adapter._record_ack_response(
            {
                "fromId": "!9e77edec",
                "decoded": {"requestId": 81002, "routing": {"errorReason": "NONE"}},
            },
            dest,
            "hi",
        )
        self.assertEqual(self.adapter.get_ack_status("81002")["status"], AckStatus.IMPLICIT_ACK)

        self.adapter._record_ack_response(
            {
                "fromId": dest,
                "decoded": {"requestId": 81002, "routing": {"errorReason": "NONE"}},
            },
            dest,
            "hi",
        )
        self.assertEqual(self.adapter.get_ack_status("81002")["status"], AckStatus.ACK)

    async def test_record_ack_snapshot_isolates_waiter_from_later_mutation(self):
        """The future is resolved with a snapshot, not the live shared record dict."""
        self.adapter.loop = asyncio.get_running_loop()
        dest = "!ab12cd34"
        pkt_id = "81003"
        cf = ConcurrentFuture()
        with self.adapter._ack_lock:
            self.adapter._ack_futures[pkt_id] = cf

        self.adapter._record_ack_response(
            {
                "fromId": dest,
                "decoded": {"requestId": int(pkt_id), "routing": {"errorReason": "NONE"}},
            },
            dest,
            "hi",
        )
        # Mutate the live store after resolution (simulates a concurrent writer).
        with self.adapter._ack_lock:
            live = self.adapter._pending_acks[pkt_id]
            live["status"] = AckStatus.NAK
            live["error_reason"] = "NO_ROUTE"

        result = await asyncio.wait_for(asyncio.wrap_future(cf), timeout=1.0)
        # Snapshot frozen at real-ACK time must still report ACK.
        self.assertEqual(result["status"], AckStatus.ACK)
        self.assertNotEqual(result.get("error_reason"), "NO_ROUTE")
        # Live store can still show the later mutation.
        self.assertEqual(self.adapter.get_ack_status(pkt_id)["status"], AckStatus.NAK)

    async def test_inbound_uppercase_from_id_normalized(self):
        """Uppercase fromId is lowercased so Hermes allowlist exact-match works."""
        packet = {
            "fromId": "!AB12CD34",  # same node as allowlist !ab12cd34
            "toId": "!da1b1613",
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "payload": b"case fold"},
            "id": 9100,
        }
        self.adapter._on_receive(packet, self.adapter.get_interfaces()[0])
        await asyncio.sleep(0.05)
        self.adapter.handle_message.assert_called_once()
        event = self.adapter.handle_message.call_args[0][0]
        self.assertEqual(event.source.user_id, "!ab12cd34")
        self.assertEqual(event.source.chat_id, "meshtastic:!ab12cd34")

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
        with patch("adapter.HAS_MESHTASTIC", True):
            with patch(
                "meshtastic.util.findPorts", return_value=["/dev/cu.usbserial-mesh"]
            ) as find_ports:
                ports = self.adapter._discover_serial_ports()
        self.assertEqual(ports, ["/dev/cu.usbserial-mesh"])
        find_ports.assert_called_once_with(True)

    def test_register_declares_gateway_authz_env(self):
        """register() wires the allowlist env vars onto the PlatformEntry."""
        from adapter import register

        captured = {}

        class FakeCtx:
            def register_platform(self, **kwargs):
                captured.update(kwargs)

            def register_tool(self, **kwargs):
                pass

        register(FakeCtx())
        self.assertEqual(captured["allowed_users_env"], "MESHTASTIC_ALLOWED_NODES")
        self.assertEqual(captured["allow_all_env"], "MESHTASTIC_ALLOW_ALL_USERS")
        self.assertEqual(captured["max_message_length"], 233)
        self.assertEqual(captured["cron_deliver_env_var"], "MESHTASTIC_HOME_CHANNEL")
        self.assertTrue(callable(captured["standalone_sender_fn"]))


class TestMeshtasticTcpTransport(unittest.IsolatedAsyncioTestCase):
    """Cover the TCP/IP transport selection and connection path."""

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
        "MESHTASTIC_TCP_HOST": "",
        "MESHTASTIC_TCP_PORT": "",
    }

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
        merged = {**self._BLANK_ENV, **env}
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

    def test_parse_tcp_target(self):
        self.assertEqual(
            MeshtasticAdapter._parse_tcp_target("tcp://192.168.1.50:4403"),
            ("192.168.1.50", 4403),
        )
        # Missing port falls back to the default.
        self.assertEqual(
            MeshtasticAdapter._parse_tcp_target("tcp://meshgw.local"),
            ("meshgw.local", 4403),
        )

    def test_ipv6_target_round_trip(self):
        """IPv6 literals are bracketed when built and unbracketed when parsed."""
        adapter = self._adapter(MESHTASTIC_TCP_HOST="2001:db8::1", MESHTASTIC_TCP_PORT="8080")
        self.assertEqual(adapter._connection_targets(), ["tcp://[2001:db8::1]:8080"])
        self.assertEqual(
            MeshtasticAdapter._parse_tcp_target("tcp://[2001:db8::1]:8080"),
            ("2001:db8::1", 8080),
        )
        # Bracketed literal without a port falls back to the default.
        self.assertEqual(
            MeshtasticAdapter._parse_tcp_target("tcp://[fe80::1]"),
            ("fe80::1", 4403),
        )

    def test_env_enablement_for_tcp_only(self):
        """The platform enables on a TCP host even without a serial port."""
        with patch.dict(os.environ, {**self._BLANK_ENV, "MESHTASTIC_TCP_HOST": "10.0.0.7"}):
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
