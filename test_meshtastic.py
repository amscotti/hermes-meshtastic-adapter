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
    MeshtasticAdapter,
    MockSerialInterface,
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
        """Verify outbound messages >237 chars are split into chunks."""
        long_message = "A" * 300  # Exceeds the 237 char limit

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
            self.assertLessEqual(len(call[1]["text"].encode("utf-8")), 237)
        self.assertTrue(calls[0][1]["text"].startswith("[1/"))

    async def test_telemetry_persistence(self):
        """Test real-time telemetry logging to SQLite."""
        packet = {
            "fromId": "!ab12cd34",
            "decoded": {
                "portnum": "TELEMETRY_APP",
                "telemetry": {
                    "deviceMetrics": {"batteryLevel": 88, "voltage": 4.05, "uptime": 3600},
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
        # The emoji is four UTF-8 bytes, so this exceeds the 237 byte limit.
        long_emoji_msg = "💩" * 60

        iface = self.adapter.get_interfaces()[0]
        iface.sendText = MagicMock(return_value=True)

        res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content=long_emoji_msg)
        self.assertTrue(res.success)

        # Each chunk must be UTF-8 byte safe, including numbering prefixes.
        self.assertGreater(iface.sendText.call_count, 1)
        calls = iface.sendText.call_args_list
        for call in calls:
            self.assertLessEqual(len(call[1]["text"].encode("utf-8")), 237)
        reconstructed = "".join(call[1]["text"].split("] ", 1)[1] for call in calls)
        self.assertEqual(reconstructed, long_emoji_msg)

    def test_mixed_ascii_emoji_chunk_reconstruction(self):
        """Verify mixed ASCII and emoji chunks reconstruct without dropping spaces."""
        message = ("status update " * 30) + ("💩" * 40) + " final words"
        chunks = self.adapter._chunk_message(message)

        self.assertGreater(len(chunks), 1)
        for chunk in chunks:
            self.assertLessEqual(len(chunk.encode("utf-8")), 237)

        reconstructed = "".join(chunk.split("] ", 1)[1] for chunk in chunks)
        self.assertEqual(reconstructed, message)

    def test_default_chunk_budget_is_conservative(self):
        """With no override, chunks stay within the conservative default budget.

        237 leaves no room for encrypted-DM (PKI) overhead — the radio NAKs
        oversized DM chunks with TOO_LARGE — so the default must be lower.
        """
        self.assertEqual(self.adapter.DEFAULT_CHUNK_BYTES, 170)
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
        self.assertEqual(res.raw_response["chunks"][0]["ack"]["status"], "ack")
        self.assertEqual(self.adapter.get_ack_status("123456")["status"], "ack")

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
        self.assertEqual(res.raw_response["chunks"][0]["ack"]["status"], "nak")
        self.assertEqual(res.raw_response["chunks"][0]["ack"]["error_reason"], "NO_ROUTE")

    async def test_wait_for_ack_timeout_fails_send(self):
        """Verify missing ACK/NACK fails after the configured timeout."""
        iface = self.adapter.get_interfaces()[0]
        iface.sendText = MagicMock(return_value=SimpleNamespace(id=333444))

        with patch.dict(os.environ, {"MESHTASTIC_ACK_TIMEOUT": "0.01"}):
            res = await self.adapter.send(chat_id="meshtastic:!ab12cd34", content="timeout check")

        self.assertFalse(res.success)
        self.assertIn("ACK timeout", res.error)
        self.assertEqual(res.raw_response["chunks"][0]["ack"]["status"], "timeout")

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

        self.assertTrue(self.adapter._is_retriable_failure(r({"status": "timeout"})))
        self.assertTrue(
            self.adapter._is_retriable_failure(r({"status": "nak", "error_reason": "NO_ROUTE"}))
        )
        self.assertFalse(
            self.adapter._is_retriable_failure(r({"status": "nak", "error_reason": "TOO_LARGE"}))
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
                self.adapter._is_retriable_failure(r({"status": "nak", "error_reason": reason})),
                f"{reason} should be permanent",
            )
        self.assertFalse(self.adapter._is_retriable_failure(r({"status": "ack"})))
        self.assertFalse(self.adapter._is_retriable_failure(r(None)))  # pre-send error

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
            self.adapter._track_pending_ack(str(i), "!ab12cd34", "x")

        self.assertLessEqual(len(self.adapter._pending_acks), 5)
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
            config = MagicMock()
            config.extra = {}
            adapter = MeshtasticAdapter(config)
            env_config = _env_enablement()

        self.assertEqual(adapter.serial_port, "mock_port")
        self.assertEqual(adapter.baud_rate, 57600)
        self.assertTrue(adapter.allow_all)
        self.assertIn("ab12cd34", adapter.allowed_nodes)
        self.assertIn("!ab12cd34", adapter.allowed_nodes)
        self.assertNotIn("bad55555", adapter.allowed_nodes)
        self.assertEqual(env_config["allowed_nodes"], "ab12cd34")

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
