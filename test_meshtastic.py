"""
Unit and Integration Test Suite for Meshtastic Platform Adapter.
"""

import asyncio
import json
import os
import sys
import tempfile
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
from adapter import MeshtasticAdapter, MockSerialInterface, _env_enablement, _standalone_send
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

    async def test_inbound_channel_scoping(self):
        """Test broadcasts create shared channel sessions."""
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


if __name__ == "__main__":
    unittest.main()
