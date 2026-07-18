"""Fallback Meshtastic interface classes used when hardware or deps are absent.

``MockSerialInterface`` and ``MockLocalNode`` are substituted in by
``adapter._open_interface`` when the Meshtastic / pyserial libraries are not
installed or no port was discovered, so the plugin always loads and registers
its tools even on a host that cannot reach a radio. When this fallback is
active the adapter logs ``Initialized Mock Serial Connection on <port>`` and the
inbound/outbound paths exercise the same code as real hardware minus the wire.
"""

import logging
import time
from types import SimpleNamespace

logger = logging.getLogger(__name__)


class MockLocalNode:
    def __init__(self, interface):
        self.interface = interface
        self.nodeId = "!da1b1613"
        self.channels = [
            {"index": 0, "name": "Primary", "psk": "AES128"},
            {"index": 1, "name": "Telemetry", "psk": "AES128"},
        ]


class MockSerialInterface:
    """Mock Meshtastic interface that simulates hardware behaviour."""

    def __init__(self, devPath=None, noProto=True):
        self.devPath = devPath or "mock_port"
        self.nodes = {
            "!da1b1613": {
                "num": 3659208211,
                "user": {
                    "id": "!da1b1613",
                    "longName": "Phoenix HQ",
                    "shortName": "PHX",
                    "hwModel": "HELTEC_V3",
                    "role": "CLIENT_BASE",
                    "publicKey": "mock_pub_key_hq",
                },
                "deviceMetrics": {
                    "batteryLevel": 85,
                    "voltage": 4.12,
                    "uptimeSeconds": 1200,
                },
                "position": {
                    "latitude": 42.6983,
                    "longitude": -71.1234,
                    "altitude": 105,
                },
                "snr": 8.5,
                "rssi": -92,
                "lastHeard": time.time(),
            },
            "!ab12cd34": {
                "num": 2870135092,
                "user": {
                    "id": "!ab12cd34",
                    "longName": "Park Sensor Node",
                    "shortName": "PARK",
                    "hwModel": "SENSECAP_T1000",
                    "role": "SENSOR",
                    "publicKey": "mock_pub_key_sensor",
                },
                "deviceMetrics": {
                    "batteryLevel": 92,
                    "voltage": 4.15,
                    "uptimeSeconds": 5000,
                },
                "environmentMetrics": {
                    "temperature": 22.4,
                    "relativeHumidity": 54.2,
                    "barometricPressure": 1013.25,
                },
                "snr": 5.0,
                "rssi": -105,
                "lastHeard": time.time() - 300,
            },
        }
        self.localNode = MockLocalNode(self)
        self.metadata = {"firmwareVersion": "2.3.15"}
        logger.info(f"Initialized Mock Serial Connection on {self.devPath}")

    def getMyNodeId(self):
        return "!da1b1613"

    def sendText(self, text, destinationId=None, channelIndex=0, **kwargs):
        logger.info(
            f"[Mock] Sent message to {destinationId or 'broadcast'} on channel {channelIndex}: {text}"
        )
        return SimpleNamespace(id=int(time.time() * 1000) & 0xFFFFFFFF)

    def close(self):
        logger.info("[Mock] Closed connection")
