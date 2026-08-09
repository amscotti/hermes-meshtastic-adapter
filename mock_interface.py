"""Dry-run Meshtastic interface classes for tests and explicit mock targets.

``MockSerialInterface`` and ``MockLocalNode`` are returned by
``transport.open_interface`` only for ``mock_port`` targets, or when the
meshtastic library is missing *and* ``MESHTASTIC_MOCK=1`` opts into a dry-run.
Real serial/TCP targets never fall back here silently: a missing library raises
with install instructions (after one ``MESHTASTIC_AUTOINSTALL`` attempt). When
active, logs mark the connection as mock/dry-run so production traffic is not
confused with the two fake nodes (``!da1b1613`` / ``!ab12cd34``).
"""

import logging
import threading
import time
from types import SimpleNamespace

logger = logging.getLogger(__name__)

# The real library hands out monotonically increasing 32-bit packet ids. The
# previous wall-clock millisecond timestamp aliased back-to-back sends within
# the same millisecond (the adapter keys ACK records and chunk identities by
# this id); a counter can never collide for a mock. The read-modify-write is
# guarded so uniqueness does not depend on callers being externally serialized
# (today every mock send goes through the single transport worker thread, but a
# future direct caller racing it must not corrupt ACK/chunk state).
_next_packet_id = time.time_ns() & 0xFFFFFFFF
_packet_id_lock = threading.Lock()


def _allocate_packet_id() -> int:
    """Return the next monotonic 32-bit packet id for the mock."""
    global _next_packet_id
    with _packet_id_lock:
        _next_packet_id = (_next_packet_id + 1) & 0xFFFFFFFF
        return _next_packet_id


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
        # The mock never validates delivery: full outbound text is only logged
        # at DEBUG, mirroring the adapter's own log discipline (outbound
        # payload can carry secrets the user asked the agent about).
        target = destinationId or "broadcast"
        logger.info(
            "[Mock] Sent message to %s on channel %d (%d bytes)",
            target,
            channelIndex,
            len(text.encode("utf-8")),
        )
        logger.debug("[Mock] Message body: %s", text)
        return SimpleNamespace(id=_allocate_packet_id())

    def sendData(self, data, destinationId=None, portNum=None, wantResponse=False, **kwargs):
        """Serialize-and-send only; the mock cannot validate delivery.

        Mirrors the real ``sendData`` return (a packet with an ``id``) but never
        fires ``onResponse`` / ACK callbacks, so ACK-waited or solicited sends
        against the dry-run mock resolve by timeout, never by delivery.
        """
        logger.info(
            "[Mock] sendData portNum=%s to %s (wantResponse=%s)",
            portNum,
            destinationId,
            wantResponse,
        )
        return SimpleNamespace(id=_allocate_packet_id())

    def getMyNodeInfo(self):
        # Mirror the real library: the local node's entry from the node DB.
        # Returning the live entry keeps getMyNodeId() / localNode.nodeId /
        # self.nodes / getMyNodeInfo() in agreement (the adapter reads
        # getMyNodeInfo() for solicited-telemetry self-metrics and self.nodes
        # for mesh_node_info; divergent values made dry-run battery frail).
        return self.nodes[self.getMyNodeId()]

    def close(self):
        logger.info("[Mock] Closed connection")
