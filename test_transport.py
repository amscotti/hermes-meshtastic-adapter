"""Pure-function unit tests for transport.py.

These complement test_meshtastic.py by exercising branches that the adapter's
integration paths do not easily reach: parse_tcp_target's IPv6 / ValueError
fallbacks, connection_targets' auto-discovery fallback, and open_interface's
TCP-without-meshtastic warning path.
"""

import unittest
from unittest.mock import patch

import transport


class TestParseTcpTarget(unittest.TestCase):
    def test_parse_tcp_target_plain_host_port(self):
        self.assertEqual(transport.parse_tcp_target("tcp://host:4403"), ("host", 4403))

    def test_parse_tcp_target_default_port(self):
        self.assertEqual(transport.parse_tcp_target("tcp://host"), ("host", 4403))

    def test_parse_tcp_target_bad_port_falls_back(self):
        # The implementation returns ``rest`` (not ``host``) on ValueError, so
        # a malformed port yields the full "host:port" string, default port.
        self.assertEqual(transport.parse_tcp_target("tcp://host:abc"), ("host:abc", 4403))

    def test_parse_tcp_target_ipv6_bracketed(self):
        self.assertEqual(transport.parse_tcp_target("tcp://[::1]:4403"), ("::1", 4403))

    def test_parse_tcp_target_ipv6_no_port(self):
        self.assertEqual(transport.parse_tcp_target("tcp://[::1]"), ("::1", 4403))

    def test_parse_tcp_target_ipv6_bad_port(self):
        self.assertEqual(transport.parse_tcp_target("tcp://[::1]:abc"), ("::1", 4403))


class TestConnectionTargets(unittest.TestCase):
    def test_tcp_takes_precedence_over_serial(self):
        targets = transport.connection_targets("host", 4403, "/dev/ttyUSB0")
        self.assertEqual(targets, ["tcp://host:4403"])

    def test_ipv6_host_is_bracketed(self):
        targets = transport.connection_targets("::1", 4403, "")
        self.assertEqual(targets, ["tcp://[::1]:4403"])

    def test_serial_passthrough(self):
        targets = transport.connection_targets("", 4403, "/dev/ttyUSB0")
        self.assertEqual(targets, ["/dev/ttyUSB0"])

    def test_auto_falls_back_to_mock_when_no_ports(self):
        with patch("transport.discover_serial_ports", return_value=[]):
            targets = transport.connection_targets("", 4403, "auto")
        self.assertEqual(targets, ["mock_port"])

    def test_auto_returns_discovered_ports(self):
        with patch(
            "transport.discover_serial_ports",
            return_value=["/dev/cu.usbserial-X", "/dev/cu.usbmodem-Y"],
        ):
            targets = transport.connection_targets("", 4403, "auto")
        self.assertEqual(targets, ["/dev/cu.usbserial-X", "/dev/cu.usbmodem-Y"])


class TestOpenInterface(unittest.TestCase):
    def test_tcp_target_without_meshtastic_falls_back_to_mock(self):
        # When meshtastic is unavailable, a tcp:// target must not raise; it
        # logs a warning and hands back the mock interface bound to that target.
        with patch("transport.HAS_MESHTASTIC", False):
            iface = transport.open_interface("tcp://host:4403")
        self.assertEqual(iface.devPath, "tcp://host:4403")


if __name__ == "__main__":
    unittest.main()
