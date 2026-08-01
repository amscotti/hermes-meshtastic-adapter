"""Pure-function unit tests for transport.py.

These complement test_meshtastic.py by exercising branches that the adapter's
integration paths do not easily reach: parse_tcp_target's IPv6 / ValueError
fallbacks, connection_targets' auto-discovery fallback, and open_interface's
missing-library behavior (fail loud, mock opt-in, automatic install).
"""

import subprocess
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
    def tearDown(self):
        transport._autoinstall_attempted = False

    def test_explicit_mock_port_returns_mock(self):
        with patch("transport.HAS_MESHTASTIC", True):
            iface = transport.open_interface("mock_port")
        self.assertEqual(iface.devPath, "mock_port")

    def test_serial_target_without_meshtastic_raises(self):
        # A real serial target must NEVER silently fall back to the mock: the
        # gateway would report "connected" while no radio traffic flows.
        with (
            patch("transport.HAS_MESHTASTIC", False),
            patch("transport._autoinstall_enabled", return_value=False),
            patch("transport._mock_opt_in", return_value=False),
        ):
            with self.assertRaises(RuntimeError) as ctx:
                transport.open_interface("/dev/ttyUSB0")
        self.assertIn("pip install", str(ctx.exception))

    def test_tcp_target_without_meshtastic_raises(self):
        with (
            patch("transport.HAS_MESHTASTIC", False),
            patch("transport._autoinstall_enabled", return_value=False),
            patch("transport._mock_opt_in", return_value=False),
        ):
            with self.assertRaises(RuntimeError) as ctx:
                transport.open_interface("tcp://host:4403")
        self.assertIn("pip install", str(ctx.exception))

    def test_mock_opt_in_returns_mock_when_library_missing(self):
        # MESHTASTIC_MOCK=1 is the explicit dry-run escape hatch.
        with (
            patch("transport.HAS_MESHTASTIC", False),
            patch("transport._autoinstall_enabled", return_value=False),
            patch("transport._mock_opt_in", return_value=True),
        ):
            iface = transport.open_interface("tcp://host:4403")
        self.assertEqual(iface.devPath, "tcp://host:4403")

    def test_missing_library_with_autoinstall_disabled_logs_error(self):
        # ensure_meshtastic_library with autoinstall off must not run pip and
        # must not be retried within the same process.
        with (
            patch("transport.HAS_MESHTASTIC", False),
            patch("transport._autoinstall_enabled", return_value=False),
            patch("transport.subprocess.run") as run,
        ):
            transport.ensure_meshtastic_library()
            transport.ensure_meshtastic_library()
        run.assert_not_called()


class TestEnsureMeshtasticLibrary(unittest.TestCase):
    def tearDown(self):
        transport._autoinstall_attempted = False

    def test_skips_when_library_present(self):
        with patch("transport.HAS_MESHTASTIC", True), patch("transport.subprocess.run") as run:
            transport.ensure_meshtastic_library()
        run.assert_not_called()

    def test_runs_pip_once_and_reimports(self):
        # After a successful pip install the re-import must succeed (the dev
        # venv has meshtastic installed) and only one pip run may happen.
        with (
            patch("transport.HAS_MESHTASTIC", False),
            patch("transport._autoinstall_enabled", return_value=True),
            patch("transport.subprocess.run") as run,
        ):
            run.return_value.returncode = 0
            run.return_value.stdout = ""
            run.return_value.stderr = ""
            transport.ensure_meshtastic_library()
            transport.ensure_meshtastic_library()
        run.assert_called_once()
        self.assertTrue(transport.HAS_MESHTASTIC)
        self.assertIsNotNone(transport.pub)

    def test_pip_failure_keeps_library_missing(self):
        with (
            patch("transport.HAS_MESHTASTIC", False),
            patch("transport._autoinstall_enabled", return_value=True),
            patch("transport.subprocess.run") as run,
            patch("transport._import_meshtastic_libs", return_value=False),
        ):
            run.return_value.returncode = 1
            run.return_value.stdout = "some pip error"
            run.return_value.stderr = ""
            transport.ensure_meshtastic_library()
            run.assert_called_once()
            self.assertFalse(transport.HAS_MESHTASTIC)

    def test_pip_timeout_logs_error_and_keeps_library_missing(self):
        with (
            patch("transport.HAS_MESHTASTIC", False),
            patch("transport._autoinstall_enabled", return_value=True),
            patch(
                "transport.subprocess.run",
                side_effect=subprocess.TimeoutExpired(cmd="pip", timeout=300),
            ) as run,
        ):
            transport.ensure_meshtastic_library()
            run.assert_called_once()
            self.assertFalse(transport.HAS_MESHTASTIC)

    def test_pip_oserror_logs_error_and_keeps_library_missing(self):
        with (
            patch("transport.HAS_MESHTASTIC", False),
            patch("transport._autoinstall_enabled", return_value=True),
            patch("transport.subprocess.run", side_effect=OSError("boom")) as run,
        ):
            transport.ensure_meshtastic_library()
            run.assert_called_once()
            self.assertFalse(transport.HAS_MESHTASTIC)

    def test_pip_success_but_reimport_fails_keeps_library_missing(self):
        with (
            patch("transport.HAS_MESHTASTIC", False),
            patch("transport._autoinstall_enabled", return_value=True),
            patch("transport.subprocess.run") as run,
            patch("transport._import_meshtastic_libs", return_value=False),
        ):
            run.return_value.returncode = 0
            run.return_value.stdout = ""
            run.return_value.stderr = ""
            transport.ensure_meshtastic_library()
            run.assert_called_once()
            self.assertFalse(transport.HAS_MESHTASTIC)

    def test_disabled_autoinstall_skips_pip(self):
        with (
            patch("transport.HAS_MESHTASTIC", False),
            patch("transport._autoinstall_enabled", return_value=False),
            patch("transport.subprocess.run") as run,
        ):
            transport.ensure_meshtastic_library()
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
