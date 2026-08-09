"""Pure-function unit tests for transport.py.

These complement test_meshtastic.py by exercising branches that the adapter's
integration paths do not easily reach: parse_tcp_target's IPv6 / ValueError
fallbacks, connection_targets' auto-discovery fallback, and open_interface's
missing-library behavior (fail loud, mock opt-in, automatic install).
"""

import asyncio
import os
import subprocess
import threading
import time
import unittest
from concurrent.futures import Future as ConcurrentFuture
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import transport


class TestEnvBoolParsing(unittest.TestCase):
    """Direct tests of the env-flag parsing layer (_env_bool and wrappers).

    Tests above patch the wrappers; these pin the raw parsing contract so a
    typo like "0 " or "False" silently changing semantics is caught.
    """

    def test_env_bool_defaults_when_unset(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertTrue(transport._env_bool("MESHTASTIC_UNSET_FLAG", True))
            self.assertFalse(transport._env_bool("MESHTASTIC_UNSET_FLAG", False))

    def test_env_bool_truthy_values(self):
        for raw in ("1", "true", "TRUE", "yes", "on", " 1 ", "True"):
            with patch.dict(os.environ, {"MESHTASTIC_ENV_BOOL_TEST": raw}):
                self.assertTrue(
                    transport._env_bool("MESHTASTIC_ENV_BOOL_TEST", False),
                    f"expected {raw!r} truthy",
                )

    def test_env_bool_falsy_values(self):
        for raw in ("0", "false", "FALSE", "no", "off", " 0 "):
            with patch.dict(os.environ, {"MESHTASTIC_ENV_BOOL_TEST": raw}):
                self.assertFalse(
                    transport._env_bool("MESHTASTIC_ENV_BOOL_TEST", True), f"expected {raw!r} falsy"
                )

    def test_env_bool_set_but_empty_takes_default(self):
        # A set-but-empty value (a common ``VAR=`` .env mistake) must fall back
        # to the default, not read truthy — so ``MESHTASTIC_MOCK=`` cannot
        # silently arm the dry-run mock. Same for unrecognized values.
        for raw in ("", "   ", "enabled", "banana"):
            with patch.dict(os.environ, {"MESHTASTIC_ENV_BOOL_TEST": raw}):
                self.assertFalse(
                    transport._env_bool("MESHTASTIC_ENV_BOOL_TEST", False),
                    f"expected {raw!r} to take the falsy default",
                )
                self.assertTrue(
                    transport._env_bool("MESHTASTIC_ENV_BOOL_TEST", True),
                    f"expected {raw!r} to take the truthy default",
                )

    def test_mock_opt_in_parses_env(self):
        with patch.dict(os.environ, {"MESHTASTIC_MOCK": "1"}):
            self.assertTrue(transport._mock_opt_in())
        with patch.dict(os.environ, {"MESHTASTIC_MOCK": "0"}):
            self.assertFalse(transport._mock_opt_in())
        with patch.dict(os.environ, {"MESHTASTIC_MOCK": ""}):
            self.assertFalse(transport._mock_opt_in())
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(transport._mock_opt_in())

    def test_mock_opt_in_blank_var_does_not_opt_in(self):
        # The fail-loud rule: MESHTASTIC_MOCK= (empty) must not arm the dry-run
        # mock; only an explicit "1"/"true"/"yes"/"on" does.
        with patch.dict(os.environ, {"MESHTASTIC_MOCK": ""}):
            self.assertIs(transport._mock_opt_in(), False)

    def test_autoinstall_enabled_parses_env(self):
        with patch.dict(os.environ, {"MESHTASTIC_AUTOINSTALL": "false"}):
            self.assertFalse(transport._autoinstall_enabled())
        with patch.dict(os.environ, {"MESHTASTIC_AUTOINSTALL": "1"}):
            self.assertTrue(transport._autoinstall_enabled())
        with patch.dict(os.environ, {"MESHTASTIC_AUTOINSTALL": "off"}):
            self.assertFalse(transport._autoinstall_enabled())
        with patch.dict(os.environ, {"MESHTASTIC_AUTOINSTALL": ""}):
            # Blank keeps autoinstall on (the default), so the pip fallback is
            # not silently disabled by a stray ``VAR=``.
            self.assertTrue(transport._autoinstall_enabled())
        with patch.dict(os.environ, {}, clear=True):
            self.assertTrue(transport._autoinstall_enabled())


class TestParseTcpTarget(unittest.TestCase):
    def test_parse_tcp_target_plain_host_port(self):
        self.assertEqual(transport.parse_tcp_target("tcp://host:4403"), ("host", 4403))

    def test_parse_tcp_target_default_port(self):
        self.assertEqual(transport.parse_tcp_target("tcp://host"), ("host", 4403))

    def test_parse_tcp_target_bad_port_falls_back(self):
        # A malformed port yields the clean host (not the raw "host:port" tail)
        # with the default port, so TCPInterface gets a plausible hostname.
        self.assertEqual(transport.parse_tcp_target("tcp://host:abc"), ("host", 4403))

    def test_parse_tcp_target_ipv6_bracketed(self):
        self.assertEqual(transport.parse_tcp_target("tcp://[::1]:4403"), ("::1", 4403))

    def test_parse_tcp_target_ipv6_no_port(self):
        self.assertEqual(transport.parse_tcp_target("tcp://[::1]"), ("::1", 4403))

    def test_parse_tcp_target_ipv6_bad_port(self):
        self.assertEqual(transport.parse_tcp_target("tcp://[::1]:abc"), ("::1", 4403))

    def test_parse_tcp_target_unclosed_bracket(self):
        # An unclosed "[" must return the de-bracketed host, never a hostname
        # that embeds "[" / ":" verbatim.
        self.assertEqual(transport.parse_tcp_target("tcp://[::1"), ("::1", 4403))
        self.assertEqual(transport.parse_tcp_target("tcp://[::1:abc"), ("::1:abc", 4403))


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

    def test_real_target_with_empty_mock_var_raises(self):
        # MESHTASTIC_MOCK= (blank) must NOT opt into the dry-run mock: a real
        # serial target still fails loud with install instructions.
        with (
            patch("transport.HAS_MESHTASTIC", False),
            patch("transport._autoinstall_enabled", return_value=False),
            patch.dict(os.environ, {"MESHTASTIC_MOCK": ""}),
        ):
            with self.assertRaises(RuntimeError) as ctx:
                transport.open_interface("/dev/ttyUSB0")
        self.assertIn("pip install", str(ctx.exception))

    def test_inconsistent_import_state_raises(self):
        # HAS_MESHTASTIC=True with a missing module is a typed RuntimeError, not
        # an AttributeError deep inside the constructor.
        with (
            patch("transport.HAS_MESHTASTIC", True),
            patch("transport.meshtastic", None),
        ):
            with self.assertRaises(RuntimeError) as ctx:
                transport.open_interface("/dev/ttyUSB0")
        self.assertIn("inconsistent", str(ctx.exception))

    def test_serial_target_constructs_serial_interface(self):
        """A real serial target with the library present constructs SerialInterface."""
        fake_meshtastic = SimpleNamespace(
            serial_interface=SimpleNamespace(SerialInterface=MagicMock(return_value="iface"))
        )
        with (
            patch("transport.HAS_MESHTASTIC", True),
            patch("transport.meshtastic", fake_meshtastic),
        ):
            iface = transport.open_interface("/dev/ttyUSB0")
        self.assertEqual(iface, "iface")
        fake_meshtastic.serial_interface.SerialInterface.assert_called_once_with(
            devPath="/dev/ttyUSB0"
        )

    def test_tcp_target_constructs_tcp_interface(self):
        """A tcp:// target with the library present constructs TCPInterface from
        parse_tcp_target's hostname/port kwargs."""
        fake_meshtastic = SimpleNamespace(
            tcp_interface=SimpleNamespace(TCPInterface=MagicMock(return_value="iface"))
        )
        with (
            patch("transport.HAS_MESHTASTIC", True),
            patch("transport.meshtastic", fake_meshtastic),
        ):
            iface = transport.open_interface("tcp://myhost:4403")
        self.assertEqual(iface, "iface")
        fake_meshtastic.tcp_interface.TCPInterface.assert_called_once_with(
            hostname="myhost", portNumber=4403
        )


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


class TestInterfaceIsAlive(unittest.TestCase):
    """Liveness probe (moved from the adapter; the adapter delegate is pinned
    by test_lifecycle, this class pins the transport body directly)."""

    def test_socket_handle_governs_when_no_isconnected(self):
        self.assertTrue(transport.interface_is_alive(SimpleNamespace(socket=object())))
        self.assertFalse(transport.interface_is_alive(SimpleNamespace(socket=None)))

    def test_isconnected_event_is_authoritative_over_socket(self):
        # A TCP iface mid-self-heal (socket=None, isConnected set) reads alive;
        # a real drop clears the event -> dead.
        evt = threading.Event()
        tcp_iface = SimpleNamespace(socket=None, isConnected=evt)
        self.assertFalse(transport.interface_is_alive(tcp_iface))
        evt.set()
        self.assertTrue(transport.interface_is_alive(tcp_iface))

    def test_serial_stream_is_open_governs(self):
        alive = SimpleNamespace(stream=SimpleNamespace(is_open=True))
        dead = SimpleNamespace(stream=SimpleNamespace(is_open=False))
        self.assertTrue(transport.interface_is_alive(alive))
        self.assertFalse(transport.interface_is_alive(dead))

    def test_serial_stream_is_open_method_fallback(self):
        alive = SimpleNamespace(stream=SimpleNamespace(isOpen=lambda: True))
        dead = SimpleNamespace(stream=SimpleNamespace(isOpen=lambda: False))
        self.assertTrue(transport.interface_is_alive(alive))
        self.assertFalse(transport.interface_is_alive(dead))

    def test_unknown_interface_defaults_alive(self):
        # No known liveness handle (e.g. the mock interface) — assume alive.
        self.assertTrue(transport.interface_is_alive(object()))

    def test_serial_stream_without_probes_defers_to_isconnected(self):
        # A serial stream exposing neither isOpen nor is_open must not read
        # alive forever: fall through to the authoritative isConnected event.
        evt = threading.Event()
        iface = SimpleNamespace(stream=SimpleNamespace(), isConnected=evt)
        self.assertFalse(transport.interface_is_alive(iface))
        evt.set()
        self.assertTrue(transport.interface_is_alive(iface))

    def test_serial_stream_without_probes_or_event_assumes_alive(self):
        # No probe attribute and no isConnected event: assume alive (the generic
        # last-resort fallback for e.g. the mock interface).
        self.assertTrue(transport.interface_is_alive(SimpleNamespace(stream=object())))


class TestSubmitLivenessProbe(unittest.TestCase):
    """Executor-mediated liveness probe (feeds the adapter's poll loop)."""

    def _executor(self):
        return transport._DaemonTransportExecutor(name="probe-test")

    def test_alive_target_returns_true(self):
        iface = SimpleNamespace(stream=SimpleNamespace(is_open=True))
        executor = self._executor()
        try:
            fut = transport.submit_liveness_probe(
                executor, "target", iface, interfaces={"target": iface}, iface_lock=threading.Lock()
            )
            self.assertTrue(fut.result(timeout=5))
        finally:
            executor.shutdown(wait=True, timeout=5)

    def test_dead_target_is_removed_and_returns_false(self):
        iface = SimpleNamespace(stream=SimpleNamespace(is_open=False), close=MagicMock())
        interfaces = {"target": iface}
        executor = self._executor()
        try:
            fut = transport.submit_liveness_probe(
                executor, "target", iface, interfaces=interfaces, iface_lock=threading.Lock()
            )
            self.assertFalse(fut.result(timeout=5))
            self.assertNotIn("target", interfaces)
            iface.close.assert_called_once()
        finally:
            executor.shutdown(wait=True, timeout=5)

    def test_replaced_target_is_a_noop(self):
        # The interface under the key changed: probe returns None and closes
        # nothing (the replacement is someone else's to manage).
        iface = SimpleNamespace(stream=SimpleNamespace(is_open=False), close=MagicMock())
        executor = self._executor()
        try:
            fut = transport.submit_liveness_probe(
                executor, "target", iface, interfaces={}, iface_lock=threading.Lock()
            )
            self.assertIsNone(fut.result(timeout=5))
            iface.close.assert_not_called()
        finally:
            executor.shutdown(wait=True, timeout=5)

    def test_replaced_target_after_dead_probe_is_a_noop(self):
        # The probe reports dead, but the target got replaced between the probe
        # and the pop — the replacement must survive and nothing may be closed.
        original = SimpleNamespace(stream=SimpleNamespace(is_open=False), close=MagicMock())
        replacement = SimpleNamespace(close=MagicMock())
        interfaces = {"target": original}

        def probe_dead_but_replaced(_iface):
            interfaces["target"] = replacement
            return False

        result = transport.drop_interface_if_dead_serialized(
            "target",
            original,
            interfaces=interfaces,
            iface_lock=threading.Lock(),
            is_alive=probe_dead_but_replaced,
        )
        self.assertIsNone(result)
        self.assertIs(interfaces["target"], replacement)
        original.close.assert_not_called()
        replacement.close.assert_not_called()


class TestSettleFuture(unittest.TestCase):
    """Pure capture helper for daemon-thread close jobs."""

    def test_records_success_result(self):
        fut = ConcurrentFuture()
        transport._settle_future(fut, lambda: None)
        self.assertIsNone(fut.result(timeout=1))

    def test_records_exception(self):
        fut = ConcurrentFuture()

        def boom():
            raise ValueError("close failed")

        transport._settle_future(fut, boom)
        with self.assertRaises(ValueError):
            fut.result(timeout=1)

    def test_wraps_non_exception_base_exception(self):
        fut = ConcurrentFuture()

        def boom():
            raise SystemExit("bail")

        transport._settle_future(fut, boom)
        with self.assertRaises(transport.TransportJobBaseError) as caught:
            fut.result(timeout=1)
        self.assertIsInstance(caught.exception.original, SystemExit)


class TestAwaitConcurrentFutureTimeout(unittest.IsolatedAsyncioTestCase):
    """A bounded await must time out without cancelling the worker's job."""

    async def test_timeout_raises_without_cancelling_job(self):
        executor = transport._DaemonTransportExecutor(name="await-timeout-test")
        self.addCleanup(executor.shutdown, wait=True, timeout=5)
        started = threading.Event()
        release = threading.Event()

        def blocked():
            started.set()
            release.wait(timeout=5)

        fut = executor.submit(blocked)
        self.assertTrue(await asyncio.to_thread(started.wait, 1))
        start = time.monotonic()
        with self.assertRaises(TimeoutError):
            await transport.await_concurrent_future(fut, 0.05)
        self.assertLess(time.monotonic() - start, 1.0)
        # Polling (not shielding): the caller's timeout never cancels the job.
        self.assertFalse(fut.cancelled())
        self.assertFalse(fut.done())
        release.set()
        # The worker survives to run a subsequent job.
        result = await asyncio.wait_for(asyncio.wrap_future(executor.submit(lambda: 42)), 1)
        self.assertEqual(result, 42)


class TestTransportExecutorBaseException(unittest.TestCase):
    """A job raising a non-Exception BaseException must surface as a catchable
    Exception (not crash the awaiting caller or the worker)."""

    def test_base_exception_is_wrapped_for_callers(self):
        executor = transport._DaemonTransportExecutor(name="base-exc-test")
        self.addCleanup(executor.shutdown, wait=True, timeout=5)

        def boom():
            raise KeyboardInterrupt()

        future = executor.submit(boom)
        with self.assertRaises(transport.TransportJobBaseError) as caught:
            future.result(timeout=5)
        self.assertIsInstance(caught.exception.original, KeyboardInterrupt)
        # The worker survives to run a subsequent job.
        self.assertEqual(executor.submit(lambda: 7).result(timeout=5), 7)


class TestBoundedJobQueue(unittest.TestCase):
    """A wedged worker must reject new jobs once the queue bound is reached."""

    def test_submit_rejects_when_queue_is_full(self):
        executor = transport._DaemonTransportExecutor(name="queue-full-test")
        self.addCleanup(executor.shutdown, wait=True, timeout=5)
        started = threading.Event()
        release = threading.Event()

        def wedged():
            started.set()
            release.wait(timeout=5)

        executor.submit(wedged)
        self.assertTrue(started.wait(1))
        accepted = 0
        with self.assertRaises(transport.TransportBusyError):
            # The wedged job was dequeued to run, so the queue starts empty;
            # filling it past the bound must reject, never buffer unboundedly.
            for _ in range(transport.TRANSPORT_JOB_QUEUE_MAXSIZE + 1):
                executor.submit(lambda: None)
                accepted += 1
        self.assertEqual(accepted, transport.TRANSPORT_JOB_QUEUE_MAXSIZE)
        release.set()


class TestDiscoverSerialPorts(unittest.TestCase):
    """Fallback branches of serial discovery (findPorts / comports / glob)."""

    def _fake_serial(self, comports: MagicMock):
        return SimpleNamespace(tools=SimpleNamespace(list_ports=SimpleNamespace(comports=comports)))

    def test_findports_failure_falls_back_to_comports(self):
        # The comports fallback now filters by Meshtastic USB VID (mirroring
        # findPorts's whitelist), so the fake port must carry a known VID.
        fake_serial = self._fake_serial(
            MagicMock(return_value=[SimpleNamespace(device="/dev/ttyUSB7", vid=0x239A)])
        )
        with (
            patch("transport.HAS_MESHTASTIC", True),
            patch("meshtastic.util.findPorts", side_effect=OSError("usb wedged")),
            patch("transport.serial", fake_serial),
            patch("glob.glob", return_value=[]),
        ):
            ports = transport.discover_serial_ports()
        self.assertEqual(ports, ["/dev/ttyUSB7"])

    def test_comports_fallback_filters_non_meshtastic_vids(self):
        # A non-Meshtastic serial device (GPS dongle, Arduino) must NOT be
        # handed to the protocol: only whitelisted VIDs pass the filter, and a
        # non-matching VID falls through to the glob last resort.
        non_meshtastic = SimpleNamespace(device="/dev/ttyGPS1", vid=0x1A86)
        meshtastic = SimpleNamespace(device="/dev/ttyMESH", vid=0x239A)
        fake_serial = self._fake_serial(MagicMock(return_value=[non_meshtastic, meshtastic]))
        with (
            patch("transport.HAS_MESHTASTIC", True),
            patch("meshtastic.util.findPorts", side_effect=OSError("usb wedged")),
            patch("transport.serial", fake_serial),
            patch("glob.glob", return_value=[]),
        ):
            ports = transport.discover_serial_ports()
        self.assertEqual(ports, ["/dev/ttyMESH"])

    def test_comports_fallback_skips_ports_without_vid(self):
        # A port with no resolvable vid is skipped (findPorts also requires
        # ``port.vid is not None``); the empty result falls through to glob.
        no_vid = SimpleNamespace(device="/dev/ttyS0")
        # Default object lacks a vid attribute entirely.
        fake_serial = self._fake_serial(MagicMock(return_value=[no_vid]))
        with (
            patch("transport.HAS_MESHTASTIC", True),
            patch("meshtastic.util.findPorts", side_effect=OSError("usb wedged")),
            patch("transport.serial", fake_serial),
            patch("glob.glob", return_value=[]),
        ):
            ports = transport.discover_serial_ports()
        self.assertEqual(ports, [])

    def test_comports_failure_falls_back_to_glob(self):
        fake_serial = self._fake_serial(MagicMock(side_effect=OSError("no usb stack")))
        with (
            patch("transport.HAS_MESHTASTIC", True),
            patch("meshtastic.util.findPorts", return_value=[]),
            patch("transport.serial", fake_serial),
            patch(
                "glob.glob",
                side_effect=lambda pat: (
                    ["/dev/cu.usbserial-0001"] if pat == "/dev/cu.usbserial*" else []
                ),
            ),
        ):
            ports = transport.discover_serial_ports()
        self.assertEqual(ports, ["/dev/cu.usbserial-0001"])


class TestTransportShutdownError(unittest.TestCase):
    """Submitting to a shut-down executor raises the typed error (not a bare
    stdlib RuntimeError string), so callers can match on type."""

    def test_submit_after_shutdown_raises_typed_error(self):
        executor = transport._DaemonTransportExecutor(name="shutdown-error-test")
        executor.shutdown(wait=True)
        with self.assertRaises(transport.TransportShutdownError):
            executor.submit(lambda: None)

    def test_typed_error_is_a_runtime_error_subclass(self):
        # Backward compat: existing `except RuntimeError` handlers keep working.
        self.assertTrue(issubclass(transport.TransportShutdownError, RuntimeError))


if __name__ == "__main__":
    unittest.main()
