"""Blocking-transport concerns for the Meshtastic adapter.

Owns the single-worker daemon executor that serializes blocking Meshtastic
I/O (sendText / close / open constructors) off the event-loop thread, plus
target resolution and interface construction for serial/TCP transports.
"""

import logging
import os
import queue
import subprocess
import sys
import threading
from collections.abc import Callable
from concurrent.futures import Future as ConcurrentFuture
from concurrent.futures import InvalidStateError as ConcurrentInvalidStateError
from pathlib import Path
from typing import Any

try:
    from . import mock_interface
except ImportError:
    import mock_interface

logger = logging.getLogger(__name__)

# --- optional deps ---
try:
    import serial.tools.list_ports
except ImportError:  # pragma: no cover - optional dependency in tests
    serial = None

# meshtastic / pypubsub may be absent at import time (e.g. a Hermes update
# rebuilt the runtime venv and dropped the plugin's dependencies). The import
# is therefore re-runnable: ensure_meshtastic_library() pip-installs the
# plugin's requirements.txt into the running interpreter, then re-imports.
# open_interface() reads HAS_MESHTASTIC / pub at CALL time — never snapshot
# them into an importing module, or a late re-import stays invisible.
HAS_MESHTASTIC = False
pub = None
# The meshtastic package itself, bound at module level so open_interface can
# reference it even though the import only happens inside the re-runnable
# _import_meshtastic_libs().
meshtastic: Any | None = None


def _import_meshtastic_libs() -> bool:
    """(Re-)import the meshtastic + pypubsub libraries. Returns True on success."""
    global HAS_MESHTASTIC, pub, meshtastic
    try:
        import meshtastic
        import meshtastic.serial_interface  # noqa: F401 - registers the submodule
        import meshtastic.tcp_interface  # noqa: F401 - registers the submodule
        from pubsub import pub as pubsub_pub

        HAS_MESHTASTIC = True
        pub = pubsub_pub
        return True
    except ImportError:  # pragma: no cover - optional dependency in tests
        HAS_MESHTASTIC = False
        pub = None
        meshtastic = None
        return False


_import_meshtastic_libs()

# Default Meshtastic TCP API port exposed by WiFi/Ethernet-capable nodes.
DEFAULT_TCP_PORT = 4403

# pip install runs at most once per process (only when the library is missing).
_autoinstall_lock = threading.Lock()
_autoinstall_attempted = False


def _requirements_path() -> Path:
    """Path to the plugin's requirements.txt (next to this module)."""
    return Path(__file__).resolve().parent / "requirements.txt"


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in ("0", "false", "no", "off")


def _mock_opt_in() -> bool:
    """MESHTASTIC_MOCK=1 explicitly opts into a dry-run mock interface."""
    return _env_bool("MESHTASTIC_MOCK", False)


def _autoinstall_enabled() -> bool:
    """MESHTASTIC_AUTOINSTALL defaults to on; '0'/'false' disables it."""
    return _env_bool("MESHTASTIC_AUTOINSTALL", True)


def ensure_meshtastic_library() -> None:
    """Attempt one pip install of requirements.txt when the library is missing.

    The gateway's Hermes venv has no persistent plugin-dependency story: a
    Hermes self-update can rebuild the runtime and drop meshtastic/pypubsub,
    silently wedging the adapter onto the mock interface. This runs exactly
    once per process, on the transport worker (never the event-loop thread),
    then re-imports. Disable with MESHTASTIC_AUTOINSTALL=0.
    """
    global _autoinstall_attempted
    if HAS_MESHTASTIC:
        return
    with _autoinstall_lock:
        # Authoritative once-per-process gate: _autoinstall_attempted is only
        # ever written under this lock (including the disabled path below), so
        # concurrent callers can never double-run pip or double-log.
        if HAS_MESHTASTIC or _autoinstall_attempted:
            return
        _autoinstall_attempted = True
        if not _autoinstall_enabled():
            logger.error(
                "meshtastic library is not installed in %s and MESHTASTIC_AUTOINSTALL=0; "
                "install plugin dependencies manually, e.g.:\n"
                "  %s -m pip install -r %s",
                sys.executable,
                sys.executable,
                _requirements_path(),
            )
            return
        req = _requirements_path()
        logger.warning(
            "meshtastic library missing — installing plugin dependencies from %s "
            "into %s (set MESHTASTIC_AUTOINSTALL=0 to disable)",
            req,
            sys.executable,
        )
        try:
            proc = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "install",
                    "--disable-pip-version-check",
                    "--quiet",
                    "-r",
                    str(req),
                ],
                capture_output=True,
                text=True,
                timeout=300,
            )
        except subprocess.TimeoutExpired:
            logger.error(
                "pip install of Meshtastic dependencies timed out after 300s; "
                "install manually:\n  %s -m pip install -r %s",
                sys.executable,
                req,
            )
            return
        except OSError as exc:
            logger.error("pip install of Meshtastic dependencies failed to run: %s", exc)
            return
        if proc.returncode != 0:
            logger.error(
                "pip install of Meshtastic dependencies failed (exit %s):\n%s\n"
                "Install manually:\n  %s -m pip install -r %s",
                proc.returncode,
                (proc.stderr or proc.stdout or "").strip()[-2000:],
                sys.executable,
                req,
            )
            return
        if _import_meshtastic_libs():
            logger.warning(
                "Installed Meshtastic plugin dependencies into %s; re-import succeeded.",
                sys.executable,
            )
        else:
            logger.error(
                "pip install of Meshtastic dependencies reported success but import "
                "still fails; check %s",
                req,
            )


class _DaemonTransportExecutor:
    """Single-worker daemon thread for blocking Meshtastic I/O.

    Daemon so a stuck open/close cannot pin process exit. Callers await via
    ``asyncio.wrap_future(executor.submit(...))``.
    """

    def __init__(self, name: str = "meshtastic-transport") -> None:
        self._jobs: queue.Queue[
            tuple[Callable[..., Any], tuple[Any, ...], dict[str, Any], ConcurrentFuture] | None
        ] = queue.Queue()
        self._closed = False
        self._state_lock = threading.Lock()
        self._thread = threading.Thread(target=self._run, name=name, daemon=True)
        self._thread.start()

    def submit(self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> ConcurrentFuture:
        fut: ConcurrentFuture = ConcurrentFuture()
        # Check + enqueue atomically with shutdown's sentinel. Every accepted
        # job is therefore before the sentinel and cannot be stranded behind it.
        with self._state_lock:
            if self._closed:
                raise RuntimeError("cannot schedule new futures after shutdown")
            self._jobs.put((fn, args, kwargs, fut))
        return fut

    def _run(self) -> None:
        while True:
            item = self._jobs.get()
            if item is None:
                return
            fn, args, kwargs, fut = item
            if not fut.set_running_or_notify_cancel():
                continue
            try:
                result = fn(*args, **kwargs)
            except BaseException as exc:
                try:
                    fut.set_exception(exc)
                except ConcurrentInvalidStateError:
                    pass
            else:
                try:
                    fut.set_result(result)
                except ConcurrentInvalidStateError:
                    pass

    def shutdown(self, wait: bool = True, timeout: float | None = None) -> None:
        """Stop accepting work. Optionally join the worker for up to ``timeout``."""
        with self._state_lock:
            if not self._closed:
                self._closed = True
                self._jobs.put(None)
        if wait:
            self._thread.join(timeout)

    def is_alive(self) -> bool:
        return self._thread.is_alive()


def connection_targets(tcp_host: str, tcp_port: int, serial_port: str) -> list[str]:
    """Resolve the connection target keys to open.

    A configured TCP host takes precedence over serial: the two transports
    are mutually exclusive. Targets are opaque keys understood by
    ``_reconnect_loop`` and ``open_interface`` — a ``tcp://host:port`` URL
    for TCP, otherwise a serial device path (or ``mock_port`` fallback).
    """
    if tcp_host:
        host = tcp_host
        # Bracket bare IPv6 literals so "host:port" stays unambiguous.
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        return [f"tcp://{host}:{tcp_port}"]

    if serial_port == "auto":
        ports = discover_serial_ports()
        if not ports:
            logger.warning("No serial ports discovered. Using fallback mock interface.")
            return ["mock_port"]
        return ports
    return [serial_port]


def parse_tcp_target(target: str) -> tuple[str, int]:
    """Parse a ``tcp://host:port`` target key into ``(host, port)``.

    Handles bracketed IPv6 literals, e.g. ``tcp://[::1]:4403``.
    """
    rest = target[len("tcp://") :]

    if rest.startswith("["):
        # Bracketed IPv6 literal: "[host]" or "[host]:port".
        host, sep, after = rest[1:].partition("]")
        if not sep:
            return rest, DEFAULT_TCP_PORT
        if after.startswith(":") and after[1:]:
            try:
                return host, int(after[1:])
            except ValueError:
                return host, DEFAULT_TCP_PORT
        return host, DEFAULT_TCP_PORT

    host, sep, port_str = rest.rpartition(":")
    if not sep:
        return rest, DEFAULT_TCP_PORT
    try:
        return host, int(port_str)
    except ValueError:
        return rest, DEFAULT_TCP_PORT


def open_interface(target: str) -> Any:
    """Open the serial/TCP interface for a connection target.

    Runs the blocking Meshtastic constructors; callers offload this to an
    executor.

    Fails LOUD instead of silently masquerading as production: when the
    meshtastic library is unavailable and the target is a real serial/TCP
    device, this raises with install instructions (after one automatic
    ``pip install -r requirements.txt`` attempt — see
    ``ensure_meshtastic_library``) unless ``MESHTASTIC_MOCK=1`` explicitly
    opts into a dry-run mock. Only the explicit ``mock_port`` target (the
    auto-discovery fallback) and the opt-in produce a mock interface.
    """
    if target == "mock_port":
        logger.warning(
            "Using fallback mock interface for %s — DRY RUN ONLY, no real radio traffic. "
            "Connect a radio or configure MESHTASTIC_TCP_HOST.",
            target,
        )
        return mock_interface.MockSerialInterface(devPath=target)
    if not HAS_MESHTASTIC:
        ensure_meshtastic_library()
    if not HAS_MESHTASTIC:
        if _mock_opt_in():
            logger.warning(
                "MESHTASTIC_MOCK=1 with the meshtastic library missing — DRY RUN ONLY, "
                "no real radio traffic for target %s.",
                target,
            )
            return mock_interface.MockSerialInterface(devPath=target)
        raise RuntimeError(
            "meshtastic library is not installed in this Python environment "
            f"({sys.executable}). Install plugin dependencies, e.g.:\n"
            f"  {sys.executable} -m pip install -r {_requirements_path()}\n"
            "(MESHTASTIC_AUTOINSTALL=0 disables the automatic attempt; "
            "MESHTASTIC_MOCK=1 explicitly runs against the mock interface.)"
        )
    # HAS_MESHTASTIC implies the import succeeded; a typed check (not an
    # assert, which vanishes under python -O) keeps pyrefly narrowing and is
    # robust regardless of optimization level.
    if meshtastic is None:
        raise RuntimeError(
            "meshtastic import state is inconsistent (HAS_MESHTASTIC=True but "
            "the module is missing); restart the gateway to re-import."
        )
    if target.startswith("tcp://"):
        host, port = parse_tcp_target(target)
        return meshtastic.tcp_interface.TCPInterface(hostname=host, portNumber=port)
    return meshtastic.serial_interface.SerialInterface(devPath=target)


def discover_serial_ports() -> list[str]:
    """Discover likely Meshtastic serial devices cross-platform.

    Prefer ``meshtastic.util.findPorts`` (VID whitelist for known radios,
    then non-blacklisted ports) so ``auto`` does not open every USB-serial
    gadget on the host. Fall back to pyserial / glob when the library is
    unavailable.
    """
    if HAS_MESHTASTIC:
        try:
            import meshtastic.util as meshtastic_util

            ports = list(meshtastic_util.findPorts(True) or [])
            if ports:
                return ports
        except Exception as e:
            logger.debug("meshtastic.util.findPorts discovery failed: %s", e)
    try:
        if serial is not None:
            ports = [p.device for p in serial.tools.list_ports.comports()]
            if ports:
                return ports
    except Exception as e:
        logger.debug("serial.tools.list_ports discovery failed: %s", e)
    # Fallback for minimal environments where pyserial list_ports is unavailable.
    import glob

    patterns = [
        "/dev/cu.usbserial*",
        "/dev/cu.usbmodem*",
        "/dev/ttyUSB*",
        "/dev/ttyACM*",
    ]
    ports = []
    for pat in patterns:
        ports.extend(glob.glob(pat))
    return ports
