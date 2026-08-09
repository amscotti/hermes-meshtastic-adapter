"""Blocking-transport concerns for the Meshtastic adapter.

Owns the single-worker daemon executor that serializes blocking Meshtastic
I/O (sendText / close / open constructors) off the event-loop thread, plus
target resolution and interface construction for serial/TCP transports.
"""

import asyncio
import logging
import os
import queue
import subprocess
import sys
import threading
import time
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

# meshtastic / pypubsub may be absent at import time (a Hermes self-update can
# rebuild the runtime and drop them), so the import is re-runnable and read at
# CALL time — never snapshot HAS_MESHTASTIC / pub into an importing module.
HAS_MESHTASTIC = False
pub = None
# The meshtastic package, bound at module level so open_interface can reference
# it even though the import only happens inside the re-runnable importer.
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

# USB Vendor IDs for known Meshtastic hardware. Mirrors the primary (whitelist)
# pass of ``meshtastic.util.findPorts`` so the pyserial/glob fallback does not
# hand non-Meshtastic serial devices (GPS dongles, Arduino console UARTs) to
# the protocol. Hex for readability; matched against ``ListPortInfo.vid``.
MESHTASTIC_USB_VIDS = frozenset(
    {
        0x239A,  # Adafruit (Feather nRF52 / ESP32 — popular Meshtastic boards).
        0x303A,  # Espressif (native-USB ESP32-S2/S3 variants).
    }
)

# pip install runs at most once per process (only when the library is missing).
_autoinstall_lock = threading.Lock()
_autoinstall_attempted = False


def _requirements_path() -> Path:
    """Path to the plugin's requirements.txt (next to this module)."""
    return Path(__file__).resolve().parent / "requirements.txt"


def _env_bool(name: str, default: bool) -> bool:
    """Env boolean: only explicit true/false words are authoritative; a blank
    ``VAR=`` takes ``default`` (so ``MESHTASTIC_MOCK=`` cannot arm the mock)."""
    value = (os.getenv(name) or "").strip().lower()
    if value in ("1", "true", "yes", "on"):
        return True
    if value in ("0", "false", "no", "off"):
        return False
    return default


def _mock_opt_in() -> bool:
    """MESHTASTIC_MOCK=1 explicitly opts into a dry-run mock interface."""
    return _env_bool("MESHTASTIC_MOCK", False)


def _autoinstall_enabled() -> bool:
    """MESHTASTIC_AUTOINSTALL defaults to on; '0'/'false' disables it."""
    return _env_bool("MESHTASTIC_AUTOINSTALL", True)


def ensure_meshtastic_library() -> None:
    """One pip install of requirements.txt when the library is missing.

    Runs once per process on the transport worker, then re-imports.
    Disable with MESHTASTIC_AUTOINSTALL=0.
    """
    global _autoinstall_attempted
    if HAS_MESHTASTIC:
        return
    with _autoinstall_lock:
        # Once-per-process gate under this lock — callers can never double-run.
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
            cmd = [sys.executable, "-m", "pip", "install", "--disable-pip-version-check"]
            cmd += ["--quiet", "-r", str(req)]
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
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


class TransportShutdownError(RuntimeError):
    """A job was submitted to a shut-down transport executor.

    Subclass of ``RuntimeError`` for existing ``except RuntimeError`` handlers;
    ``send_path.is_executor_shutdown_error`` matches on this type.
    """

    def __init__(self, message: str = "cannot schedule new futures after shutdown") -> None:
        super().__init__(message)


class TransportBusyError(RuntimeError):
    """The transport job queue is full (worker likely wedged); ``submit`` rejects
    instead of buffering without limit. Distinct from ``TransportShutdownError``
    so callers treat it as transient, not teardown."""

    def __init__(self, message: str = "transport worker job queue is full") -> None:
        super().__init__(message)


class TransportJobBaseError(RuntimeError):
    """Wraps a non-``Exception`` ``BaseException`` from a job so callers'
    ``except Exception`` boundaries catch it; ``.original`` holds the cause."""

    def __init__(self, original: BaseException) -> None:
        self.original = original
        super().__init__(f"transport job raised {type(original).__name__}: {original}")


def _normalize_job_exception(exc: BaseException) -> Exception:
    """Wrap a non-``Exception`` ``BaseException`` so callers can catch it."""
    if isinstance(exc, Exception):
        return exc
    return TransportJobBaseError(exc)


# Bounded so a wedged blocking call cannot grow the job queue without limit.
TRANSPORT_JOB_QUEUE_MAXSIZE = 256


class _DaemonTransportExecutor:
    """Single-worker daemon thread for blocking Meshtastic I/O.

    Daemon so a stuck open/close cannot pin process exit.
    """

    def __init__(self, name: str = "meshtastic-transport") -> None:
        self._jobs: queue.Queue[
            tuple[Callable[..., Any], tuple[Any, ...], dict[str, Any], ConcurrentFuture] | None
        ] = queue.Queue(maxsize=TRANSPORT_JOB_QUEUE_MAXSIZE)
        self._closed = False
        self._stop_after_drain = False
        self._state_lock = threading.Lock()
        self._thread = threading.Thread(target=self._run, name=name, daemon=True)
        self._thread.start()

    def submit(self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> ConcurrentFuture:
        fut: ConcurrentFuture = ConcurrentFuture()
        # Check + enqueue atomically with shutdown's sentinel, so every accepted
        # job is before the sentinel and cannot be stranded behind it.
        with self._state_lock:
            if self._closed:
                raise TransportShutdownError()
            try:
                self._jobs.put_nowait((fn, args, kwargs, fut))
            except queue.Full:
                # Never block the caller on a wedged worker — reject for retry.
                raise TransportBusyError() from None
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
                    fut.set_exception(_normalize_job_exception(exc))
                except ConcurrentInvalidStateError:
                    pass
            else:
                try:
                    fut.set_result(result)
                except ConcurrentInvalidStateError:
                    pass
            with self._state_lock:
                # Shutdown raced a full queue; exit once accepted work drains.
                if self._stop_after_drain and self._jobs.empty():
                    return

    def shutdown(self, wait: bool = True, timeout: float | None = None) -> None:
        """Stop accepting work. Optionally join the worker for up to ``timeout``."""
        with self._state_lock:
            if not self._closed:
                self._closed = True
                try:
                    self._jobs.put_nowait(None)
                except queue.Full:
                    # Wedged with a full queue: flag drain-then-exit in _run.
                    self._stop_after_drain = True
        if wait:
            self._thread.join(timeout)

    def is_alive(self) -> bool:
        return self._thread.is_alive()


# --- serialized interface close machinery (moved from adapter.py) ---

# Logs under the "adapter" logger (like ack_state.py) so the lifecycle tests'
# assertLogs("adapter", ...) assertions keep matching after the move.
_close_logger = logging.getLogger("adapter")


def close_interfaces_serialized(interfaces: list[Any]) -> None:
    """Close interfaces on a worker thread, serialized with sendText."""
    for iface in interfaces:
        try:
            iface.close()
        except Exception as exc:
            _close_logger.error("Error closing Meshtastic interface: %s", exc)


async def await_concurrent_future(future: ConcurrentFuture, timeout: float | None = None) -> Any:
    """Await without propagating asyncio cancellation into queued worker jobs.

    Polling (rather than shielding) keeps a caller ``CancelledError`` away from
    the daemon worker job; the 10ms cadence is deliberate.
    """
    deadline = None if timeout is None else time.monotonic() + timeout
    while not future.done():
        if deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError
        await asyncio.sleep(0.01)
    return future.result()


def _settle_future(fut: ConcurrentFuture, fn: Callable[[], Any]) -> None:
    """Run ``fn`` and capture its outcome into ``fut`` (thread-safe set)."""
    try:
        fn()
    except BaseException as exc:
        try:
            fut.set_exception(_normalize_job_exception(exc))
        except ConcurrentInvalidStateError:
            pass
    else:
        try:
            fut.set_result(None)
        except ConcurrentInvalidStateError:
            pass


async def close_interfaces_on_daemon_thread(interfaces: list[Any], timeout: float) -> None:
    """Close via a short-lived daemon thread — never on the event-loop thread."""
    close_fut: ConcurrentFuture = ConcurrentFuture()
    threading.Thread(
        target=lambda: _settle_future(close_fut, lambda: close_interfaces_serialized(interfaces)),
        name="meshtastic-close",
        daemon=True,
    ).start()
    await await_concurrent_future(close_fut, timeout)


async def close_interfaces_after_executor(
    executor: _DaemonTransportExecutor,
    interfaces: list[Any],
    timeout: float,
) -> None:
    """Close only after a shutting-down worker drains accepted transport work."""
    close_fut: ConcurrentFuture = ConcurrentFuture()

    def _drain_then_close() -> None:
        # Unbounded join is deliberate: a bounded one could let close run
        # concurrently with an in-flight sendText. The caller's await is
        # time-bounded and this thread is a daemon, so a stuck worker still
        # cannot pin process exit.
        executor.shutdown(wait=True)
        close_interfaces_serialized(interfaces)

    threading.Thread(
        target=lambda: _settle_future(close_fut, _drain_then_close),
        name="meshtastic-close-after-worker",
        daemon=True,
    ).start()
    await await_concurrent_future(close_fut, timeout)


async def close_interfaces_via_executor(
    executor: _DaemonTransportExecutor,
    interfaces: list[Any],
    timeout: float,
) -> None:
    """Close on the transport worker; drain-then-close if it is mid-shutdown.

    Three submit outcomes:

    * success — await the worker close (serialized against ``sendText``).
    * ``TransportShutdownError`` — the executor is tearing down: drain accepted
      work first, then close (the only path that permanently marks the worker
      closed).
    * ``TransportBusyError`` — the 256-deep job queue is momentarily full
      (transient backpressure, NOT teardown). Close on a daemon thread without
      touching executor state, so a wedged-but-recoverable worker is not
      permanently bricked. The send path treats the same condition as
      re-queueable (``adapter._send_chunk``); this keeps close consistent.
    """
    try:
        close_fut = executor.submit(close_interfaces_serialized, interfaces)
    except TransportShutdownError:
        # Executor shut down mid-read: drain accepted work before closing.
        await close_interfaces_after_executor(executor, interfaces, timeout)
        return
    except TransportBusyError:
        # Transient full queue: close without disabling the worker for life.
        await close_interfaces_on_daemon_thread(interfaces, timeout)
        return
    await await_concurrent_future(close_fut, timeout)


async def close_interfaces(
    interfaces: list[Any],
    executor: _DaemonTransportExecutor | None,
    timeout: float,
) -> None:
    """Close interfaces off the event-loop thread, time-bounded.

    Dispatch: via the lifecycle transport worker when one exists (serialized
    against sendText), else a short-lived daemon thread. A TimeoutError only
    abandons the *await*; the daemon close still runs.
    """
    if not interfaces:
        return
    try:
        if executor is not None:
            await close_interfaces_via_executor(executor, interfaces, timeout)
        else:
            await close_interfaces_on_daemon_thread(interfaces, timeout)
    except TimeoutError:
        _close_logger.warning(
            "Meshtastic interface close still running after %.1fs; disconnect continues "
            "(daemon transport worker will finish in the background)",
            timeout,
        )


async def shutdown_transport_executor(executor: _DaemonTransportExecutor, timeout: float) -> None:
    """Shut down the daemon transport worker without hanging forever."""
    executor.shutdown(wait=False)
    deadline = time.monotonic() + timeout
    # Poll without blocking the platform loop; the daemon worker cannot pin exit.
    while executor.is_alive() and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
    if executor.is_alive():
        _close_logger.warning(
            "Meshtastic transport executor still busy after %.1fs during disconnect; "
            "continuing (daemon worker will finish in the background)",
            timeout,
        )


def drop_interface_if_dead_serialized(
    target: str,
    iface: Any,
    *,
    interfaces: dict[str, Any],
    iface_lock: threading.Lock,
    is_alive: Callable[[Any], bool],
) -> bool | None:
    """Atomically probe and close a dead interface.

    Returns None if the target changed, True if alive, False after removal and
    close. Runs on the daemon worker, serialized against sendText.
    """
    with iface_lock:
        if interfaces.get(target) is not iface:
            return None
    if is_alive(iface):
        return True
    with iface_lock:
        if interfaces.get(target) is not iface:
            return None
        interfaces.pop(target, None)
    try:
        iface.close()
    except Exception as exc:
        _close_logger.error("Error closing dropped Meshtastic interface: %s", exc)
    return False


# --- liveness polling (moved from adapter.py) ---


def interface_is_alive(iface: Any) -> bool:
    """Best-effort liveness probe for a connected interface.

    ``MeshInterface.isConnected`` is a ``threading.Event`` *attribute* (not a
    method), so it is checked LAST and via ``is_set()``: checking it first
    would shadow the TCP/serial branches, and calling it would raise.
    """
    # TCP: the library self-heals dead sockets by swapping in a fresh one
    # (brief socket=None window), so trust the authoritative isConnected Event.
    is_connected = getattr(iface, "isConnected", None)
    if hasattr(iface, "socket"):
        if is_connected is not None and hasattr(is_connected, "is_set"):
            return bool(is_connected.is_set())
        return iface.socket is not None
    # Serial: pyserial stream exposes is_open / isOpen().
    stream = getattr(iface, "stream", None)
    if stream is not None:
        if hasattr(stream, "isOpen"):
            return bool(stream.isOpen())
        if hasattr(stream, "is_open"):
            return bool(stream.is_open)
        # A stream with neither probe must not read alive forever on a closed
        # link — fall through to the isConnected event below.
    # Fallback: meshtastic's threading.Event liveness flag. Reuses the
    # is_connected binding from the top of this function (the attribute
    # hasn't changed); no need to re-read it.
    if hasattr(is_connected, "is_set"):
        return bool(is_connected.is_set())
    # No known liveness handle (e.g. the mock interface) — assume alive.
    return True


def submit_liveness_probe(
    executor: _DaemonTransportExecutor,
    target: str,
    iface: Any,
    *,
    interfaces: dict[str, Any],
    iface_lock: threading.Lock,
) -> ConcurrentFuture:
    """Submit one executor-mediated liveness probe for ``target`` (serialized against sendText)."""
    return executor.submit(
        drop_interface_if_dead_serialized,
        target,
        iface,
        interfaces=interfaces,
        iface_lock=iface_lock,
        is_alive=interface_is_alive,
    )


def connection_targets(tcp_host: str, tcp_port: int, serial_port: str) -> list[str]:
    """Resolve the connection target keys to open.

    A configured TCP host takes precedence over serial; ``auto`` serial
    discovers ports or falls back to ``mock_port``.
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
    """Parse a ``tcp://host:port`` target key into ``(host, port)``, handling
    bracketed IPv6 literals; malformed ports/unclosed brackets yield the host.

    The ``tcp://`` prefix is a hard contract: the only caller
    (``open_interface``) guards with ``target.startswith("tcp://")`` and
    ``connection_targets`` always emits it. Strip it explicitly via
    ``removeprefix`` so a future caller that bypasses the guard cannot silently
    truncate the first 6 bytes of an unprefixed host (a wrong host/port handed
    to ``TCPInterface``). When the prefix is absent, treat the whole string as
    a bare host with the default port — loud-but-plausible rather than a
    silent truncation.
    """
    rest = target.removeprefix("tcp://")

    if rest.startswith("["):
        # Bracketed IPv6 literal: "[host]" or "[host]:port".
        host, sep, after = rest[1:].partition("]")
        if sep and after.startswith(":") and after[1:]:
            try:
                return host, int(after[1:])
            except ValueError:
                return host, DEFAULT_TCP_PORT
        # Unclosed bracket or bare "[host]" — de-bracketed host, default port.
        return (host if sep else rest.lstrip("[")), DEFAULT_TCP_PORT

    host, sep, port_str = rest.rpartition(":")
    if not sep:
        return rest, DEFAULT_TCP_PORT
    try:
        return host, int(port_str)
    except ValueError:
        return host, DEFAULT_TCP_PORT


def open_interface(target: str) -> Any:
    """Open the serial/TCP interface for a connection target (blocking).

    Fails LOUD instead of silently masquerading as production: a real target
    with the library missing raises install instructions (after one automatic
    pip attempt) unless ``MESHTASTIC_MOCK=1`` opts into a dry-run mock. Only
    ``mock_port`` and the explicit opt-in produce a mock.
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

    Prefer ``meshtastic.util.findPorts`` (VID whitelist); fall back to pyserial
    (filtered by the known Meshtastic USB VIDs so a non-Meshtastic device —
    GPS dongle, Arduino, console UART — is not sent protocol bytes) and finally
    a ``/dev`` glob when the library is unavailable.
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
            # Mirror findPorts's primary intent: only likely-Meshtastic VIDs.
            # A port without a resolvable vid is skipped (findPorts also
            # requires ``port.vid is not None``), so an empty result falls
            # through to the glob last resort rather than opening a wrong device.
            ports = [
                p.device
                for p in serial.tools.list_ports.comports()
                if p.vid is not None and p.vid in MESHTASTIC_USB_VIDS
            ]
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
