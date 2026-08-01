"""Blocking-transport concerns for the Meshtastic adapter.

Owns the single-worker daemon executor that serializes blocking Meshtastic
I/O (sendText / close / open constructors) off the event-loop thread, plus
target resolution and interface construction for serial/TCP transports.
"""

import logging
import queue
import threading
from collections.abc import Callable
from concurrent.futures import Future as ConcurrentFuture
from concurrent.futures import InvalidStateError as ConcurrentInvalidStateError
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

try:
    import meshtastic
    import meshtastic.serial_interface
    import meshtastic.tcp_interface
    from pubsub import pub

    HAS_MESHTASTIC = True
except ImportError:  # pragma: no cover - optional dependency in tests
    HAS_MESHTASTIC = False
    pub = None

# Default Meshtastic TCP API port exposed by WiFi/Ethernet-capable nodes.
DEFAULT_TCP_PORT = 4403


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
    executor. Falls back to the mock interface when the Meshtastic libraries
    are unavailable so the plugin still loads.
    """
    if target == "mock_port" or not HAS_MESHTASTIC:
        if target.startswith("tcp://") and not HAS_MESHTASTIC:
            logger.warning(
                "Meshtastic library not installed — falling back to the mock interface "
                "for TCP target %s. Install requirements.txt to reach the real node.",
                target,
            )
        return mock_interface.MockSerialInterface(devPath=target)
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
