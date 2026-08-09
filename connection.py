"""Pure decision helpers for the adapter's connection lifecycle.

Extracted from ``MeshtasticAdapter._reconnect_loop`` /
``MeshtasticAdapter._disconnect_impl`` so the lifecycle decisions are
unit-testable without an adapter instance: reconnect backoff, pause-state
classification, per-tick reconnect steps, close-wait policies, link-drop
classification, and teardown step planning.

All functions are synchronous and pure: no I/O, no asyncio, no locks, no
adapter imports (anything adapter-specific is parameter-passed). Env reads
follow the ``ack_state`` convention: read at call time, defensive defaults.
"""

from __future__ import annotations

import math
import os
import time
from collections.abc import Mapping, Sequence
from typing import Any

# Backoff schedule for failed connection attempts: start at 1s, double on
# each failure, cap at 60s, reset to 1s on a successful open.
INITIAL_BACKOFF = 1.0
MAX_BACKOFF = 60.0

# Poll cadences of the reconnect loop (pause park / liveness poll).
PAUSE_POLL_SECS = 1.0
LIVENESS_POLL_SECS = 2.0

# Reconnect-step outcomes (see reconnect_step).
RELEASE = "release"
WAIT = "wait"
CONNECT = "connect"
POLL = "poll"

# Liveness-probe outcomes (see poll_outcome).
HEALTHY = "healthy"
DROP = "drop"
EXIT = "exit"

# Pause-state classification keys (see pause_classify).
NOT_PAUSED = "not_paused"
UNTIMED_PAUSE = "untimed"
TIMED_PAUSE = "timed"
PAUSE_EXPIRED = "expired"

# Close-wait policy defaults; ``0`` means do not wait (MESHTASTIC_OPEN_CANCEL_TIMEOUT
# / MESHTASTIC_EXECUTOR_SHUTDOWN_TIMEOUT overrides).
DEFAULT_OPEN_CANCEL_TIMEOUT = 5.0
DEFAULT_EXECUTOR_SHUTDOWN_TIMEOUT = 5.0

# How long the success-path interface open may take before it is treated as a
# connect failure (MESHTASTIC_OPEN_TIMEOUT overrides). The constructor runs on
# the daemon transport worker, so expiry only frees the reconnect-loop await;
# ``0`` disables the bound (wait indefinitely).
DEFAULT_OPEN_TIMEOUT = 20.0

# A link restored within this many seconds was a socket reset (the node stayed
# up); a longer outage means the node itself was away — a reboot, a WiFi drop,
# or a power cycle.
SOCKET_RESET_MAX_OUTAGE_SECS = 6.0

# Link-drop verdict keys (see classify_link_drop).
SOCKET_RESET = "socket_reset"
NODE_ABSENT = "node_absent"


def next_backoff(backoff: float, cap: float = MAX_BACKOFF) -> float:
    """Backoff after a failed attempt: double the previous delay, capped at ``cap``.

    Non-finite or non-positive inputs coerce to the initial backoff: ``min``
    keeps its first argument for NaN (so ``min(nan, cap)`` is ``nan``, which
    crashes ``asyncio.sleep``), and a zero backoff would make the reconnect
    loop's ``asyncio.sleep(0)`` spin hot. Neither is reachable from current
    callers, but these are public pure helpers with a defensive contract.
    """
    if not math.isfinite(backoff) or backoff <= 0:
        return INITIAL_BACKOFF
    return min(backoff * 2, cap)


def reset_backoff() -> float:
    """Backoff after a successful open: back to the initial 1s."""
    return INITIAL_BACKOFF


def reconnect_step(paused: bool, has_interface: bool) -> str:
    """What the reconnect loop should do this tick, from pause/interface state.

    ``release`` — paused with a live interface: drop the node's socket so
    another client can take its TCP slot. ``wait`` — paused without an
    interface: stay parked. ``connect`` — unpaused without an interface:
    attempt the open. ``poll`` — unpaused with an interface: the link is up;
    poll liveness until it drops.
    """
    if paused:
        return RELEASE if has_interface else WAIT
    return POLL if has_interface else CONNECT


def poll_outcome(alive: bool | None) -> str:
    """Classify a liveness probe: ``healthy``, ``drop``, or ``exit``.

    ``None`` means the probed interface is no longer the registered one
    (lifecycle turnover or a replacement open), so the poller exits rather
    than treating it as a link drop.
    """
    if alive is None:
        return EXIT
    return HEALTHY if alive else DROP


def pause_classify(paused: bool, pause_until: float | None, now: float) -> str:
    """Classify the pause state: unpaused, untimed, timed, or expired.

    A timed pause whose deadline has passed (``now >= pause_until``) is
    ``expired`` — the reconnect loop auto-resumes it so "off for a bit" cannot
    become "down all night".
    """
    if not paused:
        return NOT_PAUSED
    if pause_until is None:
        return UNTIMED_PAUSE
    # A non-finite deadline or clock is a broken pause (a NaN minutes input or a
    # NaN now would never satisfy now >= pause_until, wedging the link in "timed"
    # forever). Treat it as expired so the reconnect loop auto-resumes rather
    # than parking. ``now`` is always ``time.time()`` today, but the guard keeps
    # the helper total if it is ever reused with a monotonic/skew-derived clock.
    if not math.isfinite(pause_until) or not math.isfinite(now):
        return PAUSE_EXPIRED
    return PAUSE_EXPIRED if now >= pause_until else TIMED_PAUSE


def resumes_at_str(pause_until: float | None) -> str | None:
    """Local timestamp string for a pause deadline (``None`` when untimed)."""
    if pause_until is None or not math.isfinite(pause_until):
        return None
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(pause_until))
    except (OverflowError, OSError, ValueError):
        # A huge-but-finite deadline can exceed the platform's time_t range
        # (time.localtime raises once the timestamp is out of range). Report
        # None instead of raising for every pause_state caller.
        return None


def resumes_in_minutes(pause_until: float | None, now: float) -> float | None:
    """Minutes until a timed pause ends, clamped at 0 (``None`` when untimed).

    A non-finite ``now`` cannot be compared meaningfully (``pause_until - nan``
    is NaN); report ``0.0`` — consistent with ``pause_classify`` treating a
    broken clock as expired — rather than propagating a misleading NaN.
    """
    if pause_until is None or not math.isfinite(pause_until):
        return None
    if not math.isfinite(now):
        return 0.0
    return max(0.0, round((pause_until - now) / 60, 1))


def open_cancel_timeout() -> float:
    """Seconds to wait for a cancelled open before abandoning the await.

    ``0`` means do not wait (abandon immediately); the constructor still runs
    on the daemon transport worker and closes a stale result via its
    lifecycle_id. Override with MESHTASTIC_OPEN_CANCEL_TIMEOUT.
    """
    return _timeout_env("MESHTASTIC_OPEN_CANCEL_TIMEOUT", DEFAULT_OPEN_CANCEL_TIMEOUT)


def executor_shutdown_timeout() -> float:
    """Seconds to wait for transport-worker drain / close during disconnect.

    ``0`` means do not wait. Override with MESHTASTIC_EXECUTOR_SHUTDOWN_TIMEOUT.
    """
    return _timeout_env("MESHTASTIC_EXECUTOR_SHUTDOWN_TIMEOUT", DEFAULT_EXECUTOR_SHUTDOWN_TIMEOUT)


def open_timeout() -> float:
    """Seconds to bound the success-path interface open before treating it as a
    connect failure (the constructor still runs on the daemon worker).

    ``0`` disables the bound (wait indefinitely). Operational cost of ``0``: a
    wedged serial/TCP constructor parks that target's ``_reconnect_loop`` inside
    the open await, so pause-expiry detection (and a ``mesh_pause`` issued
    during the hung open) is not honoured until the open resolves or disconnect
    cancels the task. The default is safe; only set ``MESHTASTIC_OPEN_TIMEOUT=0``
    with that tradeoff in mind.
    """
    return _timeout_env("MESHTASTIC_OPEN_TIMEOUT", DEFAULT_OPEN_TIMEOUT)


def _timeout_env(name: str, default: float) -> float:
    """Read a non-negative finite float timeout env var, falling back defensively."""
    raw = os.getenv(name) or str(default)
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    # A NaN/Inf "timeout" would defeat the bound it exists to enforce:
    # ``max(0.0, nan)`` returns 0.0 (the comparison is False), silently
    # disabling the open bound or abandoning cancel waits. Treat non-finite
    # input like garbage and fall back to the default.
    return max(0.0, value) if math.isfinite(value) else default


def classify_link_drop(outage_secs: float, threshold: float = SOCKET_RESET_MAX_OUTAGE_SECS) -> str:
    """Classify a link outage: ``socket_reset`` (node stayed up) vs ``node_absent``.

    A genuine reset that takes longer than ``threshold`` to re-establish
    (backoff, DHCP) is logged as an absence. Heuristic by design — the counts
    are approximate, not a contract.
    """
    return SOCKET_RESET if outage_secs <= threshold else NODE_ABSENT


def teardown_owner_current(
    disconnecting: bool,
    disconnect_future: Any,
    disconnect_task: Any,
    completion: Any,
    current_task: Any,
) -> bool:
    """Whether ``current_task`` still owns the teardown for ``completion``.

    The disconnect ownership gate: every teardown stage mutates shared state
    only while the disconnect epoch (``disconnect_future is completion``) and
    the owner task are still this task's, and ``disconnecting`` is still set.
    """
    return disconnecting and disconnect_future is completion and disconnect_task is current_task


def teardown_task_list(
    reconnect_tasks: Mapping[str, Any],
    queue_drain_task: Any | None,
    incoming_consumer_task: Any | None,
) -> list[Any]:
    """Lifecycle tasks teardown must cancel, in cancel order."""
    tasks = list(reconnect_tasks.values())
    if queue_drain_task:
        tasks.append(queue_drain_task)
    if incoming_consumer_task:
        tasks.append(incoming_consumer_task)
    return tasks


def tasks_on_loop(tasks: Sequence[Any], loop: Any) -> list[Any]:
    """Subset of ``tasks`` owned by ``loop`` — the only ones that loop can await."""
    return [task for task in tasks if task.get_loop() is loop]
