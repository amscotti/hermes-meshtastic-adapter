"""Solicited-request response waiters for the Meshtastic adapter.

Owns the ``_response_waiters`` registry and the ``_response_lock`` that
serializes it, plus the ``solicit`` wait path used by the telemetry /
position / traceroute tools — the only tools that transmit on the shared LoRa
channel. Extracted from :class:`adapter.MeshtasticAdapter` so the registry
lifecycle (register -> resolve / discard / abandon) and the wait/abandon
semantics stay testable in isolation.

The tracker never imports the adapter: everything adapter-specific
(interface fetching, transport-executor access, node-id normalization, the
``MeshLinkLost`` link-drop exception) is constructor-injected as callables,
and the actual request construction (``_post_request`` / ``_telemetry_request``)
stays in the adapter, passed in per call as ``send``.

Waiters are ``concurrent.futures.Future`` — the same loop-independent
storage/resolution model as ACK waiters — so pubsub delivery (background
thread) resolves them and ``_on_connection_lost`` (also a background thread)
can abandon them without touching any event loop.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future as ConcurrentFuture
from concurrent.futures import InvalidStateError as ConcurrentInvalidStateError
from typing import Any

logger = logging.getLogger(__name__)

# Max plausible skew between the gateway's packet timestamp (rxTime) and the
# host wall clock before we stop trusting the comparison. Firmware stamps
# rxTime from the gateway node's clock, which is boot-seconds (uptime) unless
# the node has a synced RTC — so an rxTime that is wildly far from ``time.time()``
# is a different clock domain, not evidence the packet predates the request.
_RX_TIME_SKEW_BOUND_SECS = 3 * 3600


class SolicitedRequestTracker:
    """Registry of in-flight solicited requests plus the shared wait path."""

    def __init__(
        self,
        *,
        normalize_node_id: Callable[[Any], str | None],
        interfaces_provider: Callable[[], list[Any]],
        executor_provider: Callable[[], Any],
        link_lost_exc: type[Exception],
    ) -> None:
        self._normalize_node_id = normalize_node_id
        self._interfaces_provider = interfaces_provider
        self._executor_provider = executor_provider
        self._link_lost_exc = link_lost_exc
        # Waiters for *solicited* replies (telemetry / position / traceroute
        # requested with wantResponse). Keyed (kind, node_id) -> ConcurrentFuture
        # list; _on_receive resolves them when the matching packet arrives.
        self._response_waiters: dict[tuple[str, str], list[ConcurrentFuture]] = {}
        # When each waiter was armed (time.time()). A telemetry/position packet
        # received before its request went out is a periodic broadcast or a stale
        # queued delivery — not a reply — so it must not resolve the waiter.
        self._waiter_sent_at: dict[ConcurrentFuture, float] = {}
        self._response_lock = threading.Lock()

    def register_waiter(self, kind: str, node_id: str) -> ConcurrentFuture:
        """Arm a waiter for a solicited reply of *kind* from *node_id*."""
        future: ConcurrentFuture = ConcurrentFuture()
        with self._response_lock:
            self._response_waiters.setdefault((kind, node_id), []).append(future)
            self._waiter_sent_at[future] = time.time()
        return future

    def resolve_waiters(
        self, kind: str, node_id: str, payload: dict[str, Any], *, rx_time: Any = None
    ) -> None:
        """Hand *payload* to anyone waiting on a *kind* reply from *node_id*.

        ``rx_time`` is the receiving node's timestamp for the packet (absent on
        envelopes that don't carry one). A packet received before the request
        went out is a pre-existing broadcast, not a response — those waiters
        stay registered so the real reply can still resolve them. Without a
        usable ``rx_time`` the packet is accepted (backward compatible).

        The predating split and the re-registration of the kept waiters happen
        under a single lock acquisition: ``abandon_all`` (link drop) can never
        interleave between the pop and the re-add and miss a waiter, which
        would leave it sitting out the full timeout instead of failing fast.
        """
        to_resolve: list[ConcurrentFuture] = []
        with self._response_lock:
            futures = self._response_waiters.pop((kind, node_id), [])
            remaining: list[ConcurrentFuture] = []
            for future in futures:
                sent_at = self._waiter_sent_at.pop(future, None)
                if sent_at is not None and self._predates_request(rx_time, sent_at):
                    self._waiter_sent_at[future] = sent_at
                    # Skip futures abandon_all already failed between arming
                    # and this resolve — a done waiter must not be re-registered.
                    if not future.done():
                        remaining.append(future)
                    continue
                to_resolve.append(future)
            if remaining:
                self._response_waiters.setdefault((kind, node_id), []).extend(remaining)
        for future in to_resolve:
            if not future.done():
                self._set_future_result(future, payload)

    @staticmethod
    def _predates_request(rx_time: Any, sent_at: float) -> bool:
        """Whether a packet timestamped before ``sent_at`` is not a reply.

        A reply can only be received after the request was transmitted, so a
        packet with an earlier ``rxTime`` is a periodic broadcast or a stale
        queued delivery. Missing/unparseable timestamps never reject, and a
        ``0`` (firmware "not set") never rejects either. ``rxTime`` is the
        *gateway's* clock, not the host's: only packets whose timestamp is
        plausibly in the same clock domain as ``sent_at`` (within
        ``_RX_TIME_SKEW_BOUND_SECS``) are compared at all, so an unsynced
        gateway (uptime-sized rxTime) never has its genuine replies discarded.
        """
        if not rx_time:
            return False
        try:
            rx = float(rx_time)
        except (TypeError, ValueError):
            return False
        if abs(rx - sent_at) > _RX_TIME_SKEW_BOUND_SECS:
            return False
        return rx < sent_at

    def maybe_resolve(self, from_id: str, decoded: dict, *, rx_time: Any = None) -> None:
        """Feed a telemetry/position/traceroute packet to any matching waiter."""
        if not isinstance(decoded, dict):
            return
        portnum = decoded.get("portnum")
        dest = self._normalize_node_id(from_id) or from_id
        if portnum in ("TELEMETRY_APP", 67):
            self.resolve_waiters(
                "telemetry", dest, decoded.get("telemetry", decoded), rx_time=rx_time
            )
        elif portnum in ("POSITION_APP", 3):
            self.resolve_waiters(
                "position", dest, decoded.get("position", decoded), rx_time=rx_time
            )
        elif portnum in ("TRACEROUTE_APP", 70):
            route = decoded.get("traceroute") or decoded.get("routeDiscovery") or {}
            self.resolve_waiters("traceroute", dest, route, rx_time=rx_time)

    def discard_waiter(self, kind: str, node_id: str, future: ConcurrentFuture) -> None:
        """Drop a waiter that timed out so the registry can't grow unbounded.

        The detached future is also cancelled so it settles immediately instead
        of lingering PENDING until GC; a late ``set_result``/``set_exception``
        is then swallowed by the existing ``InvalidStateError`` guards.
        """
        removed = False
        with self._response_lock:
            pending = self._response_waiters.get((kind, node_id))
            if not pending:
                return
            if future in pending:
                pending.remove(future)
                self._waiter_sent_at.pop(future, None)
                removed = True
            if not pending:
                self._response_waiters.pop((kind, node_id), None)
        if removed:
            future.cancel()

    def abandon_all(self, reason: str) -> None:
        """Fail every in-flight solicited request when the link goes down."""
        with self._response_lock:
            pending = [f for futures in self._response_waiters.values() for f in futures]
            self._response_waiters.clear()
            self._waiter_sent_at.clear()
        if not pending:
            return
        logger.info("Abandoning %d in-flight Meshtastic request(s): %s", len(pending), reason)
        for future in pending:
            if future.done():
                continue
            try:
                future.set_exception(self._link_lost_exc(reason))
            except ConcurrentInvalidStateError:
                pass

    async def solicit(
        self,
        kind: str,
        node_id: str,
        send: Callable[[Any], Any],
        timeout: float,
    ) -> dict[str, Any]:
        """Send a request to one node and await its reply.

        Returns ``{"ok": True, "data": ...}`` or ``{"ok": False, "error": ...}``
        — never raises for an unanswered request, since silence is the normal
        outcome for a distant node.
        """
        dest = self._normalize_node_id(node_id) or node_id
        ifaces = self._interfaces_provider()
        executor = self._executor_provider()
        if not ifaces or executor is None:
            return {"ok": False, "error": "No active Meshtastic interfaces connected"}
        iface = ifaces[0]

        future = self.register_waiter(kind, dest)
        try:
            await asyncio.wrap_future(executor.submit(lambda: send(iface)))
        except asyncio.CancelledError:
            # A8 contract: re-raise after discarding the waiter — cancellation
            # is a BaseException, so the Exception handler below cannot catch
            # it and the registry would otherwise leak the waiter.
            self.discard_waiter(kind, dest, future)
            raise
        except BaseException as e:
            # The library calls our_exit(...) (SystemExit, a BaseException) when
            # it cannot resolve a destination to a node number — for a plain
            # Exception that is the normal "failed to send" report below, but
            # any BaseException must still discard the waiter before re-raising
            # (mirroring the CancelledError branch) so the registry can never
            # leak on a transport failure.
            self.discard_waiter(kind, dest, future)
            if not isinstance(e, Exception):
                raise
            logger.error("Meshtastic %s request to %s failed to send: %s", kind, dest, e)
            return {"ok": False, "error": f"Could not send {kind} request: {e}"}

        logger.info("Meshtastic %s requested from %s (timeout=%.0fs)", kind, dest, timeout)
        try:
            data = await asyncio.wait_for(asyncio.wrap_future(future), timeout=timeout)
        except TimeoutError:
            self.discard_waiter(kind, dest, future)
            logger.info("Meshtastic %s request to %s timed out", kind, dest)
            return {
                "ok": False,
                "error": (
                    f"{dest} did not answer the {kind} request within {timeout:.0f}s. "
                    "The node may be out of range, asleep, or the reply was lost."
                ),
            }
        except asyncio.CancelledError:
            self.discard_waiter(kind, dest, future)
            raise
        except self._link_lost_exc as e:
            # The link dropped mid-wait — the reply can't return, so fail fast
            # instead of sitting out the full timeout.
            logger.info("Meshtastic %s request to %s abandoned: %s", kind, dest, e)
            return {
                "ok": False,
                "error": (
                    f"The Meshtastic link dropped before {dest} answered the {kind} "
                    f"request ({e}). The packet may have gone out; try again."
                ),
            }
        logger.info("Meshtastic %s reply received from %s", kind, dest)
        return {"ok": True, "data": data}

    @staticmethod
    def _set_future_result(future: ConcurrentFuture, payload: dict[str, Any]) -> None:
        """Complete a waiter (concurrent.futures storage is thread-safe).

        ``done()`` then ``set_result()`` is not atomic: pubsub and disconnect
        can both race. Swallow InvalidStateError when another thread won.
        """
        try:
            future.set_result(payload)
        except ConcurrentInvalidStateError:
            pass
