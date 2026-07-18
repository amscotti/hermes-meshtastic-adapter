"""ACK/NACK tracking state machine for the Meshtastic adapter.

Owns the seven ACK bookkeeping dicts and the ``_ack_lock`` that serializes
them, plus the lifecycle-aware record/prune/resolve logic. Extracted from
:class:`adapter.MeshtasticAdapter` to keep the threading invariants (lock
acquisition order, stale-lifecycle early returns, ``onAckNak`` magic-name
callback) in one place.

The tracker holds a back-reference to its adapter for the few lifecycle/loop
pieces it needs (``_lifecycle_lock`` / ``_lifecycle_id`` / ``_running`` /
``loop`` / ``_cross_loop_send_logged`` / ``_normalize_node_id``). All ACK
state lives here.
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
import time
from concurrent.futures import Future as ConcurrentFuture
from concurrent.futures import InvalidStateError as ConcurrentInvalidStateError
from contextlib import ExitStack
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from gateway.platforms.base import SendResult

# ACK bookkeeping was extracted from adapter.py; keep these logs on the
# "adapter" logger so log routing (and tests that assertLogs("adapter", ...))
# is unchanged from when the code lived there.
logger = logging.getLogger("adapter")


class AckStatus(StrEnum):
    """Lifecycle of an outbound chunk's ACK bookkeeping.

    Stored on ACK records as these string values (``StrEnum`` serializes to the
    value), so ``SendResult.raw_response`` / ``get_ack_status`` stay JSON-friendly
    and backward-compatible with plain-string consumers.

    ``ACK`` is a real end-to-end confirmation (routing ACK sender == destination).
    ``IMPLICIT_ACK`` is a relay-only confirmation (official client DELIVERED vs
    RECEIVED) — not delivery for our purposes.
    """

    PENDING = "pending"
    ACK = "ack"
    IMPLICIT_ACK = "implicit_ack"
    NAK = "nak"
    TIMEOUT = "timeout"


# Upper bound on retained ACK/NACK bookkeeping records to avoid unbounded
# memory growth on a long-running gateway. Oldest non-pending records evict first.
ACK_RECORD_LIMIT = 1000

# NAK reasons where re-sending the identical packet cannot help — retrying
# would only waste shared airtime. Transient failures (timeouts, no-route,
# max-retransmit) are NOT listed here and remain eligible for retry. See
# mesh_pb2.Routing.Error for the full enum. INVALID_REQUEST is intentionally
# absent — it is not a real Routing.Error value (BAD_REQUEST is).
# DUTY_CYCLE_LIMIT / RATE_LIMIT_EXCEEDED are included because our fixed
# retry backoff (MESHTASTIC_RETRY_BACKOFF, ~seconds) is far shorter than
# their reset windows (minutes); retrying would only compound the limit.
PERMANENT_NAK_REASONS = frozenset(
    {
        "TOO_LARGE",
        "NO_CHANNEL",
        "BAD_REQUEST",
        "NOT_AUTHORIZED",
        "PKI_FAILED",
        "PKI_UNKNOWN_PUBKEY",
        "PKI_SEND_FAIL_PUBLIC_KEY",
        "ADMIN_PUBLIC_KEY_UNAUTHORIZED",
        "DUTY_CYCLE_LIMIT",
        "RATE_LIMIT_EXCEEDED",
    }
)


def ack_wait_config(metadata: dict[str, Any] | None) -> tuple[bool, float]:
    """Return whether to wait for ACK/NACK responses and for how long."""
    timeout_raw = os.getenv("MESHTASTIC_ACK_TIMEOUT", "0")
    if metadata and "meshtastic_ack_timeout" in metadata:
        timeout_raw = metadata["meshtastic_ack_timeout"]

    try:
        timeout = max(0.0, float(timeout_raw or 0))
    except (TypeError, ValueError):
        timeout = 0.0

    wait = timeout > 0
    if metadata and "meshtastic_wait_for_ack" in metadata:
        wait = bool(metadata["meshtastic_wait_for_ack"])
        if wait and timeout <= 0:
            timeout = 30.0
    return wait, timeout


def send_retries(metadata: dict[str, Any] | None) -> int:
    """Number of extra delivery attempts for un-ACKed chunks (0 = no retry)."""
    raw = os.getenv("MESHTASTIC_SEND_RETRIES", "0")
    if metadata and "meshtastic_send_retries" in metadata:
        raw = metadata["meshtastic_send_retries"]
    try:
        return max(0, int(raw or 0))
    except (TypeError, ValueError):
        return 0


def retry_backoff() -> float:
    """Seconds to wait between delivery retries (default 5.0).

    An explicit ``0`` is honored (no delay); a missing/empty/garbage value
    falls back to the default so a misconfiguration can't remove all pacing.
    """
    raw = os.getenv("MESHTASTIC_RETRY_BACKOFF", "")
    if not raw:
        return 5.0
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return 5.0


class AckTracker:
    """Owns the ACK/NACK bookkeeping dicts and the lock that serializes them.

    Holds a back-reference to its adapter for lifecycle/loop state. All
    ``_ack_*`` / ``_pending_acks`` state and the ``_ack_lock`` live here; the
    adapter exposes thin delegates and read-only properties so existing call
    sites (and tests) keep resolving to this tracker.
    """

    def __init__(self, adapter: Any) -> None:
        self._adapter = adapter
        self._pending_acks: dict[str, dict[str, Any]] = {}
        self._ack_responses: dict[str, dict[str, Any]] = {}
        # Internal generation tags prevent a reused packet id from consuming an
        # early response that belonged to an older send.
        self._ack_tokens: dict[str, object] = {}
        self._ack_response_tokens: dict[str, object] = {}
        # sendText can invoke onAckNak before returning the packet id. Stage
        # those responses by send generation until _track_pending_ack installs
        # the packet-id token; this also keeps stale-lifecycle callbacks out of
        # shared ACK history.
        self._ack_inflight_tokens: dict[object, int] = {}
        self._early_ack_packets: dict[object, tuple[dict, str, str, int]] = {}
        # concurrent.futures.Future: set_result is thread-safe from any thread
        # (including disconnect on another loop). Awaiters use asyncio.wrap_future.
        self._ack_futures: dict[str, ConcurrentFuture] = {}
        self._ack_lock = threading.Lock()

    def _maybe_record_pubsub_ack(self, packet: dict) -> bool:
        """Record a routing ACK from pubsub when it matches an outbound packet."""
        if not isinstance(packet, dict):
            return False
        decoded = packet.get("decoded", {})
        if not isinstance(decoded, dict):
            return False
        request_id = decoded.get("requestId")
        if request_id is None:
            request_id = decoded.get("request_id")
        routing = decoded.get("routing")
        if request_id is None or not isinstance(routing, dict):
            return False
        pkt_id = str(request_id)
        with self._ack_lock:
            record = self._pending_acks.get(pkt_id)
            # This fallback exists for exactly one case: a DM whose first
            # response was a relay confirmation recorded as IMPLICIT_ACK. The
            # meshtastic library removes the onResponse handler after the first
            # invocation, so the real end-to-end routing ACK that follows arrives
            # only via pubsub (_on_receive) and must upgrade the record to ACK.
            #
            # For a still-PENDING waiter the magic-named onAckNak callback is the
            # authoritative channel for the first response (the library invokes
            # it for the routing ACK when wantAck + onResponse are set), so the
            # pubsub path is intentionally *not* used there. Routing a pubsub
            # ACK for a PENDING waiter would also risk misattributing a reused
            # packet id. Non-waiting sends already get callback observability.
            if (
                record is None
                or pkt_id not in self._ack_futures
                or record.get("status") != AckStatus.IMPLICIT_ACK
            ):
                return False
            dest = str(record.get("dest") or "")
        self._record_ack_response(packet, dest, "")
        return True

    def _track_pending_ack(
        self,
        pkt_id: str | None,
        dest: str,
        content: str,
        *,
        create_future: bool = False,
        send_token: object | None = None,
    ) -> ConcurrentFuture | None:
        """Track packet IDs for ACK/NACK response observability.

        Waiters are stored as ``concurrent.futures.Future`` so pubsub/disconnect
        can ``set_result`` from any thread without needing the awaiter's event
        loop to be running. ``_wait_for_ack`` wraps it on the caller's loop.
        """
        if not pkt_id:
            return None

        cf_future: ConcurrentFuture | None = None
        log_cross_loop = False
        cross_platform_id = 0
        cross_send_id = 0
        if create_future:
            cf_future = ConcurrentFuture()
            try:
                send_loop = asyncio.get_running_loop()
            except RuntimeError:
                send_loop = None
            if (
                send_loop is not None
                and self._adapter.loop is not None
                and send_loop is not self._adapter.loop
            ):
                cross_platform_id = id(self._adapter.loop)
                cross_send_id = id(send_loop)
                log_cross_loop = True
        with self._ack_lock:
            if log_cross_loop and not self._adapter._cross_loop_send_logged:
                self._adapter._cross_loop_send_logged = True
            else:
                log_cross_loop = False
            existing_response = self._ack_responses.get(pkt_id)
            if send_token is not None:
                response_token = self._ack_response_tokens.get(pkt_id)
                if existing_response is not None and response_token is not send_token:
                    # Same numeric packet id from an older send/lifecycle.
                    existing_response = None

            active_future = self._ack_futures.get(pkt_id)
            if active_future is not None and not active_future.done():
                collision = {
                    "dest": dest,
                    "bytes": len(content.encode("utf-8")),
                    "sent_at": time.time(),
                    "response_at": time.time(),
                    "status": AckStatus.NAK,
                    "error_reason": "DUPLICATE_PACKET_ID",
                }
                self._ack_futures.pop(pkt_id, None)
                self._pending_acks[pkt_id] = collision
                self._ack_responses[pkt_id] = collision
                self._set_ack_future_result(active_future, dict(collision))
                if cf_future is not None:
                    # Poison this reused id so neither old nor new generation
                    # callbacks can overwrite the definitive collision result.
                    self._ack_tokens[pkt_id] = object()
                    self._ack_response_tokens.pop(pkt_id, None)
                    self._set_ack_future_result(cf_future, dict(collision))
                    return cf_future
                # Fire-and-forget collision: the old waiter is terminated, and
                # the new send's token now owns the id so its real ACK callback
                # can still update the record (a delayed old-token ACK is
                # ignored as stale by _record_ack_response).
                if send_token is not None:
                    self._ack_tokens[pkt_id] = send_token
                logger.warning(
                    "Meshtastic packet id collision with active ACK waiter: packet_id=%s",
                    pkt_id,
                )
                return None

            prior_token = self._ack_tokens.get(pkt_id)
            if (
                cf_future is not None
                and send_token is not None
                and prior_token is not None
                and prior_token is not send_token
            ):
                # A delayed wire ACK for the older packet would be
                # indistinguishable from an ACK for this reuse. Fail safe.
                collision = {
                    "dest": dest,
                    "bytes": len(content.encode("utf-8")),
                    "sent_at": time.time(),
                    "response_at": time.time(),
                    "status": AckStatus.NAK,
                    "error_reason": "DUPLICATE_PACKET_ID",
                }
                self._pending_acks[pkt_id] = collision
                self._ack_responses[pkt_id] = collision
                self._ack_tokens[pkt_id] = object()
                self._ack_response_tokens.pop(pkt_id, None)
                self._set_ack_future_result(cf_future, dict(collision))
                self._prune_ack_history_locked()
                return cf_future

            record = existing_response or {
                "dest": dest,
                "bytes": len(content.encode("utf-8")),
                "sent_at": time.time(),
                "status": AckStatus.PENDING,
            }
            if send_token is not None:
                self._ack_tokens[pkt_id] = send_token
            # sendText can finish after disconnect's ACK sweep. Never register
            # a fresh waiter into a stopped lifecycle; preserve a real ACK/NAK
            # that arrived early, otherwise settle as disconnected now.
            if (
                create_future
                and not self._adapter._running
                and record.get("status")
                not in (
                    AckStatus.ACK,
                    AckStatus.NAK,
                )
            ):
                record["status"] = AckStatus.TIMEOUT
                record["error_reason"] = "DISCONNECTED"
                record["response_at"] = time.time()
            self._pending_acks[pkt_id] = record
            if cf_future is not None and self._adapter._running:
                self._ack_futures[pkt_id] = cf_future
            elif cf_future is not None:
                existing_response = record
            self._prune_ack_history_locked()

        if log_cross_loop:
            logger.info(
                "Meshtastic send/ACK running on a different event loop than "
                "connect() (platform loop id=%s, send loop id=%s). ACK waiters "
                "use concurrent.futures (loop-independent settle); inbound "
                "traffic stays on the platform loop. Transport I/O is "
                "serialized on the daemon worker.",
                cross_platform_id,
                cross_send_id,
            )

        # If a definitive response (real ACK / NAK) already arrived before the
        # waiter was created, resolve immediately. An early *implicit* ACK is not
        # definitive — leave the waiter open so a real ACK (or timeout) decides.
        if (
            cf_future is not None
            and existing_response
            and not cf_future.done()
            and existing_response.get("status") != AckStatus.IMPLICIT_ACK
        ):
            self._set_ack_future_result(cf_future, existing_response)

        return cf_future

    def _fail_pending_acks(self, reason: str = "DISCONNECTED") -> None:
        """Resolve outstanding ACK waiters (e.g. on disconnect).

        ``concurrent.futures.Future.set_result`` is thread-safe, so waiters on
        any agent-session loop unblock without requiring that loop to be running
        for the *set* (only for the awaiter to resume).
        """
        to_resolve: list[tuple[ConcurrentFuture, dict[str, Any]]] = []
        with self._ack_lock:
            self._ack_inflight_tokens.clear()
            self._early_ack_packets.clear()
            items = list(self._ack_futures.items())
            self._ack_futures.clear()
            for pkt_id, future in items:
                record = self._pending_acks.get(pkt_id)
                if record is None:
                    record = {
                        "status": AckStatus.TIMEOUT,
                        "error_reason": reason,
                        "response_at": time.time(),
                    }
                elif record.get("status", AckStatus.PENDING) not in (
                    AckStatus.ACK,
                    AckStatus.NAK,
                ):
                    record["status"] = AckStatus.TIMEOUT
                    record["error_reason"] = reason
                    record["response_at"] = time.time()
                self._pending_acks[pkt_id] = record
                self._ack_responses[pkt_id] = record
                if future is not None and not future.done():
                    to_resolve.append((future, dict(record)))
            self._prune_ack_history_locked()

        for future, snapshot in to_resolve:
            self._set_ack_future_result(future, snapshot)

    def get_ack_status(self, packet_id: str) -> dict[str, Any] | None:
        """Return the latest ACK/NACK status for a packet id, if observed."""
        with self._ack_lock:
            status = self._pending_acks.get(packet_id)
            return dict(status) if status else None

    def _prune_ack_history_locked(self) -> None:
        """Bound ACK bookkeeping growth. Caller must hold ``_ack_lock``.

        Records still awaiting a result (present in ``_ack_futures``) are never
        evicted; the oldest completed records are dropped first.
        """
        for store in (self._pending_acks, self._ack_responses):
            excess = len(store) - self._adapter.ACK_RECORD_LIMIT
            if excess <= 0:
                continue
            evictable = [key for key in store if key not in self._ack_futures]
            for key in evictable[:excess]:
                store.pop(key, None)
        retained = set(self._pending_acks) | set(self._ack_responses) | set(self._ack_futures)
        for tokens in (self._ack_tokens, self._ack_response_tokens):
            for key in list(tokens):
                if key not in retained:
                    tokens.pop(key, None)

    def _make_ack_callback(self, dest: str, content: str):
        """Build a Meshtastic onResponse callback that receives ACK/NACK packets."""

        return self._make_ack_callback_for_send(dest, content, None)

    def _make_ack_callback_for_send(
        self,
        dest: str,
        content: str,
        send_token: object | None,
        lifecycle_id: int | None = None,
    ):
        """Build the magic-named callback with an optional send generation tag."""

        def onAckNak(packet):
            self._record_ack_response(
                packet,
                dest,
                content,
                send_token=send_token,
                lifecycle_id=lifecycle_id,
            )

        return onAckNak

    def _record_ack_response(
        self,
        packet: dict,
        dest: str,
        content: str,
        *,
        send_token: object | None = None,
        lifecycle_id: int | None = None,
    ) -> None:
        """Log and store Meshtastic ACK/NACK responses without blocking send().

        Distinguishes a **real** end-to-end ACK (routing ACK sender IS the
        destination → :attr:`AckStatus.ACK`) from an **implicit** ACK relayed by
        another node (sender ≠ destination → :attr:`AckStatus.IMPLICIT_ACK`).
        Mirrors the official client's RECEIVED vs DELIVERED. Only real ACK /
        NAK resolve a waiter; implicit ACKs leave it open for a real ACK or
        timeout.

        Definitive results (ACK/NAK) are never downgraded by a later implicit
        ACK. When scheduling the waiter, a **snapshot** of the record is passed
        so concurrent updates cannot mutate the dict the future will resolve to.
        """
        decoded = packet.get("decoded", {}) if isinstance(packet, dict) else {}
        routing = decoded.get("routing", {}) or {}
        request_id = decoded.get("requestId")
        if request_id is None:
            request_id = decoded.get("request_id")
        error_reason = routing.get("errorReason") or routing.get("error_reason")
        pkt_id = str(request_id) if request_id is not None else "unknown"

        # Who sent this ACK. Applied to DMs only (dest is a "!node" id).
        # Missing sender still counts as a real ACK (backward compatible).
        ack_from_raw = None
        if isinstance(packet, dict):
            ack_from_raw = packet.get("fromId") or packet.get("from")
        ack_from = self._adapter._normalize_node_id(ack_from_raw)
        dest_norm = self._adapter._normalize_node_id(dest) if dest.startswith("!") else None

        if error_reason not in (None, "", "NONE"):
            status = AckStatus.NAK
        elif dest_norm and ack_from and ack_from != dest_norm:
            status = AckStatus.IMPLICIT_ACK
        else:
            status = AckStatus.ACK

        # Hold lifecycle ownership through the ACK-store commit. This closes the
        # check-to-commit window where disconnect/reconnect could otherwise
        # advance the generation after validation but before _ack_lock.
        with ExitStack() as stack:
            if lifecycle_id is not None:
                stack.enter_context(self._adapter._lifecycle_lock)
                if lifecycle_id != self._adapter._lifecycle_id or not self._adapter._running:
                    logger.debug(
                        "Ignoring ACK callback from stale lifecycle: packet_id=%s",
                        pkt_id,
                    )
                    return
            stack.enter_context(self._ack_lock)
            if send_token is not None:
                inflight_lifecycle = self._ack_inflight_tokens.get(send_token)
                if inflight_lifecycle is not None:
                    if lifecycle_id is not None and inflight_lifecycle != lifecycle_id:
                        return
                    self._early_ack_packets[send_token] = (
                        packet,
                        dest,
                        content,
                        inflight_lifecycle,
                    )
                    return
                active_token = self._ack_tokens.get(pkt_id)
                # Two-level stale defense: the lifecycle_id check above already
                # rejects callbacks from dead lifecycles; this token check is
                # the second level, rejecting same-lifecycle packet-id reuse
                # where an older send's id is still tracked. A missing token
                # entry is only possible after lifecycle turnover, which the
                # first level already caught — so no entry means accept.
                if active_token is not None and active_token is not send_token:
                    logger.debug("Ignoring stale ACK callback for packet_id=%s", pkt_id)
                    return
                self._ack_response_tokens[pkt_id] = send_token
            record = self._pending_acks.get(pkt_id, {})
            prior = record.get("status")
            # Never let a weaker/later relay confirmation overwrite a definitive
            # real ACK or NAK already stored on the shared record.
            if status == AckStatus.IMPLICIT_ACK and prior in (
                AckStatus.ACK,
                AckStatus.NAK,
            ):
                record["response_at"] = time.time()
                applied_status = prior
                snapshot = None  # no waiter resolution for a discarded implicit
            else:
                record.update(
                    {
                        "dest": record.get("dest", dest),
                        "bytes": record.get("bytes", len(content.encode("utf-8"))),
                        "status": status,
                        "error_reason": error_reason,
                        "ack_from": ack_from,
                        "response_at": time.time(),
                        "response": {
                            "packet_id": (packet.get("id") if isinstance(packet, dict) else None),
                            "request_id": request_id,
                            "from_id": ack_from,
                            "to_id": packet.get("toId") if isinstance(packet, dict) else None,
                            "routing": routing,
                        },
                    }
                )
                applied_status = status
                # Snapshot so a concurrent update cannot mutate the future result.
                snapshot = dict(record)
            self._pending_acks[pkt_id] = record
            self._ack_responses[pkt_id] = record
            if applied_status in (AckStatus.ACK, AckStatus.NAK):
                future = self._ack_futures.pop(pkt_id, None)
            else:
                future = self._ack_futures.get(pkt_id)
            self._prune_ack_history_locked()

        # Resolve the waiter only on a DEFINITIVE outcome (real ACK or NAK). An
        # implicit ACK updates the record but keeps the wait open, so a real ACK
        # can still arrive — and if it doesn't, the timeout drives a retry.
        # concurrent.futures.Future.set_result is thread-safe (pubsub thread OK).
        if (
            snapshot is not None
            and applied_status in (AckStatus.ACK, AckStatus.NAK)
            and future
            and not future.done()
        ):
            self._set_ack_future_result(future, snapshot)

        if applied_status == AckStatus.ACK and status == AckStatus.ACK:
            logger.info("Meshtastic ACK received (delivered): packet_id=%s dest=%s", pkt_id, dest)
        elif status == AckStatus.IMPLICIT_ACK and applied_status == AckStatus.IMPLICIT_ACK:
            logger.info(
                "Meshtastic implicit ACK: packet_id=%s dest=%s relayed_by=%s (dest not confirmed)",
                pkt_id,
                dest,
                ack_from,
            )
        elif applied_status == AckStatus.NAK and status == AckStatus.NAK:
            logger.warning(
                "Meshtastic NAK received: packet_id=%s dest=%s reason=%s",
                pkt_id,
                dest,
                error_reason,
            )
        elif status == AckStatus.IMPLICIT_ACK:
            logger.debug(
                "Meshtastic implicit ACK ignored after definitive status=%s: packet_id=%s",
                applied_status,
                pkt_id,
            )

    def _set_ack_future_result(self, future: ConcurrentFuture, record: dict[str, Any]) -> None:
        """Complete an ACK waiter (concurrent.futures is the storage type, thread-safe).

        ``done()`` then ``set_result()`` is not atomic: pubsub and disconnect can
        both race. Swallow InvalidStateError when another thread won.
        """
        try:
            future.set_result(record)
        except ConcurrentInvalidStateError:
            pass

    async def _wait_for_ack(
        self,
        pkt_id: str,
        future: ConcurrentFuture,
        timeout: float,
    ) -> dict[str, Any]:
        """Wait for ACK/NACK response or mark the packet timed out."""
        try:
            wrapped = asyncio.wrap_future(future)
            return await asyncio.wait_for(asyncio.shield(wrapped), timeout=timeout)
        except TimeoutError:
            with self._ack_lock:
                record = self._pending_acks.get(pkt_id, {})
                # Only stamp TIMEOUT while still pending. A concurrent real ACK,
                # NAK, or implicit ACK that landed between wait_for timing out
                # and this lock acquisition must not be overwritten.
                if record.get("status", AckStatus.PENDING) == AckStatus.PENDING:
                    record["status"] = AckStatus.TIMEOUT
                    record["error_reason"] = "ACK_TIMEOUT"
                record["response_at"] = time.time()
                self._pending_acks[pkt_id] = record
                self._ack_responses[pkt_id] = record
            logger.warning(
                "Meshtastic ACK timeout: packet_id=%s timeout=%.1fs final_status=%s",
                pkt_id,
                timeout,
                record.get("status"),
            )
            return record
        finally:
            with self._ack_lock:
                if self._ack_futures.get(pkt_id) is future:
                    self._ack_futures.pop(pkt_id, None)
                self._prune_ack_history_locked()

    def _is_retriable_failure(self, result: SendResult) -> bool:
        """Decide whether a failed chunk send is worth re-sending.

        Only ACK-observed failures qualify: a timeout, an implicit (relay-only)
        ACK, or a NAK whose reason is not permanent. Pre-send errors (no
        interface, missing pubkey, bad chat_id) carry no ACK record and are
        never retried — re-sending can't fix them.
        """
        ack = (result.raw_response or {}).get("ack")
        if not isinstance(ack, dict):
            return False
        status = ack.get("status")
        reason = str(ack.get("error_reason") or "").upper()
        # Adapter teardown — do not retry into a closed transport.
        if reason == "DISCONNECTED":
            return False
        # Adapter-internal synthetic NAK: the packet id collided with an
        # in-flight waiter. The chunk was already transmitted by sendText before
        # the collision was detected, so retrying would duplicate it on-air.
        # Fail safe and leave delivery to the (already sent) original packet.
        if reason == "DUPLICATE_PACKET_ID":
            return False
        # No confirmation, or only a relay confirmed — both warrant a retry.
        if status in (AckStatus.TIMEOUT, AckStatus.IMPLICIT_ACK):
            return True
        if status == AckStatus.NAK:
            return reason not in PERMANENT_NAK_REASONS
        return False
