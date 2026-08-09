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
    from typing import Protocol

    from gateway.platforms.base import SendResult

    class LifecycleHost(Protocol):
        """The minimal adapter surface AckTracker dereferences (R4).

        Duck-typed contract: ``MeshtasticAdapter`` (and test stubs) satisfy it
        structurally; the tracker never reaches past these members.
        ``_cross_loop_send_logged`` is read-write — the tracker flips it after
        the first cross-loop send.
        """

        loop: asyncio.AbstractEventLoop | None
        _running: bool
        _cross_loop_send_logged: bool
        _lifecycle_lock: threading.Lock
        _lifecycle_id: int
        ACK_RECORD_LIMIT: int

        @staticmethod
        def _normalize_node_id(node_id: Any) -> str | None: ...


logger = logging.getLogger(__name__)


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

# Internal synthetic NAK reason for a packet-id collision (reused id still
# in-flight when a new send adopted it). Prefixed with ``_`` so it cannot
# collide with the attacker-controllable wire reason namespace
# (``routing.errorReason``): a forged wire NAK carrying
# ``errorReason="DUPLICATE_PACKET_ID"`` must not trigger the internal
# "don't retry" branch in :func:`is_retriable_failure`.
INTERNAL_NAK_DUPLICATE_PACKET_ID = "_DUPLICATE_PACKET_ID"


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
        # The JSON/config value arrives as a string on tool-call paths; a bare
        # bool() would treat "false"/"0"/"no" as truthy and force a 30s wait.
        raw = metadata["meshtastic_wait_for_ack"]
        if isinstance(raw, bool):
            wait = raw
        else:
            wait = str(raw).strip().lower() in {"1", "true", "yes", "on"}
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


# Stable internal error tokens the adapter produces for transient failures that
# carry no ACK record (nothing went out): no-interface and the classified
# transport-failure class. Both the send() retry loop and the drain requeue
# path treat these as retriable-with-budget.
TRANSIENT_TRANSPORT_ERRORS = frozenset(
    {
        "No active interfaces connected",
        "No active interfaces connected; cannot wait for ACK",
        "Meshtastic send failed",
    }
)


def is_retriable_failure(result: SendResult) -> bool:
    """Decide whether a failed chunk send is worth re-sending.

    Retry only on **evidence of non-delivery**, so a lost message gets
    another chance without flooding the mesh with duplicates:

    * ``TIMEOUT`` — nothing came back at all.
    * non-permanent ``NAK`` — e.g. ``MAX_RETRANSMIT``, the firmware's own
      "reliable send failed" verdict after its ``NUM_RELIABLE_RETX`` tries.

    An ``IMPLICIT_ACK`` is deliberately **not** retried: a relay rebroadcast
    our packet, so the mesh carried it and non-delivery is not established —
    the destination's real ACK may still arrive (``_maybe_record_pubsub_ack``
    upgrades the record if it does). Retrying on implicit is what re-sent one
    reply many times on a relayed path (each app attempt is ~3 radio
    transmissions) — and every copy actually reached the user.

    Pre-send errors (no interface, missing pubkey, bad chat_id) carry no ACK
    record and are never retried — re-sending can't fix them. The exception is
    the transient no-interface / transport-failure class: nothing went out, so
    there is no ACK record, and the failure is exactly the flapping-link case
    retries exist to ride through — those ARE retried within the attempt
    budget (see ``TRANSIENT_TRANSPORT_ERRORS``).
    """
    if result.retryable or result.error in TRANSIENT_TRANSPORT_ERRORS:
        return True
    ack = (result.raw_response or {}).get("ack")
    if not isinstance(ack, dict):
        return False
    status = ack.get("status")
    reason = str(ack.get("error_reason") or "").upper()
    # Adapter teardown — do not retry into a closed transport. Only the
    # internal disconnect sentinel (TIMEOUT status + DISCONNECTED reason)
    # matches; a forged wire NAK carrying errorReason="DISCONNECTED" has NAK
    # status (any errorReason classifies as NAK) and falls through to the
    # normal NAK retry classification below, where it is retriable because
    # "DISCONNECTED" is not in PERMANENT_NAK_REASONS.
    if reason == "DISCONNECTED" and status == AckStatus.TIMEOUT:
        return False
    # Adapter-internal synthetic NAK: the packet id collided with an
    # in-flight waiter. The chunk was already transmitted by sendText before
    # the collision was detected, so retrying would duplicate it on-air.
    # Fail safe and leave delivery to the (already sent) original packet.
    # The internal token is ``_``-prefixed so a forged wire NAK with the
    # unprefixed ``errorReason="DUPLICATE_PACKET_ID"`` cannot match.
    if reason == INTERNAL_NAK_DUPLICATE_PACKET_ID:
        return False
    if status == AckStatus.TIMEOUT:
        return True
    if status == AckStatus.NAK:
        return not is_permanent_nak_reason(reason)
    return False


def is_permanent_nak_reason(reason: str) -> bool:
    """Whether a NAK reason is permanent (re-sending the packet cannot help)."""
    return reason in PERMANENT_NAK_REASONS


# A verdict that settles an ACK waiter (never downgraded by a later implicit).
DEFINITIVE_ACK_STATUSES = (AckStatus.ACK, AckStatus.NAK)


def classify_ack_verdict(
    error_reason: str | None,
    dest_norm: str | None,
    ack_from: str | None,
) -> AckStatus:
    """Classify a routing ACK as NAK / implicit / real.

    Any ``errorReason`` is a NAK. A relay confirmation (sender ≠ destination,
    DM dest) is an IMPLICIT_ACK; everything else is a real end-to-end ACK
    (missing sender counts as real, backward compatible).
    """
    if error_reason not in (None, "", "NONE"):
        return AckStatus.NAK
    if dest_norm and ack_from and ack_from != dest_norm:
        return AckStatus.IMPLICIT_ACK
    return AckStatus.ACK


def ack_hop_info(packet: dict) -> tuple[int | None, int | None, int | None]:
    """Extract ``(hops_away, hop_start, hop_limit)`` from a raw ACK packet."""
    hop_start = packet.get("hopStart")
    hop_limit = packet.get("hopLimit")
    if isinstance(hop_start, int) and isinstance(hop_limit, int):
        return max(0, hop_start - hop_limit), hop_start, hop_limit
    return None, hop_start, hop_limit


def safe_ack_envelope(packet: dict) -> tuple[dict[str, Any], dict[str, Any]]:
    """Safely extract ``(decoded, routing)`` dicts from a raw ACK packet.

    Both fields are attacker-influenceable off the radio; non-dict truthy
    values are defaulted to ``{}`` (mirroring ``_maybe_record_pubsub_ack``).
    """
    decoded = packet.get("decoded", {}) if isinstance(packet, dict) else {}
    if not isinstance(decoded, dict):
        decoded = {}
    routing_raw = decoded.get("routing")
    routing = routing_raw if isinstance(routing_raw, dict) else {}
    return decoded, routing


def new_pending_record(dest: str, content: str) -> dict[str, Any]:
    """A fresh PENDING bookkeeping record for an outbound chunk."""
    return {
        "dest": dest,
        "bytes": len(content.encode("utf-8")),
        "sent_at": time.time(),
        "status": AckStatus.PENDING,
    }


def collision_record(dest: str, content: str) -> dict[str, Any]:
    """The definitive NAK record for a reused packet id.

    The chunk was already transmitted before the collision was detected, so
    retrying would duplicate it on-air; the collision verdict is authoritative
    for both the old waiter and any new one.
    """
    return {
        "dest": dest,
        "bytes": len(content.encode("utf-8")),
        "sent_at": time.time(),
        "response_at": time.time(),
        "status": AckStatus.NAK,
        "error_reason": INTERNAL_NAK_DUPLICATE_PACKET_ID,
    }


def discount_stale_response(
    existing_response: dict[str, Any] | None,
    response_token: object | None,
    send_token: object | None,
) -> dict[str, Any] | None:
    """Early-response window: drop a response owned by a different send token.

    A reused numeric packet id from an older send/lifecycle must not resolve
    the new waiter, so its response is discounted (``None``).
    """
    if (
        send_token is not None
        and existing_response is not None
        and response_token is not send_token
    ):
        return None
    return existing_response


class AckTracker:
    """Owns the ACK/NACK bookkeeping dicts and the lock that serializes them.

    Holds a back-reference to its adapter for lifecycle/loop state. All
    ``_ack_*`` / ``_pending_acks`` state and the ``_ack_lock`` live here; the
    adapter exposes thin delegates and read-only properties so existing call
    sites (and tests) keep resolving to this tracker.
    """

    def __init__(self, adapter: Any) -> None:
        self._adapter: LifecycleHost = adapter
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
        # pkt_id -> the lifecycle that created the record. ACK history survives
        # lifecycle turnover by design (_fail_pending_acks keeps it), so the
        # pubsub upgrade path must not promote a record from a DEAD lifecycle:
        # a delayed routing packet after reconnect would otherwise mint a
        # phantom "delivered" verdict in the fresh lifecycle's stores.
        self._record_lifecycles: dict[str, int] = {}
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
            # This fallback exists for a DM whose first response was a relay
            # confirmation recorded as IMPLICIT_ACK. The meshtastic library
            # removes the onResponse handler after the first invocation, so the
            # real end-to-end routing ACK that follows arrives only via pubsub
            # (_on_receive) and must upgrade the record to ACK.
            #
            # For a still-PENDING waiter the magic-named onAckNak callback is
            # the authoritative channel for the first response (the library
            # invokes it for the routing ACK when wantAck + onResponse are
            # set), so the pubsub path is intentionally *not* used there — the
            # status check (only IMPLICIT_ACK records upgrade) keeps a PENDING
            # waiter waiting.
            #
            # No live waiter is required: a fire-and-forget DM's relay
            # confirmation also consumes the one-shot callback, and its real
            # routing ACK must still upgrade the record for observability. A
            # reused packet id from an older send is protected by the
            # send-token check in _record_ack_response (a different owner's
            # token drops the stale response).
            if record is None or record.get("status") != AckStatus.IMPLICIT_ACK:
                return False
            # Only promote a record created by the CURRENT lifecycle. ACK
            # records survive reconnect (_fail_pending_acks keeps them), so a
            # delayed routing packet for an old send would otherwise upgrade a
            # stale IMPLICIT_ACK into a phantom "delivered" verdict in the
            # fresh lifecycle's observability stores.
            if self._record_lifecycles.get(pkt_id) != self._adapter._lifecycle_id:
                return False
            # Capture the lifecycle id under the lock so the subsequent
            # _record_ack_response call (outside the lock) can re-validate
            # atomically under its combined lifecycle_lock→ack_lock hold.
            # Without this, lifecycle_id defaults to None inside
            # _record_ack_response and the staleness re-check is skipped — a
            # disconnect/reconnect in the window between the check above and
            # the call below could let a stale upgrade slip through.
            lifecycle_id = self._adapter._lifecycle_id
            dest = str(record.get("dest") or "")
            send_token = self._ack_tokens.get(pkt_id)
            # Defense-in-depth (MEDIUM): only upgrade an implicit ACK when the
            # routing ACK itself arrived directly. On a shared/PSK channel any
            # node that observed the transmission can forge a routing ACK (our
            # requestId + the destination's id) to mark delivery confirmed; the
            # hop envelope is the only per-packet provenance available. An
            # end-to-end ACK from the destination is expected directly, so a
            # relayed packet keeps the record at IMPLICIT_ACK — still a
            # delivered outcome (never a retry), just not an upgraded one.
            # The hop fields must be present ints: a forged routing ACK that
            # omits the envelope entirely is treated as direct only when the
            # fields exist to disprove it. Inherited protocol trust limits
            # this: a spoofed packet can also fake a 0-hop envelope, so this
            # is defense-in-depth, not a guarantee.
            hops, hop_start, hop_limit = ack_hop_info(packet)
            if not (isinstance(hop_start, int) and isinstance(hop_limit, int) and hops == 0):
                return False
        self._record_ack_response(
            packet, dest, "", send_token=send_token, lifecycle_id=lifecycle_id
        )
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
            cross_platform_id, cross_send_id, log_cross_loop = self._cross_loop_context()
        with self._ack_lock:
            if log_cross_loop and not self._adapter._cross_loop_send_logged:
                self._adapter._cross_loop_send_logged = True
            else:
                log_cross_loop = False
            existing_response = discount_stale_response(
                self._ack_responses.get(pkt_id),
                self._ack_response_tokens.get(pkt_id) if send_token is not None else None,
                send_token,
            )

            active_future = self._ack_futures.get(pkt_id)
            if active_future is not None and not active_future.done():
                return self._handle_active_waiter_collision(
                    pkt_id, dest, content, active_future, cf_future, send_token
                )
            if cf_future is not None and self._token_generation_collision(pkt_id, send_token):
                return self._settle_poisoned_reuse(pkt_id, dest, content, cf_future)

            record = existing_response or new_pending_record(dest, content)
            if send_token is not None:
                self._ack_tokens[pkt_id] = send_token
            # sendText can finish after disconnect's ACK sweep. Never register
            # a fresh waiter into a stopped lifecycle; preserve a real ACK/NAK
            # that arrived early, otherwise settle as disconnected now.
            self._settle_not_running_record(record, create_future)
            self._pending_acks[pkt_id] = record
            # Stamp the record with the lifecycle that created it so the pubsub
            # upgrade path (which has no lifecycle_id of its own) can reject a
            # stale record from a previous lifecycle.
            self._record_lifecycles[pkt_id] = self._adapter._lifecycle_id
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
        self._resolve_early_waiter(cf_future, existing_response)
        return cf_future

    def _cross_loop_context(self) -> tuple[int, int, bool]:
        """Detect a send loop different from the platform loop.

        Returns ``(platform_loop_id, send_loop_id, is_cross_loop)``; ACK
        waiters are concurrent.futures so they settle loop-independently, but
        the first cross-loop send logs once for diagnosis.
        """
        try:
            send_loop = asyncio.get_running_loop()
        except RuntimeError:
            return 0, 0, False
        platform_loop = self._adapter.loop
        if platform_loop is None or send_loop is platform_loop:
            return 0, 0, False
        return id(platform_loop), id(send_loop), True

    def _handle_active_waiter_collision(
        self,
        pkt_id: str,
        dest: str,
        content: str,
        active_future: ConcurrentFuture,
        cf_future: ConcurrentFuture | None,
        send_token: object | None,
    ) -> ConcurrentFuture | None:
        """Settle a packet-id collision against a still-active waiter.

        The old waiter is terminated with the collision verdict. If a new
        waiter exists the id is poisoned so neither generation's callbacks can
        overwrite the definitive result; a fire-and-forget send instead re-arms
        the id with its own token so a delayed old-token ACK is ignored as
        stale by ``_record_ack_response``.
        """
        collision = collision_record(dest, content)
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

    def _token_generation_collision(self, pkt_id: str, send_token: object | None) -> bool:
        """Whether a reused id still carries an older send's generation token."""
        prior_token = self._ack_tokens.get(pkt_id)
        return send_token is not None and prior_token is not None and prior_token is not send_token

    def _settle_poisoned_reuse(
        self,
        pkt_id: str,
        dest: str,
        content: str,
        cf_future: ConcurrentFuture,
    ) -> ConcurrentFuture:
        """Fail a new waiter whose id was reused from an older send.

        A delayed wire ACK for the older packet would be indistinguishable
        from an ACK for this reuse, so the id is poisoned for both
        generations. Fail safe.
        """
        collision = collision_record(dest, content)
        self._pending_acks[pkt_id] = collision
        self._ack_responses[pkt_id] = collision
        self._ack_tokens[pkt_id] = object()
        self._ack_response_tokens.pop(pkt_id, None)
        self._set_ack_future_result(cf_future, dict(collision))
        self._prune_ack_history_locked()
        return cf_future

    def _settle_not_running_record(self, record: dict[str, Any], create_future: bool) -> None:
        """Mark a fresh waiter as disconnected when the lifecycle is stopped.

        A definitive verdict (ACK/NAK) or TIMEOUT already on the record is
        preserved. An early ``IMPLICIT_ACK`` (relay confirmation) is rewritten
        to ``TIMEOUT/DISCONNECTED``: the packet was relayed but the waiter
        cannot stay open in a stopped lifecycle. The outcome is correct —
        ``DISCONNECTED`` is non-retriable so a carried packet is not
        duplicated — at the cost of losing the "relay confirmed" label.
        """
        if (
            create_future
            and not self._adapter._running
            and record.get("status") not in DEFINITIVE_ACK_STATUSES
        ):
            record["status"] = AckStatus.TIMEOUT
            record["error_reason"] = "DISCONNECTED"
            record["response_at"] = time.time()

    def _resolve_early_waiter(
        self,
        cf_future: ConcurrentFuture | None,
        existing_response: dict[str, Any] | None,
    ) -> None:
        """Resolve a newly created waiter from an early definitive response."""
        if (
            cf_future is not None
            and existing_response
            and not cf_future.done()
            and existing_response.get("status") != AckStatus.IMPLICIT_ACK
        ):
            self._set_ack_future_result(cf_future, existing_response)

    def _fail_pending_acks(self, reason: str = "DISCONNECTED") -> None:
        """Resolve outstanding ACK waiters (e.g. on disconnect).

        ``concurrent.futures.Future.set_result`` is thread-safe, so waiters on
        any agent-session loop unblock without requiring that loop to be running
        for the *set* (only for the awaiter to resume).

        ``_pending_acks`` / ``_ack_responses`` / ``_ack_tokens`` are
        intentionally NOT cleared here: ACK history (and its packet-id poison
        markers) survives lifecycle turnover, so a reused packet id after a
        reconnect is still rejected via ``_token_generation_collision``. History
        is bounded by ``_prune_ack_history_locked``.
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
                    AckStatus.TIMEOUT,
                ):
                    # TIMEOUT is preserved: a waiter already stamped by
                    # _wait_for_ack within the timeout->finally gap keeps its
                    # original reason (e.g. ACK_TIMEOUT) instead of being
                    # rewritten to DISCONNECTED.
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
        for tokens in (self._ack_tokens, self._ack_response_tokens, self._record_lifecycles):
            for key in list(tokens):
                if key not in retained:
                    tokens.pop(key, None)

    def _make_ack_callback(self, dest: str, content: str):
        """Build a Meshtastic onResponse callback that receives ACK/NACK packets.

        Tokenless compat shim. Synthesizes the current lifecycle id at
        construction time so ``_record_ack_response``'s staleness guard always
        applies: a callback that fires after disconnect/reconnect is dropped
        rather than written into a fresh lifecycle's ACK stores.
        """

        return self._make_ack_callback_for_send(
            dest, content, None, lifecycle_id=self._adapter._lifecycle_id
        )

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
        decoded, routing = safe_ack_envelope(packet)
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
        status = classify_ack_verdict(error_reason, dest_norm, ack_from)

        # Diagnostic dump of the raw ACK packet, so the real-vs-implicit verdict
        # can be checked against what the radio actually saw: who sent it, how
        # far away, signal, and whether it came via a relay / MQTT.
        self._log_ack_packet_dump(
            packet, pkt_id, status, ack_from, ack_from_raw, dest, dest_norm, error_reason
        )

        # An ACK with no request id cannot be tied to an outbound packet. Drop
        # it (logging only) rather than persisting an orphan "unknown" record
        # that a forged id-less routing packet could inflate.
        if pkt_id == "unknown":
            logger.debug(
                "Ignoring ACK response without a request id: packet_id=%s",
                packet.get("id") if isinstance(packet, dict) else None,
            )
            return

        # Hold lifecycle ownership through the ACK-store commit. This closes the
        # check-to-commit window where disconnect/reconnect could otherwise
        # advance the generation after validation but before _ack_lock.
        with ExitStack() as stack:
            if lifecycle_id is not None:
                stack.enter_context(self._adapter._lifecycle_lock)
                if self._lifecycle_is_stale(lifecycle_id):
                    logger.debug(
                        "Ignoring ACK callback from stale lifecycle: packet_id=%s",
                        pkt_id,
                    )
                    return
            stack.enter_context(self._ack_lock)
            if send_token is not None and self._stage_ack_via_send_token(
                pkt_id, packet, dest, content, send_token, lifecycle_id
            ):
                return
            record = self._pending_acks.get(pkt_id, {})
            applied_status, snapshot = self._merge_ack_into_record(
                record, packet, dest, content, status, error_reason, ack_from, request_id, routing
            )
            self._pending_acks[pkt_id] = record
            self._ack_responses[pkt_id] = record
            if applied_status in DEFINITIVE_ACK_STATUSES:
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
            and applied_status in DEFINITIVE_ACK_STATUSES
            and future
            and not future.done()
        ):
            self._set_ack_future_result(future, snapshot)

        self._log_ack_outcome(status, applied_status, pkt_id, dest, error_reason, ack_from, packet)

    def _lifecycle_is_stale(self, lifecycle_id: int) -> bool:
        """Whether a callback's lifecycle no longer owns the ACK stores."""
        return lifecycle_id != self._adapter._lifecycle_id or not self._adapter._running

    def _log_ack_packet_dump(
        self,
        packet: dict,
        pkt_id: str,
        status: AckStatus,
        ack_from: str | None,
        ack_from_raw: Any,
        dest: str,
        dest_norm: str | None,
        error_reason: Any,
    ) -> None:
        if not isinstance(packet, dict):
            return
        hops_away, hop_start, hop_limit = ack_hop_info(packet)
        logger.info(
            "Meshtastic ACK packet: req=%s verdict=%s from=%s (raw=%r) dest=%s (norm=%s) "
            "to=%s hops=%s (start=%s limit=%s) snr=%s rssi=%s relay=%s mqtt=%s error=%s",
            pkt_id,
            status,
            ack_from,
            ack_from_raw,
            dest,
            dest_norm,
            packet.get("toId") or packet.get("to"),
            hops_away,
            hop_start,
            hop_limit,
            packet.get("rxSnr"),
            packet.get("rxRssi"),
            packet.get("relayNode"),
            packet.get("viaMqtt"),
            error_reason,
        )

    def _stage_ack_via_send_token(
        self,
        pkt_id: str,
        packet: dict,
        dest: str,
        content: str,
        send_token: object,
        lifecycle_id: int | None,
    ) -> bool:
        """Early-ACK window: route a response whose send is still in flight.

        ``sendText`` can invoke ``onAckNak`` before returning the packet id;
        those responses are staged by send generation (``_early_ack_packets``)
        until ``_track_pending_ack`` installs the packet-id token, and
        stale-lifecycle callbacks stay out of shared ACK history. Returns True
        when the response was consumed (staged or dropped) without recording.
        """
        inflight_lifecycle = self._ack_inflight_tokens.get(send_token)
        if inflight_lifecycle is not None:
            if lifecycle_id is not None and inflight_lifecycle != lifecycle_id:
                return True
            self._early_ack_packets[send_token] = (packet, dest, content, inflight_lifecycle)
            return True
        active_token = self._ack_tokens.get(pkt_id)
        # Two-level stale defense: the lifecycle_id check above already rejects
        # callbacks from dead lifecycles; this token check is the second level,
        # rejecting same-lifecycle packet-id reuse where an older send's id is
        # still tracked. A missing token entry is only possible after lifecycle
        # turnover, which the first level already caught — so no entry means
        # accept.
        if active_token is not None and active_token is not send_token:
            logger.debug("Ignoring stale ACK callback for packet_id=%s", pkt_id)
            return True
        self._ack_response_tokens[pkt_id] = send_token
        return False

    def _merge_ack_into_record(
        self,
        record: dict[str, Any],
        packet: dict,
        dest: str,
        content: str,
        status: AckStatus,
        error_reason: Any,
        ack_from: str | None,
        request_id: Any,
        routing: Any,
    ) -> tuple[AckStatus, dict[str, Any] | None]:
        """Apply a verdict to the shared record; return ``(applied, snapshot)``.

        Never let a weaker/later relay confirmation overwrite a definitive
        real ACK or NAK already stored on the shared record — the snapshot is
        passed to the waiter so concurrent updates cannot mutate the dict the
        future resolves to.
        """
        prior = record.get("status")
        if status == AckStatus.IMPLICIT_ACK and prior in DEFINITIVE_ACK_STATUSES:
            record["response_at"] = time.time()
            return prior, None
        prior_reason = str(record.get("error_reason") or "").upper()
        if (
            status in DEFINITIVE_ACK_STATUSES
            and prior in DEFINITIVE_ACK_STATUSES
            and prior_reason != INTERNAL_NAK_DUPLICATE_PACKET_ID
        ):
            # First-definitive-wins: a late second wire verdict for the same
            # pkt_id (e.g. NAK after ACK, or vice versa) must not overwrite
            # the record. The first definitive already resolved and popped
            # the waiter, so this only protects get_ack_status correctness —
            # the docstring's "definitive results are never downgraded by a
            # later" claim applies to a second definitive too, not just
            # implicit ACKs.
            #
            # An internal collision NAK (_DUPLICATE_PACKET_ID) is exempt: it
            # is a bookkeeping sentinel, not a radio verdict. In the
            # fire-and-forget collision path the new token owner's real ACK
            # must still be able to upgrade the record.
            record["response_at"] = time.time()
            return prior, None
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
        return status, dict(record)

    def _log_ack_outcome(
        self,
        status: AckStatus,
        applied_status: AckStatus,
        pkt_id: str,
        dest: str,
        error_reason: Any,
        ack_from: str | None,
        packet: dict,
    ) -> None:
        if applied_status == AckStatus.ACK and status == AckStatus.ACK:
            logger.info("Meshtastic ACK received (delivered): packet_id=%s dest=%s", pkt_id, dest)
        elif status == AckStatus.IMPLICIT_ACK and applied_status == AckStatus.IMPLICIT_ACK:
            # NB: ack_from is the packet ORIGINATOR (our own node) — we heard our
            # own packet rebroadcast. It is NOT the relay; the rebroadcaster hint
            # is relayNode. Wording it "relayed_by=<us>" was misleading.
            logger.info(
                "Meshtastic implicit ACK: packet_id=%s dest=%s "
                "(our packet was rebroadcast; dest did not confirm) origin=%s relay_node=%s",
                pkt_id,
                dest,
                ack_from,
                packet.get("relayNode") if isinstance(packet, dict) else None,
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
        """Wait for ACK/NACK response or mark the packet timed out.

        An implicit ACK must NOT early-return this wait: the record stays
        `implicit_ack` while the waiter stays open so a real end-to-end ACK can
        still upgrade it (``_maybe_record_pubsub_ack`` needs the live waiter).
        The cost is deliberate — an implicit-only reply waits out the full
        ``timeout`` before ``_send_immediate`` reports it as delivered.
        """
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
            # Snapshot, not the live shared record: a late ACK/NAK on the
            # pubsub thread between this stamp and the caller's read of the
            # return value must not flip the verdict _send_immediate reports.
            return dict(record)
        finally:
            with self._ack_lock:
                if self._ack_futures.get(pkt_id) is future:
                    self._ack_futures.pop(pkt_id, None)
                self._prune_ack_history_locked()
