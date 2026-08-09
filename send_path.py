"""Pure decision helpers for the adapter's outbound send path.

Extracted from ``adapter.send`` / ``MeshtasticAdapter._send_immediate`` /
``MeshtasticAdapter._send_text_serialized`` so the send-path decisions are
unit-testable without an adapter instance: retry eligibility and budget,
transport-error mapping, ACK-outcome classification, DM node / channel
selection, and chunk pacing.

All functions are synchronous and pure: no I/O, no asyncio, no adapter
imports (anything adapter-specific is parameter-passed). Env reads follow the
``ack_state`` convention: read at call time, defensive defaults.
"""

from __future__ import annotations

import math
import os
import time
from collections.abc import Callable
from concurrent.futures import Future as ConcurrentFuture
from typing import Any

try:
    from . import ack_state
except ImportError:
    import ack_state

try:
    from . import transport
except ImportError:
    import transport

AckStatus = ack_state.AckStatus

# Default pause between numbered LoRa chunks (``MESHTASTIC_CHUNK_DELAY``).
DEFAULT_CHUNK_DELAY = 4.0
# ACK timeout forced on when delivery retries are enabled without one.
DEFAULT_RETRY_ACK_TIMEOUT = 30.0

# Canonical "no interface" token. The longer user-facing variants in the adapter
# (``...; cannot wait for ACK`` / ``... and queueing disabled``) are built from
# this so every "no interfaces" error shares one stable prefix. It is also a
# member of ``ack_state.TRANSIENT_TRANSPORT_ERRORS``, which ``_send_chunk``
# matches to decide requeueing — keeping the spelling identical across both
# modules is what makes the queueing decision reliable.
NO_INTERFACES_ERROR = "No active interfaces connected"


def dest_from_chat_id(chat_id: str) -> str:
    """The recipient part of a ``meshtastic:...`` chat id (``""`` when malformed)."""
    return chat_id.split(":", 2)[1] if ":" in chat_id else ""


def retry_implies_ack_wait(
    retries: int,
    is_dm: bool,
    wait_for_ack: bool,
    ack_timeout: float,
) -> tuple[bool, float]:
    """Retrying a DM implies ACK-waiting so delivery stays observable.

    Returns the possibly-upgraded ``(wait_for_ack, ack_timeout)``. When
    ``MESHTASTIC_SEND_RETRIES > 0`` targets a DM without an ACK wait, waiting
    is forced on with a 30s default timeout — a retry must never re-send a
    chunk whose delivery was already confirmed.
    """
    if retries > 0 and is_dm and not wait_for_ack:
        wait_for_ack = True
        if ack_timeout <= 0:
            ack_timeout = DEFAULT_RETRY_ACK_TIMEOUT
    return wait_for_ack, ack_timeout


def max_send_attempts(retries: int, wait_for_ack: bool, is_dm: bool) -> int:
    """Attempts allowed per chunk: ``retries + 1`` for retry-eligible DMs, else 1.

    Broadcasts have no per-recipient ACK and a non-waiting send cannot observe
    delivery, so neither may retry regardless of the configured retries.
    """
    if retries > 0 and wait_for_ack and is_dm:
        return retries + 1
    return 1


def chunk_send_result(
    content: str, chunk_fn: Callable[[str], list[str]]
) -> tuple[list[str], str | None]:
    """Chunk ``content`` for send, returning ``(chunks, error)``.

    Empty / whitespace-only content and content over the chunk cap fail with an
    explicit error instead of a silent no-op (false success) or an unbounded
    channel flood. ``chunk_fn`` is injected (the adapter's ``_chunk_message``)
    so this stays a pure send-path decision.
    """
    if not content or not content.strip():
        return [], "message content is empty"
    try:
        return chunk_fn(content), None
    except ValueError as exc:
        return [], str(exc)


def chunk_pacing_delay() -> float:
    """Seconds to pause between numbered LoRa chunks (``MESHTASTIC_CHUNK_DELAY``).

    Preserves the original inline semantics exactly: defaults to 4.0, an
    explicit ``0`` is honored, and a non-numeric value raises so a
    misconfiguration surfaces instead of silently disabling pacing. A set-but-
    empty value (``MESHTASTIC_CHUNK_DELAY=""``) falls back to the default like
    every other env reader, so a blank var cannot abort a multi-chunk send
    mid-stream. Non-finite values (``inf`` / ``nan``) also raise: ``inf`` would
    hang ``asyncio.sleep`` on the second chunk forever and ``nan`` never fires
    the timer, so they are a self-DoS that the strict path must surface (the
    drain loop's ``safe_chunk_pacing_delay`` falls back to the default).
    """
    value = float(os.getenv("MESHTASTIC_CHUNK_DELAY") or DEFAULT_CHUNK_DELAY)
    if not math.isfinite(value):
        raise ValueError(f"MESHTASTIC_CHUNK_DELAY must be finite, got {value!r}")
    return value


def safe_chunk_pacing_delay() -> float:
    """Chunk pacing that never raises: falls back to the default on misconfiguration.

    Used by the drain loop, where a garbage ``MESHTASTIC_CHUNK_DELAY`` must not
    raise AFTER an item was already delivered (that would requeue a duplicate);
    the send path's strict ``chunk_pacing_delay`` keeps surfacing the error.
    """
    try:
        return chunk_pacing_delay()
    except (TypeError, ValueError):
        return DEFAULT_CHUNK_DELAY


def drain_retry_decision(error: str | None, attempts: int, max_attempts: int) -> bool:
    """Whether a failed queued send deserves another drain attempt.

    Mirrors ``_send_chunk`` via ``ack_state.is_retriable_failure``: only a
    transient failure (no interface, classified transport error, or an
    unexpected send raise, ``error is None``) is retriable; a permanent
    failure (invalid chat id, missing pubkey, bad format) can never succeed
    and must not block the items behind it. The attempt budget bounds a
    flapping interface so the queue keeps making forward progress.
    """
    if error is not None and error not in ack_state.TRANSIENT_TRANSPORT_ERRORS:
        return False
    return attempts < max_attempts


def should_retry_chunk(
    success: bool,
    attempt: int,
    max_attempts: int,
    retriable: bool,
) -> bool:
    """Whether a failed chunk deserves another send attempt.

    Retry only while the attempt budget remains and the failure is retriable
    (``ack_state.is_retriable_failure``); a success always stops the loop.
    """
    return not success and attempt < max_attempts and retriable


def normalize_dm_dest(dest: str, normalize_fn: Callable[[Any], str | None]) -> str:
    """Canonicalize a ``!``-prefixed DM destination; falls back to the input."""
    if dest.startswith("!"):
        return normalize_fn(dest) or dest
    return dest


def is_executor_shutdown_error(exc: Exception) -> bool:
    """Whether an exception means the transport executor rejected the job.

    ``_DaemonTransportExecutor.submit`` raises the typed
    ``transport.TransportShutdownError`` after ``shutdown``; the send then
    surfaces as ``no_iface`` so the caller queues. Matching on type (rather
    than the stdlib message) keeps the mapping intact across CPython versions
    / runtime implementations; the legacy message check is retained for
    ``RuntimeError`` raised before the typed exception was introduced.
    """
    if isinstance(exc, transport.TransportShutdownError):
        return True
    return "cannot schedule new futures after shutdown" in str(exc).lower()


def map_transport_error(error_code: str | None, dest: str) -> str | None:
    """Human ``SendResult.error`` for a pre-send transport code, else ``None``.

    ``_send_text_serialized`` returns ``"no_iface"`` / ``"no_pubkey"`` /
    ``"no_channel"`` (or ``None`` once sendText ran). Maps to the exact
    gateway-facing error strings — ``_send_chunk`` pattern-matches
    ``ack_state.TRANSIENT_TRANSPORT_ERRORS`` (which includes
    ``NO_INTERFACES_ERROR``) to decide queueing.
    """
    if error_code == "no_iface":
        return NO_INTERFACES_ERROR
    if error_code == "no_pubkey":
        return f"Target node {dest} has no public key; direct message cannot be encrypted"
    if error_code == "no_channel":
        return "Requested channel index is not available on any connected interface"
    return None


def disconnect_ack_record(dest: str, content: str) -> dict[str, Any]:
    """ACK record for a send killed by lifecycle turnover (TIMEOUT/DISCONNECTED).

    Deliberately never stored in the ACK stores — a stale-lifecycle send must
    not pollute the new lifecycle's bookkeeping; it is surfaced only via the
    send's ``raw_response``.
    """
    return {
        "dest": dest,
        "bytes": len(content.encode("utf-8")),
        "status": AckStatus.TIMEOUT,
        "error_reason": "DISCONNECTED",
        "response_at": time.time(),
    }


def disconnect_error(wait_for_ack: bool, pkt_id: str | None) -> str:
    """Human error for a stale-lifecycle send outcome."""
    if wait_for_ack and pkt_id:
        return f"Meshtastic disconnected while waiting for ACK on packet {pkt_id}"
    return "Meshtastic disconnected while transport send was in progress"


def stale_send_raw_response(
    pkt_id: str | None,
    dest: str,
    wait_for_ack: bool,
    ack_timeout: float,
    ack_record: dict[str, Any],
) -> dict[str, Any]:
    """``raw_response`` describing a send interrupted by lifecycle turnover."""
    return {
        "packet_id": pkt_id,
        "dest": dest,
        "ack_requested": True,
        "ack_waited": wait_for_ack,
        "ack_timeout": ack_timeout if wait_for_ack else None,
        "ack": ack_record,
    }


def outbound_raw_response(
    pkt_id: str | None,
    dest: str,
    wait_for_ack: bool,
    ack_timeout: float,
    ack_status: Any,
) -> dict[str, Any]:
    """``raw_response`` for a chunk just handed to the radio (live ACK status)."""
    return {
        "packet_id": pkt_id,
        "dest": dest,
        "ack_requested": True,
        "ack_waited": wait_for_ack,
        "ack_timeout": ack_timeout if wait_for_ack else None,
        "ack": ack_status,
    }


def classify_ack_outcome(
    ack_record: dict[str, Any],
    pkt_id: str,
) -> tuple[bool, str | None]:
    """Map an ACK-wait result to ``(success, error)`` for the caller's SendResult.

    ACK and IMPLICIT_ACK deliver — the mesh carried the packet; the implicit
    case keeps ``status=implicit_ack`` in ``raw_response`` so callers can
    still tell relay from end-to-end confirmation. A NAK fails with its
    reason; a TIMEOUT fails with a disconnected-specific message when the
    record says ``DISCONNECTED``.
    """
    status = ack_record.get("status")
    if status in (AckStatus.ACK, AckStatus.IMPLICIT_ACK):
        return True, None
    if status == AckStatus.NAK:
        reason = ack_record.get("error_reason") or "unknown"
        return False, f"Meshtastic NAK for packet {pkt_id}: {reason}"
    if ack_record.get("error_reason") == "DISCONNECTED":
        return False, f"Meshtastic disconnected while waiting for ACK on packet {pkt_id}"
    return False, f"Meshtastic ACK timeout for packet {pkt_id}"


def waitable_ack_wait(
    pkt_id: str | None,
    ack_future: ConcurrentFuture | None,
) -> tuple[str, ConcurrentFuture] | None:
    """The ``(pkt_id, waiter)`` pair when an ACK wait is viable, else ``None``.

    An ACK wait needs both a packet id (to key the record) and a live future;
    without either the send must fail immediately instead of awaiting. Both
    values narrow together, so callers can pass them straight to the wait.
    """
    if pkt_id and ack_future is not None:
        return pkt_id, ack_future
    return None


def is_node_dest(dest: str) -> bool:
    """Whether a destination is a Meshtastic node id (``!``-hex or node number).

    Group/broadcast destinations (channel names) are not node ids. Node
    numbers are unsigned 32-bit, mirroring ``_normalize_node_id`` — so a
    ``meshtastic:2870135092`` chat id routes to the DM path, not to a channel.
    A bare numeric string is validated with ``isdigit`` after stripping so
    Python ``int()`` quirks (underscore separators like ``"1_000"``) do not
    route a malformed chat id to an unintended DM target; surrounding
    whitespace is tolerated via the strip.
    """
    if dest.startswith("!"):
        return True
    stripped = dest.strip()
    if not stripped.isdigit():
        return False
    value = int(stripped, 10)
    return 0 <= value < 2**32


def resolve_dm_node(dest: str, nodes: dict) -> tuple[str, Any | None]:
    """Locate a DM destination in one interface's node DB.

    Exact key first, then a case-insensitive scan whose match rewrites dest
    to the library's key form (required by sendText). Returns ``(dest, None)``
    when the DB does not know the destination.

    The node DB is the meshtastic library's *live* dict, mutated on its
    background reader thread on every NodeInfo packet (and reset on
    reconnect). This helper runs on the daemon transport worker, i.e.
    concurrent with that reader, so it snapshots ``nodes`` once before
    scanning — a bare ``items()`` scan can raise ``RuntimeError: dictionary
    changed size during iteration`` mid-send, and the snapshot also closes
    the ``dest in nodes`` / ``nodes[dest]`` TOCTOU window.
    """
    snapshot = dict(nodes)
    if dest in snapshot:
        return dest, snapshot[dest]
    dest_bare = dest.lstrip("!")
    for nid, ninfo in snapshot.items():
        if str(nid).lower().lstrip("!") == dest_bare:
            return str(nid), ninfo
    return dest, None


def dm_send_target(dest: str, ifaces: list[Any]) -> tuple[Any, str, bool]:
    """Resolve a DM send: ``(owning_iface, dest, sendable)``.

    The destination goes out on the first interface whose node DB knows it. A
    KNOWN node with no public key is not sendable (encryption impossible,
    ``no_pubkey``) — but only if EVERY interface that knows the node lacks the
    key: a later interface knowing the same node with a public key is strictly
    more capable and is preferred over the keyless owner. A destination
    unknown to every node DB is still attempted on the first interface with
    the original dest (the library may resolve it). Interfaces are duck-typed:
    ``nodes`` is optional.
    """
    if not ifaces:
        raise ValueError("no interfaces to send through")
    keyless: tuple[Any, str] | None = None
    for iface in ifaces:
        nodes = getattr(iface, "nodes", None) or {}
        resolved_dest, node_info = resolve_dm_node(dest, nodes)
        # The node DB is mesh-influenced and may hold non-dict entries on odd
        # firmware; treat a malformed entry as "not known here" rather than
        # letting the ``.get`` below raise AttributeError mid-send.
        if node_info is None or not isinstance(node_info, dict):
            continue
        if not node_info.get("user", {}).get("publicKey"):
            if keyless is None:
                keyless = (iface, resolved_dest)
            continue
        return iface, resolved_dest, True
    if keyless is not None:
        return keyless[0], keyless[1], False
    return ifaces[0], dest, True


def _numeric_channel_spec(spec: str) -> int | None:
    """The integer value of a numeric channel spec, else ``None``.

    Accepts signed integer strings so a ``"-1"`` spec is rejected like any
    other out-of-range index instead of silently falling through to channel 0.
    """
    try:
        return int(spec)
    except ValueError:
        return None


def _channel_index_owners(
    ifaces: list[Any], channel_field: Callable[[Any, str], Any]
) -> dict[int, Any] | None:
    """Map each exposed channel index to the first interface that owns it.

    Returns ``None`` when no interface exposes ``localNode.channels`` — the
    numeric spec cannot be validated and is passed through, matching the
    unvalidated fallback on hardware with no channel table. A non-``None`` (but
    possibly empty) map means a table exists, so an out-of-range index is
    rejected rather than silently broadcast. Each index maps to the FIRST
    interface exposing it (consistent with first-match semantics elsewhere).
    """
    owners: dict[int, Any] | None = None
    for iface in ifaces:
        channels = getattr(getattr(iface, "localNode", None), "channels", None)
        if channels is None:
            continue
        if owners is None:
            owners = {}
        for ch in channels:
            index = channel_field(ch, "index")
            if isinstance(index, int) and index >= 0:
                if index not in owners:
                    owners[index] = iface
    return owners


def channel_send_target(
    parts: list[str],
    ifaces: list[Any],
    channel_field: Callable[[Any, str], Any],
) -> tuple[int | None, Any | None]:
    """Resolve a channel send: ``(channel_index, iface)``.

    Numeric specs map to a raw channel index, validated against the union of
    the interfaces' ``localNode.channels`` when any interface exposes them —
    an out-of-range index returns the ``(None, None)`` reject sentinel instead
    of being handed to the radio unverified. A valid index is dispatched to an
    interface that actually exposes it (channel indexes are per-radio): the
    first owner wins, so a spec present only on a later interface is not
    silently handed to ``ifaces[0]`` which may lack that channel. Named specs
    scan each interface's ``localNode.channels`` for the first case-insensitive
    match, and first match wins across interfaces (matches the DM path's
    first-match semantics in ``dm_send_target``) — the earliest interface
    exposing the name owns the send, even if a later interface also exposes it.
    A named spec that matches NO channel on any channel-table-exposing
    interface returns the same reject sentinel: silently falling back to
    channel 0 would broadcast to the wrong audience while the caller believes
    it addressed the named channel. Only when NO interface exposes a channel
    table is the spec unvalidatable and the default (channel 0 on the first
    interface) is used. ``channel_field`` reads a field from a dict (mock) or
    protobuf Channel (hardware).
    """
    if not ifaces:
        raise ValueError("no interfaces to send through")
    spec = parts[2] if len(parts) > 2 else "0"
    numeric = _numeric_channel_spec(spec)
    if numeric is not None:
        owners = _channel_index_owners(ifaces, channel_field)
        if owners is not None and numeric not in owners:
            return None, None
        # Dispatch to the owning interface when one exposes this index;
        # otherwise (no channel table anywhere) fall back to ifaces[0].
        owner = owners.get(numeric) if owners is not None else None
        return numeric, owner if owner is not None else ifaces[0]
    saw_channel_table = False
    for iface in ifaces:
        channels = getattr(getattr(iface, "localNode", None), "channels", None)
        if channels is None:
            continue
        saw_channel_table = True
        for ch in channels:
            ch_name = channel_field(ch, "name")
            if ch_name and ch_name.lower() == spec.lower():
                return channel_field(ch, "index") or 0, iface
    if saw_channel_table:
        return None, None
    return 0, ifaces[0]
