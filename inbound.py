"""Receive-stage inbound packet pipeline (extracted from ``adapter._on_receive``).

The Meshtastic pubsub framework delivers packets on a background thread; the
adapter marshals them onto its *platform* loop and calls
``MeshtasticAdapter._on_receive`` there. This module owns the receive-stage
decisions that used to live inline in that method:

- packet normalization (dict or protobuf-like envelope → canonical fields,
  ``fromId``/``from`` → ``!``-prefixed node id via the injected normalizer),
- live freshness accounting for every heard node (before any auth gate),
- self-echo filtering,
- observability routing (signal / telemetry / position persistence),
- the authz pre-check that guards the only attacker-controlled path (text),
- text extraction and the metadata / message-id / timestamp decisions.

Everything here is synchronous by design: it runs on the platform loop inside
``_on_receive``. Blocking side effects (SQLite writes) are delegated to writer
callbacks the adapter wires through ``_run_db_write`` (off-loop executor). The
module never imports ``adapter`` (that would be a cycle) — the adapter injects
its state explicitly.

The ACK hooking (``_maybe_record_pubsub_ack``) and the solicited-request
waiter resolution intentionally stay in the adapter: both are owned by state
machines (``ack_state.AckTracker`` / the response-waiter registry) that hold an
adapter back-reference.
"""

import asyncio
import hashlib
import json
import logging
import math
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

try:
    from . import telemetry_db
except ImportError:
    import telemetry_db

try:
    from . import chunking
except ImportError:
    import chunking

try:
    from .mesh_helpers import first_not_none as _first_not_none
    from .mesh_helpers import normalize_position_payload
except ImportError:
    from mesh_helpers import first_not_none as _first_not_none
    from mesh_helpers import normalize_position_payload

logger = logging.getLogger(__name__)

# Portnum constants — values mirror portnums_pb2.PortNum; the string names are
# what MessageToDict emits for real hardware, the ints arrive on numeric
# envelopes (and the mock).
TELEMETRY_PORTNUMS = ("TELEMETRY_APP", 67)
POSITION_PORTNUMS = ("POSITION_APP", 3)
TEXT_PORTNUMS = ("TEXT_MESSAGE_APP", 1, "TEXT_MESSAGE")

# Numeric broadcast destination (0xFFFFFFFF) and the string forms the phone
# apps / mock use.
_BROADCAST_IDS = (4294967295, 0xFFFFFFFF)
_BROADCAST_STRINGS = ("^all", "broadcast", "4294967295", "0xffffffff", "ffffffff", "!ffffffff")

# Defense-in-depth bounds on untrusted envelope values.
# A sane epoch floor for rxTime (anything older is a skewed/hostile clock);
# a cap on persisted node-id length (real node ids are ``!`` + 8 hex = 9 chars).
_RX_TIME_FLOOR = datetime(2020, 1, 1)
_NODE_ID_MAX_LEN = 32

# Envelope keys surfaced in the packet metadata block.
_METADATA_KEYS = (
    "id",
    "rxTime",
    "priority",
    "wantAck",
    "pkiEncrypted",
    "publicKey",
    "nextHop",
    "relayNode",
    "transportMechanism",
)


class FreshnessStore(Protocol):
    """Minimal live-observation overlay the pipeline records into.

    Implemented by ``node_freshness.NodeFreshness``; the protocol keeps the
    pipeline testable with a hand-built fake and free of that import.
    """

    def update(
        self,
        node_id: str,
        rx_time: Any,
        snr: Any,
        rssi: Any,
        hop_count: int | None,
    ) -> None: ...


@dataclass(frozen=True)
class NormalizedPacket:
    """Canonical envelope fields for one inbound packet."""

    sender: str | None
    rx_time: Any
    snr: Any
    rssi: Any
    hop_count: int | None
    hop_limit: Any
    hop_start: Any
    to_id: Any
    channel_index: Any
    decoded: dict[str, Any]
    portnum: Any


@dataclass(frozen=True)
class InboundResult:
    """What the receive stage decided about one packet.

    ``dropped`` means "ignore silently" (telemetry/position routed, self-echo,
    non-text portnum, no extractable text, missing sender). An unauthorized
    TEXT packet comes back with ``dropped=False`` and ``authorized=False`` so
    the adapter keeps its "Unauthorized node skipped" warning; every other
    rejection is silent by design.
    """

    sender: str | None
    kind: str
    dropped: bool
    authorized: bool = False
    text: str | None = None
    decoded: dict[str, Any] = field(default_factory=dict)
    to_id: Any = None
    is_broadcast: bool = False
    channel_index: Any = 0
    snr: Any = None
    rssi: Any = None
    hop_count: int | None = None
    hop_limit: Any = None
    hop_start: Any = None


def _packet_get(packet: Any, *keys: str, default: Any = None) -> Any:
    """Read the first present key from a dict or protobuf-like packet.

    Dicts (the real pubsub/mock shape) use ``in``/``.get`` so a present-but-
    None value is preserved; protobuf-like objects fall back to attribute
    access, treating a missing attribute as absent.
    """
    if isinstance(packet, dict):
        for key in keys:
            if key in packet:
                return packet[key]
        return default
    for key in keys:
        value = getattr(packet, key, None)
        if value is not None:
            return value
    return default


def normalize_packet(packet: Any, normalize_id: Callable[[Any], str | None]) -> NormalizedPacket:
    """Extract the canonical envelope fields from a dict or protobuf packet.

    Field-name mapping mirrors what the meshtastic library and the mock emit:
    ``fromId``/``from`` (either may be a numeric node number or a ``!``-hex
    string), ``rxSnr``/``snr`` and ``rxRssi``/``rssi`` (``rx*`` preferred, with
    is-not-None semantics so a legitimate 0.0 SNR survives), and
    ``hopStart``/``hopLimit`` → hop count. ``decoded`` non-dicts (or absent)
    normalize to ``{}`` so no later stage can crash on a malformed payload.
    """
    sender = normalize_id(_packet_get(packet, "fromId") or _packet_get(packet, "from"))
    # Defense-in-depth: ``_normalize_node_id`` canonicalizes the common shapes
    # (!-prefixed 8-hex, 32-bit ints) but returns ``str(node_id).strip().lower()``
    # verbatim for any other shape — unbounded. Cap it here (one chokepoint for
    # freshness / signal / telemetry / position writers) so a hostile envelope
    # cannot plant an oversized row. Real node ids are 9 chars; the cap is far
    # above that and does not affect auth matching (allowed ids are exact 8-hex).
    if sender is not None and len(sender) > _NODE_ID_MAX_LEN:
        sender = sender[:_NODE_ID_MAX_LEN]
    rx_time = _packet_get(packet, "rxTime")
    snr = _packet_get(packet, "rxSnr")
    if snr is None:
        snr = _packet_get(packet, "snr")
    rssi = _packet_get(packet, "rxRssi")
    if rssi is None:
        rssi = _packet_get(packet, "rssi")
    hop_limit = _packet_get(packet, "hopLimit")
    hop_start = _packet_get(packet, "hopStart")
    hop_count = None
    if hop_limit is not None and hop_start is not None:
        try:
            hop_count = max(0, int(hop_start) - int(hop_limit))
        except (TypeError, ValueError):
            # A hostile/malformed envelope can carry non-int hop fields
            # (strings, mixed types). Drop the hop count rather than let the
            # exception escape into _on_receive's per-packet traceback flood.
            hop_count = None
    to_id = _packet_get(packet, "toId") or _packet_get(packet, "to")
    channel_index = _packet_get(packet, "channel", default=0)
    # A present-but-None channel must not become the literal string "None" in
    # the agent context, nor match the first index-less channel dict in
    # resolve_channel_name (None == None). Coerce to the documented default.
    if channel_index is None:
        channel_index = 0
    decoded = _packet_get(packet, "decoded")
    if not isinstance(decoded, dict):
        decoded = {}
    return NormalizedPacket(
        sender=sender,
        rx_time=rx_time,
        snr=snr,
        rssi=rssi,
        hop_count=hop_count,
        hop_limit=hop_limit,
        hop_start=hop_start,
        to_id=to_id,
        channel_index=channel_index,
        decoded=decoded,
        portnum=decoded.get("portnum"),
    )


def classify_portnum(portnum: Any) -> str:
    """Route a decoded portnum to its pipeline kind.

    ``TELEMETRY_APP`` (67) / ``POSITION_APP`` (3) / ``TEXT_MESSAGE_APP`` (1) —
    MessageToDict usually emits the string names, numeric envelopes the ints.
    Older code treated 4/33 as telemetry/env — those are NODEINFO_APP and
    IP_TUNNEL_APP and must classify as "other".
    """
    if portnum in TELEMETRY_PORTNUMS:
        return "telemetry"
    if portnum in POSITION_PORTNUMS:
        return "position"
    if portnum in TEXT_PORTNUMS:
        return "text"
    return "other"


def is_broadcast_dest(to_id: Any) -> bool:
    """Whether ``to_id`` addresses the whole mesh (broadcast/channel) vs one node."""
    if to_id in _BROADCAST_IDS:
        return True
    if isinstance(to_id, str):
        return to_id.strip().lower() in _BROADCAST_STRINGS
    return False


def canonicalize_to_id(to_id: Any) -> tuple[Any, bool]:
    """Canonicalize a destination id for an authorized TEXT packet.

    Broadcast destinations (numeric ``0xFFFFFFFF`` or the string forms the
    apps/mock use) collapse to the library form ``^all``. A numeric DM dest
    formats as ``!<8hex>`` when it is a valid unsigned 32-bit value; an
    out-of-range int is returned unchanged so a malformed envelope never mints
    a bogus ``!``-id. ``bool`` (a subclass of ``int``) is excluded up-front so
    ``True``/``False`` are not formatted as ``!00000001`` / ``!00000000``.

    Returns the (possibly rewritten) ``to_id`` and the broadcast flag.
    """
    is_broadcast = is_broadcast_dest(to_id)
    if isinstance(to_id, bool):
        return to_id, is_broadcast
    if isinstance(to_id, int):
        if is_broadcast:
            return "^all", True
        if 0 <= to_id < 2**32:
            return f"!{to_id:08x}", False
        return to_id, False  # out-of-range: never mint a malformed !-id
    if isinstance(to_id, str) and is_broadcast:
        return "^all", True
    return to_id, is_broadcast


def is_authorized_node(node_id: str, *, allow_all: bool, allowed_nodes: set[str]) -> bool:
    """Check whether a node ID is permitted to speak with the agent.

    ``allow_all`` bypasses the allowlist; otherwise the id matches with or
    without a single leading ``!`` and case-insensitively. Ids with more than
    one leading ``!`` are malformed and never authorized — stripping every bang
    would let ``"!!<allowed-id>"`` pass the gate.
    """
    if allow_all:
        return True
    nid = node_id.strip().lower()
    if nid.startswith("!!"):
        return False
    bare = nid[1:] if nid.startswith("!") else nid
    return nid in allowed_nodes or bare in allowed_nodes


def extract_text(decoded: dict[str, Any]) -> str | None:
    """Extract the text carried by a TEXT packet (payload bytes or text field).

    Only ``bytes`` payloads (decoded with ``errors="replace"``) and ``str``
    text fields carry message text. Anything else (a dict, an int, ...) is an
    alternate/malformed envelope shape — return ``None`` so the packet drops
    rather than bridging a Python repr to the agent as if it were the message.
    Returns ``None`` when neither is present.
    """
    payload = decoded.get("payload")
    text_field = decoded.get("text")
    if isinstance(payload, bytes):
        return payload.decode("utf-8", errors="replace")
    if isinstance(payload, str):
        return payload
    if isinstance(text_field, str):
        return text_field
    return None


def log_chunk_fragment(text: str, sender: str) -> None:
    """Make an inbound ``[i/n]`` fragment observable instead of silent.

    Reassembly of the numbered fragment format is deliberately left to the
    agent (docs/DEVELOPING.md: the LLM reads the raw prefixes and replies in
    chunks), so this side does not buffer/reassemble. Each fragment is still
    bridged as its own message; this diagnostic just surfaces the format so a
    dropped part is at least discoverable in the logs.
    """
    if chunking.parse_chunk_prefix(text) is not None:
        logger.info(
            "Inbound [i/n] chunk from %s: reassembly is left to the agent; "
            "a missing fragment is not detected on this side",
            sender,
        )


def _packet_fingerprint(packet: Any) -> str:
    """Deterministic message-id fallback for packets lacking both id and rxTime.

    A mesh-level retransmission without a stamped id is byte-identical, so
    hashing the envelope yields the same message id and the gateway's dedup key
    collapses the duplicate. Non-serializable leaves (bytes, protobufs) fall
    back to a stable repr.
    """
    if isinstance(packet, dict):
        try:
            canonical = json.dumps(packet, sort_keys=True, default=_json_leaf)
        except (TypeError, ValueError):
            canonical = repr(packet)
    else:
        canonical = repr(packet)
    return hashlib.sha256(canonical.encode("utf-8", "replace")).hexdigest()[:16]


def _json_leaf(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return repr(value)


def resolve_packet_id(packet: Any) -> str:
    """Message-id for the event — the gateway's dedup key.

    Explicitly distinguishes "absent" (``None``) from a falsy-but-valid 0,
    since ``or`` would skip an id of 0; absent falls back to the radio time,
    and with neither present to a deterministic hash of the packet so a
    retransmission is deduplicated instead of minting a new id per call.
    """
    pkt_id = _packet_get(packet, "id")
    if pkt_id is None:
        pkt_id = _packet_get(packet, "rxTime")
    if pkt_id is not None:
        return str(pkt_id)
    return _packet_fingerprint(packet)


def event_timestamp(packet: Any) -> datetime:
    """Prefer the radio's rxTime so session history reflects airtime, not
    loop-drain time (packets can sit in the incoming queue across reconnects).

    A skewed or garbage rxTime must never drop the message — fall back to now,
    and clamp a far-future rxTime to now (mirroring NodeFreshness) so a hostile
    packet cannot skew session ordering / age computations. A small-negative
    or pre-epoch rxTime is also clamped to now: it parses cleanly through
    ``datetime.fromtimestamp`` but would otherwise produce a 1969-era event
    timestamp (and an authorized node could plant a message at the front of
    session-history ordering).
    """
    event_ts = datetime.now()
    rx_time = _packet_get(packet, "rxTime")
    if rx_time:
        try:
            parsed = datetime.fromtimestamp(float(rx_time))
            if _RX_TIME_FLOOR <= parsed <= event_ts:
                event_ts = parsed
        except (TypeError, ValueError, OverflowError, OSError):
            pass
    return event_ts


def channel_field(ch: Any, key: str) -> Any:
    """Read a channel field from a dict (mock) or a protobuf Channel (hardware).

    ``localNode.channels`` is a list of dicts under the mock interface but a
    list of protobuf ``Channel`` objects on real hardware — those have no
    ``.get()``, and their name lives under ``settings`` (``ch.settings.name``).
    """
    if isinstance(ch, dict):
        return ch.get(key)
    if key == "name":
        settings = getattr(ch, "settings", None)
        return getattr(settings, "name", None) if settings is not None else None
    return getattr(ch, key, None)


def enqueue_incoming(incoming_queue: asyncio.Queue, packet: Any, interface: Any) -> None:
    """Push an inbound packet onto the bounded platform-loop queue.

    Runs on the platform loop (scheduled via the adapter's ``_schedule_on_loop``).
    When the queue is full — a sustained flood faster than the consumer drains —
    shed the OLDEST packet so fresh telemetry/text is not lost behind a stale
    backlog; the queue can never exceed its bound.
    """
    try:
        incoming_queue.put_nowait((packet, interface))
    except asyncio.QueueFull:
        try:
            incoming_queue.get_nowait()
            incoming_queue.task_done()
        except asyncio.QueueEmpty:  # pragma: no cover - single-consumer queue
            pass
        try:
            incoming_queue.put_nowait((packet, interface))
        except asyncio.QueueFull:
            logger.warning("Inbound Meshtastic queue full; dropping packet")


def resolve_channel_name(interface: Any, channel_index: Any) -> str:
    """Display name for a channel index, falling back to the index itself.

    Mirrors the official client's channel naming; returns ``str(index)`` when
    the interface exposes no channel list or the index has no named entry.
    """
    channel_name = str(channel_index)
    if interface and hasattr(interface, "localNode") and hasattr(interface.localNode, "channels"):
        for ch in interface.localNode.channels:
            if channel_field(ch, "index") == channel_index and channel_field(ch, "name"):
                channel_name = channel_field(ch, "name")
                break
    return channel_name


def resolve_sender_name(interface: Any, sender: str) -> str:
    """Long name (falling back to short name) for a node, else its id."""
    sender_name = sender
    if interface and hasattr(interface, "nodes") and sender in interface.nodes:
        user = interface.nodes[sender].get("user", {})
        sender_name = user.get("longName") or user.get("shortName") or sender
    return sender_name


def _format_node_num(num: Any) -> str | None:
    """Format a node number as ``!<8-hex>``, or None if it is unusable."""
    if isinstance(num, int):
        return f"!{num:08x}"
    if num is not None:
        try:
            return f"!{int(num):08x}"
        except (TypeError, ValueError):
            return None
    return None


def interface_node_id(interface: Any, *, normalize_id: Callable[[Any], str | None]) -> str | None:
    """Return the local Meshtastic node ID for an interface, if known.

    Prefers the library's ``getMyNodeInfo()`` (real MeshInterface) which
    returns the node-DB entry including ``user.id``. Falls back to
    ``myInfo.my_node_num`` (protobuf) and the mock's ``getMyNodeId()``.
    """
    if hasattr(interface, "getMyNodeInfo") and callable(interface.getMyNodeInfo):
        try:
            info = interface.getMyNodeInfo()
        except Exception:
            info = None
        if isinstance(info, dict):
            user = info.get("user") or {}
            user_id = user.get("id") if isinstance(user, dict) else None
            if isinstance(user_id, str) and user_id:
                return normalize_id(user_id) or user_id
            formatted = _format_node_num(info.get("num"))
            if formatted is not None:
                return formatted

    my_info = getattr(interface, "myInfo", None)
    my_node_num = None
    if isinstance(my_info, dict):
        my_node_num = my_info.get("my_node_num")
    elif my_info is not None:
        my_node_num = getattr(my_info, "my_node_num", None)

    formatted = _format_node_num(my_node_num)
    if formatted is not None:
        return formatted

    get_my = getattr(interface, "getMyNodeId", None)
    if callable(get_my):
        try:
            return normalize_id(get_my())
        except Exception:
            return None
    return None


def build_packet_context(
    packet: Any,
    *,
    sender: str,
    sender_name: str,
    to_id: Any,
    chat_id: str,
    chat_type: str,
    channel_index: Any,
    snr: Any,
    rssi: Any,
    hop_count: int | None,
    hop_limit: Any,
    hop_start: Any,
) -> str:
    """Build the compact LoRa metadata block attached to the MessageEvent.

    Keep this compact but include the LoRa metadata that matters for
    agent decisions/debugging: link metrics, hop envelope, and the raw
    envelope keys (id, rxTime, wantAck, publicKey presence, ...).

    Numeric fields are coerced through the finite-numeric filter first so a
    hostile or malformed envelope value (NaN, a string) cannot render as a
    literal ``nan`` / repr into the agent-visible context — mirroring the
    coercion applied at every other persistence boundary.
    """
    snr = _finite_or_none(snr)
    rssi = _finite_or_none(rssi)
    hop_count = _finite_or_none(hop_count)
    hop_limit = _finite_or_none(hop_limit)
    hop_start = _finite_or_none(hop_start)
    meta_lines = ["[Meshtastic packet metadata]"]
    meta_lines.append(f"from: {sender} ({sender_name})")
    meta_lines.append(f"to: {to_id}")
    meta_lines.append(f"chat_scope: {chat_id} ({chat_type})")
    meta_lines.append(f"channel: {channel_index}")
    if snr is not None:
        meta_lines.append(f"rx_snr: {snr} dB")
    if rssi is not None:
        meta_lines.append(f"rx_rssi: {rssi} dBm")
    if hop_count is not None:
        meta_lines.append(f"hop_count: {hop_count}")
    if hop_limit is not None:
        meta_lines.append(f"hop_limit: {hop_limit}")
    if hop_start is not None:
        meta_lines.append(f"hop_start: {hop_start}")
    if isinstance(packet, dict):
        for key in _METADATA_KEYS:
            if key in packet:
                val = packet.get(key)
                if key == "publicKey":
                    val = "present" if val else "absent"
                meta_lines.append(f"{key}: {val}")
    return "\n".join(meta_lines)


def _coerce_float(value: Any) -> float | None:
    """Coerce an untrusted telemetry/position field to a finite float.

    Non-numeric strings, bools, and NaN/inf are rejected (``None``) so a
    malformed value drops that field instead of poisoning the persisted row.
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        num = float(value)
    except (TypeError, ValueError, OverflowError):
        # A huge int (e.g. 10**400) overflows float() with OverflowError; catch
        # it so a hostile huge-int field drops instead of crashing the inbound
        # path (mirrors node_freshness._coerce_float / chunking).
        return None
    return num if math.isfinite(num) else None


def _coerce_int(value: Any) -> int | None:
    """Coerce an untrusted field to an int (via the float guard, rejecting NaN)."""
    num = _coerce_float(value)
    return int(num) if num is not None else None


def _finite_or_none(value: Any) -> Any:
    """Return ``value`` if it is a finite number, else ``None``.

    Unlike ``_coerce_float``, this preserves the original numeric type so
    context rendering keeps the envelope's presentation (e.g. an int rssi of
    ``-95`` renders as ``-95``, not ``-95.0``). Bools are excluded
    (``bool ⊂ int``); non-numeric strings, ``NaN``, and ``±inf`` drop.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    try:
        num = float(value)
    except (TypeError, ValueError, OverflowError):
        # A huge-int string (e.g. "1e400") overflows float() with OverflowError;
        # catch it so a hostile metadata value cannot crash build_packet_context
        # (mirrors _coerce_float / node_freshness._coerce_float).
        return None
    return num if math.isfinite(num) else None


def log_telemetry_packet(node_id: str, decoded: dict[str, Any]) -> None:
    """Extract device/environment metrics from a telemetry payload and persist them.

    Runs off the event loop (via the adapter's DB-write executor). ``0`` is a
    real value — batteryLevel 0 means external power on many devices, so
    truthiness must not drop it. Real mesh dicts use uptimeSeconds
    (MessageToDict); accept legacy "uptime" too (mock / older payloads).

    Every field is coerced through a finite-numeric filter first: these writes
    happen BEFORE the auth gate, so a hostile node must not be able to plant
    non-numeric / NaN / absurd values into the telemetry the agent reads back.
    """
    try:
        telemetry = decoded.get("telemetry", {})
        if not telemetry:
            telemetry = decoded

        metrics = telemetry.get("deviceMetrics", {}) or {}
        env = telemetry.get("environmentMetrics", {}) or {}

        battery = _first_not_none(metrics.get("batteryLevel"), telemetry.get("batteryLevel"))
        voltage = _first_not_none(metrics.get("voltage"), telemetry.get("voltage"))
        uptime = _first_not_none(
            metrics.get("uptimeSeconds"),
            metrics.get("uptime"),
            telemetry.get("uptimeSeconds"),
            telemetry.get("uptime"),
        )

        temp = _first_not_none(
            env.get("temperature"),
            env.get("barometric_temperature"),
            telemetry.get("temperature"),
        )
        humidity = _first_not_none(env.get("relativeHumidity"), telemetry.get("relativeHumidity"))
        pressure = _first_not_none(
            env.get("barometricPressure"), telemetry.get("barometricPressure")
        )

        battery = _coerce_int(battery)
        voltage = _coerce_float(voltage)
        uptime = _coerce_int(uptime)
        temp = _coerce_float(temp)
        humidity = _coerce_float(humidity)
        pressure = _coerce_float(pressure)

        if any(val is not None for val in (battery, voltage, temp, humidity, pressure, uptime)):
            telemetry_db.log_telemetry(
                node_id=node_id,
                battery_level=battery,
                voltage=voltage,
                temperature=temp,
                humidity=humidity,
                pressure=pressure,
                uptime=uptime,
            )
            logger.debug(f"Logged telemetry for node {node_id}")
    except Exception as e:
        logger.error(f"Error logging telemetry packet: {e}")


def log_position_packet(node_id: str, decoded: dict[str, Any]) -> None:
    """Extract coordinates from a position payload and persist them.

    Runs off the event loop. Real hardware delivers ``MessageToDict``
    (``latitudeI``/``longitudeI``) while the mock uses ``latitude``/
    ``longitude``; ``normalize_position_payload`` accepts both, undoes the
    protobuf 1e7 scale, and filters to finite floats clamped to valid lat/lon
    ranges so a hostile node cannot persist absurd values the agent reads back.
    """
    try:
        pos = decoded.get("position", {}) or decoded
        norm = normalize_position_payload(pos)
        lat, lon, alt = norm["latitude"], norm["longitude"], norm["altitude"]

        if lat is not None and lon is not None:
            telemetry_db.log_position(node_id=node_id, latitude=lat, longitude=lon, altitude=alt)
            logger.debug(f"Logged position for node {node_id}")
    except Exception as e:
        logger.error(f"Error logging position packet: {e}")


class InboundProcessor:
    """Receive-stage packet pipeline: normalize → observe → route → authorize.

    Runs synchronously on the platform loop (invoked from the adapter's queue
    consumer). Dependencies are injected explicitly — the adapter is never
    imported (that would be a cycle), so the pipeline is unit-testable with
    hand-built packets, a fake freshness store, and recording writers.

    Writers are optional: with none wired the pipeline performs no I/O and is
    a pure classifier, which is how the stage unit tests exercise it.
    """

    def __init__(
        self,
        *,
        normalize_id: Callable[[Any], str | None],
        freshness: FreshnessStore,
        allow_all: Callable[[], bool],
        allowed_nodes: Callable[[], set[str]],
        write_signal: Callable[[str, Any, Any, int | None], None] | None = None,
        write_telemetry: Callable[[str, dict[str, Any]], None] | None = None,
        write_position: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> None:
        self._normalize_id = normalize_id
        self._freshness = freshness
        self._allow_all = allow_all
        self._allowed_nodes = allowed_nodes
        self._write_signal = write_signal
        self._write_telemetry = write_telemetry
        self._write_position = write_position

    def process(self, packet: Any, *, my_node_id: str | None = None) -> InboundResult:
        """Run the receive-stage pipeline for one packet.

        ``my_node_id`` is the local node's canonical id (from the interface),
        used to filter self-echoes before the auth gate — the local node is
        normally NOT in the allowlist, so checking auth first would log every
        self-echo as "Unauthorized" and make the filter unreachable.
        """
        norm = normalize_packet(packet, self._normalize_id)
        if norm.sender is None:
            return InboundResult(sender=None, kind="skipped", dropped=True)

        # Freshness for EVERY heard node — before the auth gate, so
        # last_heard/signal stay current even for nodes that aren't allowed to
        # talk to Hermes (e.g. a node the user just wants to watch).
        self._freshness.update(norm.sender, norm.rx_time, norm.snr, norm.rssi, norm.hop_count)

        if my_node_id and norm.sender == my_node_id:
            return self._outcome(norm, "echo", dropped=True)

        # Observability (signal / telemetry / position) is recorded for EVERY
        # heard node, BEFORE the auth gate — same rationale as freshness. The
        # allowlist controls who may *talk to the agent* (the prompt-injection
        # surface), not what the agent may see of the mesh. Safe pre-auth:
        # these handlers persist numeric fields only. The signal fields are
        # coerced here too (they arrive as raw envelope values that can be
        # strings or NaN), matching the telemetry/position handlers' coercion.
        snr = _coerce_float(norm.snr)
        rssi = _coerce_float(norm.rssi)
        hop_count = _coerce_int(norm.hop_count)
        if snr is not None or rssi is not None:
            if self._write_signal is not None:
                self._write_signal(norm.sender, snr, rssi, hop_count)

        kind = classify_portnum(norm.portnum)
        if kind == "telemetry":
            if self._write_telemetry is not None:
                self._write_telemetry(norm.sender, norm.decoded)
            return self._outcome(norm, "telemetry", dropped=True)
        if kind == "position":
            if self._write_position is not None:
                self._write_position(norm.sender, norm.decoded)
            return self._outcome(norm, "position", dropped=True)
        if kind != "text":
            return self._outcome(norm, "other", dropped=True)

        # The authz pre-check guards the only path carrying attacker-controlled
        # text into the agent. The verdict rides back in the result so the
        # adapter keeps its "Unauthorized node skipped" warning.
        authorized = is_authorized_node(
            norm.sender, allow_all=self._allow_all(), allowed_nodes=self._allowed_nodes()
        )
        if not authorized:
            return self._outcome(norm, "unauthorized", dropped=False, authorized=False)

        text = extract_text(norm.decoded)
        if text is None:
            return self._outcome(norm, "no_text", dropped=True, authorized=True)
        log_chunk_fragment(text, norm.sender)

        to_id, is_broadcast = canonicalize_to_id(norm.to_id)
        return self._outcome(
            norm,
            "text",
            dropped=False,
            authorized=True,
            text=text,
            to_id=to_id,
            is_broadcast=is_broadcast,
        )

    def _outcome(
        self,
        norm: NormalizedPacket,
        kind: str,
        *,
        dropped: bool,
        authorized: bool = False,
        text: str | None = None,
        to_id: Any = None,
        is_broadcast: bool = False,
    ) -> InboundResult:
        """Build a result carrying the envelope fields the adapter needs."""
        return InboundResult(
            sender=norm.sender,
            kind=kind,
            dropped=dropped,
            authorized=authorized,
            text=text,
            decoded=norm.decoded,
            to_id=norm.to_id if to_id is None else to_id,
            is_broadcast=is_broadcast,
            channel_index=norm.channel_index,
            snr=norm.snr,
            rssi=norm.rssi,
            hop_count=norm.hop_count,
            hop_limit=norm.hop_limit,
            hop_start=norm.hop_start,
        )
