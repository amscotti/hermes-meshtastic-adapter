"""
Pure helpers shared by the mesh_* tool handlers.

Extracted from mesh_tools.py (maintainability P3.4): these helpers carry no
adapter coupling beyond the ``adapter_instance`` passed in as an argument, so
they are unit-testable without a live Meshtastic connection. mesh_tools.py
re-imports them (dual-import convention, see CLAUDE.md); this module never
imports mesh_tools or adapter — telemetry_db is its only repo dependency.
"""

import math
import time
from typing import Any

try:
    from . import telemetry_db
except ImportError:
    import telemetry_db

# How long a 0-hop reception keeps counting as "in direct range". Signal history
# is kept for 30 days, so without a bound a node heard directly weeks ago would
# still report as a neighbour. A day comfortably covers nodes that only speak up
# occasionally, while a node that has moved or gone quiet drops out on its own.
DIRECT_RANGE_WINDOW_SECS = 24 * 3600

# Position fixes age the same way, but far more consequentially: a stale fix
# still plots as a confident dot on a coverage map. Beyond this, callers are
# told the fix is old rather than left to assume it is current.
POSITION_STALE_AFTER_SECS = 6 * 3600

# Ceiling for a time-windowed history request. The window may exceed the
# retention period harmlessly (there is simply nothing older), but the row cap
# keeps one busy node's month of fixes from flooding a reply: the chattiest node
# here logs ~53 positions a day, so 500 rows is still well over a week.
HISTORY_MAX_WINDOW_HOURS = 30 * 24
HISTORY_WINDOW_ROW_CAP = 500

__all__ = [
    "DIRECT_RANGE_WINDOW_SECS",
    "HISTORY_MAX_WINDOW_HOURS",
    "HISTORY_WINDOW_ROW_CAP",
    "POSITION_STALE_AFTER_SECS",
    "assess_signal_quality",
    "clamp",
    "coerce_float",
    "decode_snr_value",
    "device_uptime",
    "fetch_history_rows",
    "first_not_none",
    "format_node_summary",
    "format_route",
    "history_window_params",
    "link_facts",
    "node_display_name",
    "normalize_position_payload",
    "numeric_epoch",
    "position_age",
    "requested_node",
    "resolve_node",
    "signal_source_for_hops",
]


def _name_matches(
    query: str, query_norm: str, interfaces: list
) -> list[tuple[Any, dict[str, Any]]]:
    """Every distinct node whose long/short name equals the query.

    The same node seen on several interfaces counts once (deduped by id), so a
    node present on two interfaces never looks like a name collision. Names are
    matched against both the raw query and the bang-stripped form, so a
    ``!alpha`` query still finds a node named "Alpha".
    """
    matches: list[tuple[Any, dict[str, Any]]] = []
    seen: set[str] = set()
    for iface in interfaces:
        nodes = getattr(iface, "nodes", {}) or {}
        # Snapshot: the library mutates this dict on its background receive
        # thread, so iterating the live view can raise "dictionary changed size
        # during iteration" between two steps.
        for nid, info in list(nodes.items()):
            user = info.get("user", {})
            long_name = str(user.get("longName", "")).lower()
            short_name = str(user.get("shortName", "")).lower()
            if (
                query == long_name
                or query == short_name
                or query_norm == long_name
                or query_norm == short_name
            ):
                key = str(user.get("id") or nid)
                if key in seen:
                    continue
                seen.add(key)
                matches.append((iface, info))
    return matches


def resolve_node(
    node_id_or_name: str, adapter_instance: Any
) -> tuple[Any | None, dict[str, Any] | None]:
    """
    Search all active interfaces (serial or TCP) for a node matching the ID or name.

    Returns (interface, node_info_dict).
    """
    if not node_id_or_name:
        return None, None
    # A numeric/boolean node_id from a model must degrade to a clean JSON error,
    # not an AttributeError on .strip(). Coerce so the lookup just misses.
    if not isinstance(node_id_or_name, str):
        node_id_or_name = str(node_id_or_name)

    query = node_id_or_name.strip().lower()
    query_norm = query.lstrip("!")

    # Try resolving across all interfaces
    interfaces = adapter_instance.get_interfaces()

    # 1. Direct ID lookup (exact with or without '!'). Ids are unique to a node,
    # so the first interface that knows it is authoritative. Keys can be the
    # "!hex" string or a numeric node number (mesh_list_nodes-style iterations
    # see int keys) — both normalize to the !hex form.
    for iface in interfaces:
        nodes = getattr(iface, "nodes", {}) or {}
        # Snapshot — see _name_matches: the library mutates iface.nodes off-thread.
        for nid, info in list(nodes.items()):
            nid_lower = f"!{nid:08x}" if isinstance(nid, int) else str(nid).lower()
            if query == nid_lower or query_norm == nid_lower.lstrip("!"):
                return iface, info

    # 2. Name search (long name or short name). A name is only as good as its
    # uniqueness: longName comes from NodeInfo packets any node can send, so a
    # hostile node can squat a trusted node's display name. When two distinct
    # nodes share the queried name, refuse to pick one — silently returning the
    # first match would DM whichever node claimed the name first.
    name_matches = _name_matches(query, query_norm, interfaces)
    if name_matches:
        if len(name_matches) == 1:
            return name_matches[0]
        return None, None

    # 3. Numeric string ID lookup
    for iface in interfaces:
        nodes = getattr(iface, "nodes", {}) or {}
        # Snapshot — see _name_matches: the library mutates iface.nodes off-thread.
        for _nid, info in list(nodes.items()):
            num = info.get("num")
            if num is not None and query == str(num):
                return iface, info

    return None, None


def assess_signal_quality(snr: float | None) -> str:
    """Classify signal quality based on SNR (Signal to Noise Ratio)."""
    # A non-numeric / non-finite snr (string, bool, NaN) must not raise on the
    # comparison or render as a literal NaN — reject it up front like the
    # finite filters elsewhere in this module.
    if isinstance(snr, bool) or not isinstance(snr, (int, float)):
        return "Unknown"
    snr = float(snr)
    if not math.isfinite(snr):
        return "Unknown"
    if snr >= 8.0:
        return "Excellent"
    elif snr >= 3.0:
        return "Good"
    elif snr >= -3.0:
        return "Fair"
    elif snr >= -12.0:
        return "Poor"
    else:
        return "No signal"


def first_not_none(*values: Any) -> Any:
    """Return the first value that is not None (0 / 0.0 are kept).

    Mirrored as ``MeshtasticAdapter._first_not_none`` in adapter.py; keep both
    in sync (the tools module loads as ``meshtastic_tools`` and cannot import
    the adapter at module load without a cycle risk through the gateway stack).
    """
    for value in values:
        if value is not None:
            return value
    return None


def device_uptime(metrics: dict[str, Any] | None) -> Any:
    """Read uptime from node metrics (real mesh uses uptimeSeconds)."""
    metrics = metrics or {}
    return first_not_none(metrics.get("uptimeSeconds"), metrics.get("uptime"))


def position_age(pos: dict[str, Any] | None, node_id: str) -> dict[str, Any]:
    """Date a position fix, so an old one can't pass for the node's current spot.

    Coordinates from the node DB carry no age of their own, and a fix from last
    week plots on a coverage map exactly like one from a minute ago — confident,
    and wrong. The library's ``position.time`` is preferred; our persisted
    history fills in when the node DB has coordinates but no timestamp.

    **Note: this does I/O.** The no-timestamp fallback runs a synchronous
    ``telemetry_db.get_position_history`` read, so async callers must off-load
    it (``await asyncio.to_thread(position_age, pos, node_id)``) — never call it
    inline on the event loop.
    """
    pos = pos or {}
    fix_time = first_not_none(pos.get("time"), pos.get("timestamp"))
    if fix_time is None and node_id:
        recorded = telemetry_db.get_position_history(node_id, limit=1)
        if recorded:
            fix_time = recorded[0].get("timestamp")
    if not fix_time:
        return {"position_time": None, "position_age_hours": None, "position_is_stale": None}
    # fix_time is untrusted at the boundary — a hostile/malformed Position
    # payload could carry a truthy non-numeric `time` that survives the
    # truthiness gate above and would crash float(). Route it through the same
    # finite filter the rest of this module applies (matching the guard
    # numeric_epoch already gives lastHeard); a non-numeric value yields the
    # nulls dict rather than a ValueError out of the tool handler.
    fix_epoch = coerce_float(fix_time)
    if fix_epoch is None:
        return {"position_time": None, "position_age_hours": None, "position_is_stale": None}
    age = time.time() - fix_epoch
    return {
        "position_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(fix_epoch)),
        "position_age_hours": round(age / 3600, 1),
        "position_is_stale": age > POSITION_STALE_AFTER_SECS,
    }


def signal_source_for_hops(hops: int | None) -> str:
    """Classify a signal reading by the hop count of the row it came from."""
    if hops == 0:
        return "direct"
    if hops is not None:
        return "relayed"
    return "unknown"


def link_facts(
    info: dict,
    obs: dict,
    latest: dict | None = None,
    latest_direct: dict | None = None,
) -> dict[str, Any]:
    """Resolve how far a node is and whether its signal actually describes it.

    **Hops** come from the freshest source that knows: live observations, then
    the library node DB (``hopsAway`` — what the official app shows), then our
    persisted history. That last fallback is the only one that survives a
    gateway restart, which wipes the in-memory observations.

    **Signal** is attributed to the node itself only when it came off a 0-hop
    packet. A relayed packet's SNR/RSSI describe the last hop, not the origin,
    so presenting them as the node's own signal is actively misleading: asked
    which nodes were in direct range, the agent had no hop data in this payload
    and answered by picking the ones with an RSSI — listing nodes 1 to 5 hops
    out as directly audible. Signal strength cannot stand in for distance;
    locally heard nodes here span −60 to −112 dBm, fully overlapping the
    relayed ones.
    """
    # Live hops describe the node *now* (this session's packets, and the node DB
    # the library keeps current); the persisted fallback may be weeks old, so it
    # fills in the reported distance but cannot by itself assert direct range.
    live_hops = first_not_none(obs.get("hops_away"), info.get("hopsAway"))
    hops = first_not_none(live_hops, (latest or {}).get("hop_count"))

    snr = rssi = None
    source = "unknown"
    if obs.get("snr") is not None or obs.get("rssi") is not None:
        # _update_observed records these off direct packets only.
        snr, rssi, source = obs.get("snr"), obs.get("rssi"), "direct"
    elif latest_direct:
        snr, rssi, source = latest_direct.get("snr"), latest_direct.get("rssi"), "direct"
    else:
        # obs empty: the reading can only come from the node DB or the persisted
        # history. Their provenances can disagree — hopsAway describes the node
        # DB's latest path, a persisted row its own hop_count — so the
        # attribution must follow the row that actually supplied the reading
        # (AUG-TODO A3): a direct history sample must not be labelled "relayed"
        # against a different row's hops, and vice versa.
        if info.get("snr") is not None or info.get("rssi") is not None:
            snr, rssi, source = (
                info.get("snr"),
                info.get("rssi"),
                # The reading came from the node DB row, so its own hopsAway
                # classifies it — obs hops describe a different (possibly
                # relayed) observation of the same node (AUG-TODO A3).
                signal_source_for_hops(info.get("hopsAway")),
            )
        else:
            hist = latest or {}
            if hist.get("snr") is not None or hist.get("rssi") is not None:
                snr, rssi, source = (
                    hist.get("snr"),
                    hist.get("rssi"),
                    signal_source_for_hops(hist.get("hop_count")),
                )

    # "In direct range" is about having heard the node over the air, not about
    # the path the newest packet happened to take: successive packets from one
    # node routinely arrive direct and relayed as the mesh reroutes, so keying
    # this off the latest hop count alone would flip a neighbour in and out of
    # range packet by packet. hops_away stays the *latest* distance.
    #
    # It does expire, though. The signal history is retained for 30 days, and
    # without a window a node heard directly three weeks ago — since moved, or
    # switched off — would report as in direct range forever. Live observations
    # and a current 0-hop reading need no window: both describe now.
    now = time.time()
    last_direct = (latest_direct or {}).get("timestamp")
    observed_live = obs.get("snr") is not None or obs.get("rssi") is not None
    direct_recently = last_direct is not None and (now - last_direct) <= DIRECT_RANGE_WINDOW_SECS
    return {
        # Signal is receiver-measured but can still arrive non-finite from a
        # malformed envelope (NaN is representable in the float field): filter
        # every source at the render boundary so the tools never emit a literal
        # NaN or crash assess_signal_quality on a string.
        "snr": coerce_float(snr),
        "rssi": coerce_float(rssi),
        "hops_away": hops,
        "heard_directly": _asserts_direct_range(
            obs,
            live_hops,
            observed_live,
            obs_recent=_obs_is_recent(obs, now),
            direct_recently=direct_recently,
        ),
        "signal_source": source,
        "last_direct_heard": (
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last_direct)) if last_direct else None
        ),
        "last_direct_heard_age_hours": (
            round((now - last_direct) / 3600, 1) if last_direct else None
        ),
    }


def _obs_is_recent(obs: dict, now: float) -> bool:
    """Whether a live-observed overlay entry still counts as current.

    The overlay keeps its values for the whole session with no expiry of its
    own, so a direct observation (or a 0-hop hops_away) recorded in it needs the
    same window as the persisted history: a node heard directly at session start
    and silent for weeks must not keep asserting direct range. ``obs.last_heard``
    ages it. An entry without a last_heard has not been through NodeFreshness
    yet and is treated as current.
    """
    heard = obs.get("last_heard")
    if heard is None:
        return True
    try:
        return (now - float(heard)) <= DIRECT_RANGE_WINDOW_SECS
    except (TypeError, ValueError):
        return True


def _asserts_direct_range(
    obs: dict,
    live_hops: Any,
    observed_live: bool,
    *,
    obs_recent: bool,
    direct_recently: bool,
) -> bool:
    """Whether the freshest sources say the node is in direct range.

    Live signal and a 0-hop overlay hops_away assert only while the overlay
    entry is recent (``_obs_is_recent``); the library node DB's hopsAway — which
    the library keeps current — asserts on its own, and so does a persisted
    direct row inside its window (``direct_recently``).
    """
    direct = observed_live and obs_recent
    obs_hops = obs.get("hops_away")
    if obs_hops is not None:
        direct = direct or (obs_recent and obs_hops == 0)
    elif live_hops == 0:
        direct = True
    return bool(direct or direct_recently)


def format_node_summary(
    adapter_inst: Any,
    nid: str,
    info: dict,
    latest_by_node: dict,
    latest_direct_by_node: dict,
) -> dict[str, Any]:
    """Build the per-node summary entry for mesh_list_nodes.

    One node's snapshot: identity fields, the live-observed overlay (fresher
    than the library node DB, which only refreshes lastHeard/signal from
    periodic NodeInfo packets), and the link facts resolved from the freshest
    source that knows. Deduplication across interfaces stays in the handler.
    """
    user = info.get("user", {})
    metrics = info.get("deviceMetrics", {})

    obs = adapter_inst.get_observed_node(nid)
    link = link_facts(info, obs, latest_by_node.get(nid), latest_direct_by_node.get(nid))

    # last_heard: freshest of the library value and what we've observed.
    # Both fields are untrusted at the boundary — a hostile/malformed NodeInfo
    # could carry a truthy non-numeric lastHeard that survives `or 0` and
    # crashes max(); numeric_epoch guards the type first.
    last_heard = (
        max(numeric_epoch(info.get("lastHeard")), numeric_epoch(obs.get("last_heard"))) or None
    )
    last_heard_str = "Never"
    if last_heard:
        last_heard_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last_heard))

    return {
        "node_id": nid,
        "long_name": user.get("longName", "Unknown"),
        "short_name": user.get("shortName", "???"),
        "hw_model": user.get("hwModel", "Unknown"),
        "role": user.get("role", "Unknown"),
        "battery_level": metrics.get("batteryLevel", "N/A"),
        "snr": link["snr"] if link["snr"] is not None else "N/A",
        "rssi": link["rssi"] if link["rssi"] is not None else "N/A",
        "signal_quality": assess_signal_quality(link["snr"]),
        "signal_source": link["signal_source"],
        "hops_away": link["hops_away"],
        "heard_directly": link["heard_directly"],
        "last_direct_heard": link["last_direct_heard"],
        "last_direct_heard_age_hours": link["last_direct_heard_age_hours"],
        "last_heard": last_heard_str,
    }


def history_window_params(args: dict) -> tuple[int, float | None, str | None]:
    """Validate the since_hours window and limit args for mesh_telemetry_history.

    Returns ``(limit, since, error)`` — ``error`` is a ready-to-return message
    when an arg is malformed. A period ("the last 3 days") and a count ("the
    last 10 rows") answer different questions, and a count cannot stand in for
    a period: how far back N rows reach depends entirely on how chatty the node
    is — 100 rows is five days for one node here and a month for another.
    Asking by time therefore raises the cap, since the window, not the number,
    is the ask.
    """
    since_hours = args.get("since_hours")
    since = None
    if since_hours is not None:
        # A bool coerces to a number (float(True) == 1.0), silently asking for
        # a 1-hour window — a plausible LLM tokenization accident; reject it
        # like any other non-number.
        if isinstance(since_hours, bool):
            return 10, None, "Parameter 'since_hours' must be a number."
        try:
            hours = float(since_hours)
        except (TypeError, ValueError):
            return 10, None, "Parameter 'since_hours' must be a number."
        # NaN survives float() and the positivity check (nan <= 0 is False), then
        # poisons `since = time.time() - nan*3600` and the caller's
        # time.localtime(nan) raises — reject non-finite explicitly.
        if not math.isfinite(hours):
            return 10, None, "Parameter 'since_hours' must be a number."
        if hours <= 0:
            return 10, None, "Parameter 'since_hours' must be positive."
        hours = min(hours, HISTORY_MAX_WINDOW_HOURS)
        since = time.time() - hours * 3600

    default_limit = HISTORY_WINDOW_ROW_CAP if since is not None else 10
    row_cap = HISTORY_WINDOW_ROW_CAP if since is not None else 100
    limit_arg = args.get("limit", default_limit)
    if isinstance(limit_arg, bool):
        return default_limit, since, "Parameter 'limit' must be a number."
    try:
        limit = min(max(1, int(limit_arg)), row_cap)
    except (TypeError, ValueError):
        return default_limit, since, "Parameter 'limit' must be a number."
    return limit, since, None


def fetch_history_rows(
    metric_type: str, node_id: str, limit: int, since: float | None
) -> list[dict] | None:
    """Dispatch a history request to the right telemetry_db table.

    Returns ``None`` for an unknown ``metric_type`` — the caller answers with
    the invalid-type error, and no table getter is ever called. The table
    getters themselves always return a list, so ``None`` is unambiguous.
    """
    if metric_type == "telemetry":
        return telemetry_db.get_telemetry_history(node_id, limit=limit, since=since)
    elif metric_type == "positions":
        return telemetry_db.get_position_history(node_id, limit=limit, since=since)
    elif metric_type == "signal_quality":
        return telemetry_db.get_signal_history(node_id, limit=limit, since=since)
    return None


def clamp(value: Any, default: float, low: float, high: float) -> float:
    """Coerce a model-supplied number into a sane range.

    Non-numeric input and NaN/±inf fall back to ``default``: NaN compares
    False against both bounds, so ``min(high, nan)`` silently returns ``high``
    — a ``timeout: nan`` argument would otherwise become the max wait.
    """
    try:
        num = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(num):
        return default
    return max(low, min(high, num))


def coerce_float(value: Any) -> float | None:
    """Coerce an untrusted numeric field to a finite float.

    Non-numeric strings, bools, and NaN/inf are rejected (``None``) so a
    hostile/malformed value drops that field instead of rendering as a
    non-standard ``NaN``/``Infinity`` JSON literal (the same finite filter the
    inbound path applies before persisting).
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        num = float(value)
    except (TypeError, ValueError, OverflowError):
        # A huge int (e.g. 10**400) overflows float() with OverflowError; catch
        # it so a hostile huge-int field drops instead of crashing the caller
        # (mirrors node_freshness._coerce_float / chunking._effective_chunk_bytes).
        return None
    return num if math.isfinite(num) else None


def numeric_epoch(value: Any) -> float:
    """Coerce a ``lastHeard``-style field to a finite epoch, else ``0``.

    ``lastHeard`` is a protobuf ``fixed32`` (int) on well-formed NodeInfo, but
    the field is untrusted at the boundary: a truthy non-numeric value (e.g. a
    hostile string) survives ``value or 0`` and would crash ``max()`` with a
    ``TypeError``. Returns ``0`` (not ``None``) so the existing
    ``max(...) or None`` call sites keep working unchanged.
    """
    if isinstance(value, bool):
        return 0.0
    if not isinstance(value, (int, float)):
        return 0.0
    if not math.isfinite(value):
        return 0.0
    return float(value)


def decode_snr_value(raw: Any) -> float | None:
    """Decode one firmware SNR entry to a finite dB float, or ``None``.

    Firmware packs SNR as ``int`` (raw × 4); ``-128`` is the "unknown"
    sentinel. A non-numeric entry (string, ``None``, list) is filtered to
    ``None`` rather than raising on the division — the type guard
    ``format_route`` already applies per-hop. Shared by ``format_route`` and
    the traceroute handler's per-segment lists so the two paths cannot drift
    (a hostile reply cannot smuggle a ``TypeError`` or a non-finite literal).
    """
    if not isinstance(raw, (int, float)) or isinstance(raw, bool):
        return None
    return coerce_float(None if raw == -128 else raw / 4.0)


def normalize_position_payload(pos: Any) -> dict[str, Any]:
    """Normalize an untrusted Position payload to finite decimal degrees.

    Real hardware delivers ``MessageToDict(Position(...))`` where the protobuf
    fields are camelCase (``latitudeI``/``longitudeI``); the mock and older
    shapes use ``latitude``/``longitude``. Both are accepted, the protobuf 1e7
    scale is undone when present, and every field is filtered to a finite float
    clamped to valid lat/lon ranges so a hostile node cannot plant NaN or
    absurd values.
    """
    if not isinstance(pos, dict):
        return {"latitude": None, "longitude": None, "altitude": None}
    # Fall back only when the camelCase key is *absent* or None — a present
    # `latitudeI: None` with a decimal `latitude` beside it must still parse.
    lat = pos.get("latitudeI")
    if lat is None:
        lat = pos.get("latitude")
    lon = pos.get("longitudeI")
    if lon is None:
        lon = pos.get("longitude")
    lat = coerce_float(lat)
    lon = coerce_float(lon)
    alt = coerce_float(pos.get("altitude"))
    # Undo the protobuf 1e7 scale per axis, not across both: deciding on either
    # axis being out of decimal range would corrupt a valid coordinate on the
    # other — {"latitude": 95, "longitude": 37.61} must keep 37.61, not turn it
    # into ~3.8e-6. Only an axis whose magnitude is protobuf-int-like divides.
    if lat is not None and abs(lat) > 1000:
        lat = lat / 1e7
    if lon is not None and abs(lon) > 1000:
        lon = lon / 1e7
    if lat is not None:
        lat = min(90.0, max(-90.0, lat))
    if lon is not None:
        lon = min(180.0, max(-180.0, lon))
    return {"latitude": lat, "longitude": lon, "altitude": alt}


def requested_node(args: dict, adapter_inst: Any) -> tuple[str | None, str | None]:
    """Resolve the target node id from args. Returns (node_id, error)."""
    query = args.get("node_id")
    if not query:
        return None, (
            "node_id is required — retry this call with the target node's ID "
            "(e.g. node_id='!9eabacac') or its name (e.g. node_id='Цаца'). "
            "Use mesh_list_nodes if you need the ID."
        )
    _iface, info = resolve_node(query, adapter_inst)
    if info:
        resolved = (info.get("user", {}) or {}).get("id")
        if resolved:
            return resolved, None
    # A name shared by two nodes is ambiguous — resolve_node refuses to pick,
    # and so do we: silently choosing the first match would DM whichever node
    # claimed the name first. Surface it so the model disambiguates by id.
    if isinstance(query, str):
        norm = query.strip().lower().lstrip("!")
        if len(_name_matches(norm, norm, adapter_inst.get_interfaces())) > 1:
            return None, (
                f"'{query}' matches multiple nodes — pass the exact node ID "
                "(e.g. '!9eabacac') to pick one."
            )
    # Not in the node DB yet — still allow an explicit !id, the node may simply
    # not have broadcast NodeInfo to us yet. But the id must at least be a
    # plausible node id: the library hex-parses ids of len >= 8 and otherwise
    # does a node-DB lookup that calls our_exit (sys.exit) on a miss, so a
    # short/garbage `!abcd` would surface a SystemExit instead of a clean error.
    # Normalize whitespace and case to the canonical lowercase !hex form the
    # rest of the codebase uses, so resolve_node and the validate stage agree.
    if isinstance(query, str):
        stripped = query.strip()
        if stripped.startswith("!"):
            bare = stripped.lstrip("!")
            if len(bare) == 8 and all(c in "0123456789abcdefABCDEF" for c in bare):
                return f"!{bare.lower()}", None
            return None, (
                f"'{query}' is not a valid node ID — node IDs are '!' followed by 8 "
                "hex digits (e.g. '!9eabacac'). Use mesh_list_nodes for known IDs."
            )
    return None, (
        f"No node matched '{query}'. Pass an exact node ID (e.g. '!9eabacac') "
        "or a name from mesh_list_nodes."
    )


def node_display_name(adapter_inst: Any, node_id: str | None) -> str:
    """Resolve a node id to a human name (long → short), falling back to the id.

    IDs like '!6982b824' are hard to read; the agent should report the name the
    user knows the node by whenever the node DB has one.
    """
    if not node_id:
        return node_id or ""
    _iface, info = resolve_node(node_id, adapter_inst)
    user = (info or {}).get("user", {}) or {}
    name = user.get("longName") or user.get("shortName")
    return str(name) if name else node_id


def format_route(route: list, snr: list, adapter_inst: Any) -> list[dict[str, Any]]:
    """Pair route hops (as readable names) with their SNR. SNR is scaled by 4.

    Route ints are masked to the unsigned 32-bit node-number space so a
    negative (hostile or malformed) hop can't render a `!-000005` id that can
    never resolve, and each hop's SNR goes through the same finite filter /
    -128 "unknown" sentinel handling the handler applies to the segment lists.
    """
    hops: list[dict[str, Any]] = []
    for i, num in enumerate(route or []):
        if isinstance(num, int):
            node_id = f"!{(num & 0xFFFFFFFF):08x}"
        else:
            node_id = str(num)
        entry: dict[str, Any] = {
            "name": node_display_name(adapter_inst, node_id),
            "node_id": node_id,
        }
        if i < len(snr or []):
            raw = snr[i]
            # Only a numeric entry gets an "snr" key at all: a non-numeric
            # value (string/None/list/bool) is malformed and omitted, while a
            # numeric -128 sentinel yields "snr": None (firmware reported
            # "unknown" for a real hop). decode_snr_value centralizes the
            # division + finite filter + sentinel handling so this and the
            # traceroute handler's segment lists cannot drift.
            if isinstance(raw, (int, float)) and not isinstance(raw, bool):
                entry["snr"] = decode_snr_value(raw)
        hops.append(entry)
    return hops


# Channel.Role enum values (protobuf); the mock stores plain dicts without a role.
_CHANNEL_ROLES = {0: "DISABLED", 1: "PRIMARY", 2: "SECONDARY"}


def psk_kind(psk: Any) -> str:
    """Describe a channel PSK without exposing it.

    Firmware semantics: empty = unencrypted, one byte = a well-known key
    (``0x01`` is the default ``AQ==`` key, others are "simple" variants),
    16/32 bytes = AES-128/256. The mock stores a label string, passed through.
    """
    if isinstance(psk, str):
        return psk or "none"
    if not psk:
        return "none"
    if len(psk) == 1:
        return "default" if psk[0] == 1 else "simple"
    return {16: "aes128", 32: "aes256"}.get(len(psk), "custom")


def preset_channel_name(iface: Any) -> str | None:
    """Name the firmware shows for an unnamed primary channel (e.g. ``LongFast``)."""
    local_config = getattr(getattr(iface, "localNode", None), "localConfig", None)
    lora = getattr(local_config, "lora", None)
    if lora is None:
        return None
    try:
        from meshtastic.protobuf import config_pb2

        preset = config_pb2.Config.LoRaConfig.ModemPreset.Name(lora.modem_preset)
    except Exception:
        return None
    return "".join(part.capitalize() for part in preset.split("_"))


def channel_entry(ch: Any, iface: Any) -> dict[str, Any] | None:
    """Normalize one ``localNode.channels`` entry; ``None`` for a disabled slot.

    Dict under the mock, protobuf ``Channel`` (``index``/``role``/``settings``)
    on hardware. The PSK itself is never returned — only its kind.
    """
    if isinstance(ch, dict):
        index = ch.get("index")
        return {
            "index": index,
            "name": ch.get("name") or "",
            "role": ch.get("role") or ("PRIMARY" if index == 0 else "SECONDARY"),
            "encryption": psk_kind(ch.get("psk")),
        }
    raw_role = getattr(ch, "role", 0)
    role = _CHANNEL_ROLES.get(raw_role, str(raw_role))
    if role == "DISABLED":
        return None
    settings = getattr(ch, "settings", None)
    name = getattr(settings, "name", "") or ""
    entry: dict[str, Any] = {
        "index": getattr(ch, "index", None),
        "name": name,
        "role": role,
        "encryption": psk_kind(getattr(settings, "psk", b"")),
        "uplink_enabled": bool(getattr(settings, "uplink_enabled", False)),
        "downlink_enabled": bool(getattr(settings, "downlink_enabled", False)),
    }
    module_settings = getattr(settings, "module_settings", None)
    precision = getattr(module_settings, "position_precision", None)
    if precision is not None:
        entry["position_precision"] = precision
    if not name and role == "PRIMARY":
        entry["name"] = preset_channel_name(iface) or ""
        entry["name_from_preset"] = True
    return entry
