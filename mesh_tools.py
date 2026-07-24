"""
Meshtastic Tool Handlers for Hermes Agent.
"""

import json
import logging
import threading
import time
from typing import Any

try:
    from . import telemetry_db
except ImportError:
    import telemetry_db

logger = logging.getLogger(__name__)

# JSON Schemas are imported for exposure in __init__.py
try:
    from .schemas import (
        MESH_LIST_NODES_SCHEMA,
        MESH_NODE_INFO_SCHEMA,
        MESH_REQUEST_POSITION_SCHEMA,
        MESH_REQUEST_TELEMETRY_SCHEMA,
        MESH_SEND_BROADCAST_SCHEMA,
        MESH_SEND_DM_SCHEMA,
        MESH_SIGNAL_QUALITY_SCHEMA,
        MESH_TELEMETRY_HISTORY_SCHEMA,
        MESH_TELEMETRY_SCHEMA,
        MESH_TRACEROUTE_SCHEMA,
    )
except ImportError:
    from schemas import (
        MESH_LIST_NODES_SCHEMA,
        MESH_NODE_INFO_SCHEMA,
        MESH_REQUEST_POSITION_SCHEMA,
        MESH_REQUEST_TELEMETRY_SCHEMA,
        MESH_SEND_BROADCAST_SCHEMA,
        MESH_SEND_DM_SCHEMA,
        MESH_SIGNAL_QUALITY_SCHEMA,
        MESH_TELEMETRY_HISTORY_SCHEMA,
        MESH_TELEMETRY_SCHEMA,
        MESH_TRACEROUTE_SCHEMA,
    )

__all__ = [
    "MESH_LIST_NODES_SCHEMA",
    "MESH_NODE_INFO_SCHEMA",
    "MESH_REQUEST_POSITION_SCHEMA",
    "MESH_REQUEST_TELEMETRY_SCHEMA",
    "MESH_SEND_BROADCAST_SCHEMA",
    "MESH_SEND_DM_SCHEMA",
    "MESH_SIGNAL_QUALITY_SCHEMA",
    "MESH_TELEMETRY_HISTORY_SCHEMA",
    "MESH_TELEMETRY_SCHEMA",
    "MESH_TRACEROUTE_SCHEMA",
    "set_adapter",
    "handle_mesh_list_nodes",
    "handle_mesh_node_info",
    "handle_mesh_request_position",
    "handle_mesh_request_telemetry",
    "handle_mesh_send_broadcast",
    "handle_mesh_send_dm",
    "handle_mesh_signal_quality",
    "handle_mesh_telemetry",
    "handle_mesh_telemetry_history",
    "handle_mesh_traceroute",
]

_adapter_instance: Any | None = None
_adapter_lock = threading.RLock()

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


def set_adapter(adapter: Any) -> None:
    """Set the active Meshtastic adapter instance."""
    global _adapter_instance
    with _adapter_lock:
        _adapter_instance = adapter


def _get_adapter() -> Any | None:
    """Retrieve the active Meshtastic adapter instance."""
    with _adapter_lock:
        return _adapter_instance


def resolve_node(
    node_id_or_name: str, adapter_instance: Any
) -> tuple[Any | None, dict[str, Any] | None]:
    """
    Search all active interfaces (serial or TCP) for a node matching the ID or name.

    Returns (interface, node_info_dict).
    """
    if not node_id_or_name:
        return None, None

    query = node_id_or_name.strip().lower()
    query_norm = query.lstrip("!")

    # Try resolving across all interfaces
    interfaces = adapter_instance.get_interfaces()
    for iface in interfaces:
        nodes = getattr(iface, "nodes", {}) or {}

        # 1. Direct ID lookup (exact with or without '!')
        for nid, info in nodes.items():
            nid_lower = nid.lower()
            if query == nid_lower or query_norm == nid_lower.lstrip("!"):
                return iface, info

        # 2. Name search (long name or short name)
        for _nid, info in nodes.items():
            user = info.get("user", {})
            long_name = str(user.get("longName", "")).lower()
            short_name = str(user.get("shortName", "")).lower()
            if query == long_name or query == short_name:
                return iface, info

        # 3. Numeric string ID lookup
        for _nid, info in nodes.items():
            num = info.get("num")
            if num is not None and query == str(num):
                return iface, info

    return None, None


def assess_signal_quality(snr: float | None) -> str:
    """Classify signal quality based on SNR (Signal to Noise Ratio)."""
    if snr is None:
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


def _first_not_none(*values: Any) -> Any:
    """Return the first value that is not None (0 / 0.0 are kept).

    Mirrored as ``MeshtasticAdapter._first_not_none`` in adapter.py; keep both
    in sync (tools loads as ``meshtastic_tools`` and cannot import the adapter
    at module load without a cycle risk through the gateway stack).
    """
    for value in values:
        if value is not None:
            return value
    return None


def _device_uptime(metrics: dict[str, Any] | None) -> Any:
    """Read uptime from node metrics (real mesh uses uptimeSeconds)."""
    metrics = metrics or {}
    return _first_not_none(metrics.get("uptimeSeconds"), metrics.get("uptime"))


def _position_age(pos: dict[str, Any] | None, node_id: str) -> dict[str, Any]:
    """Date a position fix, so an old one can't pass for the node's current spot.

    Coordinates from the node DB carry no age of their own, and a fix from last
    week plots on a coverage map exactly like one from a minute ago — confident,
    and wrong. The library's ``position.time`` is preferred; our persisted
    history fills in when the node DB has coordinates but no timestamp.
    """
    pos = pos or {}
    fix_time = _first_not_none(pos.get("time"), pos.get("timestamp"))
    if fix_time is None and node_id:
        recorded = telemetry_db.get_position_history(node_id, limit=1)
        if recorded:
            fix_time = recorded[0].get("timestamp")
    if not fix_time:
        return {"position_time": None, "position_age_hours": None, "position_is_stale": None}
    age = time.time() - float(fix_time)
    return {
        "position_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(fix_time))),
        "position_age_hours": round(age / 3600, 1),
        "position_is_stale": age > POSITION_STALE_AFTER_SECS,
    }


# --- Tool Handlers ---


def _link_facts(
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
    live_hops = _first_not_none(obs.get("hops_away"), info.get("hopsAway"))
    hops = _first_not_none(live_hops, (latest or {}).get("hop_count"))

    snr = rssi = None
    source = "unknown"
    if obs.get("snr") is not None or obs.get("rssi") is not None:
        # _update_observed records these off direct packets only.
        snr, rssi, source = obs.get("snr"), obs.get("rssi"), "direct"
    elif latest_direct:
        snr, rssi, source = latest_direct.get("snr"), latest_direct.get("rssi"), "direct"
    else:
        snr = _first_not_none(info.get("snr"), (latest or {}).get("snr"))
        rssi = _first_not_none(info.get("rssi"), (latest or {}).get("rssi"))
        if snr is not None or rssi is not None:
            source = "direct" if hops == 0 else ("relayed" if hops is not None else "unknown")

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
    last_direct = (latest_direct or {}).get("timestamp")
    observed_live = obs.get("snr") is not None or obs.get("rssi") is not None
    direct_recently = (
        last_direct is not None and (time.time() - last_direct) <= DIRECT_RANGE_WINDOW_SECS
    )
    return {
        "snr": snr,
        "rssi": rssi,
        "hops_away": hops,
        "heard_directly": bool(observed_live or live_hops == 0 or direct_recently),
        "signal_source": source,
        "last_direct_heard": (
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last_direct)) if last_direct else None
        ),
        "last_direct_heard_age_hours": (
            round((time.time() - last_direct) / 3600, 1) if last_direct else None
        ),
    }


async def handle_mesh_list_nodes(args: dict, **kwargs) -> str:
    """Get a formatted list of all visible Meshtastic nodes in the mesh."""
    adapter_inst = _get_adapter()
    if not adapter_inst:
        return json.dumps({"error": "Meshtastic platform adapter is not connected or active."})

    results = []
    interfaces = adapter_inst.get_interfaces()
    seen_nodes = set()

    # Two batch reads instead of a per-node query inside the loop.
    latest_by_node = telemetry_db.get_latest_signal_by_node()
    latest_direct_by_node = telemetry_db.get_latest_signal_by_node(direct_only=True)

    for iface in interfaces:
        nodes = getattr(iface, "nodes", {}) or {}
        for nid, info in nodes.items():
            if nid in seen_nodes:
                continue
            seen_nodes.add(nid)

            user = info.get("user", {})
            metrics = info.get("deviceMetrics", {})

            # Live-observed overlay (fresher than the library node DB, which only
            # refreshes lastHeard/signal from periodic NodeInfo packets).
            obs = adapter_inst.get_observed_node(nid)
            link = _link_facts(info, obs, latest_by_node.get(nid), latest_direct_by_node.get(nid))

            # last_heard: freshest of the library value and what we've observed.
            last_heard = max(info.get("lastHeard") or 0, obs.get("last_heard") or 0) or None
            last_heard_str = "Never"
            if last_heard:
                last_heard_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last_heard))

            results.append(
                {
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
            )

    return json.dumps({"nodes": results}, indent=2)


async def handle_mesh_node_info(args: dict, **kwargs) -> str:
    """Retrieve detailed configuration and hardware status for a specific node."""
    node_id_query = args.get("node_id")
    if not node_id_query:
        return json.dumps({"error": "Parameter 'node_id' is required."})

    adapter_inst = _get_adapter()
    if not adapter_inst:
        return json.dumps({"error": "Meshtastic platform adapter is not connected or active."})

    iface, info = resolve_node(node_id_query, adapter_inst)
    if not info:
        return json.dumps({"error": f"Node '{node_id_query}' was not found in the mesh database."})

    # Build complete details
    user = info.get("user", {})
    metrics = info.get("deviceMetrics", {})
    pos = info.get("position", {})

    # Check for public key to support security checking
    has_public_key = bool(user.get("publicKey"))

    # Live-observed overlay (fresher than the library node DB).
    node_id = info.get("user", {}).get("id", "")
    obs = adapter_inst.get_observed_node(node_id)
    history = telemetry_db.get_signal_history(node_id, limit=1)
    direct_history = telemetry_db.get_latest_signal_by_node(direct_only=True)
    link = _link_facts(info, obs, history[0] if history else None, direct_history.get(node_id))
    last_heard = max(info.get("lastHeard") or 0, obs.get("last_heard") or 0) or None

    details = {
        "node_id": info.get("user", {}).get("id", ""),
        "num": info.get("num"),
        "long_name": user.get("longName"),
        "short_name": user.get("shortName"),
        "hardware_model": user.get("hwModel"),
        "role": user.get("role"),
        "firmware_version": getattr(iface, "metadata", {}).get("firmwareVersion", "Unknown")
        if iface
        else "Unknown",
        "battery_level": metrics.get("batteryLevel"),
        "voltage": metrics.get("voltage"),
        "uptime": _device_uptime(metrics),
        "latitude": pos.get("latitude"),
        "longitude": pos.get("longitude"),
        "altitude": pos.get("altitude"),
        **_position_age(pos, node_id),
        "snr": link["snr"],
        "rssi": link["rssi"],
        "signal_source": link["signal_source"],
        "hops_away": link["hops_away"],
        "heard_directly": link["heard_directly"],
        "last_direct_heard": link["last_direct_heard"],
        "last_direct_heard_age_hours": link["last_direct_heard_age_hours"],
        "last_heard": (
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last_heard))
            if last_heard
            else "Never"
        ),
        "last_heard_epoch": last_heard,
        "has_public_key": has_public_key,
        "raw_info": info,
    }

    return json.dumps(details, indent=2)


async def handle_mesh_signal_quality(args: dict, **kwargs) -> str:
    """Check the signal strength and quality assessment for a specific node."""
    node_id_query = args.get("node_id")
    if not node_id_query:
        return json.dumps({"error": "Parameter 'node_id' is required."})

    adapter_inst = _get_adapter()
    if not adapter_inst:
        return json.dumps({"error": "Meshtastic platform adapter is not connected."})

    _, info = resolve_node(node_id_query, adapter_inst)
    node_id = info.get("user", {}).get("id") if info else node_id_query

    # Look up historic trend if available
    history = telemetry_db.get_signal_history(node_id, limit=5)

    obs = adapter_inst.get_observed_node(node_id) if node_id else {}
    direct_history = telemetry_db.get_latest_signal_by_node(direct_only=True)
    link = _link_facts(
        info or {}, obs, history[0] if history else None, direct_history.get(node_id)
    )
    snr, rssi = link["snr"], link["rssi"]

    if snr is None:
        return json.dumps(
            {
                "node_id": node_id,
                "error": f"No signal quality readings available for '{node_id_query}'.",
            }
        )

    trend = []
    for h in history:
        t_str = time.strftime("%H:%M:%S", time.localtime(h["timestamp"]))
        # hop_count per reading: a trend that mixes direct and relayed samples
        # is not a trend of one link, and reads as fluctuation that isn't there.
        trend.append(
            {"time": t_str, "snr": h["snr"], "rssi": h["rssi"], "hops_away": h.get("hop_count")}
        )

    quality_label = assess_signal_quality(snr)

    result = {
        "node_id": node_id,
        "name": info.get("user", {}).get("longName", "Unknown") if info else "Unknown",
        "current": {
            "snr": snr,
            "rssi": rssi,
            "quality": quality_label,
            "signal_source": link["signal_source"],
            "hops_away": link["hops_away"],
            "heard_directly": link["heard_directly"],
            "last_direct_heard": link["last_direct_heard"],
            "last_direct_heard_age_hours": link["last_direct_heard_age_hours"],
        },
        "trend_history": trend,
    }

    return json.dumps(result, indent=2)


async def handle_mesh_send_dm(args: dict, **kwargs) -> str:
    """Send a private direct message (DM) to a specific node."""
    node_id_query = args.get("node_id")
    message = args.get("message")

    if not node_id_query or not message:
        return json.dumps({"error": "Parameters 'node_id' and 'message' are required."})

    adapter_inst = _get_adapter()
    if not adapter_inst:
        return json.dumps({"error": "Meshtastic platform adapter is not connected."})

    iface, info = resolve_node(node_id_query, adapter_inst)
    if not info:
        return json.dumps({"error": f"Node '{node_id_query}' could not be resolved."})

    target_node_id = info.get("user", {}).get("id")

    # Direct messages require node public key metadata for Meshtastic PKC.
    if not info.get("user", {}).get("publicKey"):
        return json.dumps(
            {
                "success": False,
                "error": (
                    f"Target node {target_node_id} does not have a registered public key. "
                    "Pair the node with the Meshtastic mobile app at least once and wait for node info to propagate."
                ),
                "target_node": target_node_id,
            },
            indent=2,
        )

    # Send using adapter's internal send channel
    chat_id = f"meshtastic:{target_node_id}"
    res = await adapter_inst.send(chat_id=chat_id, content=message)

    return json.dumps(
        {
            "success": res.success,
            "message_id": res.message_id,
            "error": res.error,
            "target_node": target_node_id,
        },
        indent=2,
    )


async def handle_mesh_send_broadcast(args: dict, **kwargs) -> str:
    """Broadcast a text message to all nodes on primary or secondary channel."""
    message = args.get("message")
    channel_query = args.get("channel", "0")

    if not message:
        return json.dumps({"error": "Parameter 'message' is required."})

    adapter_inst = _get_adapter()
    if not adapter_inst:
        return json.dumps({"error": "Meshtastic platform adapter is not connected."})

    chat_id = f"meshtastic:channel:{channel_query}"
    res = await adapter_inst.send(chat_id=chat_id, content=message)

    return json.dumps(
        {
            "success": res.success,
            "message_id": res.message_id,
            "error": res.error,
            "channel": channel_query,
        },
        indent=2,
    )


async def handle_mesh_telemetry(args: dict, **kwargs) -> str:
    """Fetch the most recent telemetry readings from a sensor-equipped node."""
    node_id_query = args.get("node_id")
    if not node_id_query:
        return json.dumps({"error": "Parameter 'node_id' is required."})

    adapter_inst = _get_adapter()
    if not adapter_inst:
        return json.dumps({"error": "Meshtastic platform adapter is not connected."})

    _, info = resolve_node(node_id_query, adapter_inst)
    node_id = info.get("user", {}).get("id") if info else node_id_query

    # Try fetching telemetry from memory/node info
    env_metrics = info.get("environmentMetrics", {}) if info else {}
    dev_metrics = info.get("deviceMetrics", {}) if info else {}

    # Fall back to SQLite database if memory is empty
    history = telemetry_db.get_telemetry_history(node_id, limit=1)

    # Prefer live fields; keep 0 / 0.0 (battery 0 = external power on many nodes).
    temperature = _first_not_none(
        env_metrics.get("temperature"), env_metrics.get("barometric_temperature")
    )
    humidity = env_metrics.get("relativeHumidity")
    pressure = env_metrics.get("barometricPressure")
    battery_level = dev_metrics.get("batteryLevel")
    voltage = dev_metrics.get("voltage")
    uptime = _device_uptime(dev_metrics)

    if history and (temperature is None or battery_level is None):
        h = history[0]
        temperature = _first_not_none(temperature, h.get("temperature"))
        humidity = _first_not_none(humidity, h.get("humidity"))
        pressure = _first_not_none(pressure, h.get("pressure"))
        battery_level = _first_not_none(battery_level, h.get("battery_level"))
        voltage = _first_not_none(voltage, h.get("voltage"))
        uptime = _first_not_none(uptime, h.get("uptime"))

    if temperature is None and battery_level is None:
        return json.dumps(
            {
                "node_id": node_id,
                "error": f"No telemetry data is available for node '{node_id_query}'.",
            }
        )

    return json.dumps(
        {
            "node_id": node_id,
            "name": info.get("user", {}).get("longName", "Unknown") if info else "Unknown",
            "battery_level": battery_level,
            "voltage": voltage,
            "temperature": temperature,
            "humidity": humidity,
            "pressure": pressure,
            "uptime": uptime,
        },
        indent=2,
    )


async def handle_mesh_telemetry_history(args: dict, **kwargs) -> str:
    """Query historical telemetry, positions, or signal qualities."""
    node_id_query = args.get("node_id")
    metric_type = args.get("metric_type", "telemetry")

    # A period ("the last 3 days") and a count ("the last 10 rows") answer
    # different questions, and a count cannot stand in for a period: how far
    # back N rows reach depends entirely on how chatty the node is — 100 rows
    # is five days for one node here and a month for another. Asking by time
    # therefore raises the cap, since the window, not the number, is the ask.
    since_hours = args.get("since_hours")
    since = None
    if since_hours is not None:
        try:
            hours = float(since_hours)
        except (TypeError, ValueError):
            return json.dumps({"error": "Parameter 'since_hours' must be a number."})
        if hours <= 0:
            return json.dumps({"error": "Parameter 'since_hours' must be positive."})
        hours = min(hours, HISTORY_MAX_WINDOW_HOURS)
        since = time.time() - hours * 3600

    default_limit = HISTORY_WINDOW_ROW_CAP if since is not None else 10
    row_cap = HISTORY_WINDOW_ROW_CAP if since is not None else 100
    try:
        limit = min(max(1, int(args.get("limit", default_limit))), row_cap)
    except (TypeError, ValueError):
        limit = default_limit

    if not node_id_query:
        return json.dumps({"error": "Parameter 'node_id' is required."})

    adapter_inst = _get_adapter()
    if not adapter_inst:
        return json.dumps({"error": "Meshtastic platform adapter is not connected."})

    _, info = resolve_node(node_id_query, adapter_inst)
    node_id = info.get("user", {}).get("id") if info else node_id_query

    if metric_type == "telemetry":
        history = telemetry_db.get_telemetry_history(node_id, limit=limit, since=since)
    elif metric_type == "positions":
        history = telemetry_db.get_position_history(node_id, limit=limit, since=since)
    elif metric_type == "signal_quality":
        history = telemetry_db.get_signal_history(node_id, limit=limit, since=since)
    else:
        return json.dumps({"error": f"Invalid metric_type '{metric_type}'."})

    # Format timestamps
    for h in history:
        if "timestamp" in h:
            h["time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(h["timestamp"]))

    result = {
        "node_id": node_id,
        "name": info.get("user", {}).get("longName", "Unknown") if info else "Unknown",
        "metric_type": metric_type,
        "returned": len(history),
        "history": history,
    }
    if since is not None:
        # Say what was actually covered. Hitting the cap means the oldest rows
        # of the requested window are missing, and a truncated window read as a
        # complete one is how "no data before X" gets asserted wrongly.
        result["window_requested_from"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(since))
        oldest = history[-1]["timestamp"] if history else None
        result["oldest_returned"] = (
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(oldest)) if oldest else None
        )
        result["truncated"] = len(history) >= limit
    return json.dumps(result, indent=2)


# --- Solicited requests ------------------------------------------------------
# These transmit on the shared LoRa channel, unlike everything above which
# serves already-heard data. Addressed to one node, never retried.


def _clamp(value: Any, default: float, low: float, high: float) -> float:
    """Coerce a model-supplied number into a sane range."""
    try:
        return max(low, min(high, float(value)))
    except (TypeError, ValueError):
        return default


def _requested_node(args: dict, adapter_inst: Any) -> tuple[str | None, str | None]:
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
    # Not in the node DB yet — still allow an explicit !id, the node may simply
    # not have broadcast NodeInfo to us yet.
    if isinstance(query, str) and query.startswith("!"):
        return query, None
    return None, (
        f"No node matched '{query}'. Pass an exact node ID (e.g. '!9eabacac') "
        "or a name from mesh_list_nodes."
    )


async def handle_mesh_request_telemetry(args: dict, **kwargs) -> str:
    """Ask a node over the air for its current device metrics."""
    adapter_inst = _get_adapter()
    if not adapter_inst:
        return json.dumps({"error": "Meshtastic platform adapter is not connected or active."})

    node_id, err = _requested_node(args, adapter_inst)
    if err:
        return json.dumps({"error": err})

    timeout = _clamp(args.get("timeout"), 45.0, 5.0, 120.0)
    result = await adapter_inst.request_telemetry(node_id, timeout=timeout)
    if not result.get("ok"):
        return json.dumps({"node_id": node_id, "answered": False, "error": result.get("error")})

    data = result.get("data") or {}
    metrics = data.get("deviceMetrics", data) or {}
    return json.dumps(
        {
            "node_id": node_id,
            "answered": True,
            "battery_level": metrics.get("batteryLevel"),
            "voltage": metrics.get("voltage"),
            "uptime_seconds": metrics.get("uptimeSeconds") or metrics.get("uptime"),
            "channel_utilization": metrics.get("channelUtilization"),
            "air_util_tx": metrics.get("airUtilTx"),
        },
        ensure_ascii=False,
    )


async def handle_mesh_request_position(args: dict, **kwargs) -> str:
    """Ask a node over the air for its current position."""
    adapter_inst = _get_adapter()
    if not adapter_inst:
        return json.dumps({"error": "Meshtastic platform adapter is not connected or active."})

    node_id, err = _requested_node(args, adapter_inst)
    if err:
        return json.dumps({"error": err})

    timeout = _clamp(args.get("timeout"), 45.0, 5.0, 120.0)
    result = await adapter_inst.request_position(node_id, timeout=timeout)
    if not result.get("ok"):
        return json.dumps({"node_id": node_id, "answered": False, "error": result.get("error")})

    pos = result.get("data") or {}
    lat, lon = pos.get("latitude"), pos.get("longitude")
    # protobuf stores coordinates scaled by 1e7
    if isinstance(lat, (int, float)) and abs(lat) > 90.0:
        lat = lat / 1e7
    if isinstance(lon, (int, float)) and abs(lon) > 180.0:
        lon = lon / 1e7
    return json.dumps(
        {
            "node_id": node_id,
            "answered": True,
            "latitude": lat,
            "longitude": lon,
            "altitude": pos.get("altitude"),
        },
        ensure_ascii=False,
    )


def _node_display_name(adapter_inst: Any, node_id: str | None) -> str:
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


def _format_route(route: list, snr: list, adapter_inst: Any) -> list[dict[str, Any]]:
    """Pair route hops (as readable names) with their SNR. SNR is scaled by 4."""
    hops: list[dict[str, Any]] = []
    for i, num in enumerate(route or []):
        node_id = f"!{num:08x}" if isinstance(num, int) else str(num)
        entry: dict[str, Any] = {
            "name": _node_display_name(adapter_inst, node_id),
            "node_id": node_id,
        }
        if i < len(snr or []):
            raw = snr[i]
            if isinstance(raw, (int, float)):
                entry["snr"] = raw / 4.0
        hops.append(entry)
    return hops


async def handle_mesh_traceroute(args: dict, **kwargs) -> str:
    """Discover the actual radio route to a node, with per-hop SNR."""
    adapter_inst = _get_adapter()
    if not adapter_inst:
        return json.dumps({"error": "Meshtastic platform adapter is not connected or active."})

    node_id, err = _requested_node(args, adapter_inst)
    if err:
        return json.dumps({"error": err})

    hop_limit = int(_clamp(args.get("hop_limit"), 5, 1, 7))
    timeout = _clamp(args.get("timeout"), 60.0, 5.0, 120.0)
    result = await adapter_inst.request_traceroute(node_id, hop_limit=hop_limit, timeout=timeout)
    if not result.get("ok"):
        return json.dumps({"node_id": node_id, "answered": False, "error": result.get("error")})

    route = result.get("data") or {}
    logger.info("Meshtastic traceroute raw reply for %s: %s", node_id, route)
    towards = _format_route(route.get("route", []), route.get("snrTowards", []), adapter_inst)
    back = _format_route(route.get("routeBack", []), route.get("snrBack", []), adapter_inst)
    # Per-segment SNR (dB), one more value than there are relays — for a 0-hop
    # direct trace these carry the direct link's SNR each way (the relay lists
    # are empty then). -128 is the firmware's "unknown" sentinel.
    snr_towards = [None if v == -128 else v / 4.0 for v in (route.get("snrTowards") or [])]
    snr_back = [None if v == -128 else v / 4.0 for v in (route.get("snrBack") or [])]
    return json.dumps(
        {
            "node_id": node_id,
            "name": _node_display_name(adapter_inst, node_id),
            "answered": True,
            "hops_towards": len(towards),
            "route_towards": towards,
            "route_back": back,
            "snr_towards_db": snr_towards,
            "snr_back_db": snr_back,
            "note": (
                "route_towards/route_back list the relays each way (empty = direct). "
                "snr_towards_db is per-segment SNR toward the node, snr_back_db back to "
                "us; for a direct (0-hop) trace each holds the direct link's SNR. "
                "Asymmetry between the two directions explains messages that arrive but "
                "are never confirmed."
            ),
        },
        ensure_ascii=False,
    )
