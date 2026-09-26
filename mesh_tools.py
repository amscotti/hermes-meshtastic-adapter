"""
Meshtastic Tool Handlers for Hermes Agent.
"""

import asyncio
import json
import logging
import math
import threading
import time
from typing import Any

try:
    from . import telemetry_db
except ImportError:
    import telemetry_db

try:
    from . import inbound, send_path
except ImportError:
    import inbound
    import send_path

logger = logging.getLogger(__name__)

PAUSE_MAX_MINUTES = 12 * 60  # 12h cap on a timed pause

# JSON Schemas are imported for exposure in __init__.py
try:
    from .schemas import (
        MESH_LIST_CHANNELS_SCHEMA,
        MESH_LIST_NODES_SCHEMA,
        MESH_NODE_INFO_SCHEMA,
        MESH_PAUSE_SCHEMA,
        MESH_REQUEST_POSITION_SCHEMA,
        MESH_REQUEST_TELEMETRY_SCHEMA,
        MESH_RESUME_SCHEMA,
        MESH_SEND_BROADCAST_SCHEMA,
        MESH_SEND_DM_SCHEMA,
        MESH_SIGNAL_QUALITY_SCHEMA,
        MESH_TELEMETRY_HISTORY_SCHEMA,
        MESH_TELEMETRY_SCHEMA,
        MESH_TRACEROUTE_SCHEMA,
    )
except ImportError:
    from schemas import (
        MESH_LIST_CHANNELS_SCHEMA,
        MESH_LIST_NODES_SCHEMA,
        MESH_NODE_INFO_SCHEMA,
        MESH_PAUSE_SCHEMA,
        MESH_REQUEST_POSITION_SCHEMA,
        MESH_REQUEST_TELEMETRY_SCHEMA,
        MESH_RESUME_SCHEMA,
        MESH_SEND_BROADCAST_SCHEMA,
        MESH_SEND_DM_SCHEMA,
        MESH_SIGNAL_QUALITY_SCHEMA,
        MESH_TELEMETRY_HISTORY_SCHEMA,
        MESH_TELEMETRY_SCHEMA,
        MESH_TRACEROUTE_SCHEMA,
    )

__all__ = [
    "MESH_LIST_CHANNELS_SCHEMA",
    "MESH_LIST_NODES_SCHEMA",
    "MESH_NODE_INFO_SCHEMA",
    "MESH_PAUSE_SCHEMA",
    "MESH_REQUEST_POSITION_SCHEMA",
    "MESH_REQUEST_TELEMETRY_SCHEMA",
    "MESH_RESUME_SCHEMA",
    "MESH_SEND_BROADCAST_SCHEMA",
    "MESH_SEND_DM_SCHEMA",
    "MESH_SIGNAL_QUALITY_SCHEMA",
    "MESH_TELEMETRY_HISTORY_SCHEMA",
    "MESH_TELEMETRY_SCHEMA",
    "MESH_TRACEROUTE_SCHEMA",
    "set_adapter",
    "handle_mesh_list_channels",
    "handle_mesh_list_nodes",
    "handle_mesh_node_info",
    "handle_mesh_pause",
    "handle_mesh_request_position",
    "handle_mesh_request_telemetry",
    "handle_mesh_resume",
    "handle_mesh_send_broadcast",
    "handle_mesh_send_dm",
    "handle_mesh_signal_quality",
    "handle_mesh_telemetry",
    "handle_mesh_telemetry_history",
    "handle_mesh_traceroute",
]

_adapter_instance: Any | None = None
_adapter_lock = threading.RLock()

# Shared pure helpers live in mesh_helpers.py (extracted P3.4). Dual-import so
# the plugin works both as a package (in Hermes) and as flat modules (in
# tests/CI); the private-name aliases below keep pre-extraction callers
# referencing meshtastic_tools._link_facts etc. working unchanged.
try:
    from .mesh_helpers import (
        assess_signal_quality,
        channel_entry,
        clamp,
        coerce_float,
        decode_snr_value,
        device_uptime,
        fetch_history_rows,
        first_not_none,
        format_node_summary,
        format_route,
        history_window_params,
        link_facts,
        node_display_name,
        normalize_position_payload,
        numeric_epoch,
        position_age,
        requested_node,
        resolve_node,
    )
except ImportError:
    from mesh_helpers import (
        assess_signal_quality,
        channel_entry,
        clamp,
        coerce_float,
        decode_snr_value,
        device_uptime,
        fetch_history_rows,
        first_not_none,
        format_node_summary,
        format_route,
        history_window_params,
        link_facts,
        node_display_name,
        normalize_position_payload,
        numeric_epoch,
        position_age,
        requested_node,
        resolve_node,
    )

_first_not_none = first_not_none
_clamp = clamp
_device_uptime = device_uptime
_link_facts = link_facts
_node_display_name = node_display_name
_position_age = position_age
_requested_node = requested_node
_format_route = format_route
_decode_snr_value = decode_snr_value
_numeric_epoch = numeric_epoch
# history_window_params was previously the private _history_window_params here;
# keep the private alias so pre-extraction callers keep working.
_history_window_params = history_window_params


def set_adapter(adapter: Any) -> None:
    """Set the active Meshtastic adapter instance."""
    global _adapter_instance
    with _adapter_lock:
        _adapter_instance = adapter


def _get_adapter() -> Any | None:
    """Retrieve the active Meshtastic adapter instance."""
    with _adapter_lock:
        return _adapter_instance


# --- Tool Handlers ---


async def handle_mesh_list_nodes(args: dict, **kwargs) -> str:
    """Get a formatted list of all visible Meshtastic nodes in the mesh."""
    adapter_inst = _get_adapter()
    if not adapter_inst:
        return json.dumps({"error": "Meshtastic platform adapter is not connected or active."})

    results = []
    interfaces = adapter_inst.get_interfaces()
    seen_nodes = set()

    # Two batch reads instead of a per-node query inside the loop, off the event
    # loop: a windowed full-table scan on a busy mesh near the row ceiling can
    # stall the loop (which also owns inbound pubsub bridging) for its duration.
    latest_by_node = await asyncio.to_thread(telemetry_db.get_latest_signal_by_node)
    latest_direct_by_node = await asyncio.to_thread(telemetry_db.get_latest_signal_by_node, True)

    for iface in interfaces:
        nodes = getattr(iface, "nodes", {}) or {}
        # Snapshot: the meshtastic reader thread mutates iface.nodes on NodeInfo.
        # Iterating the live dict can raise "dictionary changed size during
        # iteration" (same pattern as send_path.resolve_dm_node / mesh_helpers).
        for nid, info in list(nodes.items()):
            # Dedupe on the canonical !hex id: the observed overlay and the DB
            # latest-by-node maps are both keyed that way, so a node keyed as an
            # int on one interface and a string on another must collapse into a
            # single entry and still get its overlay applied.
            normalized = adapter_inst._normalize_node_id(nid)
            if normalized is None or normalized in seen_nodes:
                continue
            seen_nodes.add(normalized)
            results.append(
                format_node_summary(
                    adapter_inst, normalized, info, latest_by_node, latest_direct_by_node
                )
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
    # Three independent DB reads run concurrently off the event loop (which
    # also owns inbound pubsub bridging). The latest-direct sample uses the
    # single-node getter rather than the all-nodes window scan — the latter
    # serializes against every telemetry writer for a full-table pass and this
    # handler only needs one node's row.
    history, latest_direct, position_aging = await asyncio.gather(
        asyncio.to_thread(telemetry_db.get_signal_history, node_id, 1),
        asyncio.to_thread(telemetry_db.get_latest_direct_signal, node_id),
        asyncio.to_thread(position_age, pos, node_id),
    )
    link = link_facts(info, obs, history[0] if history else None, latest_direct)
    # lastHeard is untrusted at the boundary — a hostile/malformed NodeInfo
    # could carry a truthy non-numeric value that survives `or 0` and crashes
    # max(); numeric_epoch guards the type first.
    last_heard = (
        max(numeric_epoch(info.get("lastHeard")), numeric_epoch(obs.get("last_heard"))) or None
    )

    details = {
        "node_id": info.get("user", {}).get("id", ""),
        "num": info.get("num"),
        "long_name": user.get("longName"),
        "short_name": user.get("shortName"),
        "hardware_model": user.get("hwModel"),
        "role": user.get("role"),
        "firmware_version": getattr(iface, "metadata", {}).get("firmwareVersion", "Unknown"),
        "battery_level": metrics.get("batteryLevel"),
        "voltage": metrics.get("voltage"),
        "uptime": device_uptime(metrics),
        "latitude": pos.get("latitude"),
        "longitude": pos.get("longitude"),
        "altitude": pos.get("altitude"),
        **position_aging,
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

    # Look up historic trend if available, plus the latest direct sample.
    # Both run off the event loop; the latest-direct sample uses the
    # single-node getter rather than the all-nodes window scan (the latter
    # serializes against every telemetry writer for a full-table pass and this
    # handler only needs one node's row).
    history, latest_direct = await asyncio.gather(
        asyncio.to_thread(telemetry_db.get_signal_history, node_id, 5),
        asyncio.to_thread(telemetry_db.get_latest_direct_signal, node_id),
    )

    obs = adapter_inst.get_observed_node(node_id) if node_id else {}
    link = link_facts(info or {}, obs, history[0] if history else None, latest_direct)
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
        # snr/rssi are filtered at the render boundary (like link_facts) so a
        # non-numeric row can never surface verbatim in tool output.
        trend.append(
            {
                "time": t_str,
                "snr": coerce_float(h["snr"]),
                "rssi": coerce_float(h["rssi"]),
                "hops_away": h.get("hop_count"),
            }
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
    if not target_node_id:
        # A resolved node dict that lacks user.id is malformed NodeInfo; without
        # this guard the chat_id below would be "meshtastic:None", which the
        # adapter's send path cannot resolve to a real destination.
        return json.dumps(
            {"error": f"Node '{node_id_query}' has no known node ID and cannot be direct-messaged."}
        )

    # Send using adapter's internal send channel. No pubkey pre-check here: the
    # adapter's send path resolves the node across ALL interfaces (dm_send_target)
    # and prefers a keyed copy over a keyless owner — the tool's resolve_node only
    # sees the first interface that knows the node, so a pre-check here would
    # reject a DM the adapter would deliver. The adapter surfaces no_pubkey.
    chat_id = f"meshtastic:{target_node_id}"
    res = await adapter_inst.send(chat_id=chat_id, content=message)

    error = res.error
    if not res.success and error and "no public key" in error:
        error = (
            f"{error} Pair the node with the Meshtastic mobile app at least once "
            "and wait for node info to propagate."
        )

    return json.dumps(
        {
            "success": res.success,
            "message_id": res.message_id,
            "error": error,
            "target_node": target_node_id,
        },
        indent=2,
    )


async def handle_mesh_send_broadcast(args: dict, **kwargs) -> str:
    """Broadcast a text message to all nodes on primary or secondary channel."""
    message = args.get("message")
    # Normalize to str so validation and chat_id encoding agree: a non-string
    # scalar (e.g. a bool True) would otherwise pass _numeric_channel_spec
    # (int(True) == 1) but encode as "meshtastic:channel:True" — a named
    # channel the adapter re-resolves and rejects.
    channel_query = str(args.get("channel", "0"))

    if not message:
        return json.dumps({"error": "Parameter 'message' is required."})

    adapter_inst = _get_adapter()
    if not adapter_inst:
        return json.dumps({"error": "Meshtastic platform adapter is not connected."})

    # Resolve the requested channel against the interfaces' channel tables up
    # front so a mistyped name/index is rejected instead of silently falling
    # back to channel 0 while the reply claims the requested channel. Mirrors
    # the adapter's send-path resolution (channel_send_target) exactly; when no
    # interface exposes a table there is nothing to validate against and the
    # adapter's pass-through behavior applies.
    channel_index = None
    ifaces = adapter_inst.get_interfaces()
    if ifaces:
        channel_index, _iface = send_path.channel_send_target(
            ["meshtastic", "channel", channel_query], ifaces, inbound.channel_field
        )
        if channel_index is None:
            return json.dumps(
                {
                    "error": (
                        f"Channel '{channel_query}' is not available on any connected interface."
                    )
                }
            )

    chat_id = f"meshtastic:channel:{channel_query}"
    res = await adapter_inst.send(chat_id=chat_id, content=message)

    payload = {
        "success": res.success,
        "message_id": res.message_id,
        "error": res.error,
        "channel": channel_query,
    }
    if channel_index is not None:
        payload["channel_index"] = channel_index
    return json.dumps(payload, indent=2)


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

    history = await asyncio.to_thread(telemetry_db.get_telemetry_history, node_id, 1)

    # Prefer live fields; keep 0 / 0.0 (battery 0 = external power on many nodes).
    # Real hardware MessageToDict payloads are camelCase (barometricTemperature);
    # the snake_case form is accepted as a fallback.
    temperature = first_not_none(
        env_metrics.get("temperature"),
        env_metrics.get("barometricTemperature"),
        env_metrics.get("barometric_temperature"),
    )
    humidity = env_metrics.get("relativeHumidity")
    pressure = env_metrics.get("barometricPressure")
    battery_level = dev_metrics.get("batteryLevel")
    voltage = dev_metrics.get("voltage")
    uptime = device_uptime(dev_metrics)

    # Fall back to SQLite database for any metric the live node DB lacks.
    # The gate is per-field (not "is the node reporting at all"): a node
    # reporting live temperature and battery but no humidity still gets its
    # humidity backfilled from history when available, so a missing sensor
    # reading does not silently drop. first_not_none below keeps live values
    # authoritative — DB rows only fill gaps, never override.
    if history and any(
        v is None for v in (temperature, humidity, pressure, battery_level, voltage, uptime)
    ):
        h = history[0]
        temperature = first_not_none(temperature, h.get("temperature"))
        humidity = first_not_none(humidity, h.get("humidity"))
        pressure = first_not_none(pressure, h.get("pressure"))
        battery_level = first_not_none(battery_level, h.get("battery_level"))
        voltage = first_not_none(voltage, h.get("voltage"))
        uptime = first_not_none(uptime, h.get("uptime"))

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

    if not node_id_query:
        return json.dumps({"error": "Parameter 'node_id' is required."})

    limit, since, err = history_window_params(args)
    if err:
        return json.dumps({"error": err})

    adapter_inst = _get_adapter()
    if not adapter_inst:
        return json.dumps({"error": "Meshtastic platform adapter is not connected."})

    _, info = resolve_node(node_id_query, adapter_inst)
    node_id = info.get("user", {}).get("id") if info else node_id_query

    fetch_limit = limit + 1 if since is not None else limit
    history = await asyncio.to_thread(fetch_history_rows, metric_type, node_id, fetch_limit, since)
    if history is None:
        return json.dumps({"error": f"Invalid metric_type '{metric_type}'."})

    truncated = len(history) > limit
    history = history[:limit] if truncated else history

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
        result["truncated"] = truncated
    return json.dumps(result, indent=2)


# --- Solicited requests ------------------------------------------------------
# These transmit on the shared LoRa channel, unlike everything above which
# serves already-heard data. Addressed to one node, never retried.


async def handle_mesh_request_telemetry(args: dict, **kwargs) -> str:
    """Ask a node over the air for its current device metrics."""
    adapter_inst = _get_adapter()
    if not adapter_inst:
        return json.dumps({"error": "Meshtastic platform adapter is not connected or active."})

    node_id, err = requested_node(args, adapter_inst)
    if err:
        return json.dumps({"error": err})

    timeout = clamp(args.get("timeout"), 45.0, 5.0, 120.0)
    result = await adapter_inst.request_telemetry(node_id, timeout=timeout)
    if not result.get("ok"):
        return json.dumps({"node_id": node_id, "answered": False, "error": result.get("error")})

    data = result.get("data") or {}
    metrics = data.get("deviceMetrics", data) or {}
    # Coerce every metric through a finite filter: a hostile reply carrying a
    # NaN/inf voltage or SNR must not render as a non-standard JSON literal.
    battery_level = coerce_float(metrics.get("batteryLevel"))
    voltage = coerce_float(metrics.get("voltage"))
    # first_not_none (not `or`): a freshly-booted node reporting uptimeSeconds=0
    # must survive as 0 rather than falling through to the (absent) uptime key.
    uptime = coerce_float(first_not_none(metrics.get("uptimeSeconds"), metrics.get("uptime")))
    channel_utilization = coerce_float(metrics.get("channelUtilization"))
    air_util_tx = coerce_float(metrics.get("airUtilTx"))
    return json.dumps(
        {
            "node_id": node_id,
            "answered": True,
            "battery_level": battery_level,
            "voltage": voltage,
            "uptime_seconds": uptime,
            "channel_utilization": channel_utilization,
            "air_util_tx": air_util_tx,
        },
        ensure_ascii=False,
    )


async def handle_mesh_request_position(args: dict, **kwargs) -> str:
    """Ask a node over the air for its current position."""
    adapter_inst = _get_adapter()
    if not adapter_inst:
        return json.dumps({"error": "Meshtastic platform adapter is not connected or active."})

    node_id, err = requested_node(args, adapter_inst)
    if err:
        return json.dumps({"error": err})

    timeout = clamp(args.get("timeout"), 45.0, 5.0, 120.0)
    result = await adapter_inst.request_position(node_id, timeout=timeout)
    if not result.get("ok"):
        return json.dumps({"node_id": node_id, "answered": False, "error": result.get("error")})

    # Real hardware replies arrive as MessageToDict(Position(...)) with
    # camelCase latitudeI/longitudeI keys; the mock uses latitude/longitude.
    # normalize_position_payload accepts both, undoes the protobuf 1e7 scale,
    # and filters to finite values.
    pos = normalize_position_payload(result.get("data") or {})
    return json.dumps(
        {
            "node_id": node_id,
            "answered": True,
            "latitude": pos.get("latitude"),
            "longitude": pos.get("longitude"),
            "altitude": pos.get("altitude"),
        },
        ensure_ascii=False,
    )


async def handle_mesh_traceroute(args: dict, **kwargs) -> str:
    """Discover the actual radio route to a node, with per-hop SNR."""
    adapter_inst = _get_adapter()
    if not adapter_inst:
        return json.dumps({"error": "Meshtastic platform adapter is not connected or active."})

    node_id, err = requested_node(args, adapter_inst)
    if err:
        return json.dumps({"error": err})

    hop_limit = int(clamp(args.get("hop_limit"), 5, 1, 7))
    timeout = clamp(args.get("timeout"), 60.0, 5.0, 120.0)
    result = await adapter_inst.request_traceroute(node_id, hop_limit=hop_limit, timeout=timeout)
    if not result.get("ok"):
        return json.dumps({"node_id": node_id, "answered": False, "error": result.get("error")})

    route = result.get("data") or {}
    logger.info("Meshtastic traceroute raw reply for %s: %s", node_id, route)
    towards = format_route(route.get("route", []), route.get("snrTowards", []), adapter_inst)
    back = format_route(route.get("routeBack", []), route.get("snrBack", []), adapter_inst)
    # Per-segment SNR (dB), one more value than there are relays — for a 0-hop
    # direct trace these carry the direct link's SNR each way (the relay lists
    # are empty then). -128 is the firmware's "unknown" sentinel. Decoded
    # through decode_snr_value so a hostile/non-numeric entry filters to null
    # instead of raising on the division or smuggling NaN into the JSON.
    snr_towards = [decode_snr_value(v) for v in (route.get("snrTowards") or [])]
    snr_back = [decode_snr_value(v) for v in (route.get("snrBack") or [])]
    return json.dumps(
        {
            "node_id": node_id,
            "name": node_display_name(adapter_inst, node_id),
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


async def handle_mesh_pause(args: dict, **kwargs) -> str:
    """Release the node so something else can connect to it."""
    adapter_inst = _get_adapter()
    if not adapter_inst:
        return json.dumps({"error": "Meshtastic platform adapter is not connected or active."})

    minutes = args.get("minutes")
    if minutes is not None:
        try:
            minutes = float(minutes)
        except (TypeError, ValueError):
            return json.dumps({"error": "Parameter 'minutes' must be a number."})
        if not math.isfinite(minutes):
            # NaN survives float() and min(nan, cap) is nan; a nan deadline would
            # wedge the link in a timed pause that can never auto-resume and
            # crash pause_state's localtime() — reject it like any other bad number.
            return json.dumps({"error": "Parameter 'minutes' must be a number."})
        if minutes <= 0:
            return json.dumps({"error": "Parameter 'minutes' must be positive."})
        minutes = min(minutes, PAUSE_MAX_MINUTES)

    state = adapter_inst.pause_link(minutes)
    return json.dumps(
        {
            **state,
            "note": (
                "Radio released — the node is free for another client. Outbound messages "
                "queue until the link resumes."
            ),
        },
        indent=2,
    )


async def handle_mesh_resume(args: dict, **kwargs) -> str:
    """Reconnect to the node after a pause."""
    adapter_inst = _get_adapter()
    if not adapter_inst:
        return json.dumps({"error": "Meshtastic platform adapter is not connected or active."})

    state = adapter_inst.resume_link()
    return json.dumps({**state, "note": "Reconnecting to the node; it takes a second."}, indent=2)


async def handle_mesh_list_channels(args: dict, **kwargs) -> str:
    """List the channels configured on the locally connected node."""
    adapter_inst = _get_adapter()
    if not adapter_inst:
        return json.dumps({"error": "Meshtastic platform adapter is not connected or active."})

    channels: list[dict[str, Any]] = []
    for iface in adapter_inst.get_interfaces():
        local_node = getattr(iface, "localNode", None)
        for ch in getattr(local_node, "channels", None) or []:
            entry = channel_entry(ch, iface)
            if entry is not None:
                channels.append(entry)

    if not channels:
        return json.dumps(
            {"error": "No channel configuration available yet (node not connected or not loaded)."}
        )
    return json.dumps(
        {
            "channels": channels,
            "channel_replies_enabled": bool(getattr(adapter_inst, "allow_channels", False)),
        },
        indent=2,
        ensure_ascii=False,
    )
