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
        MESH_SEND_BROADCAST_SCHEMA,
        MESH_SEND_DM_SCHEMA,
        MESH_SIGNAL_QUALITY_SCHEMA,
        MESH_TELEMETRY_HISTORY_SCHEMA,
        MESH_TELEMETRY_SCHEMA,
    )
except ImportError:
    from schemas import (
        MESH_LIST_NODES_SCHEMA,
        MESH_NODE_INFO_SCHEMA,
        MESH_SEND_BROADCAST_SCHEMA,
        MESH_SEND_DM_SCHEMA,
        MESH_SIGNAL_QUALITY_SCHEMA,
        MESH_TELEMETRY_HISTORY_SCHEMA,
        MESH_TELEMETRY_SCHEMA,
    )

__all__ = [
    "MESH_LIST_NODES_SCHEMA",
    "MESH_NODE_INFO_SCHEMA",
    "MESH_SEND_BROADCAST_SCHEMA",
    "MESH_SEND_DM_SCHEMA",
    "MESH_SIGNAL_QUALITY_SCHEMA",
    "MESH_TELEMETRY_HISTORY_SCHEMA",
    "MESH_TELEMETRY_SCHEMA",
    "set_adapter",
    "handle_mesh_list_nodes",
    "handle_mesh_node_info",
    "handle_mesh_send_broadcast",
    "handle_mesh_send_dm",
    "handle_mesh_signal_quality",
    "handle_mesh_telemetry",
    "handle_mesh_telemetry_history",
]

_adapter_instance: Any | None = None
_adapter_lock = threading.RLock()


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


# --- Tool Handlers ---


async def handle_mesh_list_nodes(args: dict, **kwargs) -> str:
    """Get a formatted list of all visible Meshtastic nodes in the mesh."""
    adapter_inst = _get_adapter()
    if not adapter_inst:
        return json.dumps({"error": "Meshtastic platform adapter is not connected or active."})

    results = []
    interfaces = adapter_inst.get_interfaces()
    seen_nodes = set()

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

            # Prefer observed signal, then library, then persisted history.
            snr = obs.get("snr", info.get("snr"))
            rssi = obs.get("rssi", info.get("rssi"))
            if snr is None or rssi is None:
                history = telemetry_db.get_signal_history(nid, limit=1)
                if history:
                    snr = snr if snr is not None else history[0].get("snr")
                    rssi = rssi if rssi is not None else history[0].get("rssi")

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
                    "snr": snr if snr is not None else "N/A",
                    "rssi": rssi if rssi is not None else "N/A",
                    "signal_quality": assess_signal_quality(snr),
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
    obs = adapter_inst.get_observed_node(info.get("user", {}).get("id", ""))
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
        "uptime": metrics.get("uptime"),
        "latitude": pos.get("latitude"),
        "longitude": pos.get("longitude"),
        "altitude": pos.get("altitude"),
        "snr": obs.get("snr", info.get("snr")),
        "rssi": obs.get("rssi", info.get("rssi")),
        "hops_away": obs.get("hops_away", info.get("hopsAway")),
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

    # Prefer live-observed SNR/RSSI (fresher than the library node DB), then the
    # library value, then persisted history below.
    obs = adapter_inst.get_observed_node(node_id) if node_id else {}
    snr = obs.get("snr", info.get("snr") if info else None)
    rssi = obs.get("rssi", info.get("rssi") if info else None)

    # Look up historic trend if available
    history = telemetry_db.get_signal_history(node_id, limit=5)

    if snr is None and history:
        snr = history[0].get("snr")
        rssi = history[0].get("rssi")

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
        trend.append({"time": t_str, "snr": h["snr"], "rssi": h["rssi"]})

    quality_label = assess_signal_quality(snr)

    result = {
        "node_id": node_id,
        "name": info.get("user", {}).get("longName", "Unknown") if info else "Unknown",
        "current": {
            "snr": snr,
            "rssi": rssi,
            "quality": quality_label,
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

    temperature = env_metrics.get("temperature") or env_metrics.get("barometric_temperature")
    humidity = env_metrics.get("relativeHumidity")
    pressure = env_metrics.get("barometricPressure")
    battery_level = dev_metrics.get("batteryLevel")
    voltage = dev_metrics.get("voltage")
    uptime = dev_metrics.get("uptime")

    if history and (temperature is None or battery_level is None):
        h = history[0]
        temperature = temperature if temperature is not None else h.get("temperature")
        humidity = humidity if humidity is not None else h.get("humidity")
        pressure = pressure if pressure is not None else h.get("pressure")
        battery_level = battery_level if battery_level is not None else h.get("battery_level")
        voltage = voltage if voltage is not None else h.get("voltage")
        uptime = uptime if uptime is not None else h.get("uptime")

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
    try:
        limit = min(max(1, int(args.get("limit", 10))), 100)
    except (TypeError, ValueError):
        limit = 10

    if not node_id_query:
        return json.dumps({"error": "Parameter 'node_id' is required."})

    adapter_inst = _get_adapter()
    if not adapter_inst:
        return json.dumps({"error": "Meshtastic platform adapter is not connected."})

    _, info = resolve_node(node_id_query, adapter_inst)
    node_id = info.get("user", {}).get("id") if info else node_id_query

    if metric_type == "telemetry":
        history = telemetry_db.get_telemetry_history(node_id, limit=limit)
    elif metric_type == "positions":
        history = telemetry_db.get_position_history(node_id, limit=limit)
    elif metric_type == "signal_quality":
        history = telemetry_db.get_signal_history(node_id, limit=limit)
    else:
        return json.dumps({"error": f"Invalid metric_type '{metric_type}'."})

    # Format timestamps
    for h in history:
        if "timestamp" in h:
            h["time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(h["timestamp"]))

    return json.dumps(
        {
            "node_id": node_id,
            "name": info.get("user", {}).get("longName", "Unknown") if info else "Unknown",
            "metric_type": metric_type,
            "history": history,
        },
        indent=2,
    )
