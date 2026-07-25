"""
Meshtastic integration plugin for Hermes Agent.

Registers the platform adapter and the meshtastic toolset.
"""

from .adapter import register as register_platform
from .mesh_tools import (
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
    handle_mesh_list_nodes,
    handle_mesh_node_info,
    handle_mesh_pause,
    handle_mesh_request_position,
    handle_mesh_request_telemetry,
    handle_mesh_resume,
    handle_mesh_send_broadcast,
    handle_mesh_send_dm,
    handle_mesh_signal_quality,
    handle_mesh_telemetry,
    handle_mesh_telemetry_history,
    handle_mesh_traceroute,
)


def register(ctx):
    """
    Register the Meshtastic platform adapter and toolset.
    Called once by the plugin loader.
    """
    # Register the platform adapter
    register_platform(ctx)

    # Register the tools
    ctx.register_tool(
        name="mesh_list_nodes",
        toolset="meshtastic",
        schema=MESH_LIST_NODES_SCHEMA,
        handler=handle_mesh_list_nodes,
        is_async=True,
        emoji="📡",
    )
    ctx.register_tool(
        name="mesh_node_info",
        toolset="meshtastic",
        schema=MESH_NODE_INFO_SCHEMA,
        handler=handle_mesh_node_info,
        is_async=True,
        emoji="ℹ️",
    )
    ctx.register_tool(
        name="mesh_signal_quality",
        toolset="meshtastic",
        schema=MESH_SIGNAL_QUALITY_SCHEMA,
        handler=handle_mesh_signal_quality,
        is_async=True,
        emoji="📶",
    )
    ctx.register_tool(
        name="mesh_send_dm",
        toolset="meshtastic",
        schema=MESH_SEND_DM_SCHEMA,
        handler=handle_mesh_send_dm,
        is_async=True,
        emoji="💬",
    )
    ctx.register_tool(
        name="mesh_send_broadcast",
        toolset="meshtastic",
        schema=MESH_SEND_BROADCAST_SCHEMA,
        handler=handle_mesh_send_broadcast,
        is_async=True,
        emoji="📢",
    )
    ctx.register_tool(
        name="mesh_telemetry",
        toolset="meshtastic",
        schema=MESH_TELEMETRY_SCHEMA,
        handler=handle_mesh_telemetry,
        is_async=True,
        emoji="📊",
    )
    ctx.register_tool(
        name="mesh_telemetry_history",
        toolset="meshtastic",
        schema=MESH_TELEMETRY_HISTORY_SCHEMA,
        handler=handle_mesh_telemetry_history,
        is_async=True,
        emoji="📈",
    )
    # Solicited requests — these transmit on the shared LoRa channel.
    ctx.register_tool(
        name="mesh_request_telemetry",
        toolset="meshtastic",
        schema=MESH_REQUEST_TELEMETRY_SCHEMA,
        handler=handle_mesh_request_telemetry,
        is_async=True,
        emoji="🔋",
    )
    ctx.register_tool(
        name="mesh_request_position",
        toolset="meshtastic",
        schema=MESH_REQUEST_POSITION_SCHEMA,
        handler=handle_mesh_request_position,
        is_async=True,
        emoji="📍",
    )
    ctx.register_tool(
        name="mesh_traceroute",
        toolset="meshtastic",
        schema=MESH_TRACEROUTE_SCHEMA,
        handler=handle_mesh_traceroute,
        is_async=True,
        emoji="🛰️",
    )
    ctx.register_tool(
        name="mesh_pause",
        toolset="meshtastic",
        schema=MESH_PAUSE_SCHEMA,
        handler=handle_mesh_pause,
        is_async=True,
        emoji="⏸️",
    )
    ctx.register_tool(
        name="mesh_resume",
        toolset="meshtastic",
        schema=MESH_RESUME_SCHEMA,
        handler=handle_mesh_resume,
        is_async=True,
        emoji="▶️",
    )
