"""
JSON Schemas for Meshtastic AI Agent Tools.
"""

MESH_LIST_NODES_SCHEMA = {
    "type": "function",
    "function": {
        "name": "mesh_list_nodes",
        "description": "Get a formatted list of all visible Meshtastic nodes in the mesh network with their IDs, names, signal metrics, and status.",
        "parameters": {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        },
    },
}

MESH_NODE_INFO_SCHEMA = {
    "type": "function",
    "function": {
        "name": "mesh_node_info",
        "description": "Retrieve detailed configuration and hardware status for a specific node in the mesh network.",
        "parameters": {
            "type": "object",
            "properties": {
                "node_id": {
                    "type": "string",
                    "description": "The unique node ID (e.g. '!da1b1613') or name of the node.",
                }
            },
            "required": ["node_id"],
            "additionalProperties": False,
        },
    },
}

MESH_SIGNAL_QUALITY_SCHEMA = {
    "type": "function",
    "function": {
        "name": "mesh_signal_quality",
        "description": "Check the signal strength (SNR and RSSI) and quality label (Excellent, Good, Fair, Poor) for a specific node.",
        "parameters": {
            "type": "object",
            "properties": {
                "node_id": {
                    "type": "string",
                    "description": "The node ID (e.g. '!da1b1613') or name of the node.",
                }
            },
            "required": ["node_id"],
            "additionalProperties": False,
        },
    },
}

MESH_SEND_DM_SCHEMA = {
    "type": "function",
    "function": {
        "name": "mesh_send_dm",
        "description": "Send a private direct message (DM) to a specific node by ID or name.",
        "parameters": {
            "type": "object",
            "properties": {
                "node_id": {
                    "type": "string",
                    "description": "The target node ID (e.g. '!da1b1613') or name of the node.",
                },
                "message": {
                    "type": "string",
                    "description": "The text message content to send. Keep it brief; longer text is automatically split into numbered ~170-byte LoRa chunks.",
                },
            },
            "required": ["node_id", "message"],
            "additionalProperties": False,
        },
    },
}

MESH_SEND_BROADCAST_SCHEMA = {
    "type": "function",
    "function": {
        "name": "mesh_send_broadcast",
        "description": "Broadcast a text message to all nodes on the primary channel or a specific secondary channel.",
        "parameters": {
            "type": "object",
            "properties": {
                "message": {
                    "type": "string",
                    "description": "The text message content to broadcast. Keep it brief; longer text is automatically split into numbered ~170-byte LoRa chunks.",
                },
                "channel": {
                    "type": "string",
                    "description": "Optional channel index (e.g. '0') or channel name (e.g. 'Primary'). Default is primary channel '0'.",
                },
            },
            "required": ["message"],
            "additionalProperties": False,
        },
    },
}

MESH_TELEMETRY_SCHEMA = {
    "type": "function",
    "function": {
        "name": "mesh_telemetry",
        "description": "Fetch the most recent telemetry readings (battery, voltage, temperature, humidity, pressure, uptime) from a specific sensor-equipped node.",
        "parameters": {
            "type": "object",
            "properties": {
                "node_id": {
                    "type": "string",
                    "description": "The node ID (e.g. '!da1b1613') or name of the node.",
                }
            },
            "required": ["node_id"],
            "additionalProperties": False,
        },
    },
}

MESH_TELEMETRY_HISTORY_SCHEMA = {
    "type": "function",
    "function": {
        "name": "mesh_telemetry_history",
        "description": "Query historical telemetry, position, or signal quality records from the persistent SQLite database for analysis.",
        "parameters": {
            "type": "object",
            "properties": {
                "node_id": {
                    "type": "string",
                    "description": "The node ID (e.g. '!da1b1613') or name of the node.",
                },
                "metric_type": {
                    "type": "string",
                    "enum": ["telemetry", "positions", "signal_quality"],
                    "description": "The type of historical records to fetch. Default is 'telemetry'.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of historical records to return (default: 10, max: 100).",
                },
            },
            "required": ["node_id"],
            "additionalProperties": False,
        },
    },
}

# --- Solicited requests ------------------------------------------------------
# Unlike the read-only tools above, these transmit on the shared LoRa channel.
# Each is addressed to a single node and is never retried, so the agent cannot
# turn a question into a mesh-wide sweep.

MESH_REQUEST_TELEMETRY_SCHEMA = {
    "type": "function",
    "function": {
        "name": "mesh_request_telemetry",
        "description": (
            "REQUIRED: node_id (the node's ID like '!9eabacac' or its name). "
            "Actively ask ONE node over the air for its current device metrics "
            "(battery, voltage, uptime) and wait for the reply. This transmits on the "
            "shared LoRa channel, so use it only when the user asks about a specific "
            "node's live state; prefer mesh_telemetry for already-known data. A node "
            "that is out of range or asleep simply will not answer."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "node_id": {
                    "type": "string",
                    "description": "The node ID (e.g. '!da1b1613') or name of the node to query.",
                },
                "timeout": {
                    "type": "number",
                    "description": "Seconds to wait for the reply (default 45, max 120).",
                },
            },
            "required": ["node_id"],
            "additionalProperties": False,
        },
    },
}

MESH_REQUEST_POSITION_SCHEMA = {
    "type": "function",
    "function": {
        "name": "mesh_request_position",
        "description": (
            "REQUIRED: node_id (the node's ID like '!9eabacac' or its name). "
            "Actively ask ONE node over the air for its current position and wait for "
            "the reply. This transmits on the shared LoRa channel — use it only when "
            "the user asks where a specific node is right now; prefer mesh_node_info "
            "or mesh_telemetry_history for the last known position."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "node_id": {
                    "type": "string",
                    "description": "The node ID (e.g. '!da1b1613') or name of the node to query.",
                },
                "timeout": {
                    "type": "number",
                    "description": "Seconds to wait for the reply (default 45, max 120).",
                },
            },
            "required": ["node_id"],
            "additionalProperties": False,
        },
    },
}

MESH_TRACEROUTE_SCHEMA = {
    "type": "function",
    "function": {
        "name": "mesh_traceroute",
        "description": (
            "REQUIRED: node_id (the node's ID like '!9eabacac' or its name). "
            "Discover the actual radio route to ONE node: which relay nodes carry the "
            "traffic and the SNR of each hop, in both directions. The best tool for "
            "diagnosing why messages to a node are slow, unconfirmed, or lost. "
            "Transmits on the shared LoRa channel, so use it deliberately."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "node_id": {
                    "type": "string",
                    "description": "The node ID (e.g. '!da1b1613') or name of the node to trace.",
                },
                "hop_limit": {
                    "type": "integer",
                    "description": "Maximum hops to traverse (default 5, max 7).",
                },
                "timeout": {
                    "type": "number",
                    "description": "Seconds to wait for the reply (default 60, max 120).",
                },
            },
            "required": ["node_id"],
            "additionalProperties": False,
        },
    },
}
