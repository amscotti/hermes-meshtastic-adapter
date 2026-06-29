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
