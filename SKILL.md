# Meshtastic LoRa Mesh Channel & Network Tools

The `meshtastic-platform` plugin integrates Meshtastic LoRa radios as a messaging channel and AI toolset for Hermes Agent. This provides fully off-grid, secure, infrastructure-independent communication capabilities.

---

## Technical Constraints & Airtime Limits

LoRa mesh radios operate over unlicensed sub-GHz bands (such as 915 MHz in the US or 868 MHz in Europe) with highly constrained bandwidth. Please observe the following constraints:

1. **Length Limits**: The maximum payload size is approximately **237 UTF-8 bytes**. Direct messages or replies exceeding this limit are automatically chunked and delivered sequentially.
2. **Delivery Speed**: Message propagation is slow, averaging 1–3 seconds per hop. Responses may have noticeable latency.
3. **No Rich Media**: Images, voice, or files are **not supported**. The channel relies strictly on plain-text messaging.
4. **Duty Cycle Limits**: European operators must adhere to standard 10% duty-cycle limits to manage shared channel airtime.

---

## Configuration Variables

Configure the adapter via your `config.yaml` or directly using these environment variables in your `.env`:

| Env Variable | Type | Description | Default |
|---|---|---|---|
| `MESHTASTIC_SERIAL_PORT` | String | Path to serial device (e.g. `/dev/cu.usbserial-110`) or `auto` for discovery. | `auto` |
| `MESHTASTIC_BAUD_RATE` | Integer | Connection speed of the serial interface. | `115200` |
| `MESHTASTIC_ALLOWED_NODES` | List | Comma-separated list of permitted node IDs (e.g., `!da1b1613`). | None |
| `MESHTASTIC_ALLOWED_USERS` | List | Legacy alias for `MESHTASTIC_ALLOWED_NODES`. | None |
| `MESHTASTIC_ALLOW_ALL_USERS`| Boolean| If set to `true`, permits any node in the mesh to interact with Hermes. | `false` |
| `MESHTASTIC_HOME_CHANNEL` | String | Default delivery channel target for automated cron jobs. | `meshtastic:channel:0` |
| `MESHTASTIC_ACK_TIMEOUT` | Float | Seconds to wait for ACK/NACK per outbound chunk. `0` means non-blocking logging only. | `0` |

---

## User & Channel Scoping

Scoping works identically to Telegram's chat partition rules:

1. **Direct Messages (DMs)**:
   * Map to unique session keys in the format `meshtastic:<nodeId>` (e.g., `meshtastic:!da1b1613`).
   * Each user node maintains its own isolated AI conversation thread.
2. **Channel Broadcasts**:
   * Map to shared group chat session keys in the format `meshtastic:channel:<channel_index_or_name>` (e.g., `meshtastic:channel:0`).
   * All messages shared on the channel are viewed and replied to within a shared session.

---

## Network & Management Tools

Once configured, the AI agent is equipped with the following toolset:

- **`mesh_list_nodes`**: Displays a table of all visible nodes in the mesh along with signal quality metrics.
- **`mesh_node_info`**: Retrieves hardware model, firmware version, latitude/longitude position, and battery level for a specific node.
- **`mesh_signal_quality`**: Analyzes SNR (Signal-to-Noise Ratio) and RSSI values along with historic quality trends.
- **`mesh_send_dm`**: Dispatches a private direct message to a node.
- **`mesh_send_broadcast`**: Broadcasts a message to all nodes on the primary/secondary channel.
- **`mesh_telemetry`**: Retrieves current device metrics and sensor telemetry (battery level, voltage, temperature, humidity, pressure).
- **`mesh_telemetry_history`**: Queries past telemetry, position logs, or link signal trends from the SQLite DB.

---

## Troubleshooting

### Direct Messages (DMs) Failing Silently
> [!WARNING]
> If direct messages to a node are failing silently, the destination node's public key might not be initialized.
> 
> **Resolution**: Connect the destination node to the official Meshtastic mobile app (iOS/Android) via Bluetooth at least once. This triggers the firmware to fully generate public/private key pairs and upload key metadata to the mesh, enabling direct message encryption.

### Unrecognized Serial Port
If the node cannot connect over USB, check that you have the appropriate CP210X / CH34X virtual COM port drivers installed on your operating system.
