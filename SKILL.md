# Meshtastic LoRa Mesh Channel & Network Tools

The `meshtastic-platform` plugin integrates Meshtastic LoRa radios as a messaging channel and AI toolset for Hermes Agent. This provides fully off-grid, secure, infrastructure-independent communication capabilities.

---

## Technical Constraints & Airtime Limits

LoRa mesh radios operate over unlicensed sub-GHz bands (such as 915 MHz in the US or 868 MHz in Europe) with highly constrained bandwidth. Please observe the following constraints:

1. **Length Limits**: The raw payload ceiling is **233 UTF-8 bytes** (`mesh_pb2.Constants.DATA_PAYLOAD_LEN`), but encrypted direct messages (PKI) leave less usable room, so outbound replies are chunked at a conservative **~170 UTF-8 bytes** by default (override via `MESHTASTIC_CHUNK_BYTES`, clamped to 233). Longer messages are automatically split into numbered chunks and delivered sequentially.
2. **Delivery Speed**: Message propagation is slow, averaging 1–3 seconds per hop. Responses may have noticeable latency.
3. **No Rich Media**: Images, voice, or files are **not supported**. The channel relies strictly on plain-text messaging.
4. **Duty Cycle Limits**: European operators must adhere to standard 10% duty-cycle limits to manage shared channel airtime.
5. **Tool progress is short**: while tools run, the mesh may show a brief emoji
   status (e.g. searching / reading), not full tool arguments or multi-step
   dumps. Wait for the final reply for the real answer.
6. **Airtime discipline for solicited tools**: `mesh_request_telemetry`,
   `mesh_request_position`, and `mesh_traceroute` each transmit to **exactly one**
   node, are **never retried**, and return `answered: false` on silence rather
   than sweeping the mesh.

---

## Configuration Variables

Configure the adapter via your `config.yaml` or directly using these environment variables in your `.env`:

| Env Variable | Type | Description | Default |
|---|---|---|---|
| `MESHTASTIC_SERIAL_PORT` | String | Path to serial device (e.g. `/dev/cu.usbserial-110`), `auto` for discovery, or `mock_port` for dry-run. | `auto` |
| `MESHTASTIC_BAUD_RATE` | Integer | Informational only — meshtastic library always opens serial at 115200. | `115200` |
| `MESHTASTIC_TCP_HOST` | String | Hostname/IP of a WiFi/Ethernet node. When set, connects over TCP instead of serial. | None |
| `MESHTASTIC_TCP_PORT` | Integer | TCP API port of the Meshtastic node. | `4403` |
| `MESHTASTIC_ALLOWED_NODES` | List | Comma-separated list of permitted node IDs (e.g., `!da1b1613`). | None |
| `MESHTASTIC_ALLOWED_USERS` | List | Legacy alias for `MESHTASTIC_ALLOWED_NODES`. | None |
| `MESHTASTIC_ALLOW_ALL_USERS`| Boolean| If set to `true`, permits any node in the mesh to interact with Hermes. | `false` |
| `MESHTASTIC_ALLOW_CHANNELS` | Boolean| If `true`, the agent also answers channel/broadcast messages (replies into the shared channel). Off by default so only DMs are answered. | `false` |
| `MESHTASTIC_HOME_CHANNEL` | String | Default delivery target for automated cron jobs (e.g. `meshtastic:!da1b1613` or `meshtastic:channel:0`). Bare node/`channel:N` values are auto-prefixed. | Empty |
| `MESHTASTIC_CHUNK_BYTES` | Integer | Max UTF-8 bytes per outbound LoRa chunk. Clamped to `[30, 233]`. | `170` |
| `MESHTASTIC_CHUNK_DELAY` | Float | Delay in seconds between chunk sends. | `4.0` |
| `MESHTASTIC_ACK_TIMEOUT` | Float | Seconds to wait for ACK/NACK per outbound chunk. `0` means non-blocking logging only. | `0` |
| `MESHTASTIC_SEND_RETRIES` | Integer | Extra delivery attempts for un-ACKed **direct-message** chunks. `> 0` implies waiting for the ACK; broadcasts are never retried. | `0` |
| `MESHTASTIC_RETRY_BACKOFF` | Float | Seconds to wait between delivery retries. | `5.0` |
| `MESHTASTIC_TELEMETRY_RETENTION_DAYS` | Integer | Age (days) at which persisted telemetry/position/signal rows are pruned from SQLite. `0` disables pruning. | `30` |
| `MESHTASTIC_TELEMETRY_MAX_ROWS` | Integer | Hard ceiling on rows per SQLite table (newest first). `0` disables the cap. | `100000` |
| `MESHTASTIC_OPEN_TIMEOUT` | Float | Seconds to bound a success-path interface open. `0` waits indefinitely. | `20` |
| `MESHTASTIC_OPEN_CANCEL_TIMEOUT` | Float | Seconds to wait for a cancelled open to settle. `0` abandons immediately. | `5` |
| `MESHTASTIC_EXECUTOR_SHUTDOWN_TIMEOUT` | Float | Seconds to drain the transport worker on disconnect. `0` does not wait. | `5` |
| `MESHTASTIC_MOCK` | Boolean | Dry-run mock only when the meshtastic library is **missing**. With the library installed, use `MESHTASTIC_SERIAL_PORT=mock_port` instead. | `false` |
| `MESHTASTIC_AUTOINSTALL` | Boolean | When the library is missing, one automatic `pip install -r requirements.txt` per process. Set `0`/`false` to disable. | `true` |

---

## User & Channel Scoping

Scoping works identically to Telegram's chat partition rules:

1. **Direct Messages (DMs)**:
   * Map to unique session keys in the format `meshtastic:<nodeId>` (e.g., `meshtastic:!da1b1613`).
   * Each user node maintains its own isolated AI conversation thread.
2. **Channel Broadcasts**:
   * Map to shared group chat session keys in the format `meshtastic:channel:<channel_index_or_name>` (e.g., `meshtastic:channel:0`).
   * All messages shared on the channel are viewed and replied to within a shared session.
   * Bridging is **opt-in** via `MESHTASTIC_ALLOW_CHANNELS` (DMs only by default).

---

## Network & Management Tools

Once configured, the AI agent is equipped with the following **13** tools:

### Read-only (already-heard data; no transmit)

- **`mesh_list_nodes`**: Visible nodes with hop distance, direct-range flags, and signal provenance.
- **`mesh_list_channels`**: The gateway node's channels — index, name, role, encryption kind (never the key), MQTT flags.
- **`mesh_node_info`**: Hardware model, firmware, position, battery for a specific node.
- **`mesh_signal_quality`**: SNR/RSSI and historic quality trends (direct vs relayed provenance).
- **`mesh_telemetry`**: Latest device metrics and sensor telemetry (battery, voltage, environment).
- **`mesh_telemetry_history`**: Past telemetry, position, or signal rows from SQLite (`since_hours` / limit).

### Direct send

- **`mesh_send_dm`**: Private direct message to one node.
- **`mesh_send_broadcast`**: Message on the primary/secondary channel.

### Solicited (transmit once; never retry)

- **`mesh_request_telemetry`**: Ask one node for a telemetry response.
- **`mesh_request_position`**: Ask one node for a position response.
- **`mesh_traceroute`**: Trace route + per-hop SNR to one node (both directions).

### Radio pause (free the node for phone app / web UI)

- **`mesh_pause`**: Release the gateway radio link (optional timed auto-resume, tool-capped).
- **`mesh_resume`**: Resume reconnecting to the gateway node.

---

## Troubleshooting

### Direct Messages (DMs) Failing Silently
> [!WARNING]
> If direct messages to a node are failing silently, the destination node's public key might not be initialized.
>
> **Resolution**: Connect the destination node to the official Meshtastic mobile app (iOS/Android) via Bluetooth at least once. This triggers the firmware to fully generate public/private key pairs and upload key metadata to the mesh, enabling direct message encryption.

### Unrecognized Serial Port
If the node cannot connect over USB, check that you have the appropriate CP210X / CH34X virtual COM port drivers installed on your operating system.
