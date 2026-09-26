# Hermes Meshtastic Adapter

**Languages:** [English](README.md) · [Español](README.es-ES.md) · [Français](README.fr-FR.md)

`hermes-meshtastic-adapter` is a Hermes Agent platform plugin that connects Hermes to a Meshtastic LoRa mesh. It receives plain-text messages from mesh nodes, forwards them into Hermes sessions, and sends replies back over LoRa as direct messages or channel broadcasts.

<p align="center">
  <img src="assets/demo-meshtastic-chat.jpg" alt="Chatting with the Hermes agent from the Meshtastic phone app, with replies split into numbered chunks and per-message SNR/RSSI" width="300">
  <br>
  <em>Talking to the Hermes agent over LoRa from the Meshtastic app: long replies are split into numbered chunks, each tagged with live signal quality.</em>
</p>

Public naming:

- GitHub repo: `hermes-meshtastic-adapter`
- Hermes plugin name: `meshtastic-platform`
- Hermes platform name: `meshtastic`

## What It Does

- Bridges Meshtastic text messages into Hermes Agent.
- Creates separate Hermes sessions for individual node DMs, such as `meshtastic:!da1b1613`.
- Creates shared Hermes sessions for channel broadcasts, such as `meshtastic:channel:0` or `meshtastic:channel:Primary`.
- Sends Hermes replies back to the source node or channel.
- Splits long replies into numbered LoRa-safe chunks.
- Exposes mesh tools for listing nodes, checking node info, reading signal quality, sending mesh messages, and querying telemetry.
- Stores telemetry, position, and signal history in SQLite.

## Supported Hardware

The adapter connects to a gateway node over USB serial or over TCP/IP.

- ESP32 USB-serial boards such as Heltec WiFi LoRa 32 V3 are supported and are the recommended gateway hardware.
- The gateway node should be wall-powered or USB-powered and configured as a stable base/client node.
- SenseCAP T1000-E and similar nRF52 tracker devices are BLE-first; their USB port is primarily for flashing and serial logs, not reliable control. They are not supported over USB serial by this plugin.
- BLE support is not included in v1.

Recommended gateway node settings:

- Role: `CLIENT` or `CLIENT_BASE`.
- Power: USB or wall power.
- Disable deep sleep and aggressive power saving on the gateway-connected node.
- Bluetooth may be disabled after initial Meshtastic app configuration.
- Region and modem preset must match your mesh.
- `LongFast` or your chosen preset must be configured consistently across nodes.

## Installation

Clone or download the plugin, then copy it into Hermes' plugin directory:

```bash
git clone https://github.com/amscotti/hermes-meshtastic-adapter
mkdir -p ~/.hermes/plugins/meshtastic
cp -R hermes-meshtastic-adapter/* ~/.hermes/plugins/meshtastic/
```

Install dependencies into the Hermes virtual environment:

```bash
~/.hermes/hermes-agent/venv/bin/python -m pip install -r ~/.hermes/plugins/meshtastic/requirements.txt
```

Enable the plugin:

```bash
hermes plugins enable meshtastic-platform
```

Restart the Hermes gateway after changing plugin files or environment variables.

### Plugin installation & updates

`plugin.yaml`'s `version` is display-only — `hermes plugins update` is a `git pull`, so `main` is the update channel. `optional_env` does not render in `hermes config` for user-installed plugins; set env vars via `.env` / config instead. With the symlink install used here, update by directory basename: `hermes plugins update meshtastic` (not `meshtastic-platform`).

## Development

Contributors: [`docs/DEVELOPING.md`](docs/DEVELOPING.md) covers the workflow — repo `.venv` setup, running the test suite and gates (format/lint/types/coverage plus lightweight architecture gates complexity / layering / extraction), the mock-interface smoke test, and a hardware checklist. [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) maps the modules, data flows, and **anti-god-class rules for AI-assisted PRs**. Read both before editing code.

## Configuration

Copy the bundled template and edit it for your node and mesh:

```bash
cp .env.example .env
```

Minimum `.env` example:

```env
MESHTASTIC_SERIAL_PORT=/dev/cu.usbserial-0001
MESHTASTIC_ALLOWED_NODES=!da1b1613
MESHTASTIC_HOME_CHANNEL=meshtastic:!da1b1613
MESHTASTIC_CHUNK_BYTES=170
MESHTASTIC_CHUNK_DELAY=4.0
MESHTASTIC_ACK_TIMEOUT=0
```

Environment variables:

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `MESHTASTIC_SERIAL_PORT` | No* | `auto` | Serial path such as `/dev/cu.usbserial-0001`, or `auto` for discovery. *Configure either this or `MESHTASTIC_TCP_HOST`. |
| `MESHTASTIC_BAUD_RATE` | No | `115200` | Informational only — the meshtastic library always opens serial at 115200. |
| `MESHTASTIC_TCP_HOST` | No | None | Hostname or IP of a WiFi/Ethernet node. When set, the adapter connects over TCP instead of serial. |
| `MESHTASTIC_TCP_PORT` | No | `4403` | TCP API port of the Meshtastic node. |
| `MESHTASTIC_ALLOWED_NODES` | No | Empty | Preferred allowlist. Comma-separated node IDs that may talk to Hermes. |
| `MESHTASTIC_ALLOWED_USERS` | No | Empty | Legacy alias for `MESHTASTIC_ALLOWED_NODES`. |
| `MESHTASTIC_ALLOW_ALL_USERS` | No | `false` | If true, any mesh node may talk to Hermes. Use with caution. |
| `MESHTASTIC_ALLOW_CHANNELS` | No | `false` | If true, the agent also answers **channel/broadcast** messages (replying into the shared channel). Off by default so the agent only responds to direct messages and never spams a public channel's airtime. |
| `MESHTASTIC_HOME_CHANNEL` | No | Empty | Cron/default delivery target, such as `meshtastic:!da1b1613` or `meshtastic:channel:0`. A bare node id (`!da1b1613` or `da1b1613`) / `channel:N` value is auto-prefixed to `meshtastic:` with a warning. |
| `MESHTASTIC_CHUNK_BYTES` | No | `170` | Max UTF-8 bytes per outbound LoRa chunk. `170` is conservative for multi-hop reliability and leaves headroom for encrypted-DM (PKI) overhead; the raw protocol payload ceiling (and the clamp for this value) is `233`. |
| `MESHTASTIC_CHUNK_DELAY` | No | `4.0` | Delay in seconds between chunk sends. |
| `MESHTASTIC_ACK_TIMEOUT` | No | `0` | Seconds to wait for ACK/NACK per outbound chunk. `0` is non-blocking. Set `30` to fail sends on NAK or timeout. |
| `MESHTASTIC_SEND_RETRIES` | No | `0` | Extra delivery attempts for un-ACKed **direct-message** chunks. `> 0` implies waiting for the ACK; transient failures (timeout, no-route) are re-sent, permanent ones (e.g. `TOO_LARGE`) are not. Broadcasts are never retried. |
| `MESHTASTIC_RETRY_BACKOFF` | No | `5.0` | Seconds to wait between delivery retries. |
| `MESHTASTIC_TELEMETRY_RETENTION_DAYS` | No | `30` | Age (days) at which persisted telemetry/position/signal rows are pruned from SQLite. `0` disables pruning. Pruning runs at most hourly, lazily on writes. |
| `MESHTASTIC_TELEMETRY_MAX_ROWS` | No | `100000` | Hard ceiling on rows **per SQLite table** (`telemetry` / `positions` / `signal_quality`), newest first — not per node. Under a flood one chatty node can crowd out others. `0` disables the cap. Age-based retention (`MESHTASTIC_TELEMETRY_RETENTION_DAYS`) still applies. |
| `MESHTASTIC_OPEN_TIMEOUT` | No | `20` | Seconds to bound the success-path interface open before treating it as a connect failure (the constructor still runs on the daemon worker). `0` disables the bound (wait indefinitely). Relevant on slow serial/WiFi where discovery can hang. |
| `MESHTASTIC_OPEN_CANCEL_TIMEOUT` | No | `5` | Seconds to wait for a cancelled in-flight interface open to settle. `0` abandons the cancelled open immediately. |
| `MESHTASTIC_EXECUTOR_SHUTDOWN_TIMEOUT` | No | `5` | Seconds to wait for the transport worker thread to drain pending interface close/liveness jobs during disconnect. `0` does not wait. |
| `MESHTASTIC_MOCK` | No | `false` | `true` runs the adapter against the mock interface (dry-run, no real radio traffic). Only relevant when the meshtastic library is missing; otherwise the adapter always opens the real serial/TCP interface. |
| `MESHTASTIC_AUTOINSTALL` | No | `true` | When the meshtastic library is missing, the adapter runs `pip install -r requirements.txt` into the gateway's Python environment once per process (Hermes updates can wipe plugin deps). Set `0`/`false` to disable and fail with install instructions instead. |

## Connecting Over IP (TCP)

WiFi- or Ethernet-capable nodes expose a TCP API (default port `4403`). Set `MESHTASTIC_TCP_HOST` to connect over the network instead of USB serial:

```env
MESHTASTIC_TCP_HOST=192.168.1.50
MESHTASTIC_TCP_PORT=4403
```

When `MESHTASTIC_TCP_HOST` is set it takes precedence and serial discovery is skipped — the adapter uses a single transport at a time. Enable WiFi/Ethernet and the network API on the node through the Meshtastic app first. Reconnect with exponential backoff and the outbound queue work the same as over serial.

## Chat IDs

Direct messages use node-scoped chat IDs:

```text
meshtastic:!da1b1613
```

Channel messages use group-scoped chat IDs:

```text
meshtastic:channel:0
meshtastic:channel:Primary
```

## Tools

The plugin registers these Hermes tools:

- `mesh_list_nodes`: list visible nodes and signal status.
- `mesh_list_channels`: list the gateway node's channels (name, role, encryption kind — never the key).
- `mesh_node_info`: inspect a node by ID or name.
- `mesh_signal_quality`: check current and recent SNR/RSSI.
- `mesh_send_dm`: send a direct message to a node.
- `mesh_send_broadcast`: send a channel broadcast.
- `mesh_telemetry`: read recent telemetry from a node.
- `mesh_telemetry_history`: query persisted telemetry, position, or signal history.
- `mesh_request_telemetry`: ask a node to send fresh telemetry (solicited request).
- `mesh_request_position`: ask a node to send its current position (solicited request).
- `mesh_traceroute`: trace the route to a node, with per-hop SNR in both directions (solicited request).
- `mesh_pause`: pause the radio — release the gateway node's connection so the phone app or web UI can use it (timed pauses auto-resume; capped at `PAUSE_MAX_MINUTES`, 12h).
- `mesh_resume`: resume the radio after `mesh_pause`.

### Node Freshness

The meshtastic library only refreshes a node's `lastHeard` from periodic **NodeInfo** packets, so it lags a node's actual transmissions. The adapter therefore tracks a live overlay from the packet stream: on every received packet it updates the sender's `last_heard` (from the packet's `rxTime`) and, for direct (0-hop) packets, its `snr`/`rssi` — mirroring the official Meshtastic client. This is done for **every** heard node (including ones not on the allowlist, so you can watch a node you don't bridge), and `mesh_list_nodes` / `mesh_node_info` / `mesh_signal_quality` report the freshest of the library value and this overlay. `mesh_node_info` also returns `last_heard` / `last_heard_epoch`.

## Tool progress (short blurbs, not step dumps)

When the agent uses tools (web search, terminal, …), Hermes can emit
**tool-progress** lines. On platforms with message editing those update in
place; on LoRa they would become permanent radio traffic.

This plugin keeps mesh airtime low:

- Progress is a **short emoji blurb** per tool (e.g. `🔍 Searching the web`),
  not the full query, URL, or shell command.
- Later progress “edits” do **not** re-transmit over the radio.
- The final answer is still delivered in full (chunked as usual).

Recommended Hermes display config (`~/.hermes/config.yaml`):

```yaml
display:
  platforms:
    meshtastic:
      tool_progress: new    # one blurb per tool
      streaming: false
```

Dangerous-command **approval** prompts are separate from tool-progress chrome
and may still appear as longer multi-line messages; reply with `/approve` (or
your configured approval flow) when prompted.

## Delivery Semantics

Meshtastic and LoRa delivery are best-effort.

- The adapter requests ACKs with `wantAck=True` for outbound packets.
- The adapter registers an `onAckNak` callback and records/logs ACK/NACK responses by packet ID when Meshtastic surfaces them.
- The adapter distinguishes a **real** end-to-end ACK (sent by the destination itself) from an **implicit** ACK relayed by another node (the packet reached the mesh but the destination did not confirm receipt) — mirroring the official client's RECEIVED vs DELIVERED. Both a real ACK and an implicit-only ACK count as **delivered** (`send_path.classify_ack_outcome`); an implicit ACK is never retried — the mesh carried the packet, so non-delivery isn't established. The price of keeping the real-ACK upgrade window open is that an implicit-only reply waits out the full ACK timeout before the send returns.
- By default, sends are non-blocking: `sendText()` returning success means the local radio accepted the packet, and later ACK/NACK callbacks are logged if they arrive.
- Set `MESHTASTIC_ACK_TIMEOUT=30` or pass send metadata `meshtastic_ack_timeout` to wait for ACK/NACK per chunk. In this mode, NAKs and timeouts make `SendResult.success` false.
- ACK results are exposed in `SendResult.raw_response["chunks"][i]["ack"]` for waited sends, and can be inspected later in code with `adapter.get_ack_status(packet_id)`.
- Set `MESHTASTIC_SEND_RETRIES=3` to automatically re-send un-ACKed **direct-message** chunks. A retry only fires on a transient failure (ACK timeout, no-route, max-retransmit); permanent NAKs (`TOO_LARGE`, `NO_CHANNEL`, auth/PKI errors) are not retried, and broadcasts are never retried (no per-recipient ACK). Each retry waits `MESHTASTIC_RETRY_BACKOFF` seconds; the per-chunk attempt count is exposed in `SendResult.raw_response["chunks"][i]["attempts"]`. Note: if a message was actually delivered but its ACK was lost, a retry sends a duplicate.
- Long responses are split and paced, but any chunk may still be dropped by the mesh. A permanent chunk failure **aborts the rest of the sequence** for both DMs and broadcasts (`SendResult.success` is `false`, already-sent packet ids are preserved for diagnostics). That avoids flooding the shared channel after a hard error. Empty/whitespace-only content fails the send rather than reporting a false success.

Even with ACK waiting enabled, delivery is still best-effort because ACK behavior depends on route quality, node firmware behavior, and whether the destination is awake.

## Cron Delivery

Set `MESHTASTIC_HOME_CHANNEL` to let Hermes cron jobs deliver output over Meshtastic.

Examples:

```env
MESHTASTIC_HOME_CHANNEL=meshtastic:!da1b1613
MESHTASTIC_HOME_CHANNEL=meshtastic:channel:0
```

The standalone cron sender creates a short-lived adapter connection when needed and disables queueing so cron failures are visible. A future improvement should prefer reusing the already-connected gateway adapter when available.

## Power And Sleep

The plugin does not modify Meshtastic power settings or force remote nodes to stay awake.

What it does:

- Maintains an open USB serial connection to the gateway node.
- Runs reconnect checks.
- Drains queued outbound messages after reconnect.

What it does not do:

- Disable light sleep or deep sleep.
- Change Meshtastic power config.
- Send radio keepalive packets.
- Prevent battery-powered nodes from sleeping.

For a gateway/base station, configure power behavior on the node itself through Meshtastic settings.

## Safety Notes

- Do not enable `MESHTASTIC_ALLOW_ALL_USERS=true` unless you understand the risk.
- Prefer node allowlists and DMs.
- Broadcast sending can consume shared mesh airtime quickly.
- Long AI responses may be impolite on public or busy meshes.
- Mesh messages may be overheard depending on channel configuration and key sharing.
- Do not expose channel keys or private keys to Hermes prompts, logs, or tools.

## Troubleshooting

### Plugin Uses Mock Serial Connection

The mock interface is only ever used when you explicitly opt in or when serial
auto-discovery finds nothing:

- **Meshtastic library missing** — the adapter now fails loudly instead of
  silently pretending to work. The gateway log shows a `RuntimeError` with the
  install command, and the adapter attempts **one automatic**
  `pip install -r requirements.txt` into the running interpreter
  (`MESHTASTIC_AUTOINSTALL=0` disables that). Hermes self-updates rebuild the
  runtime venv and can drop the plugin's dependencies — after an update, check
  the gateway log for this error or for `mock_interface` lines.
- **`MESHTASTIC_MOCK=1`** — explicitly runs against the mock interface
  (dry-run; no real radio traffic) even when the library is missing.
- **No serial port found with `auto`** — a warning is logged and the adapter
  falls back to `mock_port`. Set `MESHTASTIC_SERIAL_PORT` explicitly instead
  of `auto`.

Manual install into the Hermes venv:

```bash
~/.hermes/hermes-agent/venv/bin/python -m pip install -r ~/.hermes/plugins/meshtastic/requirements.txt
```

### Serial Port Not Found

- macOS ports often look like `/dev/cu.usbserial-*` or `/dev/cu.usbmodem*`.
- Linux ports often look like `/dev/ttyUSB*` or `/dev/ttyACM*`.
- Install CP210X or CH34X drivers if your board requires them.
- Make sure no other Meshtastic client is holding the serial port.

### Direct Messages Fail Silently

The target node may not have initialized public key metadata. Pair the node with the official Meshtastic mobile app at least once, then let node info propagate through the mesh.

### Long Replies Are Missing Chunks

- Set `MESHTASTIC_CHUNK_BYTES=170`.
- Increase `MESHTASTIC_CHUNK_DELAY` to `5.0` or higher.
- Prefer shorter prompts and replies on weak or multi-hop meshes.

### Battery Nodes Miss Messages

Sleeping or power-saving nodes may not receive messages immediately. Configure device-side power behavior in Meshtastic.

## Known Limitations

- USB serial and TCP/IP transports are supported; BLE is not implemented.
- Serial and TCP cannot be used at the same time; setting `MESHTASTIC_TCP_HOST` selects TCP.
- ACK/NACK waiting is optional via `MESHTASTIC_ACK_TIMEOUT`; default sends are non-blocking and log later ACK/NACK callbacks when they arrive.
- Delivery retry (`MESHTASTIC_SEND_RETRIES`) is opt-in and DM-only; a lost ACK on an already-delivered message causes a duplicate.
- Cron delivery uses a short-lived serial connection rather than the live gateway adapter.
- The outbound queue is in-memory only (bounded at 100, oldest-first eviction); messages queued during a disconnect are lost if the gateway restarts before the queue drains.
- The plugin does not manage node sleep or power settings.
- Broadcast tools should be used sparingly to avoid wasting shared airtime.
