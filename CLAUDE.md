# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A **Hermes Agent platform plugin** (`meshtastic-platform`) that bridges a Meshtastic LoRa mesh to Hermes. It is not a standalone app — it is loaded by the Hermes gateway, which calls `register(ctx)` in `__init__.py`. That entry point registers the platform adapter (`adapter.register`) and the seven `mesh_*` tools.

The naming is intentionally three-way: GitHub repo `hermes-meshtastic-adapter`, Hermes plugin `meshtastic-platform`, Hermes platform `meshtastic`.

## Critical Dependency: Hermes Agent

The code imports `gateway.*` (`gateway.config`, `gateway.platforms.base`, `gateway.platform_registry`) from **Hermes Agent, which is NOT in this repo**. Nothing imports or type-checks without it resolvable on `sys.path`:

- **Locally**: Hermes is expected at `~/.hermes/hermes-agent` (the default in `test_meshtastic.py` via `HERMES_AGENT_PATH`). Commands run through the Hermes venv at `~/.hermes/hermes-agent/venv/bin/python`.
- **CI** (`.github/workflows/ci.yml`): checks out `NousResearch/hermes-agent` into `_deps/hermes-agent`, installs it editable, and points `--search-path` / `HERMES_AGENT_PATH` there.

When working in this repo without Hermes installed, the `gateway.*` imports will fail — this is expected, not a bug to fix.

## Commands

All commands run via the repo's **`.venv`** (uv-managed), which holds the dev
tooling (`ruff`/`pyrefly`/`coverage`) and resolves `gateway.*`. The Hermes venv
(`~/.hermes/hermes-agent/venv`) does **not** have ruff/pyrefly — don't use it for
these gates. Set `HERMES_AGENT_PATH` if Hermes isn't at `~/.hermes/hermes-agent`.

```bash
# Tests (mock serial + temp SQLite):
.venv/bin/python -m unittest test_meshtastic.py
# Run a single test:
.venv/bin/python -m unittest test_meshtastic.TestMeshtasticPlatform.<method_name>

# Format, lint, type-check (the exact gates CI enforces):
.venv/bin/python -m ruff format .            # CI runs: ruff format --check .
.venv/bin/python -m ruff check .
.venv/bin/python -m pyrefly check \
  --python-interpreter-path .venv/bin/python \
  --search-path ~/.hermes/hermes-agent --min-severity warn

# Coverage (also a CI gate, --fail-under=80):
.venv/bin/python -m coverage run -m unittest test_meshtastic.py \
  && .venv/bin/python -m coverage report -m
```

CI runs `ruff format --check`, `ruff check`, `pyrefly check --min-severity warn`,
and `coverage`+`unittest` — all four must pass. Pyrefly hides warnings unless
`--min-severity warn` is passed; CI uses it, so do the same locally.

## Architecture

Five source modules, no package nesting:

- **`adapter.py`** — `MeshtasticAdapter(BasePlatformAdapter)`, the heart of the plugin. Handles serial connection, the inbound→Hermes bridge, and the outbound chunked send path.
- **`tools.py`** — the seven `mesh_*` async tool handlers exposed to the agent.
- **`schemas.py`** — JSON function schemas for those tools.
- **`telemetry_db.py`** — SQLite persistence (`telemetry`, `positions`, `signal_quality` tables) at `~/.hermes/meshtastic_telemetry.db`.
- **`__init__.py`** — `register(ctx)` plugin entry point.

### Inbound path (mesh → Hermes), and its threading boundary

This is the subtlest part of the code. Meshtastic's `pubsub` delivers packets on a **background thread**, but Hermes runs on an asyncio loop. The bridge:

1. `_on_receive_pubsub` (pubsub thread) → `_schedule_on_loop(self.loop, ...)` pushes onto `self._incoming_queue` (asyncio.Queue). Always the **platform** loop from `connect()` — that loop owns the queue.
2. `_consume_incoming_queue` (loop task) drains it and calls `_on_receive`.
3. `_on_receive` records live freshness for the sender via `_update_observed` (BEFORE the auth gate, so even non-allowlisted nodes get a current `last_heard`/signal), then authorizes the sender, filters self-echo, logs signal/telemetry/position to SQLite, and for TEXT packets builds a `MessageEvent` and calls `self.handle_message(event)`.

### Dual event-loop model

`self.loop` is the **platform loop** (inbound queue, reconnect/drain tasks). Hermes agent sessions may call `send()` on a **different** running loop. Rules:

- **Inbound / lifecycle**: always `self.loop` (queue owner).
- **ACK waiters**: create on `_awaiting_loop()`, resolve with `_schedule_on_loop(future.get_loop(), ...)`. Never bind an awaitable to `self.loop` if a different loop will await it (`ValueError: future belongs to a different loop`).
- **Transport I/O**: `_iface_lock` protects only short `_interfaces` map operations. Slow `sendText` / `close` / liveness work runs on executor threads under `_transport_lock` (`_send_text_locked`, `_close_interfaces_serialized`, `_drop_interface_if_dead_serialized`). Meshtastic is not thread-safe for concurrent sends, and an event loop must never block acquiring the transport lock.
- **Disconnect**: `_fail_pending_acks("DISCONNECTED")` settles outstanding ACK futures on their owning loops.
- First cross-loop send logs once at INFO (`_cross_loop_send_logged`, under `_ack_lock`).

### Node freshness overlay

`iface.nodes[x]["lastHeard"]` from the meshtastic library only refreshes from periodic **NodeInfo** packets, so it lags a node's actual transmissions. To fix this, `_on_receive` maintains `self._node_observed` (per node id, bounded at `OBSERVED_NODE_LIMIT`): `last_heard` is bumped from each packet's `rxTime` (clamped to now), and `snr`/`rssi` only from **direct** (0-hop) packets — mirroring the official Meshtastic client. The `mesh_list_nodes` / `mesh_node_info` / `mesh_signal_quality` tools overlay `adapter.get_observed_node(nid)` on top of the library node DB (freshest of the two).

Any new packet-handling work must respect this boundary — do not touch loop state from the pubsub thread except via `call_soon_threadsafe`.

### Chat ID / session scoping

`_on_receive` decides DM vs broadcast and forms the chat_id that becomes the Hermes session key:
- DM → `meshtastic:!da1b1613`
- Broadcast → `meshtastic:channel:0` or `meshtastic:channel:Primary`

`_send_immediate` parses these back apart (`split(":", 2)`) to choose `destinationId` vs `channelIndex`.

**Channels are opt-in.** By default `_on_receive` bridges **DMs only** — a broadcast/channel message is logged and dropped so the agent never replies into a shared channel's airtime. `MESHTASTIC_ALLOW_CHANNELS=true` (or `allow_channels` in plugin extra) enables answering channels.

### Outbound path (Hermes → mesh)

`send()` → `_chunk_message` splits content into UTF-8-byte-bounded chunks with `[i/n]` prefixes (the protocol app-payload ceiling is 233 bytes — `mesh_pb2.Constants.DATA_PAYLOAD_LEN`; `MESHTASTIC_CHUNK_BYTES` overrides, clamped to 233), paces them by `MESHTASTIC_CHUNK_DELAY` → `_send_chunk` → `_send_immediate` calls the blocking `iface.sendText(..., wantAck=True)` via `run_in_executor`.

**ACK/NACK is observability-first.** By default sends are non-blocking; `onAckNak` callbacks just record status into `_pending_acks` / `_ack_responses` (bounded at `ACK_RECORD_LIMIT`). Only when `MESHTASTIC_ACK_TIMEOUT > 0` (or send metadata requests it) does `_wait_for_ack` block and let a NAK/timeout make `SendResult.success` false.

**Real vs implicit ACK.** ACK lifecycle is the `AckStatus` `StrEnum` (`pending` / `ack` / `implicit_ack` / `nak` / `timeout`). `_record_ack_response` distinguishes a **real** end-to-end ACK (routing ACK sender IS the destination → `AckStatus.ACK`) from an **implicit** ACK relayed by another node (sender ≠ destination → `AckStatus.IMPLICIT_ACK` — packet reached the mesh but dest did not confirm). Mirrors the official client's RECEIVED vs DELIVERED. Only a real ACK (or a NAK) resolves `_wait_for_ack`; an implicit ACK keeps the wait open so a real ACK can still arrive; timeout with only implicit ACKs is retriable. Applies to DMs only (dest is a `!node` id). Values remain plain strings on `raw_response` / `get_ack_status`.

**Optional delivery retry.** `MESHTASTIC_SEND_RETRIES > 0` makes `send()` re-send un-confirmed **DM** chunks up to N times (implies ACK-waiting). `_is_retriable_failure` retries transient failures — `AckStatus.TIMEOUT`, `AckStatus.IMPLICIT_ACK` (relay-only), or a non-permanent NAK; `PERMANENT_NAK_REASONS` (e.g. `TOO_LARGE`) and broadcasts are never retried. Backoff is `MESHTASTIC_RETRY_BACKOFF`; the per-chunk attempt count lands in `raw_response["chunks"][i]["attempts"]`.

`edit_message` deliberately returns unsupported — LoRa has no edit primitive, and emulating it would flood the mesh.

### Connection lifecycle

`connect()` resolves connection *targets* via `_connection_targets()` and spawns one `_reconnect_loop` per target (exponential backoff, keepalive polling) plus `_drain_queue_loop`. A target is an opaque key: a serial devPath, `mock_port`, or a `tcp://host:port` URL. `_open_interface()` maps the key to a `SerialInterface`, `TCPInterface`, or `MockSerialInterface`. A configured `MESHTASTIC_TCP_HOST` takes precedence and is mutually exclusive with serial (one transport at a time). When no hardware/deps are present, it falls back to **`MockSerialInterface`** (two fake nodes) so the plugin always loads — "Plugin uses mock serial connection" means deps are missing or no port was found.

The outbound queue (`_outbound_queue`) is **in-memory only**, bounded at 100, oldest-first eviction; messages queued during a disconnect are lost if the gateway restarts before draining.

### Cron / standalone delivery

`_standalone_send` (wired via `cron_deliver_env_var="MESHTASTIC_HOME_CHANNEL"`) spins up a **short-lived** adapter connection with `allow_queueing=False` so cron failures surface. It does not reuse the live gateway adapter.

## Conventions and gotchas

- **`tools.py` is loaded as the module `meshtastic_tools`**, not `tools`, to avoid colliding with Hermes' own `tools` package. `adapter._load_tools_module` and `test_meshtastic.py` both do this dynamic load; preserve it.
- **The adapter↔tools link is a module-level singleton.** `connect()` calls `tools.set_adapter(self)`; handlers reach it via `_get_adapter()`. Tools return `{"error": ...}` JSON when no adapter is active.
- **Dual imports everywhere**: every cross-module import is wrapped `try: from . import x / except ImportError: import x` to work both as a package (in Hermes) and as flat modules (in tests/CI). Keep this pattern when adding modules.
- Node IDs are `!`-prefixed 8-hex (`!da1b1613`); the allowlist matches with and without the `!`.
- Ruff config (`pyproject.toml`): line length 100, double quotes, target py311. `B008` is ignored globally; `E402` is ignored in the test file (it patches `sys.path` before importing).
- Tests use `MockSerialInterface` and a temp SQLite DB — they require Hermes importable but no real hardware.
