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
- **`mesh_tools.py`** — the `mesh_*` async tool handlers exposed to the agent. Named `mesh_tools`, **not** `tools`, so it can't shadow Hermes' own top-level `tools` package (see the collision note under Conventions).
- **`schemas.py`** — JSON function schemas for those tools.
- **`telemetry_db.py`** — SQLite persistence (`telemetry`, `positions`, `signal_quality` tables) at `~/.hermes/meshtastic_telemetry.db`.
- **`__init__.py`** — `register(ctx)` plugin entry point.

### Inbound path (mesh → Hermes), and its threading boundary

This is the subtlest part of the code. Meshtastic's `pubsub` delivers packets on a **background thread**, but Hermes runs on an asyncio loop. The bridge:

1. `_on_receive_pubsub` (pubsub thread) → `_schedule_on_loop(self.loop, ...)` pushes onto `self._incoming_queue` (asyncio.Queue). Always the **platform** loop from `connect()` — that loop owns the queue.
2. `_consume_incoming_queue` (loop task) drains it and calls `_on_receive`.
3. `_on_receive` first offers routing packets to `_maybe_record_pubsub_ack` — a fallback that only *upgrades* an existing `IMPLICIT_ACK` (relay) record to a real ACK, because the one-shot `onAckNak` callback is consumed by the first response. It intentionally never resolves a still-`PENDING` waiter (packet-id reuse risk). It then records live freshness for the sender via `_update_observed`, filters self-echo, and logs signal/telemetry/position to SQLite — all **BEFORE the auth gate**, so the agent stays aware of the whole mesh (freshness, battery, position, signal of every heard node) even for nodes not allowed to talk to it. Those handlers persist numeric fields only, so there's no injection surface. The `_is_authorized_node` gate sits immediately before **TEXT** bridging — the only path carrying attacker-controlled content — and only there does an unauthorized node log a warning and get dropped. For authorized TEXT it builds a `MessageEvent` and calls `self.handle_message(event)`.

### Dual event-loop model

`self.loop` is the **platform loop** (inbound queue, reconnect/drain tasks). Hermes agent sessions may call `send()` on a **different** running loop. Rules:

- **Inbound / normal lifecycle tasks**: `self.loop` (queue owner). Disconnect
  teardown may move to a live caller loop if the original platform loop stops
  or its teardown task is cancelled; generation checks keep old tasks stale.
- **ACK waiters**: `concurrent.futures.Future` in `_ack_futures`; await via `asyncio.wrap_future`. Resolve with `_set_ack_future_result` from any thread (no target-loop schedule).
- **Transport I/O**: `_iface_lock` for short map ops; lifecycle-scoped daemon `_DaemonTransportExecutor` serializes `sendText` / `close` / liveness. Never close interfaces on the event-loop thread; close/shutdown waits are time-bounded.
- **Disconnect**: `_fail_pending_acks("DISCONNECTED")`; concurrent callers poll the shared completion future (no `to_thread` wait). Cancelled open / drain timeouts: `MESHTASTIC_OPEN_CANCEL_TIMEOUT`, `MESHTASTIC_EXECUTOR_SHUTDOWN_TIMEOUT` (`0` = no wait).
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

`send()` → `_chunk_message` splits content into UTF-8-byte-bounded chunks with `[i/n]` prefixes (the protocol app-payload ceiling is 233 bytes — `mesh_pb2.Constants.DATA_PAYLOAD_LEN`; `MESHTASTIC_CHUNK_BYTES` overrides, clamped to 233), paces them by `MESHTASTIC_CHUNK_DELAY` → `_send_chunk` → `_send_immediate` submits the blocking `iface.sendText(..., wantAck=True)` to the lifecycle-scoped `_DaemonTransportExecutor` and awaits it with `asyncio.wrap_future`.

**ACK/NACK is observability-first.** By default sends are non-blocking; `onAckNak` callbacks just record status into `_pending_acks` / `_ack_responses` (bounded at `ACK_RECORD_LIMIT`). Only when `MESHTASTIC_ACK_TIMEOUT > 0` (or send metadata requests it) does `_wait_for_ack` block and let a NAK/timeout make `SendResult.success` false.

**Real vs implicit ACK.** ACK lifecycle is the `AckStatus` `StrEnum` (`pending` / `ack` / `implicit_ack` / `nak` / `timeout`). `_record_ack_response` distinguishes a **real** end-to-end ACK (routing ACK sender IS the destination → `AckStatus.ACK`) from an **implicit** ACK relayed by another node (sender ≠ destination → `AckStatus.IMPLICIT_ACK` — packet reached the mesh but dest did not confirm). Mirrors the official client's RECEIVED vs DELIVERED. Only a real ACK (or a NAK) resolves `_wait_for_ack`; an implicit ACK keeps the wait open so a real ACK can still arrive; timeout with only implicit ACKs is retriable. Applies to DMs only (dest is a `!node` id). Values remain plain strings on `raw_response` / `get_ack_status`.

**Optional delivery retry.** `MESHTASTIC_SEND_RETRIES > 0` makes `send()` re-send un-confirmed **DM** chunks up to N times (implies ACK-waiting). `_is_retriable_failure` retries only on **evidence of non-delivery**: `AckStatus.TIMEOUT` (nothing came back) or a NAK whose reason isn't in `PERMANENT_NAK_REASONS` — notably `MAX_RETRANSMIT`, the firmware's own "reliable send failed" verdict after its `NUM_RELIABLE_RETX` (3) attempts. `AckStatus.IMPLICIT_ACK` is **not** retried: a relay rebroadcast the packet, so the mesh carried it and non-delivery isn't established — and `_maybe_record_pubsub_ack` upgrades the record to a real ACK if the destination's routing ACK arrives later. Retrying on implicit re-sent one reply many times on a relayed path (each app attempt is ~3 radio transmissions), and every copy reached the user. `PERMANENT_NAK_REASONS` (e.g. `TOO_LARGE`) and broadcasts are never retried. Backoff is `MESHTASTIC_RETRY_BACKOFF`; the per-chunk attempt count lands in `raw_response["chunks"][i]["attempts"]`.

`edit_message` deliberately returns unsupported — LoRa has no edit primitive, and emulating it would flood the mesh.

### Connection lifecycle

`connect()` resolves connection *targets* via `_connection_targets()` and spawns one `_reconnect_loop` per target (exponential backoff, keepalive polling) plus `_drain_queue_loop`. A target is an opaque key: a serial devPath, `mock_port`, or a `tcp://host:port` URL. `_open_interface()` maps the key to a `SerialInterface`, `TCPInterface`, or `MockSerialInterface`. A configured `MESHTASTIC_TCP_HOST` takes precedence and is mutually exclusive with serial (one transport at a time). When no hardware/deps are present, it falls back to **`MockSerialInterface`** (two fake nodes) so the plugin always loads — "Plugin uses mock serial connection" means deps are missing or no port was found.

The outbound queue (`_outbound_queue`) is **in-memory only**, bounded at 100, oldest-first eviction; messages queued during a disconnect are lost if the gateway restarts before draining.

### Cron / standalone delivery

`_standalone_send` (wired via `cron_deliver_env_var="MESHTASTIC_HOME_CHANNEL"`) spins up a **short-lived** adapter connection with `allow_queueing=False` so cron failures surface. It does not reuse the live gateway adapter.

## Conventions and gotchas

- **The tool module is `mesh_tools.py`, loaded under the logical name `meshtastic_tools`.** It must NOT be named `tools.py`: Hermes' own code imports `tools.registry` transitively while `gateway` is imported, and a top-level `tools.py` in this repo (which sits first on `sys.path` in the flat test/CI layout) shadows Hermes' `tools` package and breaks the whole import. Loading it dynamically as `meshtastic_tools` was not enough — the collision is at *Hermes'* import site, not ours — hence the distinct filename. `adapter._load_tools_module` and `test_meshtastic.py` both load `mesh_tools.py`; preserve the naming.
- **The adapter↔tools link is a module-level singleton.** `connect()` calls `tools.set_adapter(self)`; handlers reach it via `_get_adapter()`. Tools return `{"error": ...}` JSON when no adapter is active.
- **Dual imports everywhere**: every cross-module import is wrapped `try: from . import x / except ImportError: import x` to work both as a package (in Hermes) and as flat modules (in tests/CI). Keep this pattern when adding modules.
- Node IDs are `!`-prefixed 8-hex (`!da1b1613`); the allowlist matches with and without the `!`.
- Ruff config (`pyproject.toml`): line length 100, double quotes, target py311. `B008` is ignored globally; `E402` is ignored in the test file (it patches `sys.path` before importing).
- Tests use `MockSerialInterface` and a temp SQLite DB — they require Hermes importable but no real hardware.
