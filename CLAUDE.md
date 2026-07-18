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
# Tests (mock serial + temp SQLite; run all five test modules):
.venv/bin/python -m unittest \
  test_meshtastic.py test_chunking.py test_node_freshness.py \
  test_transport.py test_ack_state.py
# Run a single test:
.venv/bin/python -m unittest test_meshtastic.TestMeshtasticPlatform.<method_name>

# Format, lint, type-check (the exact gates CI enforces):
.venv/bin/python -m ruff format .            # CI runs: ruff format --check .
.venv/bin/python -m ruff check .
.venv/bin/python -m pyrefly check \
  --python-interpreter-path .venv/bin/python \
  --search-path ~/.hermes/hermes-agent --min-severity warn

# Coverage (also a CI gate, --fail-under=80):
.venv/bin/python -m coverage run -m unittest \
  test_meshtastic.py test_chunking.py test_node_freshness.py \
  test_transport.py test_ack_state.py \
  && .venv/bin/python -m coverage report -m
```

CI runs `ruff format --check`, `ruff check`, `pyrefly check --min-severity warn`,
and `coverage`+`unittest` — all four must pass. Pyrefly hides warnings unless
`--min-severity warn` is passed; CI uses it, so do the same locally.

## Architecture

Ten source modules, no package nesting. `adapter.py` is the orchestrator;
the five siblings below were extracted to keep each concern testable in
isolation. Every cross-module import uses the dual-import convention (see
"Conventions and gotchas") so the plugin works both as a package (in Hermes)
and as flat modules (in tests/CI).

- **`adapter.py`** — `MeshtasticAdapter(BasePlatformAdapter)`, the orchestrator.
  Owns the platform loop, connection lifecycle, the inbound→Hermes bridge
  (`_on_receive` → `handle_message`), and the outbound `send()` path that
  chunks content and paces chunks via `_send_immediate`. Policy hooks
  (`_dm_policy`, `_group_policy`, `format_tool_event`) and the Hermes-facing
  `connect`/`disconnect`/`send`/`edit_message`/`get_chat_info` surface live
  here. Delegates ACK/freshness/chunking/transport concerns to the modules
  below.
- **`ack_state.py`** — `AckTracker`, the ACK/NACK state machine. Owns the
  `_pending_acks` / `_ack_futures` / `_ack_lock` cluster and the real-vs-implicit
  ACK classification, retry classification, pubsub ACK upgrade, and ACK pruning.
  Holds a back-reference to the adapter for the lifecycle lock/state checks
  interleaved with `_ack_lock` in `_record_ack_response`. Also module-level
  `AckStatus` enum, `PERMANENT_NAK_REASONS`, `ACK_RECORD_LIMIT`, and the pure
  `ack_wait_config` / `send_retries` / `retry_backoff` env readers.
- **`transport.py`** — `_DaemonTransportExecutor` (single-worker daemon thread
  that serializes blocking Meshtastic I/O off the event loop), the lazy
  `meshtastic`/`pubsub`/`serial` imports (`HAS_MESHTASTIC`, `pub`), and the
  pure target-resolution/interface-construction helpers
  (`connection_targets`, `parse_tcp_target`, `open_interface`,
  `discover_serial_ports`, `DEFAULT_TCP_PORT`).
- **`chunking.py`** — `chunk_message` / `split_utf8` plus `MAX_MESSAGE_LENGTH`
  (233, the `DATA_PAYLOAD_LEN` ceiling) and `DEFAULT_CHUNK_BYTES` (170). Pure
  functions; the adapter delegates `_chunk_message` here.
- **`node_freshness.py`** — `NodeFreshness`, the live-observed per-node overlay
  (`last_heard` / `snr` / `rssi`) layered over the library node DB. Bounded at
  `OBSERVED_NODE_LIMIT` (2048).
- **`mock_interface.py`** — `MockLocalNode` + `MockSerialInterface`, the
  fallback used when Meshtastic deps are missing or no port is found so the
  plugin always loads.
- **`tools.py`** — the seven `mesh_*` async tool handlers exposed to the agent.
- **`schemas.py`** — JSON function schemas for those tools.
- **`telemetry_db.py`** — SQLite persistence (`telemetry`, `positions`,
  `signal_quality` tables) at `~/.hermes/meshtastic_telemetry.db`.
- **`__init__.py`** — `register(ctx)` plugin entry point.

`adapter.py` re-exports a handful of names so existing imports/tests keep
resolving: `AckStatus`, `HAS_MESHTASTIC`, `pub`, `DEFAULT_TCP_PORT`,
`_DaemonTransportExecutor`, `MockSerialInterface`, `MockLocalNode`. It also
keeps class-level aliases (`MAX_MESSAGE_LENGTH`, `DEFAULT_CHUNK_BYTES`,
`OBSERVED_NODE_LIMIT`, `ACK_RECORD_LIMIT`) and thin one-line method delegates
(`_chunk_message`, `_update_observed`, `get_observed_node`,
`_connection_targets`, `_open_interface`, `_track_pending_ack`,
`_record_ack_response`, `get_ack_status`, etc.) so call sites inside the
adapter and in `tools.py` were not churned.

### Inbound path (mesh → Hermes), and its threading boundary

This is the subtlest part of the code. Meshtastic's `pubsub` delivers packets on a **background thread**, but Hermes runs on an asyncio loop. The bridge:

1. `_on_receive_pubsub` (pubsub thread) → `_schedule_on_loop(self.loop, ...)` pushes onto `self._incoming_queue` (asyncio.Queue). Always the **platform** loop from `connect()` — that loop owns the queue.
2. `_consume_incoming_queue` (loop task) drains it and calls `_on_receive`.
3. `_on_receive` first offers routing packets to `_ack_tracker._maybe_record_pubsub_ack` (in `ack_state.py`) — a fallback that only *upgrades* an existing `IMPLICIT_ACK` (relay) record to a real ACK, because the one-shot `onAckNak` callback is consumed by the first response. It intentionally never resolves a still-`PENDING` waiter (packet-id reuse risk). It then records live freshness for the sender via `_node_freshness.update` (in `node_freshness.py`, BEFORE the auth gate, so even non-allowlisted nodes get a current `last_heard`/signal), then authorizes the sender, filters self-echo, logs signal/telemetry/position to SQLite, and for TEXT packets builds a `MessageEvent` and calls `self.handle_message(event)`.

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

`iface.nodes[x]["lastHeard"]` from the meshtastic library only refreshes from periodic **NodeInfo** packets, so it lags a node's actual transmissions. To fix this, `_on_receive` feeds every packet into `self._node_freshness` (a `NodeFreshness` instance from `node_freshness.py`): `last_heard` is bumped from each packet's `rxTime` (clamped to now), and `snr`/`rssi` only from **direct** (0-hop) packets — mirroring the official Meshtastic client. The `mesh_list_nodes` / `mesh_node_info` / `mesh_signal_quality` tools overlay `adapter.get_observed_node(nid)` (delegating to `NodeFreshness.get`) on top of the library node DB (freshest of the two).

Any new packet-handling work must respect this boundary — do not touch loop state from the pubsub thread except via `call_soon_threadsafe`.

### Chat ID / session scoping

`_on_receive` decides DM vs broadcast and forms the chat_id that becomes the Hermes session key:
- DM → `meshtastic:!da1b1613`
- Broadcast → `meshtastic:channel:0` or `meshtastic:channel:Primary`

`_send_immediate` parses these back apart (`split(":", 2)`) to choose `destinationId` vs `channelIndex`.

**Channels are opt-in.** By default `_on_receive` bridges **DMs only** — a broadcast/channel message is logged and dropped so the agent never replies into a shared channel's airtime. `MESHTASTIC_ALLOW_CHANNELS=true` (or `allow_channels` in plugin extra) enables answering channels.

### Outbound path (Hermes → mesh)

`send()` → `_chunk_message` (delegating to `chunking.chunk_message`) splits content into UTF-8-byte-bounded chunks with `[i/n]` prefixes (the protocol app-payload ceiling is 233 bytes — `mesh_pb2.Constants.DATA_PAYLOAD_LEN`; `MESHTASTIC_CHUNK_BYTES` overrides, clamped to 233), paces them by `MESHTASTIC_CHUNK_DELAY` → `_send_chunk` → `_send_immediate` submits the blocking `iface.sendText(..., wantAck=True)` to the lifecycle-scoped `_DaemonTransportExecutor` (from `transport.py`) and awaits it with `asyncio.wrap_future`.

**ACK/NACK is observability-first** and lives in `ack_state.py` (`AckTracker`). By default sends are non-blocking; the magic-named `onAckNak` callbacks (built by `_make_ack_callback_for_send`) just record status into the tracker's bounded stores. Only when `MESHTASTIC_ACK_TIMEOUT > 0` (or send metadata requests it) does `_wait_for_ack` block and let a NAK/timeout make `SendResult.success` false.

**Real vs implicit ACK.** ACK lifecycle is the `AckStatus` `StrEnum` (`pending` / `ack` / `implicit_ack` / `nak` / `timeout`). `AckTracker._record_ack_response` distinguishes a **real** end-to-end ACK (routing ACK sender IS the destination → `AckStatus.ACK`) from an **implicit** ACK relayed by another node (sender ≠ destination → `AckStatus.IMPLICIT_ACK` — packet reached the mesh but dest did not confirm). Mirrors the official client's RECEIVED vs DELIVERED. Only a real ACK (or a NAK) resolves `_wait_for_ack`; an implicit ACK keeps the wait open so a real ACK can still arrive; timeout with only implicit ACKs is retriable. Applies to DMs only (dest is a `!node` id). Values remain plain strings on `raw_response` / `get_ack_status`.

**Optional delivery retry.** `MESHTASTIC_SEND_RETRIES > 0` makes `send()` re-send un-confirmed **DM** chunks up to N times (implies ACK-waiting). `AckTracker._is_retriable_failure` retries transient failures — `AckStatus.TIMEOUT`, `AckStatus.IMPLICIT_ACK` (relay-only), or a non-permanent NAK; `PERMANENT_NAK_REASONS` (e.g. `TOO_LARGE`) and broadcasts are never retried. Backoff is `MESHTASTIC_RETRY_BACKOFF`; the per-chunk attempt count lands in `raw_response["chunks"][i]["attempts"]`.

`edit_message` deliberately returns unsupported — LoRa has no edit primitive, and emulating it would flood the mesh.

### Connection lifecycle

`connect()` resolves connection *targets* via `_connection_targets()` (delegating to `transport.connection_targets`) and spawns one `_reconnect_loop` per target (exponential backoff, keepalive polling) plus `_drain_queue_loop`. A target is an opaque key produced by `transport.py`: a serial devPath, `mock_port`, or a `tcp://host:port` URL. `transport.open_interface` maps the key to a `SerialInterface`, `TCPInterface`, or `MockSerialInterface` (from `mock_interface.py`). A configured `MESHTASTIC_TCP_HOST` takes precedence and is mutually exclusive with serial (one transport at a time). When no hardware/deps are present, it falls back to **`MockSerialInterface`** (two fake nodes) so the plugin always loads — "Plugin uses mock serial connection" means deps are missing or no port was found.

The outbound queue (`_outbound_queue`) is **in-memory only**, bounded at 100, oldest-first eviction; messages queued during a disconnect are lost if the gateway restarts before draining.

### Cron / standalone delivery

`_standalone_send` (wired via `cron_deliver_env_var="MESHTASTIC_HOME_CHANNEL"`) spins up a **short-lived** adapter connection with `allow_queueing=False` so cron failures surface. It does not reuse the live gateway adapter.

## Conventions and gotchas

- **`tools.py` is loaded as the module `meshtastic_tools`**, not `tools`, to avoid colliding with Hermes' own `tools` package. `adapter._load_tools_module` and `test_meshtastic.py` both do this dynamic load; preserve it.
- **The adapter↔tools link is a module-level singleton.** `connect()` calls `tools.set_adapter(self)`; handlers reach it via `_get_adapter()`. Tools return `{"error": ...}` JSON when no adapter is active.
- **The adapter↔AckTracker link is a back-reference.** `AckTracker.__init__(self, adapter)` stores `self._adapter`; lifecycle lock/state, the platform loop, and `_normalize_node_id` are reached via that reference. The lock-ordering in `_record_ack_response` (lifecycle_lock → ack_lock via `ExitStack`) must be preserved exactly. `adapter.py` also exposes 8 read-only `@property` bridges (`_pending_acks`, `_ack_responses`, `_ack_tokens`, `_ack_response_tokens`, `_ack_inflight_tokens`, `_early_ack_packets`, `_ack_futures`, `_ack_lock`) returning the tracker's internals so `send()` and tests that do item-level access (`self._ack_futures[id] = ...`, `with self._ack_lock:`) keep working — never reassign the properties themselves.
- **Dual imports everywhere**: every cross-module import is wrapped `try: from . import x / except ImportError: import x` so the plugin works both as a package (in Hermes) and as flat modules (in tests/CI). This now covers nine modules (`adapter`, `tools`, `schemas`, `telemetry_db`, `chunking`, `mock_interface`, `node_freshness`, `transport`, `ack_state`) — keep the pattern when adding modules.
- **Logger routing caveat**: `ack_state.py` deliberately uses `logging.getLogger("adapter")` (not `__name__`) so the cross-loop INFO log line keeps publishing under the `"adapter"` logger that `test_cross_loop_send_logs_once` asserts on. The other extracted modules use `__name__`.
- Node IDs are `!`-prefixed 8-hex (`!da1b1613`); the allowlist matches with and without the `!`.
- Ruff config (`pyproject.toml`): line length 100, double quotes, target py311. `B008` is ignored globally; `E402` is ignored in the test file (it patches `sys.path` before importing).
- Tests use `MockSerialInterface` and a temp SQLite DB — they require Hermes importable but no real hardware. Per-domain unit tests live in `test_chunking.py`, `test_node_freshness.py`, `test_transport.py`, `test_ack_state.py`; `test_meshtastic.py` holds the integration tests that exercise the assembled adapter.
