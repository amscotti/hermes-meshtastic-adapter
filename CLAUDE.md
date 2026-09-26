# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A **Hermes Agent platform plugin** (`meshtastic-platform`) that bridges a Meshtastic LoRa mesh to Hermes. It is not a standalone app — it is loaded by the Hermes gateway, which calls `register(ctx)` in `__init__.py`. That entry point registers the platform adapter (`adapter.register`) and the thirteen `mesh_*` tools.

The naming is intentionally three-way: GitHub repo `hermes-meshtastic-adapter`, Hermes plugin `meshtastic-platform`, Hermes platform `meshtastic`.

## Critical Dependency: Hermes Agent

The code imports `gateway.*` (`gateway.config`, `gateway.platforms.base`, `gateway.platform_registry`) from **Hermes Agent, which is NOT in this repo**. Nothing imports or type-checks without it resolvable on `sys.path`:

- **Locally**: Hermes is expected at `~/.hermes/hermes-agent` (the default in `test_meshtastic.py` via `HERMES_AGENT_PATH`). Commands run through the repo's `.venv` (uv-managed) — it holds the dev tooling (`ruff`/`pyrefly`/`coverage`); the Hermes venv at `~/.hermes/hermes-agent/venv/bin/python` has none of those.
- **CI** (`.github/workflows/ci.yml`): checks out `NousResearch/hermes-agent` into `_deps/hermes-agent`, installs it editable, and points `--search-path` / `HERMES_AGENT_PATH` there.

When working in this repo without Hermes installed, the `gateway.*` imports will fail — this is expected, not a bug to fix.

## Commands

All commands run via the repo's **`.venv`** (uv-managed), which holds the dev
tooling (`ruff`/`pyrefly`/`coverage`) and resolves `gateway.*`. The Hermes venv
(`~/.hermes/hermes-agent/venv`) does **not** have ruff/pyrefly — don't use it for
these gates. Set `HERMES_AGENT_PATH` if Hermes isn't at `~/.hermes/hermes-agent`.

```bash
# Tests (mock serial + temp SQLite; discovery picks up every test_*.py):
.venv/bin/python -m unittest discover -s . -p "test_*.py"
# Run a single test:
.venv/bin/python -m unittest test_meshtastic.TestMeshtasticPlatform.<method_name>

# Format, lint, type-check (the exact gates CI enforces):
.venv/bin/python -m ruff format .            # CI runs: ruff format --check .
.venv/bin/python -m ruff check .
.venv/bin/python -m pyrefly check \
  --python-interpreter-path .venv/bin/python \
  --search-path ~/.hermes/hermes-agent --min-severity warn

# Coverage (also a CI gate, --fail-under=80):
.venv/bin/python -m coverage run -m unittest discover -s . -p "test_*.py" \
  && .venv/bin/python -m coverage report -m

# Lightweight architecture gates (complexity, layering, extraction):
.venv/bin/python scripts/check_arch_gates.py
```

CI runs `ruff format --check`, `ruff check`, `pyrefly check --min-severity warn`,
`coverage`+`unittest`, and `scripts/check_arch_gates.py` (complexity / layering / extraction). Pyrefly
hides warnings unless `--min-severity warn` is passed; CI uses it, so do the
same locally.

## Architecture

**Read `docs/ARCHITECTURE.md` first** for the hub-and-spoke map, anti-god-class
rules for AI-assisted PRs, and "where new code goes". Do not grow
`adapter.py` with new domain logic — put pure decisions / state machines /
blocking I/O in sibling modules and keep the adapter as orchestration only.

Fourteen source modules, no package nesting. `adapter.py` is the orchestrator;
the siblings below were extracted to keep each concern testable in isolation.
Every cross-module import uses the dual-import convention (see "Conventions
and gotchas") so the plugin works both as a package (in Hermes) and as flat
modules (in tests/CI).

- **`adapter.py`** — `MeshtasticAdapter(BasePlatformAdapter)`, the **only hub**.
  Owns the platform loop, connection lifecycle, the inbound→Hermes bridge
  (`_on_receive` → `handle_message`), and the outbound `send()` path that
  chunks content and paces chunks via `_send_immediate`. Policy hooks
  (`_dm_policy`, `_group_policy`, `format_tool_event`,
  `SUPPORTS_MESSAGE_EDITING`) and the Hermes-facing
  `connect`/`disconnect`/`send`/`edit_message`/`get_chat_info` surface live
  here. Tool progress is short emoji blurbs over LoRa (not full chrome);
  `edit_message` is a no-op success so the gateway does not re-send every
  step. Delegates ACK/freshness/chunking/transport concerns to the modules
  below — **new domain logic goes in those modules, not here**.
- **`ack_state.py`** — `AckTracker`, the ACK/NACK state machine. Owns the
  `_pending_acks` / `_ack_futures` / `_ack_lock` cluster and the real-vs-implicit
  ACK classification, retry classification, pubsub ACK upgrade, and ACK pruning.
  Holds a back-reference to the adapter for the lifecycle lock/state checks
  interleaved with `_ack_lock` in `_record_ack_response`. Also module-level
  `AckStatus` enum, `PERMANENT_NAK_REASONS`, `ACK_RECORD_LIMIT`, and the pure
  `ack_wait_config` / `send_retries` / `retry_backoff` env readers.
- **`send_path.py`** — pure outbound send-path decision helpers, extracted
  from `send()` / `_send_immediate` / `_send_text_serialized`: retry
  budget/eligibility (`should_retry_chunk`, `max_send_attempts`,
  `retry_implies_ack_wait`), chunk pacing (`chunk_pacing_delay`),
  transport-error mapping (`map_transport_error`, `is_executor_shutdown_error`),
  ACK-outcome classification (`classify_ack_outcome`), and DM/channel target
  resolution (`resolve_dm_node`, `dm_send_target`, `channel_send_target`,
  `dest_from_chat_id`) plus the send-result record shapes. Imports `ack_state`
  and `transport` only (dual-import; `transport` supplies the
  `TransportShutdownError`/`is_executor_shutdown_error` mapping);
  everything adapter-specific is parameter-passed.
- **`solicited.py`** — `SolicitedRequestTracker`, the response-waiter registry
  for solicited requests (telemetry/position/traceroute — the only tools that
  transmit): `register_waiter` / `resolve_waiters` / `maybe_resolve` /
  `discard_waiter` / `abandon_all` plus the shared `solicit` wait path
  (discards its waiter and re-raises on cancellation OR any BaseException from
  the transport — the library calls `sys.exit` on an unresolvable destination,
  so `SystemExit` must not leak a waiter). Waiter resolution rejects packets
  whose `rxTime` predates the request only when the timestamp is plausibly in
  the host's clock domain (within a few hours' skew) — an unsynced gateway's
  uptime-sized `rxTime` or a `0` sentinel never discards a genuine reply.
  Stdlib only — it never imports the adapter: normalize-node-id fn,
  interfaces/executor providers, and the link-lost exception are
  constructor-injected; request construction is passed in per call as `send`.
- **`transport.py`** — `_DaemonTransportExecutor` (single-worker daemon thread
  that serializes blocking Meshtastic I/O off the event loop), the re-runnable
  `meshtastic`/`pubsub`/`serial` imports (`HAS_MESHTASTIC`, `pub`),
  `ensure_meshtastic_library` (one automatic `pip install -r requirements.txt`
  per process when the library is missing; `MESHTASTIC_AUTOINSTALL=0`
  disables), the serialized interface-close machinery moved from the adapter
  (`close_interfaces`, `close_interfaces_via_executor`,
  `close_interfaces_on_daemon_thread`, `close_interfaces_after_executor`,
  `close_interfaces_serialized`, `shutdown_transport_executor`,
  `await_concurrent_future`, `drop_interface_if_dead_serialized`), and the
  pure target-resolution/interface-construction helpers
  (`connection_targets`, `parse_tcp_target`, `open_interface`,
  `discover_serial_ports`, `DEFAULT_TCP_PORT`). `open_interface` raises with
  install instructions for real serial/TCP targets when the library is
  missing — the mock is only returned for `mock_port` targets or explicit
  `MESHTASTIC_MOCK=1` opt-in.
- **`connection.py`** — pure connection-lifecycle decision helpers, extracted
  from `_reconnect_loop` / `_disconnect_impl`: backoff
  (`next_backoff`/`reset_backoff`), reconnect-step and liveness-poll
  classification (`reconnect_step`, `poll_outcome`), pause classification and
  resume formatting (`pause_classify`, `resumes_at_str`, `resumes_in_minutes`),
  close-wait policies (`open_cancel_timeout`, `executor_shutdown_timeout`),
  link-drop classification (`classify_link_drop`) and teardown planning
  (`teardown_owner_current`, `teardown_task_list`, `tasks_on_loop`). Stdlib
  only — no repo imports; everything adapter-specific is parameter-passed.
- **`chunking.py`** — `chunk_message` / `split_utf8` plus `MAX_MESSAGE_LENGTH`
  (233, the `DATA_PAYLOAD_LEN` ceiling) and `DEFAULT_CHUNK_BYTES` (170).
  `MESHTASTIC_CHUNK_BYTES` is clamped to a `MIN_CHUNK_BYTES` (30) floor;
  content needing more than `MAX_CHUNKS_PER_MESSAGE` chunks raises. Content is
  preserved exactly (no strip); whitespace-only content yields no chunks.
  `parse_chunk_prefix` recognizes the `[i/n]` format for the receive-side
  diagnostic. Pure functions; the adapter delegates `_chunk_message` here.
- **`node_freshness.py`** — `NodeFreshness`, the live-observed per-node overlay
  (`last_heard` / `snr` / `rssi`) layered over the library node DB. Bounded at
  `OBSERVED_NODE_LIMIT` (2048).
- **`mock_interface.py`** — `MockLocalNode` + `MockSerialInterface`, used for
  tests, `mock_port` targets, and explicit `MESHTASTIC_MOCK=1` dry-runs. Real
  serial/TCP targets NEVER fall back to it: a missing meshtastic library
  raises at connect instead (see transport.py above).
- **`mesh_tools.py`** — the thirteen `mesh_*` async tool handlers exposed to the
  agent. Seven are read-only (they serve already-heard data); three are
  **solicited requests** that transmit — see below. `mesh_send_dm` /
  `mesh_send_broadcast` are the direct-send pair. Named `mesh_tools`, **not**
  `tools`, so it can't shadow Hermes' own top-level `tools` package (see the
  collision note under Conventions).
- **`inbound.py`** — the receive pipeline, extracted from `adapter._on_receive`
  (P3.1): packet normalization, portnum classification, authz pre-check,
  freshness accounting, and telemetry/position routing. `InboundProcessor`
  takes its dependencies by constructor injection (normalize-id fn, freshness
  store, authz callables, writer callbacks) and imports only the pure
  sibling modules `telemetry_db`, `chunking`, and `mesh_helpers`
  (`first_not_none`, `normalize_position_payload`);
  its dependencies are constructor-injected —
  `_on_receive` (cc 10) remains the adapter-side orchestrator that schedules
  onto the platform loop and hands `InboundResult` to the gateway.
- **`mesh_helpers.py`** — pure helpers for the mesh tools (`resolve_node`,
  `link_facts`, `position_age`, `clamp`, `format_route`, `assess_signal_quality`
  …), extracted from `mesh_tools.py` (P3.4); imports only `telemetry_db`.
  `mesh_tools.py` re-imports them (dual-import) so `meshtastic_tools.X` names
  keep resolving.
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
`_record_ack_response`, `get_ack_status`, `_maybe_resolve_solicited`,
`_abandon_response_waiters`, `_close_interfaces`,
`_shutdown_transport_executor`, etc.) so call sites inside the
adapter and in `mesh_tools.py` were not churned.

### Inbound path (mesh → Hermes), and its threading boundary

This is the subtlest part of the code. Meshtastic's `pubsub` delivers packets on a **background thread**, but Hermes runs on an asyncio loop. The bridge:

1. `_on_receive_pubsub` (pubsub thread) → `_schedule_on_loop(self.loop, ...)` pushes onto `self._incoming_queue` (asyncio.Queue). Always the **platform** loop from `connect()` — that loop owns the queue.
2. `_consume_incoming_queue` (loop task) drains it and calls `_on_receive`.
3. `_on_receive` first offers routing packets to `_ack_tracker._maybe_record_pubsub_ack` (in `ack_state.py`) — a fallback that only *upgrades* an existing `IMPLICIT_ACK` (relay) record to a real ACK, because the one-shot `onAckNak` callback is consumed by the first response. It intentionally never resolves a still-`PENDING` waiter (packet-id reuse risk). It then records live freshness for the sender via `_node_freshness.update` (in `node_freshness.py`), filters self-echo, and logs signal/telemetry/position to SQLite — all **BEFORE the auth gate**, so the agent stays aware of the whole mesh (freshness, battery, position, signal of every heard node) even for nodes not allowed to talk to it. Those handlers persist numeric fields only, so there's no injection surface. The `_is_authorized_node` gate sits immediately before **TEXT** bridging — the only path carrying attacker-controlled content — and only there does an unauthorized node log a warning and get dropped. For authorized TEXT it builds a `MessageEvent` and calls `self.handle_message(event)`.

### Dual event-loop model

`self.loop` is the **platform loop** (inbound queue, reconnect/drain tasks). Hermes agent sessions may call `send()` on a **different** running loop. Rules:

- **Inbound / normal lifecycle tasks**: `self.loop` (queue owner). Disconnect
  teardown may move to a live caller loop if the original platform loop stops
  or its teardown task is cancelled; generation checks keep old tasks stale.
- **ACK waiters**: `concurrent.futures.Future` in `_ack_futures`; await via `asyncio.wrap_future`. Resolve with `_set_ack_future_result` from any thread (no target-loop schedule).
- **Transport I/O**: `_iface_lock` for short map ops; lifecycle-scoped daemon `_DaemonTransportExecutor` serializes `sendText` / `close` / liveness. Never close interfaces on the event-loop thread; the serialized close machinery (`close_interfaces_*` family, `shutdown_transport_executor`, `drop_interface_if_dead_serialized`) lives in `transport.py` and close/shutdown waits are time-bounded.
- **Disconnect**: `_fail_pending_acks("DISCONNECTED")`; concurrent callers poll the shared completion future (no `to_thread` wait). Cancelled open / drain timeouts: `MESHTASTIC_OPEN_CANCEL_TIMEOUT`, `MESHTASTIC_EXECUTOR_SHUTDOWN_TIMEOUT` (`0` = no wait).
- First cross-loop send logs once at INFO (`_cross_loop_send_logged`, under `_ack_lock`).

### Node freshness overlay

`iface.nodes[x]["lastHeard"]` from the meshtastic library only refreshes from periodic **NodeInfo** packets, so it lags a node's actual transmissions. To fix this, `_on_receive` feeds every packet into `self._node_freshness` (a `NodeFreshness` instance from `node_freshness.py`): `last_heard` is bumped from each packet's `rxTime` (clamped to now), and `snr`/`rssi` only from **direct** (0-hop) packets — mirroring the official Meshtastic client. The `mesh_list_nodes` / `mesh_node_info` / `mesh_signal_quality` tools overlay `adapter.get_observed_node(nid)` (delegating to `NodeFreshness.get`) on top of the library node DB (freshest of the two).

Any new packet-handling work must respect this boundary — do not touch loop state from the pubsub thread except via `call_soon_threadsafe`.

### Direct range vs signal strength — never conflate them

`_link_facts()` in `mesh_tools.py` is the single place that answers "how far is this node, and does this reading describe it". Every node-reporting tool goes through it.

**Signal strength says nothing about distance.** A relayed packet's SNR/RSSI belong to the last hop, not the origin. In live data, directly-heard nodes span −60 to −122 dBm and fully overlap the relayed ones: `!0b477a00` at −122 dBm is direct, `!a6963ec4` at −98 dBm is two hops out. `_update_observed` gets this right (it records `snr`/`rssi` only off 0-hop packets), but `mesh_list_nodes` used to publish neither hops nor provenance, and its fallbacks pulled SNR from the library node DB and from unfiltered SQLite history. Asked which nodes were in direct range, the agent had nothing else to go on and answered by listing everything with an RSSI — nodes 1 to 5 hops away included.

So the payload now carries: **`heard_directly`** (at least one 0-hop packet ever arrived), **`last_direct_heard`** (when — how the caller judges whether that still holds), **`hops_away`** (distance of the *latest* packet, which legitimately varies as the mesh reroutes), and **`signal_source`** (`direct` / `relayed` / `unknown`). Note that `heard_directly` is deliberately NOT `hops_away == 0`: packets from one node routinely arrive both ways, and keying off the newest alone flips a neighbour in and out of range packet by packet.

Hops resolve through live observations → the library's `hopsAway` (what the official app shows) → `telemetry_db.get_latest_signal_by_node()`. Only that last source survives a gateway restart, which wipes `_node_freshness._observed` — before it existed, every restart left the agent blind to hops entirely.

### Aged data must not read as current

Signal and position history is retained for 30 days, so anything derived from it needs an explicit age or a window — otherwise the tools state three-week-old facts in the present tense.

- **`heard_directly` expires** after `DIRECT_RANGE_WINDOW_SECS` (24h). Live observations assert only while the overlay's `last_heard` is inside the window — the overlay keeps values for the whole session, so a node heard directly at session start and silent for weeks drops out on its own — while a 0-hop reading from the library node DB (which the library keeps current) asserts on its own; a persisted 0-hop row only counts inside the window. That split is why `live_hops` exists separately from `hops_away` — the reported distance may come from an old row, but direct range may not.
- **Position fixes are dated** by `_position_age()`: `position_time`, `position_age_hours`, and `position_is_stale` past `POSITION_STALE_AFTER_SECS` (6h). Coordinates from the node DB carry no age, and an old fix plots on a coverage map exactly like a fresh one — confidently, and in the wrong place.

- **History is queryable by period**, not just by row count: `mesh_telemetry_history` takes `since_hours` (capped at 720 = the retention period), which raises the row cap from 100 to `HISTORY_WINDOW_ROW_CAP` (500) because the window is the ask. A count cannot express a period — 100 rows reaches back five days for a node logging ~53 fixes/day and a month for a quiet one. The reply carries `returned`, `oldest_returned` and `truncated`, so a window cut short by the cap is never read as "nothing older exists". This distinction is not academic: `!2bcb38f2` returns 100 fixes to `limit=100` and *nothing at all* to `since_hours=24`, because it stopped reporting four days ago.

Do not "clean" the database to deal with staleness. It is ~0.3 MB, `maybe_prune()` already enforces `MESHTASTIC_TELEMETRY_RETENTION_DAYS`, and the persisted history is the only hop source that outlives a restart — wiping it blinds the agent for hours. Express age in the payload instead.

### Chat ID / session scoping

`_on_receive` decides DM vs broadcast and forms the chat_id that becomes the Hermes session key:
- DM → `meshtastic:!da1b1613`
- Broadcast → `meshtastic:channel:0` or `meshtastic:channel:Primary`

`_send_immediate` parses these back apart (`split(":", 2)`) to choose `destinationId` vs `channelIndex`.

**Channels are opt-in.** By default `_on_receive` bridges **DMs only** — a broadcast/channel message is logged and dropped so the agent never replies into a shared channel's airtime. `MESHTASTIC_ALLOW_CHANNELS=true` (or `allow_channels` in plugin extra) enables answering channels.

### Outbound path (Hermes → mesh)

The send/retry/ACK-outcome **decisions** live in `send_path.py`'s pure
helpers: `retry_implies_ack_wait` / `max_send_attempts` / `should_retry_chunk`
size the retry budget, `chunk_pacing_delay` paces chunks, `dest_from_chat_id`
+ `resolve_dm_node` / `dm_send_target` / `channel_send_target` pick the
destination, `map_transport_error` / `is_executor_shutdown_error` shape
transport failures, and `classify_ack_outcome` turns the tracked ACK record
into the `SendResult` verdict. `send()` → `_chunk_message` (delegating to
`chunking.chunk_message`) splits content into UTF-8-byte-bounded chunks with
`[i/n]` prefixes (the protocol app-payload ceiling is 233 bytes —
`mesh_pb2.Constants.DATA_PAYLOAD_LEN`; `MESHTASTIC_CHUNK_BYTES` overrides,
clamped to 233), paces them by the `chunk_pacing_delay` decision →
`_send_chunk` → `_send_immediate` submits the blocking
`iface.sendText(..., wantAck=True)` to the lifecycle-scoped
`_DaemonTransportExecutor` (from `transport.py`) and awaits it with
`asyncio.wrap_future`. Empty/whitespace-only content fails the send (no false
success); content over the chunk cap fails as too long. A permanent chunk
failure **aborts the sequence** for both DMs and broadcasts: remaining chunks
are not sent, and `SendResult` reports partial delivery (`success=False`,
already-sent packet ids in `continuation_message_ids` / `raw_response["chunks"]`).
Stopping at the first permanent failure avoids flooding the shared channel with
further chunks after a hard error (e.g. `TOO_LARGE`, missing pubkey).

**ACK/NACK is observability-first** and lives in `ack_state.py` (`AckTracker`). By default sends are non-blocking; the magic-named `onAckNak` callbacks (built by `_make_ack_callback_for_send`) just record status into the tracker's bounded stores. Only when `MESHTASTIC_ACK_TIMEOUT > 0` (or send metadata requests it) does `_wait_for_ack` block and let a NAK/timeout make `SendResult.success` false.

**Real vs implicit ACK.** ACK lifecycle is the `AckStatus` `StrEnum` (`pending` / `ack` / `implicit_ack` / `nak` / `timeout`). `AckTracker._record_ack_response` distinguishes a **real** end-to-end ACK (routing ACK sender IS the destination → `AckStatus.ACK`) from an **implicit** ACK relayed by another node (sender ≠ destination → `AckStatus.IMPLICIT_ACK` — packet reached the mesh but dest did not confirm). Mirrors the official client's RECEIVED vs DELIVERED. Only a real ACK (or a NAK) resolves `_wait_for_ack`; an implicit ACK keeps the wait open so a real ACK can still arrive, and if none does by the timeout the send is treated as **delivered** (`SendResult.success=True`, status stays `implicit_ack`) — never a retry (see below). The price of keeping the upgrade window open is that an implicit-only reply waits out the full `MESHTASTIC_ACK_TIMEOUT` before returning. Applies to DMs only (dest is a `!node` id). Values remain plain strings on `raw_response` / `get_ack_status`.

**Optional delivery retry.** `MESHTASTIC_SEND_RETRIES > 0` makes `send()` re-send un-confirmed **DM** chunks up to N times (implies ACK-waiting). `send_path.should_retry_chunk` (pure; the adapter delegates the decision to it) gates the retry on `ack_state.is_retriable_failure` — retries only on **evidence of non-delivery**: `AckStatus.TIMEOUT` (nothing came back) or a NAK whose reason isn't in `PERMANENT_NAK_REASONS` — notably `MAX_RETRANSMIT`, the firmware's own "reliable send failed" verdict after its `NUM_RELIABLE_RETX` (3) attempts. `AckStatus.IMPLICIT_ACK` is **not** retried: a relay rebroadcast the packet, so the mesh carried it and non-delivery isn't established — and `_maybe_record_pubsub_ack` upgrades the record to a real ACK if the destination's routing ACK arrives later. Retrying on implicit is what re-sent one reply many times on a relayed path (each app attempt is ~3 radio transmissions) — and every copy actually reached the user. `PERMANENT_NAK_REASONS` (e.g. `TOO_LARGE`) and broadcasts are never retried. Backoff is `MESHTASTIC_RETRY_BACKOFF`; the per-chunk attempt count lands in `raw_response["chunks"][i]["attempts"]`.

**Tool progress over LoRa (short emoji blurbs).** Hermes gateway tool chrome
would otherwise dump long lines (and re-send every step when edit fails). This
adapter keeps airtime low:

- `SUPPORTS_MESSAGE_EDITING = False` — no real edit on LoRa.
- `format_tool_event` returns a short blurb (`🔍 Searching the web`, not the
  full query/args) for the stream-dispatch path.
- `send()` runs `_compact_tool_progress_line` on single-line emoji progress so
  the classic `progress_callback` path (which bypasses `format_tool_event`)
  is shortened the same way (`… for <preview>` is stripped).
- `edit_message` returns **success without transmitting** (same `message_id`)
  so Hermes keeps treating the progress bubble as editable and does **not**
  fall back to a new permanent mesh packet per tool step. Emulating edits by
  re-sending would flood the mesh; pretending success is intentional.

Final answers still go through `send()` and are chunked as usual. Multi-line
dumps (e.g. dangerous-command approval walls) are not compacted — only
single-line tool chrome.

### Solicited requests (agent asks a node for data)

`mesh_request_telemetry`, `mesh_request_position` and `mesh_traceroute` are the only tools that **transmit**; everything else serves already-heard data. They map to the library's `sendTelemetry(wantResponse=True)` / `sendPosition(wantResponse=True)` / `sendTraceRoute`.

The response-waiter registry lives in `solicited.py` (`SolicitedRequestTracker`). `solicit()` is the shared wait path: arm a `concurrent.futures.Future` waiter via `register_waiter(kind, node_id)`, submit the transmit through the lifecycle `_transport_executor` (so it can't race close), then `await asyncio.wait_for(asyncio.wrap_future(future), timeout)` — re-raising `CancelledError` after discarding the waiter (the A8 contract). `_on_receive` calls the adapter's one-line `_maybe_resolve_solicited` delegate (→ `tracker.maybe_resolve`) **before the auth gate** — a reply is protocol data addressed to us, so the allowlist must not drop it — matching `TELEMETRY_APP` / `POSITION_APP` / `TRACEROUTE_APP` to any waiter and completing it with `_set_future_result` (the same thread-safe model as ACK waiters). A timeout drops the waiter (`discard_waiter`) so the registry can't leak; `abandon_all(reason)` fails every in-flight waiter when the link drops (`MeshLinkLost`). The tracker never imports the adapter — normalize-node-id, interfaces/executor providers, and the link-lost exception are constructor-injected; the adapter keeps the public `request_telemetry` / `request_position` / `request_traceroute` (which `mesh_tools.py` calls) as thin delegates into `solicit()`.

**Airtime discipline is a design constraint.** LoRa bandwidth is shared, so each request targets exactly ONE node, is **never retried**, and a silent node returns `answered: false` rather than raising — the schemas say so to keep the model from sweeping the mesh. Traceroute reports the real relay chain and per-hop SNR in both directions (SNR arrives scaled by 4), which is what distinguishes a weak-direct path from a healthy relayed one.

### Connection lifecycle

The lifecycle **decisions** live in `connection.py`'s pure helpers —
`next_backoff`/`reset_backoff` (exponential 1s→60s), `reconnect_step` /
`poll_outcome` (per-tick reconnect and liveness verdicts), `pause_classify` /
`resumes_at_str` / `resumes_in_minutes` (pause UX), `classify_link_drop`
(socket_reset vs node_absent), and `teardown_owner_current` /
`teardown_task_list` / `tasks_on_loop` (which task to cancel and which live
loop owns teardown). `_reconnect_loop` (cc 13) and `_disconnect_impl` (cc 17)
orchestrate around them, parameter-passing everything adapter-specific.

`connect()` resolves connection *targets* via `_connection_targets()` (delegating to `transport.connection_targets`) and spawns one `_reconnect_loop` per target (exponential backoff, keepalive polling) plus `_drain_queue_loop`. A target is an opaque key produced by `transport.py`: a serial devPath, `mock_port`, or a `tcp://host:port` URL. `transport.open_interface` maps the key to a `SerialInterface`, `TCPInterface`, or `MockSerialInterface` (from `mock_interface.py`). A configured `MESHTASTIC_TCP_HOST` takes precedence and is mutually exclusive with serial (one transport at a time). When no hardware/deps are present it used to fall back to **`MockSerialInterface`** (two fake nodes) so the plugin always loads; since the fail-loud change (2026-08) a missing library raises with install instructions (after one auto-install attempt) and only `mock_port` / `MESHTASTIC_MOCK=1` produce a mock — "Plugin uses mock serial connection" now means deps are missing or no port was found.

The outbound queue (`_outbound_queue`) is **in-memory only**, bounded at 100, oldest-first eviction; messages queued during a disconnect are lost if the gateway restarts before draining.

**TCP keepalive is armed by us, not the library.** `_apply_tcp_keepalive()` sets `SO_KEEPALIVE` plus idle/interval/count (30s/10s/3) on the node socket, through whichever knob the platform exposes — `TCP_KEEPIDLE` on Linux, `SIO_KEEPALIVE_VALS` via ioctl on Windows, `TCP_KEEPALIVE` on macOS. Without it a silently dead link stays "connected" until the library's **300s** heartbeat or our next failing send, whichever comes first. `TCPInterface._reconnect()` swaps in a fresh socket on any read/write failure and socket options do not survive that, so the liveness poll re-arms whenever the socket identity changes (`_keepalive_socket_id`); it is a cheap no-op otherwise.

**Drops are classified in the log.** `_note_link_drop` timestamps the outage and `_report_link_recovery` reports it on reconnect, splitting **socket resets** (back within `SOCKET_RESET_MAX_OUTAGE_SECS`, i.e. the node stayed up) from **node absences** (longer — reboot, WiFi drop, power loss), with running session totals. The distinction is the whole diagnosis: a handful of resets is normal for an ESP32 over WiFi, while repeated long absences are the node's own health and not something the adapter can fix. Log forensics of 2026-07-24 turned 16 apparent "drops" into 11 absences (user-initiated reboots) and 5 genuine resets — the counters exist so that analysis doesn't have to be redone by hand.

### Pausing the radio (freeing the node)

The node accepts only a couple of TCP clients, so connecting from the phone app or web UI means the gateway must let go first. `hermes plugins disable` + restart is too blunt (drops every platform and in-flight conversation) and an agent can't do it without killing its own process. `mesh_pause` / `mesh_resume` (and `pause_link` / `resume_link`) set `self._paused`; the `_reconnect_loop` checks it each tick, releases the interface via the serialized close (`_pop_interface_for_lifecycle` + the `_close_interfaces` delegate → `transport.close_interfaces`), and stops reconnecting — process, queues and other platforms stay up. A timed pause (`_pause_until`, polled by `_pause_expired`) auto-resumes so "off for a bit" can't become "down all night"; the **tool/schema** path clamps duration at `PAUSE_MAX_MINUTES` (direct `pause_link` callers should pass a finite bound themselves). Outbound messages queue while paused.

### Cron / standalone delivery

`_standalone_send` (wired via `cron_deliver_env_var="MESHTASTIC_HOME_CHANNEL"`) spins up a **short-lived** adapter connection with `allow_queueing=False` so cron failures surface. It does not reuse the live gateway adapter.

## Conventions and gotchas

- **The tool module is `mesh_tools.py`, loaded under the logical name `meshtastic_tools`.** It must NOT be named `tools.py`: Hermes' own code imports `tools.registry` transitively while `gateway` is imported, and a top-level `tools.py` in this repo (which sits first on `sys.path` in the flat test/CI layout) shadows Hermes' `tools` package and breaks the whole import. Loading it dynamically as `meshtastic_tools` was not enough — the collision is at *Hermes'* import site, not ours — hence the distinct filename. `adapter._load_tools_module` and `test_meshtastic.py` both load `mesh_tools.py`; preserve the naming.
- **The adapter↔tools link is a module-level singleton.** `connect()` calls `mesh_tools.set_adapter(self)`; handlers reach it via `_get_adapter()`. Tools return `{"error": ...}` JSON when no adapter is active.
- **The adapter↔AckTracker link is a back-reference.** `AckTracker.__init__(self, adapter)` stores `self._adapter`; lifecycle lock/state, the platform loop, and `_normalize_node_id` are reached via that reference. The lock-ordering in `_record_ack_response` (lifecycle_lock → ack_lock via `ExitStack`) must be preserved exactly. `adapter.py` also exposes 8 read-only `@property` bridges (`_pending_acks`, `_ack_responses`, `_ack_tokens`, `_ack_response_tokens`, `_ack_inflight_tokens`, `_early_ack_packets`, `_ack_futures`, `_ack_lock`) returning the tracker's internals so `send()` and tests that do item-level access (`self._ack_futures[id] = ...`, `with self._ack_lock:`) keep working — never reassign the properties themselves.
- **Dual imports everywhere**: every cross-module import is wrapped `try: from . import x / except ImportError: import x` so the plugin works both as a package (in Hermes) and as flat modules (in tests/CI). This now covers fourteen modules (`adapter`, `mesh_tools`, `schemas`, `telemetry_db`, `chunking`, `mock_interface`, `node_freshness`, `transport`, `ack_state`, `inbound`, `mesh_helpers`, `send_path`, `connection`, `solicited`) — keep the pattern when adding modules.
- **Logger routing caveat**: `ack_state.py` logs under its own name —
  `logging.getLogger(__name__)` (`ack_state.py:53`) since A11 moved it off
  the `"adapter"` logger (its tests assert on `assertLogs("ack_state")`).
  The one exception is `transport.py`'s close/shutdown family, which
  intentionally kept an `_close_logger` bound to the `"adapter"` logger name
  for the logs it took over from the adapter (`test_lifecycle.py` asserts on
  `assertLogs("adapter")` for those).
- Node IDs are `!`-prefixed 8-hex (`!da1b1613`); the allowlist matches with and without the `!`.
- Ruff config (`pyproject.toml`): line length 100, double quotes, target py311. `B008` is ignored globally; `E402` is ignored for the path-patching test modules listed in `[tool.ruff.lint.per-file-ignores]` (they insert `sys.path` before importing).
- Tests use `MockSerialInterface` and a temp SQLite DB — they require Hermes importable but no real hardware. Per-domain unit tests live in matching `test_<module>.py` files; `test_meshtastic.py` holds thin integration/smoke tests; `test_project_config.py` and the arch-gate tests (`test_cc_gate.py`, `test_layer_gate.py`, `test_extraction_gate.py`, `test_arch_gates.py`) pin CI/config contracts. Prefer `unittest discover -s . -p "test_*.py"` (same as CI) so every file is included.
