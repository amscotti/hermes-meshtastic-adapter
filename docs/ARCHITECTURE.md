# Architecture

Guidance for evolving `hermes-meshtastic-adapter` (Hermes plugin
`meshtastic-platform`, platform `meshtastic`): how the code is shaped, where
new work belongs, and which invariants you must not break. **Read this before
adding a feature or refactoring a module.**

This is not a line-count inventory or a freeze of every helper name. Prefer
the rules and flows below over chasing exact LOC or cyclomatic complexity
numbers — CI enforces the hard edges; this doc explains *why* they exist and
*where* to put new code.

## Where this doc fits

| Doc | Audience | Role |
| --- | --- | --- |
| `README.md` | users | install, config, hardware, delivery semantics |
| `docs/DEVELOPING.md` | contributors | bootstrap, tests, gates, feature workflow |
| `docs/ARCHITECTURE.md` (this file) | humans + AI agents | **how to grow the system**: hub-and-spoke map, data paths, anti-god-class rules |
| `CLAUDE.md` | AI agents | living detail (threading, freshness, delivery gotchas) |
| `AGENTS.md` | AI agents | command reference, gate semantics, silent-failure conventions |

Update this file when a **structural** decision changes (new module, new data
path, new permanent gate, new airtime/authz rule). Do not update it for every
helper rename or line-count drift.

## Design constraints

Three constraints shape almost every design choice:

1. **Blocking radio I/O never runs on an asyncio loop thread.** Serial/TCP
   open, `sendText`, close, and liveness probes run on a single daemon worker
   (`transport.py`). A stuck radio call must not freeze Hermes or pin process
   exit.
2. **Dual event loops.** Hermes may call `send()` on a session loop that is
   not the platform loop that owns the inbound queue. ACK and solicited
   waiters are therefore `concurrent.futures.Future`s (thread-safe
   `set_result` from any thread), awaited with `asyncio.wrap_future` on the
   caller’s loop — never bare `asyncio.Future`s shared across loops.
3. **Untrusted mesh input.** Every heard node may update freshness and
   telemetry (observability). Only **TEXT** from authorized nodes is bridged
   to Hermes — the only path carrying attacker-controlled content.

Secondary constraints that still drive features:

- **Airtime is scarce.** Prefer short progress blurbs, no edit spam, no mesh
  sweeps; solicited tools target one node and do not retry.
- **Payload ceiling is 233 UTF-8 bytes** (`DATA_PAYLOAD_LEN`). Outbound text is
  chunked in `chunking.py`; do not also chunk upstream when
  `splits_long_messages` is set.
- **Plugin load layout is dual.** The code runs as a Hermes package *and* as
  flat modules in tests/CI. Cross-module imports use the dual-import pattern
  (`try: from . import x / except ImportError: import x`).

## Hub-and-spoke layout

```
                    ┌─────────────────────────────────────┐
  Hermes gateway ──►│ adapter.py  (only hub / orchestrator) │
                    └──────────────┬──────────────────────┘
           ┌───────────┬───────────┼───────────┬───────────┐
           ▼           ▼           ▼           ▼           ▼
      inbound.py  ack_state.py  send_path.py  solicited.py  connection.py
           │           │           │           │           │
           ▼           │           ▼           │           │
   mesh_helpers /      │      transport.py ◄───┘           │
   telemetry_db /      │      (daemon I/O worker)          │
   chunking /          └───────────────────────────────────┘
   node_freshness / mock_interface / schemas  (leaves)
```

`adapter.py` is the **only hub**. It owns Hermes surface + lifecycle + thin
orchestration. Domain decisions, state machines, pure helpers, and blocking I/O
live in siblings so they can be unit-tested without hardware.

### Layering (enforced by CI)

| Layer | Modules | Rule |
| --- | --- | --- |
| **Hub** | `adapter.py` | May import any sibling. |
| **Siblings** | `inbound`, `ack_state`, `send_path`, `solicited`, `connection`, `transport`, `mesh_tools`, `mesh_helpers` | Never `import adapter`. Adapter-specific needs are injected or parameter-passed. |
| **Leaves** | `chunking`, `schemas`, `telemetry_db`, `node_freshness`, `mock_interface` | Import no other repo modules (stdlib only, except as already established). |

Typical sibling edges today (directional, not exhaustive): `inbound` →
`chunking` / `mesh_helpers` / `telemetry_db`; `mesh_tools` → helpers + schemas
+ `send_path` + `telemetry_db`; `send_path` → `ack_state` / `transport` types;
`transport` → `mock_interface`. Prefer adding edges inside an existing concern
over new hub growth.

## Anti-god-class rules (especially for AI-assisted PRs)

`adapter.py` once owned receive, send, ACK, tools, lifecycle, and transport.
**Do not recreate that.** File length is not the guard; **complexity and
layering** are. Prefer extracting pure decisions and small classes over
growing hub methods.

### Belongs in `adapter.py`

- Hermes `BasePlatformAdapter` surface: `connect` / `disconnect` / `send` /
  `edit_message` / `get_chat_info` / policy hooks
- Lifecycle ownership: platform loop, reconnect/drain tasks, pause flag,
  generation counters
- Thin orchestration: schedule onto the platform loop, call a sibling, return
  `SendResult` / hand `MessageEvent` to the gateway
- Tool-facing APIs that must live on the instance (`request_*`, `pause_link` /
  `resume_link`, `get_observed_node`)
- **LoRa presentation hooks** (gateway-sensitive — change carefully):
  - `format_tool_event` — short emoji + verb (stream-dispatch path)
  - `send()` compaction / `_compact_tool_progress_line` — classic
    `progress_callback` path (does **not** call `format_tool_event`)
  - `edit_message` — **no-op success** (no radio); failure causes Hermes to
    re-send each step as a permanent message
  - `SUPPORTS_MESSAGE_EDITING = False`

### Does **not** belong in `adapter.py`

| Put it here | Kind of change |
| --- | --- |
| `send_path.py` | Retry budget, chunk pacing, ACK-outcome classification, DM/channel target resolution |
| `connection.py` | Backoff, reconnect/poll/pause classification, link-drop class, teardown plan |
| `ack_state.py` | ACK/NACK state machine, real vs implicit ACK, `onAckNak` |
| `solicited.py` | Telemetry / position / traceroute waiters |
| `inbound.py` | Normalize / classify / authz pre-check / freshness routing |
| `mesh_helpers.py` | Pure tool formatting (`link_facts`, `position_age`, …) |
| `mesh_tools.py` | `mesh_*` handler bodies (singleton; never `import adapter`) |
| `transport.py` | Open / close / `sendText` / liveness on the daemon worker |
| Leaves | Pure chunking, schemas, SQLite, freshness overlay, mock interface |

### Extraction recipe when the hub must grow

1. Write pure helpers or a small class with **constructor-injected** deps
   (no `import adapter`).
2. Place them in the owning sibling (or a **new** sibling if the concern is
   genuinely new — then add coverage `source` entry + `test_<module>.py` and
   dual-import).
3. Leave at most a thin one-line delegate on the adapter if call sites require
   it; prefer testing the sibling directly.
4. Unit-test the pure path first; keep `test_meshtastic.py` for Hermes-bridge
   smoke only.

## Module roles (what each file is for)

Flat layout, dual-loadable. Names matter more than sizes.

| Module | Role when extending the system |
| --- | --- |
| **`adapter.py`** | Orchestrator only. Wire Hermes APIs to siblings; own loops and lifecycle; do not re-absorb domain policy. |
| **`inbound.py`** | Receive pipeline (`InboundProcessor`): normalize envelope, classify portnum, freshness, telemetry/position routing, TEXT authz pre-check → `InboundResult`. New portnums and receive stages land here with constructor DI. |
| **`ack_state.py`** | ACK/NACK state machine (`AckTracker`): pending waiters, real vs implicit ACK, pubsub upgrade, pruning. Magic callback name `onAckNak`. Extend pure helpers before growing the tracker’s adapter surface. |
| **`send_path.py`** | Pure outbound *decisions*: retries, pacing, transport-error mapping, ACK-outcome classification, chat-id → DM/channel targets. Keep adapter-free. |
| **`solicited.py`** | Response-waiter registry for the three transmit tools. Inject normalize / interfaces / executor / link-lost; never import the adapter. |
| **`connection.py`** | Pure lifecycle *decisions*: backoff, reconnect step, pause UX, link-drop classification, teardown planning. Orchestration stays on the adapter. |
| **`transport.py`** | Blocking I/O: daemon executor, open serial/TCP/mock, serialized close, liveness, library autoinstall. Fail loud for real targets when the library is missing. |
| **`mesh_tools.py`** | The thirteen `mesh_*` async handlers. Loaded as logical name **`meshtastic_tools`** (never `tools` — shadows Hermes). Singleton link to the adapter. |
| **`mesh_helpers.py`** | Pure formatting and resolution helpers for tools (hops, signal provenance, history windows). Imports only `telemetry_db` among repo modules. |
| **`telemetry_db.py`** | SQLite at `~/.hermes/meshtastic_telemetry.db` (telemetry / positions / signal_quality). Age retention + optional row ceiling. Express staleness in payloads; do not wipe history to “fix” age. |
| **`chunking.py`** | UTF-8-byte-bounded `[i/n]` chunks; 233-byte ceiling; env clamp. |
| **`node_freshness.py`** | Live per-node overlay (`last_heard` / `snr` / `rssi`); SNR/RSSI only from direct (0-hop) packets. |
| **`schemas.py`** | JSON function schemas for tools (airtime notes for the model). No logic. |
| **`mock_interface.py`** | Dry-run serial mock for tests / `mock_port` / missing-library + `MESHTASTIC_MOCK=1`. Never a silent production fallback for real serial/TCP. |
| **`__init__.py`** | Plugin entry: `register(ctx)` — adapter + tools + schemas. |

## Data flows (mental models)

### Inbound: mesh → Hermes

Pubsub delivers on a **background thread**; Hermes runs on asyncio. Always
bridge with `call_soon_threadsafe` onto the **platform** loop (queue owner).

```
mesh packet (pubsub thread)
      → _on_receive_pubsub → platform-loop queue
      → _consume_incoming_queue
            • lock: generation / _running check only
            • unlock, then _on_receive
      → _on_receive
            ① pubsub ACK upgrade (implicit → real only; never resolve PENDING)
            ② InboundProcessor.process  (freshness + telemetry for all nodes)
            ③ solicited maybe_resolve   (before TEXT auth drop)
            ④ TEXT + authorized → MessageEvent / handle_message
               else drop (channels still opt-in via MESHTASTIC_ALLOW_CHANNELS)
```

**Do not** hold `_lifecycle_lock` across all of `_on_receive`: ACK recording
re-acquires that lock on the multi-hop upgrade path and would deadlock the
platform loop.

### Outbound: Hermes → mesh

```
send()  (any session loop)
  → send_path decisions (retry / pacing / target)
  → chunking (≤ 233 bytes, [i/n] prefixes)
  → daemon transport worker (sendText, wantAck)
  → onAckNak / pubsub upgrade → AckTracker
  → concurrent.futures.Future → asyncio.wrap_future on caller loop
  → classify_ack_outcome → SendResult
```

Defaults are observability-first (non-blocking ACK logging). Optional ACK wait
and DM-only retries are env-gated. Implicit ACK (relay) is **not** a retry
trigger; permanent NAKs are not retried. A permanent chunk failure aborts the
rest of the sequence so a hard error does not flood the channel.

### Solicited requests

Only `mesh_request_telemetry` / `mesh_request_position` / `mesh_traceroute`
transmit for tools. One node, no retries, silence → `answered: false`. Waiters
live in `solicited.py` and resolve **before** the TEXT allowlist so protocol
replies are not dropped.

### Connection lifecycle

`connect()` resolves targets (`transport.connection_targets`) and runs
reconnect + drain on the platform loop. Pure tick decisions live in
`connection.py` (backoff, pause, liveness, link-drop class, teardown plan);
the adapter only orchestrates open/close and task ownership. Pause releases the
radio for the phone app without killing the gateway process.

### Dual-loop checklist (easy to break)

| Concern | Rule |
| --- | --- |
| Inbound / reconnect / drain | Platform loop only |
| ACK / solicited waiters | `concurrent.futures.Future` + `wrap_future` |
| Radio I/O | Daemon transport worker; never close interfaces on a loop thread |
| Disconnect | Fail pending ACKs; shared completion future for concurrent callers |

## Where new code goes

| You are adding… | Put it… |
| --- | --- |
| A new agent tool | `schemas.py` + `mesh_tools.py` handler + `__init__.py` registration; pure bits in `mesh_helpers` / `telemetry_db` as needed |
| A new packet type / portnum | `inbound.py` (+ thin adapter handoff only) |
| ACK / NACK behavior | `ack_state.py` (extend `LifecycleHost` only if the tracker needs more of the adapter) |
| Send / retry / pacing / target policy | `send_path.py` |
| Solicited request / waiter policy | `solicited.py` |
| Reconnect / pause / teardown policy | `connection.py` |
| Serial/TCP/mock open or serialized close | `transport.py` |
| Persistence query | `telemetry_db.py` |
| Freshness overlay field | `node_freshness.py` (and tool helpers that present it) |
| Chunking / payload limits | `chunking.py` |
| Hermes orchestration only | `adapter.py` — **last resort** |

### New sibling module checklist

1. Dual-import from every consumer (package + flat).
2. No `import adapter` unless it *is* the hub.
3. Add to `[tool.coverage.run].source` (use logical name `meshtastic_tools` for
   `mesh_tools.py`).
4. Add `test_<module>.py` (or deliberately extend the extraction-gate legacy
   set — shrink preferred).
5. Document role in this module-roles table and in `CLAUDE.md` if behavior is
   subtle.
6. Run `scripts/check_arch_gates.py` + the usual CI suite.

## Machine-checked invariants

Permanent architecture gates (`scripts/check_arch_gates.py` — also a CI job):

| Gate | Script | Intent |
| --- | --- | --- |
| **Complexity** | `cc_gate.py` | No function ≥ cc 20 (empty allowlist); keep adapter mean complexity low so hub growth hurts mean and trips the gate |
| **Layering** | `layer_gate.py` | Leaves import nothing; siblings never import `adapter` |
| **Extraction** | `extraction_gate.py` | Every top-level module is in coverage `source` and has a dedicated test file (a few modules still grandfathered to integration tests — shrink only with an explicit allowlist edit) |

There is **no** LOC size gate. File length is a smell signal for humans, not a
CI number.

Other invariants pinned by tests or code:

| Invariant | Why it matters |
| --- | --- |
| Chunks ≤ 233 bytes | Firmware / `sendData` hard ceiling |
| ACK callback named exactly `onAckNak` | Library magic-name filter; rename breaks real ACKs while mocks still pass |
| `_lifecycle_lock` → `_ack_lock` in ACK record | Documented lock order; do not reverse |
| Queue consumer does not hold lifecycle lock across `_on_receive` | Avoids multi-hop ACK upgrade deadlock |
| Tools module file is `mesh_tools.py` / logical `meshtastic_tools` | Avoids shadowing Hermes `tools` |
| Read `transport.HAS_MESHTASTIC` / `pub` at call time | Library can be installed mid-process; stale snapshots skip pubsub |
| Mock is opt-in (`mock_port` or missing lib + `MESHTASTIC_MOCK=1`) | Never silent production mock for real targets |
| Snapshot live `iface.nodes` when iterating | Reader thread mutates the dict |

## Mesh UX and airtime (product architecture)

LoRa is shared medium. Features should prefer:

- Short tool-progress blurbs over full tool chrome (both Hermes progress paths)
- Successful no-op `edit_message` (never “fail the edit to skip progress”)
- DMs by default; channels opt-in (`MESHTASTIC_ALLOW_CHANNELS`)
- One-node solicited requests, no automatic mesh sweeps
- Age/provenance in tool payloads (`heard_directly`, hops, stale position)
  instead of deleting history

Recommended gateway display config:

```yaml
display:
  platforms:
    meshtastic:
      tool_progress: new
      streaming: false
```

## Conventions that keep the structure honest

- **Dual imports** on every cross-module edge (package vs flat tests).
- **Singleton tools link**: `set_adapter` / `_get_adapter`; handlers return
  JSON errors when no adapter is active.
- **AckTracker back-ref**: lifecycle checks via the adapter; keep lock order.
- Read-only `@property` bridges on the adapter for tracker maps are for
  item-level access and tests — never reassign the property itself.
- `MESHTASTIC_HOME_CHANNEL` is normalized in `MeshtasticAdapter.__init__` so
  cron delivery gets a `meshtastic:` chat id.
- Prefer **parameter-passing and DI** over new static imports into the hub.

When in doubt: put policy in a sibling, keep the hub boring, and add a
unit test that does not need a radio.
