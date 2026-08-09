# AGENTS.md

Compact guidance for AI agents working in this repo. **Read
`docs/ARCHITECTURE.md` first** (where code goes, layering, anti-god-class rules),
then `CLAUDE.md` for deep conventions (inbound/outbound paths, dual loops,
freshness, delivery gotchas). Do not grow `adapter.py` with new domain logic.

## Setup gotcha: where the tooling actually lives

- **Dev tooling (`ruff`/`pyrefly`/`coverage`/`meshtastic`) is installed in the
  repo's `.venv`** (uv-managed; no `pip` inside). Use `.venv/bin/python` for all
  local commands.
- **The Hermes venv `~/.hermes/hermes-agent/venv/bin/python` does NOT have
  `ruff`/`pyrefly`/`coverage`**. CI instead `pip install`s
  `requirements-dev.txt` into its `actions/setup-python` interpreter.
- **`gateway.*` is NOT in this repo** — import failures without Hermes on
  `sys.path` are expected, not a bug. The `.venv` resolves it locally via
  `~/.hermes/hermes-agent`; CI checks out `NousResearch/hermes-agent` into
  `_deps/`. Set `HERMES_AGENT_PATH` if Hermes lives elsewhere.
- **Hermes self-updates can rebuild the runtime venv and drop the plugin's
  pip deps** (meshtastic/pypubsub). The adapter auto-installs them once per
  process on connect (`MESHTASTIC_AUTOINSTALL=0` disables); for diagnosis,
  a real connection logs `SerialInterface(...)`, a missing library now raises
  instead of silently using the mock.

## Commands (run with `.venv/bin/python`)

```bash
# Full local verification — run all before considering work done:
.venv/bin/python -m ruff format .            # CI runs: ruff format --check .
.venv/bin/python -m ruff check .
.venv/bin/python -m pyrefly check \
  --python-interpreter-path .venv/bin/python \
  --search-path ~/.hermes/hermes-agent --min-severity warn
.venv/bin/python -m coverage run -m unittest discover -s . -p "test_*.py" \
  && .venv/bin/python -m coverage report -m

# Single test:
.venv/bin/python -m unittest test_meshtastic.TestMeshtasticPlatform.<method>

# Architecture gates — complexity, layering, extraction (named; no letter codes).
# Stdlib-only; CI runs them after the four format/lint/type/test gates:
.venv/bin/python scripts/check_arch_gates.py
```

`unittest discover` (not a file list) — CI uses the same invocation, so a new
`test_*.py` (including the four arch-gate tests) can never silently drop out of
the suite.

CI's gates: `ruff format --check`, `ruff check`,
`pyrefly check --min-severity warn`, `coverage`+`unittest`, and the architecture
gates (`scripts/check_arch_gates.py`: complexity / layering / extraction).
Coverage enforces `--fail-under=80` (currently ~97% overall).

## Architecture gates

Three permanent CI gates. Names match what they check (no F1/F2 letter codes —
those were temporary extraction-phase labels and were retired).

Each gate is one stdlib-only script in `scripts/`, runnable standalone or via
the combined runner above:

- **Complexity** (`cc_gate.py`) — McCabe cyclomatic complexity
  (if/elif/for/while/except/with/assert/bool-op/ternary/comprehension +
  comprehension-if filters/async loops/lambda/match): any function cc ≥ 20
  fails (grandfather allowlist is **empty** — re-adding an entry requires an
  explicit decision and a drifted re-added entry FAILS). `adapter.py` mean cc
  must stay ≤ 4.5. Primary anti-god-class guard for AI-assisted PRs.
- **Layering** (`layer_gate.py`) — AST import check: leaf modules (chunking,
  schemas, telemetry_db, node_freshness, mock_interface) must not import any
  repo module; ack_state/mesh_tools/inbound/mesh_helpers/send_path/connection/
  solicited/transport must not import adapter; adapter may import anything.
- **Extraction** (`extraction_gate.py`) — every top-level module must be in
  `[tool.coverage.run].source` (the coverage-drops-to-0% gotcha) and have a
  `test_<module>.py` file (4 modules — adapter, mock_interface, schemas,
  telemetry_db — are still grandfathered to test_meshtastic.py; must shrink,
  and the gate fails if a grandfathered module ever gains its own test file
  without the shrink being acknowledged).

## Rules for AI-assisted changes (do not recreate a god class)

1. **Read `docs/ARCHITECTURE.md` "Where new code goes" before editing.** Put
   pure decisions in `send_path` / `connection` / helpers; state machines in
   `ack_state` / `solicited` / `inbound`; blocking I/O in `transport`. Put
   **only** Hermes orchestration / lifecycle wiring in `adapter.py`.
2. **Do not add domain logic to `adapter.py`.** Prefer a pure helper or a
   sibling module with constructor injection. New one-line delegates and
   property bridges on the adapter are last resort (tests/call sites should
   import the owning module).
3. **Never raise a function to cc ≥ 20** and never re-open the complexity-gate allowlist
   without an explicit human decision recorded in the PR.
4. **Never `import adapter` from a non-hub module** (layering gate). Tools reach the
   adapter only via the `mesh_tools` singleton; trackers use Protocols /
   injected callables.
5. **New top-level module checklist:** dual-import pattern, coverage `source`
   entry (use `meshtastic_tools` for `mesh_tools.py`), `test_<module>.py`,
   mention in `docs/ARCHITECTURE.md` + `CLAUDE.md` module maps.
6. **Keep `test_meshtastic.py` thin** — per-domain tests live in
   `test_<module>.py`; integration is for Hermes-bridge smoke only.

## Commit hygiene

PRs merge with a merge commit (not rebase) so history is not duplicated.

## Pyrefly hides warnings by default

Default `--min-severity` is `error`; warnings print only with `--min-severity
warn`. **CI runs at `--min-severity warn`**, so locally use the same to avoid
accumulating silent warnings (e.g. `unnecessary-type-conversion`). Requires
pyrefly `>=1.1.1` (pinned in `requirements-dev.txt`).

## Coverage config gotcha

The source list in `[tool.coverage.run]` is
`["adapter", "ack_state", "chunking", "connection", "inbound",
"mesh_helpers", "meshtastic_tools", "mock_interface", "node_freshness",
"send_path", "solicited", "telemetry_db", "schemas", "transport"]` (14
entries, one per top-level module) — note **`meshtastic_tools`, not
`tools`** (see the dynamic-load convention below), and that every extracted
sibling module must be added here or its coverage silently drops to 0%.
`coverage run -m unittest ...` reads this config; no `--source` flag needed.
Tests are split across `test_meshtastic.py` (integration) plus
`test_inbound.py` / `test_ack_state.py` / `test_mesh_tools.py` /
`test_mesh_helpers.py` / `test_transport.py` / `test_chunking.py` /
`test_node_freshness.py` / `test_send.py` / `test_lifecycle.py` /
`test_send_path.py` / `test_connection.py` / `test_solicited.py` /
`test_project_config.py` (CI/config invariants) plus the four arch-gate
tests (`test_cc_gate.py` / `test_extraction_gate.py` / `test_layer_gate.py` /
`test_arch_gates.py`); run them all together for an accurate number.

## Payload ceiling is 233 bytes

`mesh_pb2.Constants.DATA_PAYLOAD_LEN == 233`; `sendData` raises above it.
`chunking.MAX_MESSAGE_LENGTH = 233` and `chunking.chunk_message` (delegated via
`adapter._chunk_message`) clamps `MESHTASTIC_CHUNK_BYTES` to it.

## Conventions that cause silent failures if broken

- **The ACK callback must be literally named `onAckNak`.** The meshtastic
  library suppresses plain-ACK delivery unless `callback.__name__ == "onAckNak"`
  (magic-name check in `mesh_interface.py`). Lives inside
  `ack_state.AckTracker._make_ack_callback_for_send`. Renaming it silently
  breaks ACK tracking on real hardware — mock tests still pass because they
  invoke the callback directly.
- **The tool module is `mesh_tools.py`, imported as module `meshtastic_tools`,
  never `tools`** — the filename must not collide with Hermes' own `tools`
  package (Hermes imports `tools.registry` transitively; a top-level `tools.py`
  here shadows it and breaks the whole import). Set up by dynamic load in
  `adapter._load_tools_module` and `test_meshtastic.py`; preserve it.
- **The mock interface must never masquerade as production.** `open_interface`
  raises with install instructions when the meshtastic library is missing (real
  serial/TCP target), after one automatic `pip install -r requirements.txt`
  attempt per process (`transport.ensure_meshtastic_library`;
  `MESHTASTIC_AUTOINSTALL=0` disables, `MESHTASTIC_MOCK=1` explicitly opts into
  the dry-run mock). `mock_port` targets still return the mock — tests use it.
- **Read `transport.HAS_MESHTASTIC` / `transport.pub` at call time**, never the
  adapter's module-level snapshots (those are back-compat aliases only): the
  library can be pip-installed and re-imported after import (see the bullet
  above), and stale snapshots would silently skip pubsub subscription. Tests
  must patch `transport.X`, not `adapter.X`.
- **`MESHTASTIC_HOME_CHANNEL` is normalized in `MeshtasticAdapter.__init__`**
  (`_expand_home_channel_env_for_gateway` in `adapter.py`): a bare node id
  (`!node` or bare 8-hex) / `channel:N` value is rewritten to
  `meshtastic:!node` / `meshtastic:channel:N` with a warning, because Hermes
  cron delivery passes the env value through as the chat id and the send path
  requires the `meshtastic:` prefix.
- **Threading boundary**: meshtastic `pubsub` delivers on a background thread;
  all asyncio-loop state is touched only via `_schedule_on_loop` /
  `loop.call_soon_threadsafe`. `_on_receive` runs on the platform loop, not
  the pubsub thread.
- **Dual event-loop model** (easy to break silently):
  - **Platform loop** (`self.loop`, set in `connect()`): owns `_incoming_queue`,
    reconnect/drain tasks, and the pubsub→queue bridge. Inbound **must** always
    schedule onto `self.loop` — never onto a send loop.
  - **Send/ACK waiters**: stored as `concurrent.futures.Future` in
    `AckTracker._ack_futures` (thread-safe `set_result` from pubsub or
    disconnect on any loop). Callers await `asyncio.wrap_future(...)` on the
    send loop. Do not store bare `asyncio.Future` in `_ack_futures` — that
    reintroduces cross-loop settle failures when the awaiter's loop is not
    running.
  - **Transport serialization**: `_iface_lock` protects only short
    `_interfaces` map operations. Slow `sendText`/`close`/liveness calls run on
    the lifecycle-scoped single daemon transport worker
    (`transport._DaemonTransportExecutor`). Never run Meshtastic close on the
    event-loop thread.
  - **Disconnect** settles pending ACKs via `AckTracker._fail_pending_acks`,
    concurrent callers poll the shared completion future (no default-executor
    wait — avoids pool deadlock). Bounds cancelled-open wait
    (`MESHTASTIC_OPEN_CANCEL_TIMEOUT`, `0` = abandon immediately) and
    close/executor drain (`MESHTASTIC_EXECUTOR_SHUTDOWN_TIMEOUT`). Transport
    worker is a **daemon** thread so a stuck open cannot pin process exit.
  - **AckTracker↔adapter back-ref**: `AckTracker._record_ack_response`
    acquires `_adapter._lifecycle_lock` → checks stale lifecycle → acquires
    `_ack_lock`, in that order (ExitStack). Preserve this ordering. The 8
    read-only `@property` bridges on `MeshtasticAdapter` (`_pending_acks`,
    `_ack_responses`, `_ack_tokens`, `_ack_response_tokens`,
    `_ack_inflight_tokens`, `_early_ack_packets`, `_ack_futures`, `_ack_lock`)
    forward to the tracker — only item-level access (`self._ack_futures[x]`),
    never reassignment of the property itself.
  - Helpers: `_schedule_on_loop` (inbound queue only),
    `AckTracker._set_ack_future_result` (any thread; swallows
    InvalidStateError races), `_cancel_task_threadsafe` (cancels foreign-loop
    tasks via `call_soon_threadsafe`).
- **Dual imports everywhere**: `try: from . import x / except ImportError: import x`
  so the plugin works both as a package (in Hermes) and flat modules (in tests).
  Now covers fourteen modules — `adapter`, `mesh_tools`, `schemas`,
  `telemetry_db`, `chunking`, `mock_interface`, `node_freshness`, `transport`,
  `ack_state`, `inbound`, `mesh_helpers`, `send_path`, `connection`,
  `solicited`.
- **Adapter↔tools link is a module-level singleton** (`mesh_tools.set_adapter` /
  `_get_adapter`). Handlers return `{"error": ...}` JSON when no adapter is
  active. Node IDs are `!`-prefixed 8-hex; the allowlist matches with/without
  the `!`.

## Gateway integration hooks (recent)

These adapter members control how Hermes treats the platform; keep coherent when
touching authz or output:

- `enforces_own_access_policy = True` + `_dm_policy` / `_group_policy` (return
  `"allowlist"` when a node allowlist is active) — read by the gateway's
  `_is_user_authorized` trust path.
- **Tool progress (airtime):** short emoji blurbs only — not full query/args
  dumps and not one permanent packet per tool step.
  - `format_tool_event` → `🔍 Searching the web` (emoji + verb).
  - `send()` applies `_compact_tool_progress_line` for the gateway
    `progress_callback` path (strips ` for <preview>`).
  - `edit_message` → success, no radio (so Hermes does not fall back to
    re-sending every progress update).
  - `SUPPORTS_MESSAGE_EDITING = False`.
  - Recommended Hermes config: `display.platforms.meshtastic.tool_progress:
    new` (one blurb per tool). Multi-line approval walls are separate from
    tool chrome.
- `splits_long_messages = True` — `send()` chunks natively; do NOT also chunk
  upstream.
