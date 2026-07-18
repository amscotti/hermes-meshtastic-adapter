# AGENTS.md

Compact guidance for AI agents working in this repo. Deep architecture
(inbound/outbound paths, connection lifecycle, freshness overlay) is in
`CLAUDE.md` — read it alongside this file.

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

## Commands (run with `.venv/bin/python`)

```bash
# Full local verification — run all before considering work done:
.venv/bin/python -m ruff format .            # CI runs: ruff format --check .
.venv/bin/python -m ruff check .
.venv/bin/python -m pyrefly check \
  --python-interpreter-path .venv/bin/python \
  --search-path ~/.hermes/hermes-agent --min-severity warn
.venv/bin/python -m coverage run -m unittest \
  test_meshtastic.py test_chunking.py test_node_freshness.py \
  test_transport.py test_ack_state.py \
  && .venv/bin/python -m coverage report -m

# Single test:
.venv/bin/python -m unittest test_meshtastic.TestMeshtasticPlatform.<method>
```

CI's four gates: `ruff format --check`, `ruff check`,
`pyrefly check --min-severity warn`, and `coverage`+`unittest`. Coverage
enforces `--fail-under=80` (currently ~92% overall).

## Pyrefly hides warnings by default

Default `--min-severity` is `error`; warnings print only with `--min-severity
warn`. **CI runs at `--min-severity warn`**, so locally use the same to avoid
accumulating silent warnings (e.g. `unnecessary-type-conversion`). Requires
pyrefly `>=1.1.1` (pinned in `requirements-dev.txt`).

## Coverage config gotcha

The source list in `[tool.coverage.run]` is
`["adapter", "meshtastic_tools", "telemetry_db", "schemas", "chunking",
"mock_interface", "node_freshness", "transport", "ack_state"]` — note
**`meshtastic_tools`, not `tools`** (see the dynamic-load convention below),
and that every extracted sibling module must be added here or its coverage
silently drops to 0%. `coverage run -m unittest ...` reads this config; no
`--source` flag needed. Tests are split across `test_meshtastic.py`
(integration) plus `test_chunking.py` / `test_node_freshness.py` /
`test_transport.py` / `test_ack_state.py` (per-domain unit tests); run them
all together for an accurate number.

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
- **`tools.py` is imported as module `meshtastic_tools`, never `tools`** — it
  collides with Hermes' own `tools` package. Set up by dynamic load in
  `adapter._load_tools_module` and `test_meshtastic.py`; preserve it.
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
  Now covers nine modules — `adapter`, `tools`, `schemas`, `telemetry_db`,
  `chunking`, `mock_interface`, `node_freshness`, `transport`, `ack_state`.
- **Adapter↔tools link is a module-level singleton** (`tools.set_adapter` /
  `_get_adapter`). Handlers return `{"error": ...}` JSON when no adapter is
  active. Node IDs are `!`-prefixed 8-hex; the allowlist matches with/without
  the `!`.

## Gateway integration hooks (recent)

These adapter members control how Hermes treats the platform; keep coherent when
touching authz or output:

- `enforces_own_access_policy = True` + `_dm_policy` / `_group_policy` (return
  `"allowlist"` when a node allowlist is active) — read by the gateway's
  `_is_user_authorized` trust path.
- `format_tool_event → None` suppresses tool-progress chrome over LoRa (airtime).
- `splits_long_messages = True` — `send()` chunks natively; do NOT also chunk
  upstream.
