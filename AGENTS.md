# AGENTS.md

Compact guidance for AI agents working in this repo. Deep architecture
(inbound/outbound paths, connection lifecycle, freshness overlay) is in
`CLAUDE.md` — read it. **This file supersedes `CLAUDE.md` where they differ**:
CLAUDE.md's command paths and the "237 byte" figure are dated (corrected below).

## Setup gotcha: where the tooling actually lives

- **Dev tooling (`ruff`/`pyrefly`/`coverage`/`meshtastic`) is installed in the
  repo's `.venv`** (uv-managed; no `pip` inside). Use `.venv/bin/python` for all
  local commands.
- **The Hermes venv `~/.hermes/hermes-agent/venv/bin/python` does NOT have
  `ruff`/`pyrefly`/`coverage`** — CLAUDE.md's commands point there and fail with
  "No module named ruff". (CI instead `pip install`s `requirements-dev.txt` into
  its `actions/setup-python` interpreter.)
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
.venv/bin/python -m coverage run -m unittest test_meshtastic.py \
  && .venv/bin/python -m coverage report -m

# Single test:
.venv/bin/python -m unittest test_meshtastic.TestMeshtasticPlatform.<method>
```

CI's four gates: `ruff format --check`, `ruff check`,
`pyrefly check --min-severity warn`, and `coverage`+`unittest`. Coverage
enforces `--fail-under=80` (currently ~87%).

## Pyrefly hides warnings by default

Default `--min-severity` is `error`; warnings print only with `--min-severity
warn`. **CI runs at `--min-severity warn`**, so locally use the same to avoid
accumulating silent warnings (e.g. `unnecessary-type-conversion`). Requires
pyrefly `>=1.1.1` (pinned in `requirements-dev.txt`).

## Coverage config gotcha

The source list in `[tool.coverage.run]` is
`["adapter", "meshtastic_tools", "telemetry_db", "schemas"]` — note
**`meshtastic_tools`, not `tools`** (see the dynamic-load convention below).
`coverage run -m unittest ...` reads this config; no `--source` flag needed.

## Payload ceiling is 233, not 237

`mesh_pb2.Constants.DATA_PAYLOAD_LEN == 233`; `sendData` raises above it.
`MAX_MESSAGE_LENGTH = 233` and `_chunk_message` clamps `MESHTASTIC_CHUNK_BYTES`
to it. CLAUDE.md still quotes "237" (the LoRa *frame* size, not the app payload)
— ignore that figure.

## Conventions that cause silent failures if broken

- **The ACK callback must be literally named `onAckNak`.** The meshtastic
  library suppresses plain-ACK delivery unless `callback.__name__ == "onAckNak"`
  (magic-name check in `mesh_interface.py`). Renaming it silently breaks ACK
  tracking on real hardware — mock tests still pass because they invoke the
  callback directly.
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
  - **Send/ACK loop** (`_awaiting_loop()` / `future.get_loop()`): agent-session
    `send()` may run on a *different* loop than `connect()`. ACK futures are
    created on the awaiting loop and resolved via
    `_schedule_on_loop(future.get_loop(), ...)`. Binding them to `self.loop`
    and awaiting on the send loop raises
    `ValueError: future belongs to a different loop` (message delivered, tool
    reports failure).
  - **Transport serialization**: `_iface_lock` protects only short
    `_interfaces` map operations. Slow `sendText`/`close`/liveness calls run on
    executor threads under `_transport_lock`; never acquire a contended
    transport lock on an event-loop thread. Concurrent sessions + queue drain +
    reconnect would otherwise race packet ids / response handlers / TX queue.
    ACK waiters stay on the send loop; only transport I/O is serialized.
  - **Disconnect** settles pending ACK futures via `_fail_pending_acks` so
    foreign-loop waiters do not sit until the full ACK timeout.
  - Helpers: `_awaiting_loop()`, `_schedule_on_loop(loop, cb, *args, what=...)`
    (logs + returns False when the target loop is missing/not running/closed
    mid-schedule).
- **Dual imports everywhere**: `try: from . import x / except ImportError: import x`
  so the plugin works both as a package (in Hermes) and flat modules (in tests).
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
