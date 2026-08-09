# Developing

The contributor workflow for `hermes-meshtastic-adapter`. Read
`docs/ARCHITECTURE.md` first for hub-and-spoke guidance and where new code
goes, and `CLAUDE.md` for detailed conventions.

## Getting started

```bash
git clone https://github.com/amscotti/hermes-meshtastic-adapter
cd hermes-meshtastic-adapter
uv venv
uv pip install -r requirements.txt -r requirements-dev.txt
# Dev tools (ruff/pyrefly/coverage/meshtastic) live in requirements-dev.txt +
# requirements.txt — not in a uv [dependency-groups] block. Hermes Agent is
# still required on sys.path (see below).
```

- **All tooling runs from the repo's `.venv`** — use `.venv/bin/python` for
  every command. The Hermes venv (`~/.hermes/hermes-agent/venv`) does **not**
  have ruff/pyrefly/coverage.
- The plugin imports `gateway.*` from Hermes Agent, which is **not** in this
  repo. The `.venv` resolves it via `~/.hermes/hermes-agent`; set
  `HERMES_AGENT_PATH` if Hermes lives elsewhere (CI checks out
  `NousResearch/hermes-agent` into `_deps/`). Without it, `gateway.*` import
  failures are expected, not a bug.

### Run the suite

```bash
.venv/bin/python -m unittest discover -s . -p "test_*.py"
```

Discovery (not an explicit file list) so a new `test_*.py` — including the
arch-gate tests — is picked up automatically; CI uses the same invocation and
cannot silently skip a test file.

Per-domain unit tests live in the matching `test_<module>.py`;
`test_meshtastic.py` holds Hermes-bridge / smoke tests of the assembled
adapter. Architecture-gate unit tests: `test_cc_gate.py`, `test_layer_gate.py`,
`test_extraction_gate.py`, `test_arch_gates.py`. Config invariants:
`test_project_config.py`. All tests use `MockSerialInterface` + a temp SQLite
DB — no hardware required.

### Run all gates (the 4 CI gates + lightweight architecture gates)

```bash
.venv/bin/python -m ruff format .       # CI runs: ruff format --check .
.venv/bin/python -m ruff check .
.venv/bin/python -m pyrefly check \
  --python-interpreter-path .venv/bin/python \
  --search-path ~/.hermes/hermes-agent --min-severity warn
.venv/bin/python -m coverage run -m unittest discover -s . -p "test_*.py" \
  && .venv/bin/python -m coverage report -m   # CI enforces --fail-under=80 (currently ~97%)

.venv/bin/python scripts/check_arch_gates.py  # complexity, layering, extraction
```

Pyrefly hides warnings unless `--min-severity warn` is passed; CI runs at
`warn`, so do the same locally.

### Mock smoke test

Use **`MESHTASTIC_SERIAL_PORT=mock_port`** for a dry-run against the mock
(two fake nodes `!da1b1613` / `!ab12cd34`, no radio traffic) — this works
whether or not the meshtastic library is installed.

`MESHTASTIC_MOCK=1` is a narrower escape hatch: it only selects the mock when
the library is **missing**. With the library installed, real serial/TCP targets
always open the real interface; set `mock_port` if you want a dry-run. A missing
library with a real target raises install instructions after one
`MESHTASTIC_AUTOINSTALL` pip attempt (set `MESHTASTIC_AUTOINSTALL=0` to skip).

### Hardware checklist (before shipping a change that touches I/O)

1. **Connect**: `MESHTASTIC_SERIAL_PORT=/dev/cu.usbserial-xxxx` (or
   `MESHTASTIC_TCP_HOST=host`) — the gateway log must show `SerialInterface(...)`
   / `TCPInterface(...)`, not the mock.
2. **DM round-trip**: message the gateway node from the Meshtastic app; the
   agent should reply in `[i/n]` chunks. Reply to a chunk with `!node`/`channel` ids.
   Inbound `[i/n]` fragments are **not** reassembled on this side (the agent
   reads the raw prefixes and replies in chunks) — each arrival is bridged as
   its own message and logged with an `Inbound [i/n] chunk from ...` diagnostic
   so a dropped fragment is at least observable.
3. **ACK log lines**: with `MESHTASTIC_ACK_TIMEOUT=30`, waits resolve with
   `ack`/`nak` in `SendResult.raw_response["chunks"][i]["ack"]`; with
   `MESHTASTIC_SEND_RETRIES=3`, `attempts` > 1 on a quiet/relayed path. Verify
   the `onAckNak` callback fires at all (real ACKs are delivered only to that
   magic name).
4. **Keepalive/liveness**: kill the node's WiFi — the drop should be classified
   in the log within ~30 s (keepalive), and reconnect should recover.

## Adding a feature — the standard workflow

① **Read `docs/ARCHITECTURE.md`** — especially "Anti-god-class rules" and
"Where new code goes". Do not default to editing `adapter.py`.

② **Schema first** (for tools) — JSON function schema in `schemas.py`, with
airtime-discipline notes that keep the model from sweeping the mesh.

③ **Pure logic in the owning module** — decision tables, classification, and
formatting go in the module that owns the concern (see the table below). Keep
it free of adapter coupling: inject what you need. **Never `import adapter`
from a sibling** (layering gate).

④ **Wiring** — `__init__.py` registration for a new tool, or a thin delegate in
`adapter.py` only where orchestration genuinely lives. New adapter methods must
stay under the complexity gate (any function cc ≥ 20 fails; allowlist empty; adapter mean ≤ 4.5).

⑤ **Unit tests** in the matching `test_<module>.py` — pure functions are
testable without the adapter; use the stub pattern from `test_ack_state.py`.

⑥ **Integration test in `test_meshtastic.py` only for gateway-bridge behavior**
— authz gating, chat-id formation, chunk pacing, ACK waits through `send()`.
Keep that file thin.

⑦ **Docs** — if the module map or a data path changed, update `CLAUDE.md` and
`docs/ARCHITECTURE.md` together (convention; not a CI gate).

⑧ **Run the gates** — the 4 CI gates + `scripts/check_arch_gates.py` (complexity / layering / extraction),
including coverage (a module missing from `[tool.coverage.run].source`
silently drops to 0% — the extraction gate catches it).

⑨ **Hardware smoke test** — real serial/TCP connect + DM round-trip + ACK log
lines (see above), for anything that touches the wire.

## Where does this change go?

| Change | Module |
| --- | --- |
| New tool | `mesh_tools.py` (handler) + `schemas.py` (schema) + `__init__.py` (registration) |
| New packet type / portnum | `inbound.py` (classification) + `adapter.py` (routing/handoff) |
| New ACK/NACK behavior | `ack_state.py` (tracker/pure helpers) — extend the `LifecycleHost` Protocol surface only if the tracker needs more of the adapter |
| Send / retry / ACK-outcome policy | `send_path.py` (pure helpers — retry budget, chunk pacing, transport-error mapping, ACK-outcome classification, DM/channel target resolution) |
| Solicited-request waiter policy | `solicited.py` (tracker — waiters, resolve/discard/abandon, `solicit` wait path; no adapter import) |
| Connection lifecycle / backoff / teardown | `connection.py` (pure helpers — backoff, reconnect-step/poll/pause classification, link-drop classification, teardown planning) |
| New persistence query | `telemetry_db.py` (SQLite) |
| New transport (serial/TCP/mock) | `transport.py` (target resolution, `open_interface`, executor) |
| Serialized interface close / executor-mediated teardown | `transport.py` (the `close_interfaces_*` family, `shutdown_transport_executor`, `drop_interface_if_dead_serialized`) |
| New receive stage | `inbound.py` — dependency injection (constructor params), no adapter import |
| New helper for tool handlers | `mesh_helpers.py` (pure, imports only `telemetry_db`) |
| Freshness overlay change | `node_freshness.py` (writes on the platform loop, reads on tool handlers; serialized by an internal lock) |
| Orchestration, lifecycle, Hermes bridge | `adapter.py` — **last resort**; extract pure/stateful logic to a sibling (see ARCHITECTURE anti-god-class rules) |

## Conventions that break silently

These are checked by tests/gates or by real hardware only — mock tests often
stay green when they break:

- **The ACK callback must be literally named `onAckNak`** — the meshtastic
  library suppresses plain-ACK delivery unless `callback.__name__ == "onAckNak"`
  (magic-name check in `mesh_interface.py`). Mock tests invoke callbacks
  directly and would not notice a rename; `test_ack_state.py::TestAckCallbackNaming`
  pins the name.
- **Dual imports** — every cross-module import is `try: from . import x /
  except ImportError: import x` (package vs flat layout). New modules must
  follow it.
- **Threading boundary** — pubsub delivers on a background thread; touch
  asyncio-loop state only via `_schedule_on_loop` /
  `loop.call_soon_threadsafe`. `_on_receive` runs on the platform loop.
- **Dual event loops** — waiters in `AckTracker._ack_futures` are
  `concurrent.futures.Future` (thread-safe `set_result` from any thread),
  awaited via `asyncio.wrap_future`. Never store bare `asyncio.Future` there.
- **Lock ordering** — `_record_ack_response` acquires `_lifecycle_lock` →
  `_ack_lock` (ExitStack); preserve it. Never reassign the 8 read-only
  `@property` bridges on the adapter.
- **`MESHTASTIC_HOME_CHANNEL` normalization** — a bare node id (`!node`/8-hex)
  or `channel:N` is rewritten in `MeshtasticAdapter.__init__`
  (`_expand_home_channel_env_for_gateway` in `adapter.py`) to `meshtastic:...`
  with a warning; the send path requires the `meshtastic:` prefix.
- **Coverage registration for new modules** — add every new top-level module
  to `[tool.coverage.run].source` and give it a `test_<module>.py`, or the extraction gate
  fails and its coverage silently reads 0%.
- **`mesh_tools.py`, never `tools`** — the filename must not shadow Hermes'
  own `tools` package (imported transitively); the module is loaded as
  `meshtastic_tools`.
- **Read `transport.HAS_MESHTASTIC` / `transport.pub` at call time** — the
  adapter's copies are back-compat aliases; a stale snapshot silently skips
  pubsub subscription. Patch `transport.X` in tests, not `adapter.X`.
- **The mock never masquerades as production** — only `mock_port` targets
  (always) and `MESHTASTIC_MOCK=1` when the library is **missing** produce the
  mock; real targets raise with install instructions when the library is missing.
- **Inbound queue consumer must not hold `_lifecycle_lock` across
  `_on_receive`** — the multi-hop ACK upgrade re-acquires that lock inside
  `_record_ack_response`; holding it across dispatch deadlocks the platform loop.
- **Tool progress is short blurbs, not full chrome** — do not restore
  `format_tool_event → None` alone (the classic progress path bypasses it),
  and do not make `edit_message` return failure (Hermes re-sends each step).
  See `docs/ARCHITECTURE.md` § "Mesh UX and airtime".
