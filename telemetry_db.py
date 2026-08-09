"""
SQLite persistence for Meshtastic node telemetry, positions, and signal quality.
"""

import logging
import math
import os
import sqlite3
import threading
import time
from collections.abc import Callable
from contextlib import closing
from typing import Any, TypeVar

logger = logging.getLogger(__name__)

DB_PATH = os.path.expanduser("~/.hermes/meshtastic_telemetry.db")

# Retention: rows older than this many days are pruned. Override via the env var
# below; set to 0 to disable pruning entirely.
DEFAULT_RETENTION_DAYS = 30
# Hard per-table row ceiling independent of age: the age-based prune is throttled
# to once an hour, so a burst flood could otherwise grow a table without bound
# between prunes. When any table exceeds this the throttle is bypassed and the
# newest rows are kept. Override via MESHTASTIC_TELEMETRY_MAX_ROWS; 0 disables.
DEFAULT_MAX_ROWS_PER_TABLE = 100_000
# Pruning is throttled: the log_* helpers kick off a prune at most this often, so
# bounding the DB doesn't add per-packet overhead.
_PRUNE_INTERVAL_SECONDS = 3600.0
# Sentinel ``prune`` returns when it could not complete (DB error / locked out).
# Compared with ``==`` (not ``<``) so tests that mock ``prune`` without a return
# value don't trip a MagicMock-vs-int ordering TypeError in ``maybe_prune``.
_PRUNE_FAILED = -1
# The tables the module manages, in one place (prune, the row ceiling, and the
# in-memory estimates all iterate these).
_TABLES = ("telemetry", "positions", "signal_quality")
# SELECT-list per table, looked up inside ``_history_query`` from the validated
# table name so nothing caller-supplied can reach the SELECT list (the table
# name is the only interpolated identifier and ``_ident`` allowlists it).
_COLUMNS: dict[str, str] = {
    "telemetry": "timestamp, battery_level, voltage, temperature, humidity, pressure, uptime",
    "positions": "timestamp, latitude, longitude, altitude",
    "signal_quality": "timestamp, snr, rssi, hop_count",
}


def _ident(table: str) -> str:
    """Return a bracket-quoted SQL identifier for a known telemetry table.

    Table names are the only values ever interpolated into SQL (S608); they
    always come from the fixed ``_TABLES`` tuple — never from mesh/user input.
    Asserting membership keeps a future unvalidated caller from reaching the
    query builder, so the ``# noqa: S608`` sites below are provably safe.
    """
    if table not in _TABLES:
        raise ValueError(f"unknown telemetry table: {table!r}")
    return f'"{table}"'


# How long a read connection waits on a busy lock before giving up — SQLite's
# default 5 s timeout, made explicit so the retry below can reason about it.
_READ_TIMEOUT_SECONDS = 5.0
# A read that hits "database is locked" retries a couple of times before
# degrading to an empty result, so a transient busy is never read as "no data".
_READ_RETRY_ATTEMPTS = 3
_READ_RETRY_DELAY = 0.01

# The prune throttle clock. ``None`` means "never pruned this process". Uses
# time.monotonic() so a backward wall-clock step cannot stall pruning (the
# retention cutoff in ``prune`` correctly stays in the time.time() domain).
_last_prune_monotonic: float | None = None

# In-memory per-table row estimates for the maybe_prune flood bypass. The log_*
# helpers bump these under _WRITE_LOCK; init_db seeds them and prune /
# _any_table_over_ceiling resync them to the real counts, so they track the
# tables without a COUNT(*) scan on every write. Advisory only: a stale estimate
# can only cause an extra count query or a slightly late prune, never a wrong
# DELETE. Only the cheap estimate is read on the common write path.
_row_estimates: dict[str, int] = {}

# log_* / prune open their own connections; up to _run_db_write's slot count
# writer threads can contend on one file and hit SQLITE_BUSY. Serializing the
# writers through one lock removes that contention (the lock also covers prune,
# which shares the file, and the cheap reads, so a read can never start in the
# middle of a commit).
_WRITE_LOCK = threading.RLock()
# Serializes the maybe_prune throttle decision (reads + writes _last_prune_
# monotonic and the estimates) across the writer threads that invoke it.
_THROTTLE_LOCK = threading.Lock()


def _ensure_db_dir() -> None:
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)


def _resync_estimates(cursor: sqlite3.Cursor) -> None:
    """Set the in-memory row estimates to the real per-table counts."""
    for table in _TABLES:
        # Interpolating a validated _TABLES member (see _ident) — safe.
        row = cursor.execute(f"SELECT COUNT(*) FROM {_ident(table)}").fetchone()  # noqa: S608
        _row_estimates[table] = row[0] if row else 0


def _bump_row_estimate(table: str) -> None:
    """Record one inserted row in the in-memory estimate (call under _WRITE_LOCK)."""
    _row_estimates[table] = _row_estimates.get(table, 0) + 1


def _estimate_over_ceiling() -> bool:
    """Whether the cheap in-memory estimate says a table may exceed the ceiling.

    Not a real count — the caller confirms with ``_any_table_over_ceiling``
    before pruning. Returns False when the ceiling is disabled.
    """
    ceiling = _max_rows_per_table()
    if ceiling <= 0:
        return False
    return any(count > ceiling for count in _row_estimates.values())


def _finite_float(value: Any) -> float | None:
    """Coerce an untrusted numeric field to a finite float (None = drop)."""
    if value is None or isinstance(value, bool):
        return None
    try:
        num = float(value)
    except (TypeError, ValueError, OverflowError):
        # A huge int (e.g. 10**400) overflows float() with OverflowError; catch
        # it so a hostile huge-int field drops instead of crashing the DB write
        # (mirrors node_freshness._coerce_float / chunking._effective_chunk_bytes).
        return None
    return num if math.isfinite(num) else None


def _finite_int(value: Any) -> int | None:
    """Coerce an untrusted field to an int (via the float guard, rejecting NaN)."""
    num = _finite_float(value)
    return int(num) if num is not None else None


def init_db() -> None:
    """Initialise SQLite database tables and seed the in-memory row estimates.

    Re-raises on failure so a broken DB path is visible at adapter construction
    (a silent failure would otherwise surface only as a per-packet ERROR flood
    from the log_* helpers). Also runs one throttled prune, so age-based
    retention still fires on a mesh that has gone silent.
    """
    try:
        _ensure_db_dir()
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

            # WAL lets readers (this process's ``_read_guarded`` plus any
            # external tool inspecting the DB) take a snapshot that does not
            # block writers and vice versa, so a concurrent commit can't starve
            # a read. ``synchronous=NORMAL`` is the documented safe pairing for
            # WAL (no corruption risk; the last txn may be lost only on a hard
            # power loss, which a telemetry log tolerates). Falls back silently
            # to the default journal mode on filesystems that don't support WAL.
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")

            # Telemetry table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS telemetry (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id TEXT,
                    timestamp REAL,
                    battery_level INTEGER,
                    voltage REAL,
                    temperature REAL,
                    humidity REAL,
                    pressure REAL,
                    uptime INTEGER
                )
            """)

            # Positions table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS positions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id TEXT,
                    timestamp REAL,
                    latitude REAL,
                    longitude REAL,
                    altitude REAL
                )
            """)

            # Signal Quality table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS signal_quality (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id TEXT,
                    timestamp REAL,
                    snr REAL,
                    rssi REAL,
                    hop_count INTEGER
                )
            """)

            # Index creations for fast queries
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_telemetry_node ON telemetry(node_id, timestamp)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_positions_node ON positions(node_id, timestamp)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_signal_node ON signal_quality(node_id, timestamp)"
            )

            conn.commit()
            _resync_estimates(cursor)
        logger.info(f"Initialised Meshtastic telemetry database at {DB_PATH}")
    except Exception as e:
        logger.error(f"Failed to initialise telemetry database: {e}", exc_info=True)
        raise
    maybe_prune()


def _retention_days() -> float:
    """Read the age-based retention window from the env (0 = keep everything)."""
    raw = os.getenv("MESHTASTIC_TELEMETRY_RETENTION_DAYS", str(DEFAULT_RETENTION_DAYS))
    try:
        return float(raw)
    except (TypeError, ValueError):
        return DEFAULT_RETENTION_DAYS


def _max_rows_per_table() -> int:
    """Read the per-table row ceiling from the env (0 = no ceiling)."""
    raw = os.getenv("MESHTASTIC_TELEMETRY_MAX_ROWS", str(DEFAULT_MAX_ROWS_PER_TABLE))
    try:
        return max(0, int(raw or 0))
    except (TypeError, ValueError):
        return DEFAULT_MAX_ROWS_PER_TABLE


def _delete_over_ceiling(cursor: sqlite3.Cursor) -> int:
    """Delete rows beyond the per-table ceiling, keeping the newest by timestamp.

    Runs on every prune so a burst flood is bounded independent of the hourly
    age-based throttle. Returns the number of rows deleted across all tables.

    Eviction is node-agnostic: the newest ``ceiling`` rows per table are kept, so
    one chatty node can crowd out other nodes' history under a flood. That is
    accepted for this scale (a small mesh, where the ceiling is a hard stop
    against unbounded growth rather than a per-node budget).
    """
    ceiling = _max_rows_per_table()
    if ceiling <= 0:
        return 0
    deleted = 0
    for table in _TABLES:
        # Interpolating validated _TABLES members (see _ident) — safe.
        cursor.execute(
            f"DELETE FROM {_ident(table)} WHERE id NOT IN ("  # noqa: S608
            f"SELECT id FROM {_ident(table)} ORDER BY timestamp DESC, id DESC LIMIT ?)",
            (ceiling,),
        )
        deleted += cursor.rowcount or 0
    return deleted


def _any_table_over_ceiling() -> bool:
    """Whether any table currently exceeds the row ceiling (throttle bypass).

    Runs a real COUNT(*) (unlike the in-memory estimate) and resyncs the
    estimates to the actual counts, so they cannot drift. Reads run under the
    write lock so a concurrent commit cannot race this check.
    """
    ceiling = _max_rows_per_table()
    if ceiling <= 0:
        return False
    with _WRITE_LOCK:
        try:
            with closing(sqlite3.connect(DB_PATH, timeout=_READ_TIMEOUT_SECONDS)) as conn:
                cursor = conn.cursor()
                counts: dict[str, int] = {}
                for table in _TABLES:
                    # Interpolating a validated _TABLES member (see _ident).
                    cursor.execute(f"SELECT COUNT(*) FROM {_ident(table)}")  # noqa: S608
                    row = cursor.fetchone()
                    counts[table] = row[0] if row else 0
                _row_estimates.update(counts)
                return any(count > ceiling for count in counts.values())
        except sqlite3.Error as e:
            logger.debug("Skipping telemetry row-ceiling check: %s", e)
    return False


def prune(max_age_days: float) -> int:
    """Delete rows older than ``max_age_days`` from all telemetry tables.

    Returns the total number of deleted rows (0 if nothing was eligible or
    retention is disabled). ``max_age_days <= 0`` keeps everything (no-op).
    Also enforces the per-table row ceiling (newest kept) so a flood between
    hourly prunes cannot grow a table without bound. Returns ``-1`` if the
    prune could not complete (the connection rolls back on close, so the DB is
    left unchanged) so ``maybe_prune`` can retry promptly instead of backing
    off a full interval; direct callers treat ``prune`` as best-effort.

    Bounds long-running-gateway growth: the ACK bookkeeping and node overlay
    are already bounded; this does the same for SQLite.

    Note: this bounds the *row count* (queryable data), not the on-disk file
    size — SQLite's DELETE leaves free pages for reuse rather than returning them
    to the filesystem. For this plugin's scale (a few MB) that's an acceptable
    trade-off vs. the cost of a full VACUUM rewrite on every prune; run
    `VACUUM` manually if you ever need to reclaim the space.
    """
    if max_age_days <= 0:
        return 0
    cutoff = time.time() - max_age_days * 86400.0
    with _WRITE_LOCK:
        try:
            deleted = 0
            with closing(sqlite3.connect(DB_PATH)) as conn:
                cursor = conn.cursor()
                for table in _TABLES:
                    # Interpolating a validated _TABLES member (see _ident).
                    cursor.execute(
                        f"DELETE FROM {_ident(table)} WHERE timestamp < ?",  # noqa: S608
                        (cutoff,),
                    )
                    deleted += cursor.rowcount or 0
                deleted += _delete_over_ceiling(cursor)
                conn.commit()
                _resync_estimates(cursor)
            if deleted:
                logger.info(
                    "Pruned %d telemetry rows older than %.1f days (cutoff=%d).",
                    deleted,
                    max_age_days,
                    int(cutoff),
                )
            return deleted
        except Exception as e:
            # Nothing committed: the connection context rolls back on close, so
            # a mid-prune failure leaves the DB unchanged. Signal failure (-1)
            # so ``maybe_prune`` can retry promptly instead of backing off a
            # full interval; direct callers treat ``prune`` as best-effort.
            logger.error(f"Error pruning telemetry database: {e}")
            return _PRUNE_FAILED


def maybe_prune() -> None:
    """Throttled lazy pruning: run ``prune`` at most once per prune interval.

    Called from the ``log_*`` helpers so the DB self-bounds without a background
    task. The common case is cheap — within the interval it only reads an
    in-memory per-table row estimate, never a COUNT(*). That estimate is the
    flood bypass: when it suggests a table has crossed its hard row ceiling, a
    real count confirms it and ``prune`` runs immediately, so a burst flood is
    bounded on the write that crosses the ceiling rather than up to an hour
    later.
    """
    global _last_prune_monotonic
    now = time.monotonic()
    with _THROTTLE_LOCK:
        if (
            _last_prune_monotonic is not None
            and now - _last_prune_monotonic < _PRUNE_INTERVAL_SECONDS
        ):
            if not _estimate_over_ceiling():
                return  # throttled and under the ceiling: the cheap common case
            if not _any_table_over_ceiling():
                return  # estimate was stale; the real counts are under the ceiling
        # Pre-stamp inside the lock so two writer threads can't both decide to
        # run prune (prune is serialized under _WRITE_LOCK and idempotent, but a
        # redundant COUNT/DELETE burst on a flood is worth avoiding). Reset
        # below when prune signals failure, so a failed attempt doesn't push the
        # next one out by a full interval.
        _last_prune_monotonic = now
    if prune(_retention_days()) == _PRUNE_FAILED:  # -1 = prune failed
        with _THROTTLE_LOCK:
            _last_prune_monotonic = None


def log_telemetry(
    node_id: str,
    battery_level: int | None = None,
    voltage: float | None = None,
    temperature: float | None = None,
    humidity: float | None = None,
    pressure: float | None = None,
    uptime: int | None = None,
) -> None:
    """Insert a telemetry record.

    Numeric fields are coerced to finite numbers at the storage boundary
    (defense-in-depth — the inbound pipeline already coerces) so no caller can
    persist a non-numeric string or NaN into a REAL/INTEGER column. Rejected
    values are stored as NULL rather than raising.
    """
    battery_level = _finite_int(battery_level)
    uptime = _finite_int(uptime)
    voltage = _finite_float(voltage)
    temperature = _finite_float(temperature)
    humidity = _finite_float(humidity)
    pressure = _finite_float(pressure)
    with _WRITE_LOCK:
        try:
            with closing(sqlite3.connect(DB_PATH)) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO telemetry (node_id, timestamp, battery_level, voltage, temperature, humidity, pressure, uptime)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        node_id,
                        time.time(),
                        battery_level,
                        voltage,
                        temperature,
                        humidity,
                        pressure,
                        uptime,
                    ),
                )
                conn.commit()
                _bump_row_estimate("telemetry")
        except Exception as e:
            logger.error(f"Error logging telemetry: {e}")
    maybe_prune()


def log_position(
    node_id: str,
    latitude: float,
    longitude: float,
    altitude: float | None = None,
) -> None:
    """Insert a position record.

    Coordinates are coerced to finite floats at the storage boundary
    (defense-in-depth — the inbound pipeline's ``normalize_position_payload``
    already coerces and range-checks) so no caller can persist NaN/``inf`` or a
    non-numeric string into a REAL column. Rejected values are stored as NULL.
    """
    lat = _finite_float(latitude)
    lon = _finite_float(longitude)
    alt = _finite_float(altitude)
    with _WRITE_LOCK:
        try:
            with closing(sqlite3.connect(DB_PATH)) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO positions (node_id, timestamp, latitude, longitude, altitude)
                    VALUES (?, ?, ?, ?, ?)
                """,
                    (node_id, time.time(), lat, lon, alt),
                )
                conn.commit()
                _bump_row_estimate("positions")
        except Exception as e:
            logger.error(f"Error logging position: {e}")
    maybe_prune()


def log_signal(
    node_id: str,
    snr: float | None,
    rssi: float | None,
    hop_count: int | None = None,
) -> None:
    """Insert a signal quality record.

    ``snr``/``rssi``/``hop_count`` are coerced to finite numbers at the storage
    boundary (defense-in-depth — the inbound pipeline already coerces) so no
    caller can persist a non-numeric string or NaN into a REAL column.
    """
    snr = _finite_float(snr)
    rssi = _finite_float(rssi)
    hop_count = _finite_int(hop_count)
    with _WRITE_LOCK:
        try:
            with closing(sqlite3.connect(DB_PATH)) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO signal_quality (node_id, timestamp, snr, rssi, hop_count)
                    VALUES (?, ?, ?, ?, ?)
                """,
                    (node_id, time.time(), snr, rssi, hop_count),
                )
                conn.commit()
                _bump_row_estimate("signal_quality")
        except Exception as e:
            logger.error(f"Error logging signal quality: {e}")
    maybe_prune()


def _is_busy(error: sqlite3.OperationalError) -> bool:
    """Whether a sqlite3 error is a transient lock/busy condition worth retrying."""
    message = str(error).lower()
    return "locked" in message or "busy" in message


_T = TypeVar("_T")


def _read_guarded(error_label: str, empty: _T, run: Callable[[], _T]) -> _T:
    """Run a cheap read under the write lock, retrying transient busy errors.

    Reads run under ``_WRITE_LOCK`` so they can never collide with our own
    writers mid-commit (the lock is the correctness mechanism; WAL, enabled in
    ``init_db``, additionally means an external reader taking a snapshot won't
    block on our writer). The retry only covers an external process holding the
    file. A transient "database is locked" is retried rather than collapsed
    into an empty result — only a genuinely failing read degrades to ``empty``
    (and is logged), so a busy DB is never presented to the agent as "no data".
    """
    with _WRITE_LOCK:
        for attempt in range(_READ_RETRY_ATTEMPTS):
            try:
                return run()
            except sqlite3.OperationalError as e:
                last_attempt = attempt + 1 == _READ_RETRY_ATTEMPTS
                if _is_busy(e) and not last_attempt:
                    time.sleep(_READ_RETRY_DELAY)
                    continue
                logger.warning("Telemetry DB busy while reading %s: %s", error_label, e)
                return empty
            except Exception as e:
                logger.error(f"Error reading {error_label}: {e}")
                return empty
    # Defensive: reached only if _READ_RETRY_ATTEMPTS is ever lowered to 0, in
    # which case the loop body never runs — degrade to empty rather than None.
    return empty


def _history_query(
    table: str,
    node_id: str,
    limit: int,
    since: float | None,
    error_label: str,
) -> list[dict[str, Any]]:
    """Read the newest rows for a node, optionally bounded to a time window.

    ``since`` is a unix timestamp: pass it to ask "the last N days" rather than
    "the last N rows", which is the only way to request a period without
    guessing how chatty a node is. Rows still come back newest-first and capped
    by ``limit``, so a period denser than the cap is truncated — callers report
    that rather than presenting a partial window as complete.
    """
    # SQLite treats a negative LIMIT as "unlimited"; clamp so a non-positive
    # caller request can never turn into an unbounded read.
    limit = max(1, limit)
    # `columns` is derived from the validated `table` via the fixed _COLUMNS
    # map, never caller-supplied, so nothing untrusted reaches the SELECT list.
    columns = _COLUMNS.get(table)
    if columns is None:
        raise ValueError(f"unknown telemetry table: {table!r}")
    clauses = "WHERE node_id = ?"
    params: list[Any] = [node_id]
    if since is not None:
        clauses += " AND timestamp >= ?"
        params.append(since)
    params.append(limit)

    def run() -> list[dict[str, Any]]:
        with closing(sqlite3.connect(DB_PATH, timeout=_READ_TIMEOUT_SECONDS)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                # `table` is a validated _TABLES member; `columns` is derived
                # from it via _COLUMNS; `clauses` is a literal/bound-param
                # predicate (never user input) — safe.
                f"SELECT {columns} FROM {_ident(table)} {clauses} ORDER BY timestamp DESC LIMIT ?",  # noqa: S608
                params,
            )
            return [dict(row) for row in cursor.fetchall()]

    return _read_guarded(error_label, [], run)


def get_telemetry_history(
    node_id: str, limit: int = 50, since: float | None = None
) -> list[dict[str, Any]]:
    """Retrieve historical telemetry for a specific node."""
    return _history_query(
        "telemetry",
        node_id,
        limit,
        since,
        "telemetry history",
    )


def get_position_history(
    node_id: str, limit: int = 50, since: float | None = None
) -> list[dict[str, Any]]:
    """Retrieve historical positions for a node."""
    return _history_query(
        "positions",
        node_id,
        limit,
        since,
        "position history",
    )


def get_latest_signal_by_node(direct_only: bool = False) -> dict[str, dict[str, Any]]:
    """Latest signal reading per node, in one query, as ``{node_id: row}``.

    Callers listing the whole mesh need this per node, so a per-node query would
    be one round trip per node on every call. ``direct_only`` restricts to
    0-hop packets — the readings that actually describe the link to *that* node
    rather than to whichever relay forwarded it.

    Persisted, so unlike the adapter's in-memory observations this survives a
    gateway restart, which is the only reason hop data outlives a reconnect.
    Same-timestamp rows (back-to-back packets) resolve by id, newest last, so
    the "latest" reading is deterministic.
    """
    where = "WHERE hop_count = 0" if direct_only else ""

    def run() -> dict[str, dict[str, Any]]:
        with closing(sqlite3.connect(DB_PATH, timeout=_READ_TIMEOUT_SECONDS)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT node_id, timestamp, snr, rssi, hop_count
                FROM (
                    SELECT node_id, timestamp, snr, rssi, hop_count,
                           ROW_NUMBER() OVER (
                               PARTITION BY node_id ORDER BY timestamp DESC, id DESC
                           ) AS rn
                    FROM signal_quality
                    {where}
                )
                WHERE rn = 1
            """)  # noqa: S608 -- `where` is a module-literal predicate or empty.
            return {row["node_id"]: dict(row) for row in cursor.fetchall()}

    return _read_guarded("latest signal readings", {}, run)


def get_latest_direct_signal(node_id: str) -> dict[str, Any] | None:
    """Latest 0-hop signal reading for a single node, or ``None``.

    Single-node counterpart to ``get_latest_signal_by_node(True)``. That getter
    runs a ``ROW_NUMBER() OVER (PARTITION BY node_id …)`` window over the whole
    ``signal_quality`` table to build the all-nodes map, serializing against
    every telemetry writer for the scan — fine for ``mesh_list_nodes`` (which
    needs every node's row) but wasteful for a single-node query, which only
    needs one row. This reads it with a bounded
    ``WHERE node_id = ? AND hop_count = 0`` lookup on the ``(node_id,
    timestamp)`` index, matching the all-nodes map's ordering
    (``timestamp DESC, id DESC``) so the two paths agree on which row is
    "latest". ``hop_count = 0`` matches the all-nodes direct filter exactly —
    rows lacking hop info (NULL) are excluded either way.
    """
    error_label = f"latest direct signal for node {node_id!r}"

    def run() -> dict[str, Any] | None:
        with closing(sqlite3.connect(DB_PATH, timeout=_READ_TIMEOUT_SECONDS)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                "SELECT node_id, timestamp, snr, rssi, hop_count "
                "FROM signal_quality "
                "WHERE node_id = ? AND hop_count = 0 "
                "ORDER BY timestamp DESC, id DESC LIMIT 1",
                (node_id,),
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    return _read_guarded(error_label, None, run)


def get_signal_history(
    node_id: str, limit: int = 50, since: float | None = None
) -> list[dict[str, Any]]:
    """Retrieve historical signal quality for a node."""
    return _history_query(
        "signal_quality",
        node_id,
        limit,
        since,
        "signal history",
    )
