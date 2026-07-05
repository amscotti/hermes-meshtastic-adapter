"""
SQLite persistence for Meshtastic node telemetry, positions, and signal quality.
"""

import logging
import os
import sqlite3
import time
from contextlib import closing
from typing import Any

logger = logging.getLogger(__name__)

DB_PATH = os.path.expanduser("~/.hermes/meshtastic_telemetry.db")

# Retention: rows older than this many days are pruned. Override via the env var
# below; set to 0 to disable pruning entirely.
DEFAULT_RETENTION_DAYS = 30
# Pruning is throttled: the log_* helpers kick off a prune at most this often, so
# bounding the DB doesn't add per-packet overhead.
_PRUNE_INTERVAL_SECONDS = 3600.0
_last_prune_epoch: float = 0.0


def _ensure_db_dir() -> None:
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)


def init_db() -> None:
    """Initialise SQLite database tables."""
    try:
        _ensure_db_dir()
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()

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
        logger.info(f"Initialised Meshtastic telemetry database at {DB_PATH}")
    except Exception as e:
        logger.error(f"Failed to initialise telemetry database: {e}", exc_info=True)


def prune(max_age_days: float) -> int:
    """Delete rows older than ``max_age_days`` from all telemetry tables.

    Returns the total number of deleted rows. ``max_age_days <= 0`` keeps
    everything (no-op). Bounds long-running-gateway growth: the ACK bookkeeping
    and node overlay are already bounded; this does the same for SQLite.

    Note: this bounds the *row count* (queryable data), not the on-disk file
    size — SQLite's DELETE leaves free pages for reuse rather than returning them
    to the filesystem. For this plugin's scale (a few MB) that's an acceptable
    trade-off vs. the cost of a full VACUUM rewrite on every prune; run
    `VACUUM` manually if you ever need to reclaim the space.
    """
    if max_age_days <= 0:
        return 0
    cutoff = time.time() - max_age_days * 86400.0
    deleted = 0
    try:
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()
            for table in ("telemetry", "positions", "signal_quality"):
                cursor.execute(f"DELETE FROM {table} WHERE timestamp < ?", (cutoff,))
                deleted += cursor.rowcount or 0
            conn.commit()
        if deleted:
            logger.info(
                "Pruned %d telemetry rows older than %.1f days (cutoff=%d).",
                deleted,
                max_age_days,
                int(cutoff),
            )
    except Exception as e:
        logger.error(f"Error pruning telemetry database: {e}")
    return deleted


def maybe_prune() -> None:
    """Throttled lazy pruning: run ``prune`` at most once per prune interval.

    Called from the ``log_*`` helpers so the DB self-bounds without a background
    task. Safe to invoke on every write — it only checks the clock normally.
    """
    global _last_prune_epoch
    now = time.time()
    if now - _last_prune_epoch < _PRUNE_INTERVAL_SECONDS:
        return
    _last_prune_epoch = now
    try:
        raw = os.getenv("MESHTASTIC_TELEMETRY_RETENTION_DAYS", str(DEFAULT_RETENTION_DAYS))
        retention = float(raw)
    except (TypeError, ValueError):
        retention = DEFAULT_RETENTION_DAYS
    prune(retention)


def log_telemetry(
    node_id: str,
    battery_level: int | None = None,
    voltage: float | None = None,
    temperature: float | None = None,
    humidity: float | None = None,
    pressure: float | None = None,
    uptime: int | None = None,
) -> None:
    """Insert a telemetry record."""
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
    except Exception as e:
        logger.error(f"Error logging telemetry: {e}")
    maybe_prune()


def log_position(
    node_id: str,
    latitude: float,
    longitude: float,
    altitude: float | None = None,
) -> None:
    """Insert a position record."""
    try:
        with closing(sqlite3.connect(DB_PATH)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO positions (node_id, timestamp, latitude, longitude, altitude)
                VALUES (?, ?, ?, ?, ?)
            """,
                (node_id, time.time(), latitude, longitude, altitude),
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Error logging position: {e}")
    maybe_prune()


def log_signal(
    node_id: str,
    snr: float | None,
    rssi: float | None,
    hop_count: int | None = None,
) -> None:
    """Insert a signal quality record."""
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
    except Exception as e:
        logger.error(f"Error logging signal quality: {e}")
    maybe_prune()


def get_telemetry_history(node_id: str, limit: int = 50) -> list[dict[str, Any]]:
    """Retrieve historical telemetry for a specific node."""
    try:
        with closing(sqlite3.connect(DB_PATH)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT timestamp, battery_level, voltage, temperature, humidity, pressure, uptime
                FROM telemetry
                WHERE node_id = ?
                ORDER BY timestamp DESC
                LIMIT ?
            """,
                (node_id, limit),
            )
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Error reading telemetry history: {e}")
        return []


def get_position_history(node_id: str, limit: int = 50) -> list[dict[str, Any]]:
    """Retrieve historical positions for a node."""
    try:
        with closing(sqlite3.connect(DB_PATH)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT timestamp, latitude, longitude, altitude
                FROM positions
                WHERE node_id = ?
                ORDER BY timestamp DESC
                LIMIT ?
            """,
                (node_id, limit),
            )
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Error reading position history: {e}")
        return []


def get_signal_history(node_id: str, limit: int = 50) -> list[dict[str, Any]]:
    """Retrieve historical signal quality for a node."""
    try:
        with closing(sqlite3.connect(DB_PATH)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT timestamp, snr, rssi, hop_count
                FROM signal_quality
                WHERE node_id = ?
                ORDER BY timestamp DESC
                LIMIT ?
            """,
                (node_id, limit),
            )
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Error reading signal history: {e}")
        return []
