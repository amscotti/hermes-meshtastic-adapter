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
