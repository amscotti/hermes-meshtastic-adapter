"""
Unit and Integration Test Suite for Meshtastic Platform Adapter.
"""

import asyncio
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from contextlib import closing
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

# Add CWD to system path to ensure local imports resolve
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
hermes_agent_path = os.getenv("HERMES_AGENT_PATH", os.path.expanduser("~/.hermes/hermes-agent"))
if os.path.isdir(hermes_agent_path):
    sys.path.append(hermes_agent_path)

# Register the platform inside the registry so that Platform("meshtastic") resolves correctly in venv
from gateway.platform_registry import PlatformEntry, platform_registry

platform_registry.register(
    PlatformEntry(
        name="meshtastic",
        label="Meshtastic",
        adapter_factory=lambda cfg: None,
        check_fn=lambda: True,
    )
)

import importlib.util

# Load local mesh_tools.py dynamically, exposing it under the logical name
# "meshtastic_tools" (kept for back-compat with the module singleton). Reuse an
# instance another test module already loaded: unittest imports every module
# up front, so a fresh load would rebind the adapter singleton link and break
# this file's handler aliases.
if "meshtastic_tools" in sys.modules:
    meshtastic_tools = sys.modules["meshtastic_tools"]
else:
    tools_spec = importlib.util.spec_from_file_location(
        "meshtastic_tools",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "mesh_tools.py"),
    )
    meshtastic_tools = importlib.util.module_from_spec(tools_spec)
    sys.modules["meshtastic_tools"] = meshtastic_tools
    tools_spec.loader.exec_module(meshtastic_tools)

import telemetry_db
from adapter import (
    MeshtasticAdapter,
    MockSerialInterface,
    _env_enablement,
)
from telemetry_db import init_db

_BLANK_ENV = {
    "MESHTASTIC_SERIAL_PORT": "",
    "MESHTASTIC_BAUD_RATE": "",
    "MESHTASTIC_ALLOWED_NODES": "",
    "MESHTASTIC_ALLOWED_USERS": "",
    "MESHTASTIC_ALLOW_ALL_USERS": "",
    "MESHTASTIC_HOME_CHANNEL": "",
    "MESHTASTIC_CHUNK_BYTES": "",
    "MESHTASTIC_CHUNK_DELAY": "0",
    "MESHTASTIC_ACK_TIMEOUT": "",
    "MESHTASTIC_SEND_RETRIES": "",
    "MESHTASTIC_RETRY_BACKOFF": "0",
    "MESHTASTIC_TELEMETRY_RETENTION_DAYS": "",
    "MESHTASTIC_TCP_HOST": "",
    "MESHTASTIC_TCP_PORT": "",
}


class TestMeshtasticPlatform(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._env_patcher = patch.dict(os.environ, _BLANK_ENV)
        self._env_patcher.start()

        # Isolate SQLite database from the user's live Hermes profile.
        self._tmp_db = tempfile.NamedTemporaryFile(delete=False)
        self._tmp_db.close()
        telemetry_db.DB_PATH = self._tmp_db.name
        init_db()

        # Configure platform mock
        self.config = MagicMock()
        self.config.extra = {
            "serial_port": "mock_port",
            "baud_rate": 115200,
            "allowed_users": "!ab12cd34,!da1b1613",
            "allow_all_users": False,
            "home_channel": "meshtastic:channel:0",
        }

        # Instantiate Adapter
        self.adapter = MeshtasticAdapter(self.config)

        # Mock gateway runner's handle_message
        self.adapter.handle_message = AsyncMock()

        # Connect to mock interface
        await self.adapter.connect()
        # Give reconnect task time to initialize mock interface
        await asyncio.sleep(0.1)

    async def asyncTearDown(self):
        await self.adapter.disconnect()
        self._env_patcher.stop()
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(f"{self._tmp_db.name}{suffix}")
            except OSError:
                pass

    def test_dm_policy_reflects_access_mode(self):
        """_dm_policy mirrors the active access mode for the gateway trust path."""
        # Default fixture: allowed_nodes set, allow_all False -> allowlist policy.
        self.assertTrue(self.adapter.enforces_own_access_policy)
        self.assertEqual(self.adapter._dm_policy, "allowlist")
        # Channel broadcasts pass the same intake gate -> same policy.
        self.assertEqual(self.adapter._group_policy, "allowlist")
        # allow_all flips to "open" (adapter forwards everyone).
        self.adapter.allow_all = True
        self.assertEqual(self.adapter._dm_policy, "open")
        self.assertEqual(self.adapter._group_policy, "open")
        # No allowlist + not allow_all -> "open" (adapter default-denies at intake,
        # so the gateway never sees this traffic).
        self.adapter.allow_all = False
        self.adapter.allowed_nodes = set()
        self.assertEqual(self.adapter._dm_policy, "open")

    def test_env_parsing_prefers_allowed_nodes_alias(self):
        """Verify preferred MESHTASTIC_ALLOWED_NODES wins over legacy USERS alias."""
        with patch.dict(
            os.environ,
            {
                "MESHTASTIC_SERIAL_PORT": "mock_port",
                "MESHTASTIC_BAUD_RATE": "57600",
                "MESHTASTIC_ALLOWED_NODES": "ab12cd34",
                "MESHTASTIC_ALLOWED_USERS": "bad55555",
                "MESHTASTIC_ALLOW_ALL_USERS": "true",
                "MESHTASTIC_HOME_CHANNEL": "meshtastic:channel:0",
            },
        ):
            # Seed-from-env before the adapter expands the allowlist env for Hermes.
            env_config = _env_enablement()
            self.assertEqual(env_config["allowed_nodes"], "ab12cd34")

            config = MagicMock()
            config.extra = {}
            adapter = MeshtasticAdapter(config)
            # Hermes gateway exact-matches the env allowlist — expansion must
            # include both bang and bare forms so intake and gateway agree.
            expanded = os.environ["MESHTASTIC_ALLOWED_NODES"]
            self.assertIn("ab12cd34", expanded)
            self.assertIn("!ab12cd34", expanded)

        self.assertEqual(adapter.serial_port, "mock_port")
        self.assertEqual(adapter.baud_rate, 57600)
        self.assertTrue(adapter.allow_all)
        self.assertIn("ab12cd34", adapter.allowed_nodes)
        self.assertIn("!ab12cd34", adapter.allowed_nodes)
        self.assertNotIn("bad55555", adapter.allowed_nodes)

    async def test_get_chat_info_channel(self):
        """get_chat_info reports a channel as a group chat."""
        info = await self.adapter.get_chat_info("meshtastic:channel:Primary")
        self.assertEqual(info["type"], "group")
        self.assertIn("Primary", info["name"])

    async def test_get_chat_info_dm_resolves_name(self):
        """get_chat_info returns the long name for a known DM node."""
        info = await self.adapter.get_chat_info("meshtastic:!ab12cd34")
        self.assertEqual(info["type"], "dm")
        self.assertEqual(info["name"], "Park Sensor Node")

    async def test_get_chat_info_dm_unknown_falls_back_to_id(self):
        """An unknown DM node falls back to its raw id as the name."""
        info = await self.adapter.get_chat_info("meshtastic:!deadbeef")
        self.assertEqual(info["type"], "dm")
        self.assertEqual(info["name"], "!deadbeef")

    def test_home_channel_expansion_forms(self):
        """Bare node ids and channel:N values get the meshtastic: prefix."""
        for value, expected in (
            ("!da1b1613", "meshtastic:!da1b1613"),
            ("DA1B1613", "meshtastic:!da1b1613"),
            ("channel:0", "meshtastic:channel:0"),
            ("meshtastic:channel:0", "meshtastic:channel:0"),
            ("groupchat/42", "groupchat/42"),  # unknown values are left untouched
        ):
            with patch.dict(os.environ, {"MESHTASTIC_HOME_CHANNEL": value}):
                config = MagicMock()
                config.extra = {}
                MeshtasticAdapter(config)
                self.assertEqual(os.environ["MESHTASTIC_HOME_CHANNEL"], expected)

    def test_maybe_prune_throttles_and_respects_env(self):
        """maybe_prune runs at most once per interval and honors the env var."""
        import telemetry_db as tdb

        tdb._last_prune_monotonic = time.monotonic()  # just ran -> throttled
        with patch.object(tdb, "prune") as mock_prune:
            tdb.maybe_prune()
        mock_prune.assert_not_called()  # throttled within the interval

        tdb._last_prune_monotonic = None  # force a run
        with patch.dict(os.environ, {"MESHTASTIC_TELEMETRY_RETENTION_DAYS": "7"}):
            with patch.object(tdb, "prune") as mock_prune:
                tdb.maybe_prune()
        mock_prune.assert_called_once_with(7.0)

    def test_failed_prune_retries_soon(self):
        """A failed prune resets the throttle so the next call retries promptly."""
        import telemetry_db as tdb

        calls: list[float] = []

        def fake_prune(days: float) -> int:
            calls.append(days)
            return -1 if len(calls) == 1 else 0  # fail once, then succeed

        tdb._last_prune_monotonic = None
        with patch.object(tdb, "prune", side_effect=fake_prune):
            tdb.maybe_prune()  # prune signals failure -> stamp reset to None
            tdb.maybe_prune()  # not throttled -> retries immediately
        self.assertEqual(len(calls), 2)  # failure did not advance a full interval

    def test_finite_float_and_int_coercion(self):
        """_finite_float/_finite_int reject None/bool/NaN/inf/non-numeric."""
        from telemetry_db import _finite_float, _finite_int

        self.assertEqual(_finite_float(3.5), 3.5)
        self.assertEqual(_finite_float(0), 0.0)
        self.assertEqual(_finite_float(-2), -2.0)
        self.assertEqual(_finite_float("3.25"), 3.25)
        self.assertIsNone(_finite_float(None))
        self.assertIsNone(_finite_float(True))
        self.assertIsNone(_finite_float(False))
        self.assertIsNone(_finite_float(float("nan")))
        self.assertIsNone(_finite_float(float("inf")))
        self.assertIsNone(_finite_float(float("-inf")))
        self.assertIsNone(_finite_float("abc"))
        self.assertIsNone(_finite_float("nan"))
        self.assertEqual(_finite_int(5), 5)
        self.assertEqual(_finite_int(5.9), 5)  # truncates toward zero
        self.assertEqual(_finite_int(-5.9), -5)
        self.assertEqual(_finite_int("3"), 3)
        self.assertIsNone(_finite_int(None))
        self.assertIsNone(_finite_int(float("nan")))
        self.assertIsNone(_finite_int(True))
        self.assertIsNone(_finite_int("abc"))

    def test_finite_float_and_int_reject_huge_int(self):
        """A hostile huge-int (10**400) overflows float() with OverflowError (an
        ArithmeticError, not a ValueError); the DB-boundary coerce must drop it
        to None instead of raising on the inbound write path. Mirrors
        node_freshness._coerce_float / chunking._effective_chunk_bytes."""
        from telemetry_db import _finite_float, _finite_int

        self.assertIsNone(_finite_float(10**400))
        self.assertIsNone(_finite_float(-(10**400)))
        self.assertEqual(_finite_float(10**18), 1e18)  # representable
        self.assertIsNone(_finite_int(10**400))
        self.assertEqual(_finite_int(10**18), 10**18)

    def test_log_telemetry_rejects_nan_and_garbage(self):
        """log_telemetry coerces bad numeric fields to NULL; finite siblings persist."""
        import telemetry_db as tdb

        tdb.log_telemetry(
            "!f00112233",
            battery_level="full",  # non-numeric -> NULL
            voltage=float("nan"),  # NaN -> NULL
            temperature="hot",  # non-numeric -> NULL
            humidity=float("inf"),  # inf -> NULL
            pressure=1013.25,  # finite -> kept
            uptime=True,  # bool -> NULL
        )
        with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
            row = conn.execute(
                "SELECT battery_level, voltage, temperature, humidity, pressure, uptime "
                "FROM telemetry WHERE node_id = ?",
                ("!f00112233",),
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertIsNone(row[0])  # battery_level
        self.assertIsNone(row[1])  # voltage (NaN dropped)
        self.assertIsNone(row[2])  # temperature
        self.assertIsNone(row[3])  # humidity (inf dropped)
        self.assertEqual(row[4], 1013.25)  # finite sibling kept
        self.assertIsNone(row[5])  # uptime (bool rejected)

    def test_log_position_rejects_nan_and_garbage(self):
        """log_position coerces bad coordinates to NULL; finite siblings persist."""
        import telemetry_db as tdb

        tdb.log_position("!f00221100", latitude=float("inf"), longitude=-71.0, altitude=10.0)
        with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
            row = conn.execute(
                "SELECT latitude, longitude, altitude FROM positions WHERE node_id = ?",
                ("!f00221100",),
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertIsNone(row[0])  # inf latitude dropped, not stored as inf
        self.assertEqual(row[1], -71.0)  # finite longitude kept
        self.assertEqual(row[2], 10.0)  # altitude kept

    def test_history_query_derives_columns_internally(self):
        """_history_query derives the SELECT list from the validated table only."""
        import inspect

        import telemetry_db as tdb

        # No caller-supplied 'columns' can reach the SELECT list anymore.
        self.assertNotIn("columns", inspect.signature(tdb._history_query).parameters)
        # An unknown table is rejected before any SQL runs.
        with self.assertRaises(ValueError):
            tdb._history_query("telemetry; DROP TABLE--", "!x", 5, None, "test")
        # A known table round-trips with columns from the internal map.
        tdb.log_signal("!coltest", snr=2.0, rssi=-90, hop_count=0)
        rows = tdb._history_query("signal_quality", "!coltest", 5, None, "signal test")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["snr"], 2.0)

    def test_init_db_enables_wal_journal_mode(self):
        """init_db sets WAL so concurrent reads don't block on writers."""
        with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
            mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        self.assertEqual(str(mode).lower(), "wal")

    def test_mock_connection(self):
        """Verify mock interface connects successfully."""
        interfaces = self.adapter.get_interfaces()
        self.assertEqual(len(interfaces), 1)
        self.assertIsInstance(interfaces[0], MockSerialInterface)
        self.assertEqual(interfaces[0].getMyNodeId(), "!da1b1613")

    def test_normalize_node_id_forms(self):
        """_normalize_node_id produces stable ! + lowercase 8-hex ids."""
        norm = MeshtasticAdapter._normalize_node_id
        self.assertEqual(norm(0xAB12CD34), "!ab12cd34")
        self.assertEqual(norm("!AB12CD34"), "!ab12cd34")
        self.assertEqual(norm("ab12cd34"), "!ab12cd34")
        self.assertEqual(norm("  !Da1b1613  "), "!da1b1613")
        self.assertIsNone(norm(None))
        self.assertIsNone(norm(""))
        # Non-hex labels are lowercased as-is (not forced into !hex form).
        self.assertEqual(norm("PARK"), "park")
        # bool is a subclass of int — must not become !00000001 / !00000000.
        self.assertEqual(norm(True), "true")
        self.assertEqual(norm(False), "false")

    def test_prune_deletes_only_old_rows(self):
        """prune() removes rows older than the cutoff and keeps recent ones."""
        from telemetry_db import prune

        now = time.time()
        with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
            # One old (10 days ago) and one fresh signal row.
            conn.execute(
                "INSERT INTO signal_quality (node_id, timestamp, snr, rssi) VALUES (?, ?, ?, ?)",
                ("!old", now - 10 * 86400, 1.0, -100),
            )
            conn.execute(
                "INSERT INTO signal_quality (node_id, timestamp, snr, rssi) VALUES (?, ?, ?, ?)",
                ("!new", now, 5.0, -90),
            )
            conn.commit()

        self.assertEqual(prune(5.0), 1)  # cutoff: 5 days

        with closing(sqlite3.connect(telemetry_db.DB_PATH)) as conn:
            remaining = {r[0] for r in conn.execute("SELECT node_id FROM signal_quality")}
        self.assertNotIn("!old", remaining)
        self.assertIn("!new", remaining)

    def test_prune_disabled_when_retention_zero(self):
        """prune(0) is a no-op (retention disabled)."""
        from telemetry_db import prune

        self.assertEqual(prune(0.0), 0)

    def test_register_declares_gateway_authz_env(self):
        """register() wires the allowlist env vars onto the PlatformEntry."""
        from adapter import register

        captured = {}

        class FakeCtx:
            def register_platform(self, **kwargs):
                captured.update(kwargs)

            def register_tool(self, **kwargs):
                pass

        register(FakeCtx())
        self.assertEqual(captured["allowed_users_env"], "MESHTASTIC_ALLOWED_NODES")
        self.assertEqual(captured["allow_all_env"], "MESHTASTIC_ALLOW_ALL_USERS")
        self.assertEqual(captured["max_message_length"], 233)
        self.assertEqual(captured["cron_deliver_env_var"], "MESHTASTIC_HOME_CHANNEL")
        self.assertTrue(callable(captured["standalone_sender_fn"]))

    def test_subscribe_pubsub_gates_on_transport_has_meshtastic(self):
        """Subscription follows transport.HAS_MESHTASTIC / transport.pub at call time."""
        for available in (True, False):
            fake_pub = MagicMock()
            config = MagicMock()
            config.extra = {}
            adapter = MeshtasticAdapter(config)
            with (
                patch("transport.HAS_MESHTASTIC", available),
                patch("transport.pub", fake_pub),
                patch.object(adapter, "_pubsub_subscribed", False),
            ):
                adapter._subscribe_pubsub()
                # Assert inside the patch: its exit restores the flag's old value.
                self.assertEqual(fake_pub.subscribe.called, available)
                self.assertEqual(adapter._pubsub_subscribed, available)

    def test_temp_db_isolation(self):
        """Verify tests point telemetry writes at a temporary DB, not the live Hermes DB."""
        self.assertEqual(telemetry_db.DB_PATH, self._tmp_db.name)
        self.assertNotIn(".hermes/meshtastic_telemetry.db", telemetry_db.DB_PATH)

    def test_tool_event_chrome_is_short_blurb(self):
        """format_tool_event returns a short emoji blurb, not full args/preview."""
        # Unknown tool name (no verb table entry) → emoji + tool name only.
        line = self.adapter.format_tool_event(
            SimpleNamespace(tool_name="web_search", preview="long query text", args={})
        )
        self.assertIsNotNone(line)
        self.assertNotIn("long query text", line)
        self.assertLessEqual(len(line), 48)
        # Empty / unusable event is dropped.
        self.assertIsNone(self.adapter.format_tool_event(SimpleNamespace()))


if __name__ == "__main__":
    unittest.main()
