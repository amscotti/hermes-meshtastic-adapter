"""Unit tests for the mesh_* tool handlers in mesh_tools.py.

The handlers reach the adapter through the module-level singleton
(``set_adapter`` / ``_get_adapter``), so they can be exercised against a small
stub adapter without assembling MeshtasticAdapter. The module is loaded
dynamically under its logical name ``meshtastic_tools`` (the adapter/CI
convention), so this file tests the same module instance the plugin uses.

Everything is hermetic: telemetry_db calls are patched at the module level
(mesh_tools holds a module reference to telemetry_db, so patching its
attributes is all that is needed — no real sqlite file is ever created).

The pure helpers the handlers compose were extracted to mesh_helpers.py
(P3.4) and are unit-tested in test_mesh_helpers.py; this file pins the
handler-level behavior only.
"""

import importlib.util
import json
import os
import sys
import threading
import time
import unittest
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

tools_spec = importlib.util.spec_from_file_location(
    "meshtastic_tools", os.path.join(os.path.dirname(os.path.abspath(__file__)), "mesh_tools.py")
)
meshtastic_tools = importlib.util.module_from_spec(tools_spec)
# Deliberately NOT registered in sys.modules: test_meshtastic.py owns that name
# (its tests patch "meshtastic_tools.X" by string), and this file holds a direct
# module reference, so registering here would silently break those patches when
# the two suites share a process.
tools_spec.loader.exec_module(meshtastic_tools)

PAUSE_MAX_MINUTES = meshtastic_tools.PAUSE_MAX_MINUTES

# Hermes' tools.registry / model_tools.coerce_tool_args are the consumers whose
# behavior the schema-format contract pins (C11) — resolve hermes-agent the same
# way the rest of the suite does (HERMES_AGENT_PATH → ~/.hermes → _deps). The
# registry module is stdlib-only, so the round-trip tests stay hermetic.
hermes_agent_path = os.getenv("HERMES_AGENT_PATH", os.path.expanduser("~/.hermes/hermes-agent"))
if not os.path.isdir(hermes_agent_path):
    hermes_agent_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "_deps", "hermes-agent"
    )
if os.path.isdir(hermes_agent_path):
    sys.path.append(hermes_agent_path)

from tools.registry import registry

import mesh_helpers


def _make_iface(nodes: dict, firmware: str = "2.5.4") -> SimpleNamespace:
    """Stub library interface carrying a nodes dict like the real SerialInterface."""
    return SimpleNamespace(nodes=nodes, metadata={"firmwareVersion": firmware})


def _make_node(
    nid: str,
    long_name: str,
    short_name: str,
    *,
    user_extra: dict | None = None,
    pos: dict | None = None,
    **fields: Any,
) -> dict:
    """Minimal node-info dict in the shape the meshtastic library exposes."""
    user = {
        "id": nid,
        "longName": long_name,
        "shortName": short_name,
        "hwModel": "TBEAM",
        "role": "CLIENT",
    }
    if user_extra:
        user.update(user_extra)
    node = {
        "user": user,
        "num": int(nid.lstrip("!")[:8], 16),
        "deviceMetrics": {},
        "position": pos if pos is not None else {},
        "lastHeard": 0,
    }
    node.update(fields)
    return node


class _StubAdapter:
    """The slice of MeshtasticAdapter that the mesh_tools handlers actually use.

    Grouped by handler:
      - list_nodes:          get_interfaces(), get_observed_node(nid)
      - node_info:           get_interfaces() (via resolve_node), get_observed_node(nid)
      - signal_quality:      get_interfaces() (via resolve_node), get_observed_node(nid)
      - send_dm/broadcast:   send(chat_id, content)
      - request_*:           request_telemetry / request_position / request_traceroute
      - pause/resume:        pause_link(minutes) / resume_link()
      - list_channels:       get_interfaces() (localNode.channels), allow_channels
    """

    def __init__(self) -> None:
        self.interfaces: list[Any] = []
        self.observed: dict[str, dict] = {}
        self.send_calls: list[tuple[str, str]] = []
        self.telemetry_calls: list[tuple[str, float]] = []
        self.position_calls: list[tuple[str, float]] = []
        self.traceroute_calls: list[tuple[str, int, float]] = []
        self.pause_calls: list[float | None] = []
        self.resume_calls: int = 0
        self.send_result = SimpleNamespace(success=True, message_id="msg-1", error=None)
        self.telemetry_result: dict = {"ok": True, "data": {"deviceMetrics": {}}}
        self.position_result: dict = {"ok": True, "data": {}}
        self.traceroute_result: dict = {"ok": True, "data": {}}
        self.pause_state: dict = {
            "paused": True,
            "resumes_at": "2026-08-01 12:00:00",
            "resumes_in_minutes": 90.0,
        }

    def get_interfaces(self) -> list[Any]:
        return self.interfaces

    @staticmethod
    def _normalize_node_id(node_id: Any) -> str | None:
        """Canonicalize to ``!`` + lowercase 8-hex, mirroring the adapter's
        helper so list_nodes' dedupe/lookup normalization is exercised."""
        if node_id is None:
            return None
        if isinstance(node_id, int) and not isinstance(node_id, bool):
            if 0 <= node_id < 2**32:
                return f"!{node_id:08x}"
            return None
        text = str(node_id).strip().lower()
        if not text:
            return None
        bare = text[1:] if text.startswith("!") else text
        if len(bare) == 8 and all(c in "0123456789abcdef" for c in bare):
            return f"!{bare}"
        return text

    def get_observed_node(self, node_id: str) -> dict:
        return self.observed.get(node_id, {})

    async def send(self, chat_id: str, content: str) -> SimpleNamespace:
        self.send_calls.append((chat_id, content))
        return self.send_result

    async def request_telemetry(self, node_id: str, timeout: float = 45.0) -> dict:
        self.telemetry_calls.append((node_id, timeout))
        return self.telemetry_result

    async def request_position(self, node_id: str, timeout: float = 45.0) -> dict:
        self.position_calls.append((node_id, timeout))
        return self.position_result

    async def request_traceroute(
        self, node_id: str, hop_limit: int = 5, timeout: float = 60.0
    ) -> dict:
        self.traceroute_calls.append((node_id, hop_limit, timeout))
        return self.traceroute_result

    def pause_link(self, minutes: float | None = None) -> dict:
        self.pause_calls.append(minutes)
        return self.pause_state

    def resume_link(self) -> dict:
        self.resume_calls += 1
        return {"paused": False}


class _DbHarness:
    """Hermetic stand-ins for the telemetry_db functions mesh_tools calls.

    mesh_tools holds a module-level reference to telemetry_db, so patching
    attributes on that module object is all that is needed — no real DB file
    is ever created. Per-test overrides nest cleanly over the setUp default.
    """

    def __init__(
        self,
        *,
        latest: dict | None = None,
        latest_direct: dict | None = None,
        signal: list | None = None,
        position: list | None = None,
        telemetry: list | None = None,
    ) -> None:
        self.values = {
            "latest": latest if latest is not None else {},
            "latest_direct": latest_direct if latest_direct is not None else {},
            "signal": signal if signal is not None else [],
            "position": position if position is not None else [],
            "telemetry": telemetry if telemetry is not None else [],
        }
        self.calls: dict[str, list] = {
            "latest": [],
            "latest_direct": [],
            "signal": [],
            "position": [],
            "telemetry": [],
        }
        self._patchers: list[Any] = []

    def __enter__(self) -> "_DbHarness":
        self.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.stop()

    def start(self) -> None:
        db = meshtastic_tools.telemetry_db

        def latest_f(direct_only: bool = False) -> dict:
            self.calls["latest"].append(direct_only)
            return self.values["latest_direct"] if direct_only else self.values["latest"]

        def latest_direct_f(node_id: str, *_args: Any, **_kwargs: Any) -> dict | None:
            self.calls["latest_direct"].append(node_id)
            return self.values["latest_direct"].get(node_id)

        def signal_f(*_args: Any, **_kwargs: Any) -> list:
            self.calls["signal"].append(_kwargs)
            return self.values["signal"]

        def position_f(*_args: Any, **_kwargs: Any) -> list:
            self.calls["position"].append(_kwargs)
            return self.values["position"]

        def telemetry_f(*_args: Any, **_kwargs: Any) -> list:
            self.calls["telemetry"].append(_kwargs)
            return self.values["telemetry"]

        for name, fn in (
            ("get_latest_signal_by_node", latest_f),
            ("get_latest_direct_signal", latest_direct_f),
            ("get_signal_history", signal_f),
            ("get_position_history", position_f),
            ("get_telemetry_history", telemetry_f),
        ):
            patcher = patch.object(db, name, side_effect=fn)
            patcher.start()
            self._patchers.append(patcher)

    def stop(self) -> None:
        for patcher in self._patchers:
            patcher.stop()
        self._patchers = []


class TestMeshToolsHandlers(unittest.IsolatedAsyncioTestCase):
    """Handler-level tests against the stub adapter with a patched telemetry_db."""

    def setUp(self) -> None:
        self.adapter = _StubAdapter()
        meshtastic_tools.set_adapter(self.adapter)
        self.db = _DbHarness()
        self.db.start()
        self.addCleanup(self.db.stop)

    def tearDown(self) -> None:
        meshtastic_tools.set_adapter(None)

    def _install_nodes(self, *ifaces: Any, observed: dict | None = None) -> None:
        self.adapter.interfaces = list(ifaces)
        if observed:
            self.adapter.observed.update(observed)

    # --- mesh_list_nodes ---------------------------------------------------

    async def test_list_nodes_formats_entry_with_observed_overlay(self) -> None:
        now = time.time()
        obs = {"snr": 9.5, "rssi": -70, "hops_away": 0, "last_heard": now - 60}
        node = _make_node(
            "!aaaa1111",
            "Alpha Node",
            "ALPH",
            last_heard=now - 600,
            hopsAway=0,
            deviceMetrics={"batteryLevel": 87},
        )
        self._install_nodes(_make_iface({"!aaaa1111": node}), observed={"!aaaa1111": obs})

        payload = json.loads(await meshtastic_tools.handle_mesh_list_nodes({}))
        self.assertEqual(len(payload["nodes"]), 1)
        entry = payload["nodes"][0]
        self.assertEqual(entry["node_id"], "!aaaa1111")
        self.assertEqual(entry["long_name"], "Alpha Node")
        self.assertEqual(entry["short_name"], "ALPH")
        self.assertEqual(entry["hw_model"], "TBEAM")
        self.assertEqual(entry["battery_level"], 87)
        self.assertEqual(entry["snr"], 9.5)
        self.assertEqual(entry["rssi"], -70)
        self.assertEqual(entry["signal_source"], "direct")
        self.assertEqual(entry["hops_away"], 0)
        self.assertTrue(entry["heard_directly"])
        # last_heard is the freshest of the node DB and the observation.
        self.assertEqual(
            entry["last_heard"], time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now - 60))
        )

    async def test_list_nodes_link_facts_from_node_db_when_obs_empty(self) -> None:
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH", snr=5.0, rssi=-80, hopsAway=2)
        self._install_nodes(_make_iface({"!aaaa1111": node}))

        payload = json.loads(await meshtastic_tools.handle_mesh_list_nodes({}))
        entry = payload["nodes"][0]
        self.assertEqual(entry["snr"], 5.0)
        self.assertEqual(entry["signal_source"], "relayed")
        self.assertEqual(entry["hops_away"], 2)
        self.assertFalse(entry["heard_directly"])
        self.assertEqual(entry["last_heard"], "Never")

    async def test_list_nodes_dedupes_shared_node_across_interfaces(self) -> None:
        node_a = _make_node("!aaaa1111", "Alpha Node", "ALPH")
        node_b = _make_node("!aaaa1111", "Alpha Duplicate", "ALPH")
        self._install_nodes(_make_iface({"!aaaa1111": node_a}), _make_iface({"!aaaa1111": node_b}))

        payload = json.loads(await meshtastic_tools.handle_mesh_list_nodes({}))
        self.assertEqual(len(payload["nodes"]), 1)
        self.assertEqual(payload["nodes"][0]["long_name"], "Alpha Node")

    async def test_list_nodes_dedupes_mixed_int_string_keys(self) -> None:
        """The same node keyed as an int on one interface and a string on
        another must collapse to one entry and still get the observed overlay
        (both are keyed by the canonical !hex id)."""
        now = time.time()
        obs = {"snr": 9.5, "rssi": -70, "last_heard": now - 60}
        node = _make_node("!ab12cd34", "Beta Node", "BETA", last_heard=now - 600, hopsAway=0)
        int_iface = _make_iface({0xAB12CD34: node})
        str_iface = _make_iface({"!ab12cd34": dict(node)})
        self._install_nodes(int_iface, str_iface, observed={"!ab12cd34": obs})

        payload = json.loads(await meshtastic_tools.handle_mesh_list_nodes({}))
        self.assertEqual(len(payload["nodes"]), 1)
        entry = payload["nodes"][0]
        self.assertEqual(entry["node_id"], "!ab12cd34")
        self.assertEqual(entry["long_name"], "Beta Node")
        self.assertEqual(entry["snr"], 9.5)  # overlay applied to the deduped entry
        self.assertEqual(entry["signal_source"], "direct")

    async def test_list_nodes_skips_interface_with_none_nodes(self) -> None:
        self.adapter.interfaces = [SimpleNamespace(nodes=None)]
        payload = json.loads(await meshtastic_tools.handle_mesh_list_nodes({}))
        self.assertEqual(payload["nodes"], [])

    async def test_list_nodes_iterates_snapshot_of_live_nodes(self) -> None:
        """mesh_list_nodes must not walk the live iface.nodes view.

        The meshtastic reader thread mutates that dict on NodeInfo. A value
        whose .get() inserts another key during iteration reproduces the race
        deterministically — without list(nodes.items()) the next __next__
        raises RuntimeError (same contract as resolve_node / resolve_dm_node).
        """
        nodes: dict[str, dict] = {}

        class _RacyInfo(dict):
            def get(self, key, default=None):
                if "!cc3333" not in nodes:
                    nodes["!cc3333"] = {
                        "user": {"id": "!cc3333", "longName": "Inflight", "shortName": "INF"},
                        "lastHeard": time.time(),
                        "snr": 1.0,
                        "hopsAway": 1,
                    }
                return super().get(key, default)

        nodes["!aaaa1111"] = _RacyInfo(
            {
                "user": {"id": "!aaaa1111", "longName": "Alpha Node", "shortName": "ALPH"},
                "lastHeard": time.time(),
                "snr": 5.0,
                "hopsAway": 0,
            }
        )
        self.adapter.interfaces = [SimpleNamespace(nodes=nodes)]
        payload = json.loads(await meshtastic_tools.handle_mesh_list_nodes({}))
        self.assertGreaterEqual(len(payload["nodes"]), 1)
        self.assertEqual(payload["nodes"][0]["node_id"], "!aaaa1111")

    async def test_list_nodes_db_reads_run_off_the_event_loop(self) -> None:
        """The batch latest-signal reads must not block the event loop (which
        also owns inbound pubsub bridging): they run on a worker thread."""
        loop_thread = threading.get_ident()
        threads: list[int] = []

        def record_thread(direct_only: bool = False) -> dict:
            threads.append(threading.get_ident())
            return {}

        with patch.object(
            meshtastic_tools.telemetry_db,
            "get_latest_signal_by_node",
            side_effect=record_thread,
        ):
            await meshtastic_tools.handle_mesh_list_nodes({})
        self.assertEqual(len(threads), 2)
        self.assertTrue(all(t != loop_thread for t in threads))

    async def test_list_nodes_direct_range_expiry_from_persisted_history(self) -> None:
        now = time.time()
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH")
        self._install_nodes(_make_iface({"!aaaa1111": node}))
        recent = {"!aaaa1111": {"snr": 5.0, "rssi": -70, "timestamp": now - 3600}}
        with _DbHarness(latest=recent, latest_direct=recent):
            payload = json.loads(await meshtastic_tools.handle_mesh_list_nodes({}))
        self.assertTrue(payload["nodes"][0]["heard_directly"])
        self.assertEqual(payload["nodes"][0]["signal_source"], "direct")

        old = {"!aaaa1111": {"snr": 5.0, "rssi": -70, "timestamp": now - 25 * 3600}}
        with _DbHarness(latest=old, latest_direct=old):
            payload = json.loads(await meshtastic_tools.handle_mesh_list_nodes({}))
        self.assertFalse(payload["nodes"][0]["heard_directly"])
        self.assertEqual(payload["nodes"][0]["last_direct_heard_age_hours"], 25.0)

    # --- mesh_node_info ----------------------------------------------------

    async def test_node_info_reports_metrics_and_fresh_position(self) -> None:
        now = time.time()
        node = _make_node(
            "!aaaa1111",
            "Alpha Node",
            "ALPH",
            pos={"latitude": 37.77, "longitude": -122.41, "time": now - 600},
            deviceMetrics={"batteryLevel": 88, "voltage": 4.12, "uptimeSeconds": 12345},
        )
        iface = _make_iface({"!aaaa1111": node})
        iface.getMyNodeId = lambda: "!aaaa1111"
        self._install_nodes(iface)

        payload = json.loads(await meshtastic_tools.handle_mesh_node_info({"node_id": "!aaaa1111"}))
        self.assertEqual(payload["long_name"], "Alpha Node")
        self.assertEqual(payload["battery_level"], 88)
        self.assertEqual(payload["voltage"], 4.12)
        self.assertEqual(payload["uptime"], 12345)
        self.assertEqual(payload["firmware_version"], "2.5.4")
        self.assertEqual(payload["latitude"], 37.77)
        self.assertAlmostEqual(payload["position_age_hours"], 0.2, places=1)
        self.assertFalse(payload["position_is_stale"])
        self.assertFalse(payload["has_public_key"])
        self.assertEqual(payload["last_heard"], "Never")
        self.assertIsNone(payload["last_heard_epoch"])

    async def test_node_info_firmware_reads_protobuf_metadata(self) -> None:
        # Real hardware: iface.metadata is a protobuf DeviceMetadata, which has
        # no .get() — this used to crash with "AttributeError: get".
        iface = _make_iface({"!aaaa1111": _make_node("!aaaa1111", "Alpha", "ALPH")})
        iface.metadata = SimpleNamespace(firmware_version="2.7.10.abcdef")
        iface.getMyNodeId = lambda: "!aaaa1111"
        self._install_nodes(iface)

        payload = json.loads(await meshtastic_tools.handle_mesh_node_info({"node_id": "!aaaa1111"}))
        self.assertEqual(payload["firmware_version"], "2.7.10.abcdef")

    async def test_node_info_firmware_unknown_before_metadata_arrives(self) -> None:
        iface = _make_iface({"!aaaa1111": _make_node("!aaaa1111", "Alpha", "ALPH")})
        iface.metadata = None
        iface.getMyNodeId = lambda: "!aaaa1111"
        self._install_nodes(iface)

        payload = json.loads(await meshtastic_tools.handle_mesh_node_info({"node_id": "!aaaa1111"}))
        self.assertEqual(payload["firmware_version"], "Unknown")

    async def test_node_info_firmware_not_attributed_to_remote_node(self) -> None:
        # iface.metadata is the local radio's; a remote node must not inherit it.
        iface = _make_iface(
            {
                "!aaaa1111": _make_node("!aaaa1111", "Local", "LOCL"),
                "!bbbb2222": _make_node("!bbbb2222", "Remote", "REMO"),
            }
        )
        iface.getMyNodeId = lambda: "!aaaa1111"
        self._install_nodes(iface)

        payload = json.loads(await meshtastic_tools.handle_mesh_node_info({"node_id": "!bbbb2222"}))
        self.assertEqual(payload["firmware_version"], "Unknown")

    async def test_node_info_marks_old_position_stale(self) -> None:
        now = time.time()
        node = _make_node(
            "!aaaa1111",
            "Alpha Node",
            "ALPH",
            pos={"time": now - 7 * 3600},
            deviceMetrics={"uptime": 999},
        )
        self._install_nodes(_make_iface({"!aaaa1111": node}))

        payload = json.loads(await meshtastic_tools.handle_mesh_node_info({"node_id": "!aaaa1111"}))
        self.assertTrue(payload["position_is_stale"])
        # uptimeSeconds missing -> plain "uptime" is the fallback.
        self.assertEqual(payload["uptime"], 999)

    async def test_node_info_position_falls_back_to_db_history(self) -> None:
        now = time.time()
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH", pos={})
        self._install_nodes(_make_iface({"!aaaa1111": node}))

        with _DbHarness(position=[{"timestamp": now - 300}]):
            payload = json.loads(
                await meshtastic_tools.handle_mesh_node_info({"node_id": "!aaaa1111"})
            )
        self.assertEqual(
            payload["position_time"], time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now - 300))
        )
        self.assertAlmostEqual(payload["position_age_hours"], 300 / 3600, places=1)
        self.assertFalse(payload["position_is_stale"])

    async def test_node_info_unknown_node_returns_error(self) -> None:
        self._install_nodes(_make_iface({}))
        payload = json.loads(await meshtastic_tools.handle_mesh_node_info({"node_id": "!zzzz9999"}))
        self.assertIn("was not found in the mesh database", payload["error"])

    async def test_node_info_uses_single_node_direct_getter_not_full_scan(self) -> None:
        """Single-node queries must not trigger the all-nodes window scan (which
        serializes against every telemetry writer). The handler reads only the
        targeted latest-direct row instead."""
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH")
        self._install_nodes(_make_iface({"!aaaa1111": node}))
        await meshtastic_tools.handle_mesh_node_info({"node_id": "!aaaa1111"})
        self.assertEqual(self.db.calls["latest_direct"], ["!aaaa1111"])
        self.assertEqual(self.db.calls["latest"], [])

    async def test_node_info_tolerates_non_numeric_last_heard(self) -> None:
        """A hostile/malformed NodeInfo carrying a truthy non-numeric lastHeard
        must not crash max() with a TypeError — numeric_epoch guards it."""
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH", last_heard="not-a-number")
        self._install_nodes(_make_iface({"!aaaa1111": node}))

        payload = json.loads(await meshtastic_tools.handle_mesh_node_info({"node_id": "!aaaa1111"}))
        self.assertIsNone(payload["last_heard_epoch"])
        self.assertEqual(payload["last_heard"], "Never")

    async def test_node_info_picks_freshest_numeric_last_heard(self) -> None:
        """numeric_epoch still keeps the freshest valid value when one side is
        bad and the other is a real epoch."""
        now = int(time.time())
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH", last_heard="junk")
        self._install_nodes(
            _make_iface({"!aaaa1111": node}), observed={"!aaaa1111": {"last_heard": now}}
        )

        payload = json.loads(await meshtastic_tools.handle_mesh_node_info({"node_id": "!aaaa1111"}))
        self.assertEqual(payload["last_heard_epoch"], float(now))
        self.assertEqual(
            payload["last_heard"], time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now))
        )

    async def test_node_info_requires_node_id(self) -> None:
        payload = json.loads(await meshtastic_tools.handle_mesh_node_info({}))
        self.assertEqual(payload["error"], "Parameter 'node_id' is required.")

    async def test_handlers_degrade_cleanly_on_numeric_node_id(self) -> None:
        """A numeric/boolean node_id must yield a JSON error per handler, never
        an AttributeError traceback (resolve_node coerces before .strip())."""
        self._install_nodes(_make_iface({}))
        for node_id in (123, True):
            for name, args in (
                ("handle_mesh_node_info", {"node_id": node_id}),
                ("handle_mesh_signal_quality", {"node_id": node_id}),
                ("handle_mesh_telemetry", {"node_id": node_id}),
                ("handle_mesh_send_dm", {"node_id": node_id, "message": "hi"}),
                ("handle_mesh_request_telemetry", {"node_id": node_id}),
                ("handle_mesh_request_position", {"node_id": node_id}),
                ("handle_mesh_traceroute", {"node_id": node_id}),
            ):
                with self.subTest(handler=name, node_id=node_id):
                    handler = getattr(meshtastic_tools, name)
                    payload = json.loads(await handler(args))
                    self.assertIn("error", payload)
            # telemetry_history answers with an empty-history success payload for
            # any id (it just queries the DB) — the point is it must not raise.
            with self.subTest(handler="handle_mesh_telemetry_history", node_id=node_id):
                payload = json.loads(
                    await meshtastic_tools.handle_mesh_telemetry_history({"node_id": node_id})
                )
                self.assertEqual(payload["history"], [])

    # --- mesh_signal_quality -----------------------------------------------

    async def test_signal_quality_label_and_trend_with_hop_count(self) -> None:
        now = time.time()
        obs = {"snr": 9.5, "rssi": -65, "hops_away": 0}
        hist = [
            {"timestamp": now - 3600, "snr": 6.0, "rssi": -70, "hop_count": 1},
            {"timestamp": now - 7200, "snr": 4.0, "rssi": -75, "hop_count": 2},
        ]
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH")
        self._install_nodes(_make_iface({"!aaaa1111": node}), observed={"!aaaa1111": obs})

        with _DbHarness(signal=hist):
            payload = json.loads(
                await meshtastic_tools.handle_mesh_signal_quality({"node_id": "!aaaa1111"})
            )
        current = payload["current"]
        self.assertEqual(current["quality"], "Excellent")
        self.assertEqual(current["signal_source"], "direct")
        self.assertEqual(current["hops_away"], 0)
        self.assertEqual(payload["name"], "Alpha Node")
        self.assertEqual(len(payload["trend_history"]), 2)
        first = payload["trend_history"][0]
        self.assertEqual(first["snr"], 6.0)
        self.assertEqual(first["hops_away"], 1)
        self.assertEqual(first["time"], time.strftime("%H:%M:%S", time.localtime(now - 3600)))

    async def test_signal_quality_no_readings_returns_error(self) -> None:
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH")
        self._install_nodes(_make_iface({"!aaaa1111": node}))

        payload = json.loads(
            await meshtastic_tools.handle_mesh_signal_quality({"node_id": "!aaaa1111"})
        )
        self.assertEqual(payload["node_id"], "!aaaa1111")
        self.assertIn("No signal quality readings available", payload["error"])

    async def test_signal_quality_trend_never_renders_non_numeric(self) -> None:
        """Trend rows filter non-numeric signal values at the render boundary."""
        now = time.time()
        obs = {"snr": 9.5, "rssi": -65, "hops_away": 0}
        hist = [
            {"timestamp": now - 3600, "snr": "junk", "rssi": -70, "hop_count": 1},
            {"timestamp": now - 7200, "snr": 4.0, "rssi": "garbage", "hop_count": 2},
        ]
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH")
        self._install_nodes(_make_iface({"!aaaa1111": node}), observed={"!aaaa1111": obs})

        with _DbHarness(signal=hist):
            payload = json.loads(
                await meshtastic_tools.handle_mesh_signal_quality({"node_id": "!aaaa1111"})
            )
        trend = payload["trend_history"]
        self.assertEqual(trend[0]["snr"], None)
        self.assertEqual(trend[1]["rssi"], None)
        for entry in trend:
            for key in ("snr", "rssi"):
                if entry[key] is not None:
                    self.assertIsInstance(entry[key], (int, float))

    async def test_signal_quality_unknown_node_uses_query_id(self) -> None:
        self._install_nodes(_make_iface({}))
        with _DbHarness(latest_direct={"!zzzz9999": {"snr": 3.5, "rssi": -80}}):
            payload = json.loads(
                await meshtastic_tools.handle_mesh_signal_quality({"node_id": "!zzzz9999"})
            )
        self.assertEqual(payload["node_id"], "!zzzz9999")
        self.assertEqual(payload["name"], "Unknown")
        self.assertEqual(payload["current"]["quality"], "Good")
        self.assertEqual(payload["current"]["signal_source"], "direct")

    async def test_signal_quality_uses_single_node_direct_getter_not_full_scan(self) -> None:
        """Single-node queries must not trigger the all-nodes window scan."""
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH")
        self._install_nodes(_make_iface({"!aaaa1111": node}))
        with _DbHarness(latest_direct={"!aaaa1111": {"snr": 7.0, "rssi": -80}}) as db:
            await meshtastic_tools.handle_mesh_signal_quality({"node_id": "!aaaa1111"})
        self.assertEqual(db.calls["latest_direct"], ["!aaaa1111"])
        self.assertEqual(db.calls["latest"], [])

    async def test_signal_quality_requires_node_id(self) -> None:
        payload = json.loads(await meshtastic_tools.handle_mesh_signal_quality({}))
        self.assertEqual(payload["error"], "Parameter 'node_id' is required.")

    # --- mesh_send_dm / mesh_send_broadcast --------------------------------

    async def test_send_dm_invokes_adapter_send(self) -> None:
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH", user_extra={"publicKey": "0x1234"})
        self._install_nodes(_make_iface({"!aaaa1111": node}))

        payload = json.loads(
            await meshtastic_tools.handle_mesh_send_dm({"node_id": "!aaaa1111", "message": "hello"})
        )
        self.assertTrue(payload["success"])
        self.assertEqual(payload["message_id"], "msg-1")
        self.assertEqual(payload["target_node"], "!aaaa1111")
        self.assertEqual(self.adapter.send_calls, [("meshtastic:!aaaa1111", "hello")])

    async def test_send_dm_adapter_no_pubkey_is_surfaced(self) -> None:
        """The tool no longer pre-gates on the first interface's pubkey copy:
        the adapter's send-path verdict decides, and no_pubkey is surfaced."""
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH")
        self._install_nodes(_make_iface({"!aaaa1111": node}))
        self.adapter.send_result = SimpleNamespace(
            success=False,
            message_id=None,
            error="Target node !aaaa1111 has no public key; direct message cannot be encrypted",
        )

        payload = json.loads(
            await meshtastic_tools.handle_mesh_send_dm({"node_id": "!aaaa1111", "message": "hello"})
        )
        self.assertFalse(payload["success"])
        self.assertIn("no public key", payload["error"])
        self.assertEqual(payload["target_node"], "!aaaa1111")
        self.assertEqual(self.adapter.send_calls, [("meshtastic:!aaaa1111", "hello")])

    async def test_send_dm_keyless_first_interface_still_sends(self) -> None:
        """resolve_node returns the FIRST interface that knows the node; a
        keyless copy there must not reject a DM the adapter would deliver on a
        later keyed interface (dm_send_target prefers the keyed owner)."""
        keyless = _make_node("!aaaa1111", "Alpha Node", "ALPH")
        keyed = _make_node("!aaaa1111", "Alpha Node", "ALPH", user_extra={"publicKey": "0x1234"})
        self._install_nodes(_make_iface({"!aaaa1111": keyless}), _make_iface({"!aaaa1111": keyed}))

        payload = json.loads(
            await meshtastic_tools.handle_mesh_send_dm({"node_id": "!aaaa1111", "message": "hello"})
        )
        self.assertTrue(payload["success"])
        self.assertEqual(payload["target_node"], "!aaaa1111")
        self.assertEqual(self.adapter.send_calls, [("meshtastic:!aaaa1111", "hello")])

    async def test_send_dm_unresolved_node_returns_error(self) -> None:
        self._install_nodes(_make_iface({}))
        payload = json.loads(
            await meshtastic_tools.handle_mesh_send_dm({"node_id": "!zzzz9999", "message": "hello"})
        )
        self.assertIn("could not be resolved", payload["error"])
        self.assertEqual(self.adapter.send_calls, [])

    async def test_send_dm_resolved_node_without_user_id_returns_clean_error(self) -> None:
        """A resolved node dict that lacks user.id is malformed NodeInfo; the
        handler must surface a clean JSON error rather than building a
        "meshtastic:None" chat_id the send path cannot resolve."""
        malformed = {
            "user": {"longName": "Ghost"},
            "num": 0xAAAA1111,
            "deviceMetrics": {},
            "position": {},
            "lastHeard": 0,
        }
        self._install_nodes(_make_iface({"!aaaa1111": malformed}))

        payload = json.loads(
            await meshtastic_tools.handle_mesh_send_dm({"node_id": "!aaaa1111", "message": "hi"})
        )
        self.assertIn("no known node ID", payload["error"])
        self.assertEqual(self.adapter.send_calls, [])

    async def test_send_dm_ambiguous_name_refuses_to_pick(self) -> None:
        """Two nodes sharing a name must not silently DM whichever node claimed
        the name first — the handler refuses and asks for the exact id."""
        shared1 = _make_node("!aaaa1111", "Shared Name", "SH1")
        shared2 = _make_node("!bbbb2222", "Shared Name", "SH2")
        self._install_nodes(_make_iface({"!aaaa1111": shared1, "!bbbb2222": shared2}))

        payload = json.loads(
            await meshtastic_tools.handle_mesh_send_dm({"node_id": "shared name", "message": "hi"})
        )
        self.assertIn("could not be resolved", payload["error"])
        self.assertEqual(self.adapter.send_calls, [])

        # By exact id the same message resolves and sends to the chosen node.
        payload = json.loads(
            await meshtastic_tools.handle_mesh_send_dm({"node_id": "!aaaa1111", "message": "hi"})
        )
        self.assertTrue(payload["success"])
        self.assertEqual(payload["target_node"], "!aaaa1111")

    async def test_send_dm_requires_both_params(self) -> None:
        for args in ({}, {"node_id": "!aaaa1111"}, {"message": "hi"}):
            with self.subTest(args=args):
                payload = json.loads(await meshtastic_tools.handle_mesh_send_dm(args))
                self.assertIn("Parameters 'node_id' and 'message' are required.", payload["error"])

    async def test_send_broadcast_uses_channel_chat_id(self) -> None:
        payload = json.loads(await meshtastic_tools.handle_mesh_send_broadcast({"message": "hi"}))
        self.assertEqual(payload["channel"], "0")
        self.assertEqual(self.adapter.send_calls, [("meshtastic:channel:0", "hi")])

        payload = json.loads(
            await meshtastic_tools.handle_mesh_send_broadcast({"message": "hi", "channel": "2"})
        )
        self.assertEqual(payload["channel"], "2")
        self.assertEqual(self.adapter.send_calls[-1], ("meshtastic:channel:2", "hi"))

    async def test_send_broadcast_requires_message(self) -> None:
        payload = json.loads(await meshtastic_tools.handle_mesh_send_broadcast({}))
        self.assertEqual(payload["error"], "Parameter 'message' is required.")
        self.assertEqual(self.adapter.send_calls, [])

    async def test_send_broadcast_named_match_reports_resolved_index(self) -> None:
        iface = SimpleNamespace(
            nodes={},
            localNode=SimpleNamespace(
                channels=[{"index": 0, "name": "Primary"}, {"index": 1, "name": "Telemetry"}]
            ),
        )
        self._install_nodes(iface)

        payload = json.loads(
            await meshtastic_tools.handle_mesh_send_broadcast(
                {"message": "x", "channel": "Primary"}
            )
        )
        self.assertTrue(payload["success"])
        self.assertEqual(payload["channel"], "Primary")
        self.assertEqual(payload["channel_index"], 0)
        self.assertEqual(self.adapter.send_calls, [("meshtastic:channel:Primary", "x")])

        payload = json.loads(
            await meshtastic_tools.handle_mesh_send_broadcast(
                {"message": "x", "channel": "Telemetry"}
            )
        )
        self.assertEqual(payload["channel_index"], 1)
        self.assertEqual(self.adapter.send_calls[-1], ("meshtastic:channel:Telemetry", "x"))

    async def test_send_broadcast_unmatched_name_is_rejected(self) -> None:
        """A mistyped channel name must be rejected up front instead of
        broadcasting on channel 0 while the reply claims the named channel."""
        iface = SimpleNamespace(
            nodes={},
            localNode=SimpleNamespace(channels=[{"index": 0, "name": "Primary"}]),
        )
        self._install_nodes(iface)

        payload = json.loads(
            await meshtastic_tools.handle_mesh_send_broadcast(
                {"message": "x", "channel": "doesnotexist"}
            )
        )
        self.assertIn("not available", payload["error"])
        self.assertEqual(self.adapter.send_calls, [])

    async def test_send_broadcast_unmatched_numeric_is_rejected(self) -> None:
        iface = SimpleNamespace(
            nodes={},
            localNode=SimpleNamespace(channels=[{"index": 0, "name": "Primary"}]),
        )
        self._install_nodes(iface)

        payload = json.loads(
            await meshtastic_tools.handle_mesh_send_broadcast({"message": "x", "channel": "9"})
        )
        self.assertIn("not available", payload["error"])
        self.assertEqual(self.adapter.send_calls, [])

    async def test_send_broadcast_normalizes_non_string_channel(self) -> None:
        """A non-string channel (int/bool) must be normalized to str before both
        validation and chat_id encoding, so the two paths agree. A bool True
        would otherwise validate as channel 1 (int(True)) but encode as
        "meshtastic:channel:True" — a named channel the adapter re-resolves."""
        # int channel: encodes consistently as "0".
        payload = json.loads(
            await meshtastic_tools.handle_mesh_send_broadcast({"message": "hi", "channel": 0})
        )
        self.assertTrue(payload["success"])
        self.assertEqual(payload["channel"], "0")
        self.assertEqual(self.adapter.send_calls[-1], ("meshtastic:channel:0", "hi"))

        # bool channel: normalized to "True", which (with no matching channel
        # table on the empty interface) passes through and encodes consistently.
        payload = json.loads(
            await meshtastic_tools.handle_mesh_send_broadcast({"message": "hi", "channel": True})
        )
        self.assertTrue(payload["success"])
        self.assertEqual(payload["channel"], "True")
        self.assertEqual(self.adapter.send_calls[-1], ("meshtastic:channel:True", "hi"))

    async def test_send_propagates_adapter_result_error(self) -> None:
        self.adapter.send_result = SimpleNamespace(success=False, message_id=None, error="no route")
        payload = json.loads(
            await meshtastic_tools.handle_mesh_send_broadcast({"message": "hi", "channel": "1"})
        )
        self.assertFalse(payload["success"])
        self.assertEqual(payload["error"], "no route")

    # --- mesh_telemetry ----------------------------------------------------

    async def test_telemetry_live_fields_win_over_history(self) -> None:
        node = _make_node(
            "!aaaa1111",
            "Alpha Node",
            "ALPH",
            environmentMetrics={
                "temperature": 21.5,
                "relativeHumidity": 44.0,
                "barometricPressure": 1013.0,
            },
            deviceMetrics={"batteryLevel": 92, "voltage": 4.05, "uptimeSeconds": 3600},
        )
        self._install_nodes(_make_iface({"!aaaa1111": node}))

        with _DbHarness(telemetry=[{"temperature": 99.0, "battery_level": 1}]):
            payload = json.loads(
                await meshtastic_tools.handle_mesh_telemetry({"node_id": "!aaaa1111"})
            )
        self.assertEqual(payload["name"], "Alpha Node")
        self.assertEqual(payload["temperature"], 21.5)
        self.assertEqual(payload["humidity"], 44.0)
        self.assertEqual(payload["pressure"], 1013.0)
        self.assertEqual(payload["battery_level"], 92)
        self.assertEqual(payload["uptime"], 3600)

    async def test_telemetry_falls_back_to_db_history(self) -> None:
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH")
        self._install_nodes(_make_iface({"!aaaa1111": node}))

        with _DbHarness(
            telemetry=[
                {
                    "temperature": 17.0,
                    "humidity": 50.0,
                    "pressure": 1008.0,
                    "battery_level": 66,
                    "voltage": 3.9,
                    "uptime": 500,
                }
            ]
        ):
            payload = json.loads(
                await meshtastic_tools.handle_mesh_telemetry({"node_id": "!aaaa1111"})
            )
        self.assertEqual(payload["temperature"], 17.0)
        self.assertEqual(payload["humidity"], 50.0)
        self.assertEqual(payload["battery_level"], 66)
        self.assertEqual(payload["uptime"], 500)

    async def test_telemetry_no_data_returns_error(self) -> None:
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH")
        self._install_nodes(_make_iface({"!aaaa1111": node}))
        payload = json.loads(await meshtastic_tools.handle_mesh_telemetry({"node_id": "!aaaa1111"}))
        self.assertIn("No telemetry data is available", payload["error"])

    async def test_telemetry_reads_camelcase_barometric_temperature(self) -> None:
        """Real-hardware MessageToDict payloads are camelCase; the snake_case
        fallback must not be the only key that works."""
        node = _make_node(
            "!aaaa1111",
            "Alpha Node",
            "ALPH",
            environmentMetrics={"barometricTemperature": 21.5},
            deviceMetrics={"batteryLevel": 92},
        )
        self._install_nodes(_make_iface({"!aaaa1111": node}))
        payload = json.loads(await meshtastic_tools.handle_mesh_telemetry({"node_id": "!aaaa1111"}))
        self.assertEqual(payload["temperature"], 21.5)

    async def test_telemetry_partial_fallback_mixes_live_and_db(self) -> None:
        """Live temperature present but battery absent: the DB supplies only the
        missing field, never overwriting the live value."""
        node = _make_node(
            "!aaaa1111",
            "Alpha Node",
            "ALPH",
            environmentMetrics={"temperature": 21.5},
            deviceMetrics={"voltage": 4.05},
        )
        self._install_nodes(_make_iface({"!aaaa1111": node}))
        with _DbHarness(telemetry=[{"battery_level": 66, "voltage": 3.9, "humidity": 50.0}]):
            payload = json.loads(
                await meshtastic_tools.handle_mesh_telemetry({"node_id": "!aaaa1111"})
            )
        self.assertEqual(payload["temperature"], 21.5)  # live wins
        self.assertEqual(payload["battery_level"], 66)  # DB fills the gap
        self.assertEqual(payload["voltage"], 4.05)  # live wins over DB

    async def test_telemetry_backfills_missing_metric_when_temp_and_battery_live(self) -> None:
        """A node reporting live temperature and battery but no humidity must
        still backfill humidity from history — the gate is per-field, not "is
        the node reporting at all" — so a missing sensor reading does not
        silently drop while the DB has a recent value."""
        node = _make_node(
            "!aaaa1111",
            "Alpha Node",
            "ALPH",
            environmentMetrics={"temperature": 21.5},
            deviceMetrics={"batteryLevel": 92},
        )
        self._install_nodes(_make_iface({"!aaaa1111": node}))
        with _DbHarness(telemetry=[{"humidity": 60.0, "pressure": 1010.0}]):
            payload = json.loads(
                await meshtastic_tools.handle_mesh_telemetry({"node_id": "!aaaa1111"})
            )
        self.assertEqual(payload["temperature"], 21.5)  # live
        self.assertEqual(payload["battery_level"], 92)  # live
        self.assertEqual(payload["humidity"], 60.0)  # backfilled from DB
        self.assertEqual(payload["pressure"], 1010.0)  # backfilled from DB

    async def test_telemetry_requires_node_id(self) -> None:
        payload = json.loads(await meshtastic_tools.handle_mesh_telemetry({}))
        self.assertEqual(payload["error"], "Parameter 'node_id' is required.")

    # --- mesh_telemetry_history --------------------------------------------

    async def test_telemetry_history_since_hours_validation(self) -> None:
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH")
        self._install_nodes(_make_iface({"!aaaa1111": node}))
        for since_hours in ("abc", "12x"):
            with self.subTest(since_hours=since_hours):
                payload = json.loads(
                    await meshtastic_tools.handle_mesh_telemetry_history(
                        {"node_id": "!aaaa1111", "since_hours": since_hours}
                    )
                )
                self.assertEqual(payload["error"], "Parameter 'since_hours' must be a number.")
        for since_hours in (0, -5):
            with self.subTest(since_hours=since_hours):
                payload = json.loads(
                    await meshtastic_tools.handle_mesh_telemetry_history(
                        {"node_id": "!aaaa1111", "since_hours": since_hours}
                    )
                )
                self.assertEqual(payload["error"], "Parameter 'since_hours' must be positive.")

    async def test_telemetry_history_rejects_nan_since_hours(self) -> None:
        """NaN since_hours must return an error instead of crashing on
        time.localtime(nan) — and must not reach the DB."""
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH")
        self._install_nodes(_make_iface({"!aaaa1111": node}))
        for since_hours in ("nan", "inf"):
            with self.subTest(since_hours=since_hours):
                with _DbHarness() as db:
                    payload = json.loads(
                        await meshtastic_tools.handle_mesh_telemetry_history(
                            {"node_id": "!aaaa1111", "since_hours": since_hours}
                        )
                    )
                self.assertEqual(payload["error"], "Parameter 'since_hours' must be a number.")
                self.assertEqual(db.calls["telemetry"], [])

    async def test_telemetry_history_truncation_flag_and_oldest_returned(self) -> None:
        now = time.time()
        rows = [{"timestamp": now - 3600 * i, "temperature": 20.0 + i} for i in range(3)]
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH")
        self._install_nodes(_make_iface({"!aaaa1111": node}))

        with _DbHarness(telemetry=rows) as db:
            payload = json.loads(
                await meshtastic_tools.handle_mesh_telemetry_history(
                    {"node_id": "!aaaa1111", "since_hours": 24, "limit": 2}
                )
            )
        self.assertEqual(payload["returned"], 2)
        self.assertTrue(payload["truncated"])
        self.assertIn("window_requested_from", payload)
        self.assertEqual(
            payload["oldest_returned"],
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(rows[1]["timestamp"])),
        )
        self.assertEqual(len(payload["history"]), 2)
        self.assertIn("time", payload["history"][0])
        # Windowed mode fetches limit + 1 rows so truncation is detectable.
        self.assertEqual(db.calls["telemetry"][0]["limit"], 3)
        self.assertIn("since", db.calls["telemetry"][0])

    async def test_telemetry_history_window_not_truncated(self) -> None:
        now = time.time()
        rows = [{"timestamp": now - 3600 * i, "temperature": 20.0 + i} for i in range(3)]
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH")
        self._install_nodes(_make_iface({"!aaaa1111": node}))

        with _DbHarness(telemetry=rows) as db:
            payload = json.loads(
                await meshtastic_tools.handle_mesh_telemetry_history(
                    {"node_id": "!aaaa1111", "since_hours": 24, "limit": 5}
                )
            )
        self.assertEqual(payload["returned"], 3)
        self.assertFalse(payload["truncated"])
        self.assertEqual(db.calls["telemetry"][0]["limit"], 6)
        self.assertEqual(payload["name"], "Alpha Node")

    async def test_telemetry_history_count_mode_has_no_window_fields(self) -> None:
        now = time.time()
        rows = [{"timestamp": now - 3600 * i, "temperature": 20.0 + i} for i in range(10)]
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH")
        self._install_nodes(_make_iface({"!aaaa1111": node}))

        with _DbHarness(telemetry=rows) as db:
            payload = json.loads(
                await meshtastic_tools.handle_mesh_telemetry_history(
                    {"node_id": "!aaaa1111", "limit": 10}
                )
            )
        self.assertEqual(payload["returned"], 10)
        self.assertNotIn("truncated", payload)
        self.assertNotIn("oldest_returned", payload)
        # Count mode fetches exactly `limit`, not limit + 1.
        self.assertEqual(db.calls["telemetry"][0]["limit"], 10)
        self.assertIsNone(db.calls["telemetry"][0]["since"])

    async def test_telemetry_history_limit_is_clamped_to_row_caps(self) -> None:
        now = time.time()
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH")
        self._install_nodes(_make_iface({"!aaaa1111": node}))

        with _DbHarness(telemetry=[{"timestamp": now - 3600 * i} for i in range(100)]) as db:
            payload = json.loads(
                await meshtastic_tools.handle_mesh_telemetry_history(
                    {"node_id": "!aaaa1111", "limit": 99999}
                )
            )
        self.assertEqual(db.calls["telemetry"][0]["limit"], 100)
        self.assertEqual(payload["returned"], 100)

        with _DbHarness(telemetry=[{"timestamp": now - 3600 * i} for i in range(500)]) as db:
            payload = json.loads(
                await meshtastic_tools.handle_mesh_telemetry_history(
                    {"node_id": "!aaaa1111", "since_hours": 24, "limit": 99999}
                )
            )
        self.assertEqual(db.calls["telemetry"][0]["limit"], 501)
        self.assertEqual(payload["returned"], 500)

    async def test_telemetry_history_dispatches_metric_types(self) -> None:
        now = time.time()
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH")
        self._install_nodes(_make_iface({"!aaaa1111": node}))

        with _DbHarness(position=[{"timestamp": now}]) as db:
            payload = json.loads(
                await meshtastic_tools.handle_mesh_telemetry_history(
                    {"node_id": "!aaaa1111", "metric_type": "positions", "limit": 1}
                )
            )
        self.assertEqual(payload["metric_type"], "positions")
        self.assertEqual(db.calls["position"][0]["limit"], 1)
        self.assertEqual(
            payload["history"][0]["time"], time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now))
        )

        with _DbHarness(signal=[{"timestamp": now}]) as db:
            payload = json.loads(
                await meshtastic_tools.handle_mesh_telemetry_history(
                    {"node_id": "!aaaa1111", "metric_type": "signal_quality", "limit": 1}
                )
            )
        self.assertEqual(db.calls["signal"][0]["limit"], 1)

    async def test_telemetry_history_invalid_metric_type(self) -> None:
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH")
        self._install_nodes(_make_iface({"!aaaa1111": node}))
        payload = json.loads(
            await meshtastic_tools.handle_mesh_telemetry_history(
                {"node_id": "!aaaa1111", "metric_type": "bogus"}
            )
        )
        self.assertEqual(payload["error"], "Invalid metric_type 'bogus'.")

    async def test_telemetry_history_requires_node_id(self) -> None:
        payload = json.loads(await meshtastic_tools.handle_mesh_telemetry_history({}))
        self.assertEqual(payload["error"], "Parameter 'node_id' is required.")

    # --- Solicited requests ------------------------------------------------

    async def test_request_telemetry_passthrough_and_resolution(self) -> None:
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH")
        self._install_nodes(_make_iface({"!aaaa1111": node}))
        self.adapter.telemetry_result = {
            "ok": True,
            "data": {
                "deviceMetrics": {
                    "batteryLevel": 80,
                    "voltage": 4.1,
                    "uptimeSeconds": 99,
                    "channelUtilization": 3.5,
                    "airUtilTx": 0.5,
                }
            },
        }

        payload = json.loads(
            await meshtastic_tools.handle_mesh_request_telemetry(
                {"node_id": "!aaaa1111", "timeout": 30}
            )
        )
        self.assertEqual(payload["answered"], True)
        self.assertEqual(payload["battery_level"], 80)
        self.assertEqual(payload["voltage"], 4.1)
        self.assertEqual(payload["uptime_seconds"], 99)
        self.assertEqual(payload["channel_utilization"], 3.5)
        self.assertEqual(self.adapter.telemetry_calls, [("!aaaa1111", 30.0)])

        # Resolves by name too.
        await meshtastic_tools.handle_mesh_request_telemetry({"node_id": "alpha node"})
        self.assertEqual(self.adapter.telemetry_calls[-1], ("!aaaa1111", 45.0))

    async def test_request_telemetry_timeout_is_clamped(self) -> None:
        self._install_nodes(_make_iface({}))
        await meshtastic_tools.handle_mesh_request_telemetry(
            {"node_id": "!aaaa1111", "timeout": 9999}
        )
        self.assertEqual(self.adapter.telemetry_calls[-1], ("!aaaa1111", 120.0))
        await meshtastic_tools.handle_mesh_request_telemetry(
            {"node_id": "!aaaa1111", "timeout": "abc"}
        )
        self.assertEqual(self.adapter.telemetry_calls[-1], ("!aaaa1111", 45.0))

    async def test_request_telemetry_answered_false(self) -> None:
        self._install_nodes(_make_iface({}))
        self.adapter.telemetry_result = {"ok": False, "error": "radio busy"}
        payload = json.loads(
            await meshtastic_tools.handle_mesh_request_telemetry({"node_id": "!aaaa1111"})
        )
        self.assertEqual(payload["answered"], False)
        self.assertEqual(payload["error"], "radio busy")

    async def test_request_telemetry_ambiguous_name_errors_without_transmitting(self) -> None:
        """Solicited requests go through requested_node, which must refuse an
        ambiguous name instead of transmitting to the first match."""
        shared1 = _make_node("!aaaa1111", "Shared Name", "SH1")
        shared2 = _make_node("!bbbb2222", "Shared Name", "SH2")
        self._install_nodes(_make_iface({"!aaaa1111": shared1, "!bbbb2222": shared2}))

        payload = json.loads(
            await meshtastic_tools.handle_mesh_request_telemetry({"node_id": "shared name"})
        )
        self.assertIn("matches multiple nodes", payload["error"])
        self.assertEqual(self.adapter.telemetry_calls, [])

    async def test_request_telemetry_reads_top_level_when_no_device_metrics(self) -> None:
        self._install_nodes(_make_iface({}))
        self.adapter.telemetry_result = {
            "ok": True,
            "data": {"uptimeSeconds": 0, "batteryLevel": 80},
        }
        payload = json.loads(
            await meshtastic_tools.handle_mesh_request_telemetry({"node_id": "!aaaa1111"})
        )
        self.assertEqual(payload["answered"], True)
        # uptimeSeconds == 0 survives first_not_none instead of being dropped.
        self.assertEqual(payload["uptime_seconds"], 0)
        self.assertEqual(payload["battery_level"], 80)

    async def test_request_position_passthrough_and_protobuf_scaling(self) -> None:
        self._install_nodes(_make_iface({}))
        self.adapter.position_result = {
            "ok": True,
            "data": {"latitude": 377100000, "longitude": -1224190000, "altitude": 10},
        }
        payload = json.loads(
            await meshtastic_tools.handle_mesh_request_position({"node_id": "!aaaa1111"})
        )
        self.assertEqual(payload["answered"], True)
        self.assertEqual(payload["latitude"], 37.71)
        self.assertEqual(payload["longitude"], -122.419)
        self.assertEqual(payload["altitude"], 10)
        self.assertEqual(self.adapter.position_calls, [("!aaaa1111", 45.0)])

        self.adapter.position_result = {
            "ok": True,
            "data": {"latitude": 37.77, "longitude": -122.41},
        }
        payload = json.loads(
            await meshtastic_tools.handle_mesh_request_position({"node_id": "!aaaa1111"})
        )
        self.assertEqual(payload["latitude"], 37.77)

    async def test_request_position_reads_real_hardware_camel_case_keys(self) -> None:
        self._install_nodes(_make_iface({}))
        self.adapter.position_result = {
            "ok": True,
            "data": {"latitudeI": 377100000, "longitudeI": -1224190000, "altitude": 10},
        }
        payload = json.loads(
            await meshtastic_tools.handle_mesh_request_position({"node_id": "!aaaa1111"})
        )
        self.assertEqual(payload["answered"], True)
        self.assertEqual(payload["latitude"], 37.71)
        self.assertEqual(payload["longitude"], -122.419)
        self.assertEqual(payload["altitude"], 10)

    async def test_request_position_answered_false(self) -> None:
        self._install_nodes(_make_iface({}))
        self.adapter.position_result = {"ok": False, "error": "timeout waiting"}
        payload = json.loads(
            await meshtastic_tools.handle_mesh_request_position({"node_id": "!aaaa1111"})
        )
        self.assertEqual(payload["answered"], False)
        self.assertEqual(payload["error"], "timeout waiting")

    async def test_request_position_unknown_node_errors(self) -> None:
        self._install_nodes(_make_iface({}))
        payload = json.loads(
            await meshtastic_tools.handle_mesh_request_position({"node_id": "nobody"})
        )
        self.assertIn("No node matched 'nobody'", payload["error"])
        self.assertEqual(self.adapter.position_calls, [])
        payload = json.loads(await meshtastic_tools.handle_mesh_request_position({}))
        self.assertIn("node_id is required", payload["error"])

    async def test_traceroute_formats_route_and_per_hop_snr(self) -> None:
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH")
        relay = _make_node("!0a1b2c3d", "Relay One", "RLY1")
        self._install_nodes(_make_iface({"!aaaa1111": node, "!0a1b2c3d": relay}))
        self.adapter.traceroute_result = {
            "ok": True,
            "data": {
                "route": [0x0A1B2C3D],
                "snrTowards": [80],
                "routeBack": [0x0A1B2C3D, 0x11111111],
                "snrBack": [-128, 40],
            },
        }

        payload = json.loads(
            await meshtastic_tools.handle_mesh_traceroute({"node_id": "!aaaa1111"})
        )
        self.assertEqual(payload["answered"], True)
        self.assertEqual(payload["name"], "Alpha Node")
        self.assertEqual(payload["hops_towards"], 1)
        self.assertEqual(
            payload["route_towards"], [{"name": "Relay One", "node_id": "!0a1b2c3d", "snr": 20.0}]
        )
        # Unknown relay falls back to its id; SNR is scaled back from x4.
        self.assertEqual(payload["route_back"][0]["name"], "Relay One")
        self.assertEqual(payload["route_back"][1]["name"], "!11111111")
        self.assertEqual(payload["snr_towards_db"], [20.0])
        self.assertEqual(payload["snr_back_db"], [None, 10.0])
        self.assertEqual(self.adapter.traceroute_calls, [("!aaaa1111", 5, 60.0)])

    async def test_traceroute_hop_limit_and_timeout_clamped(self) -> None:
        self._install_nodes(_make_iface({}))
        await meshtastic_tools.handle_mesh_traceroute(
            {"node_id": "!aaaa1111", "hop_limit": 9, "timeout": 9999}
        )
        self.assertEqual(self.adapter.traceroute_calls[-1], ("!aaaa1111", 7, 120.0))
        await meshtastic_tools.handle_mesh_traceroute({"node_id": "!aaaa1111", "hop_limit": "x"})
        self.assertEqual(self.adapter.traceroute_calls[-1], ("!aaaa1111", 5, 60.0))

    async def test_traceroute_answered_false(self) -> None:
        self._install_nodes(_make_iface({}))
        self.adapter.traceroute_result = {"ok": False, "error": "no route"}
        payload = json.loads(
            await meshtastic_tools.handle_mesh_traceroute({"node_id": "!aaaa1111"})
        )
        self.assertEqual(payload["answered"], False)
        self.assertEqual(payload["error"], "no route")

    async def test_traceroute_direct_0_hop_trace(self) -> None:
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH")
        self._install_nodes(_make_iface({"!aaaa1111": node}))
        self.adapter.traceroute_result = {
            "ok": True,
            "data": {"route": [], "snrTowards": [], "routeBack": [], "snrBack": []},
        }
        payload = json.loads(
            await meshtastic_tools.handle_mesh_traceroute({"node_id": "!aaaa1111"})
        )
        self.assertEqual(payload["answered"], True)
        self.assertEqual(payload["hops_towards"], 0)
        self.assertEqual(payload["route_towards"], [])
        self.assertEqual(payload["route_back"], [])
        self.assertEqual(payload["snr_towards_db"], [])
        self.assertEqual(payload["snr_back_db"], [])
        self.assertEqual(self.adapter.traceroute_calls, [("!aaaa1111", 5, 60.0)])

    async def test_traceroute_non_numeric_snr_filters_to_null_not_raises(self) -> None:
        """A hostile/malformed traceroute reply carrying non-numeric SNR entries
        (string, None, bool) must filter each to null rather than raising a
        TypeError on the division before the finite-type guard."""
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH")
        self._install_nodes(_make_iface({"!aaaa1111": node}))
        self.adapter.traceroute_result = {
            "ok": True,
            "data": {
                "route": [0x0A1B2C3D],
                "snrTowards": ["loud", None, True, -128, 80],
                "routeBack": [],
                "snrBack": [],
            },
        }

        payload = json.loads(
            await meshtastic_tools.handle_mesh_traceroute({"node_id": "!aaaa1111"})
        )
        self.assertEqual(payload["snr_towards_db"], [None, None, None, None, 20.0])
        # format_route on the matching hop also filters the non-numeric SNR
        # (no snr key for the string entry at index 0).
        self.assertNotIn("snr", payload["route_towards"][0])

    async def test_traceroute_rejects_malformed_target(self) -> None:
        self._install_nodes(_make_iface({}))
        payload = json.loads(await meshtastic_tools.handle_mesh_traceroute({"node_id": "!bogus"}))
        self.assertIn("not a valid node ID", payload["error"])
        self.assertEqual(self.adapter.traceroute_calls, [])

    # --- mesh_list_channels -------------------------------------------------

    async def test_list_channels_reports_local_node_channels(self) -> None:
        iface = _make_iface({})
        iface.localNode = SimpleNamespace(
            channels=[
                {"index": 0, "name": "Primary", "psk": "AES128"},
                {"index": 1, "name": "Ops", "psk": "AES128"},
            ]
        )
        self._install_nodes(iface)
        self.adapter.allow_channels = True

        payload = json.loads(await meshtastic_tools.handle_mesh_list_channels({}))
        self.assertEqual([ch["name"] for ch in payload["channels"]], ["Primary", "Ops"])
        self.assertEqual(payload["channels"][1]["role"], "SECONDARY")
        self.assertTrue(payload["channel_replies_enabled"])

    async def test_list_channels_skips_disabled_and_errors_when_empty(self) -> None:
        disabled = SimpleNamespace(index=1, role=0, settings=SimpleNamespace(name="x"))
        iface = _make_iface({})
        iface.localNode = SimpleNamespace(channels=[disabled])
        self._install_nodes(iface)

        payload = json.loads(await meshtastic_tools.handle_mesh_list_channels({}))
        self.assertIn("No channel configuration", payload["error"])

    async def test_list_channels_without_local_node(self) -> None:
        self._install_nodes(_make_iface({}))
        payload = json.loads(await meshtastic_tools.handle_mesh_list_channels({}))
        self.assertIn("error", payload)

    # --- mesh_pause / mesh_resume ------------------------------------------

    async def test_pause_passthrough_and_note(self) -> None:
        payload = json.loads(await meshtastic_tools.handle_mesh_pause({"minutes": 90}))
        self.assertEqual(self.adapter.pause_calls, [90.0])
        self.assertEqual(payload["paused"], True)
        # Assert the production pause_state() contract keys (resumes_at /
        # resumes_in_minutes), not a stub-invented "until" key.
        self.assertEqual(payload["resumes_at"], "2026-08-01 12:00:00")
        self.assertEqual(payload["resumes_in_minutes"], 90.0)
        self.assertIn("note", payload)

    async def test_pause_without_minutes_passes_none(self) -> None:
        payload = json.loads(await meshtastic_tools.handle_mesh_pause({}))
        self.assertEqual(self.adapter.pause_calls, [None])
        self.assertEqual(payload["paused"], True)

    async def test_pause_validation_and_clamp(self) -> None:
        # Non-numeric minutes is rejected (None is the untimed-pause path, covered
        # separately by test_pause_without_minutes_passes_none, and must NOT be
        # sent here as a non-numeric payload).
        payload = json.loads(await meshtastic_tools.handle_mesh_pause({"minutes": "abc"}))
        self.assertEqual(payload["error"], "Parameter 'minutes' must be a number.")
        for minutes in (0, -5):
            with self.subTest(minutes=minutes):
                payload = json.loads(await meshtastic_tools.handle_mesh_pause({"minutes": minutes}))
                self.assertEqual(payload["error"], "Parameter 'minutes' must be positive.")
        await meshtastic_tools.handle_mesh_pause({"minutes": 999999})
        self.assertEqual(self.adapter.pause_calls[-1], float(PAUSE_MAX_MINUTES))

    async def test_pause_rejects_non_finite_minutes_without_pausing(self) -> None:
        """NaN/Inf minutes must be rejected BEFORE pause_link: a nan deadline
        would never auto-resume (now >= nan is always False) and would crash
        pause_state's localtime()."""
        for minutes in ("nan", "inf", "-inf", float("nan")):
            with self.subTest(minutes=minutes):
                payload = json.loads(await meshtastic_tools.handle_mesh_pause({"minutes": minutes}))
                self.assertEqual(payload["error"], "Parameter 'minutes' must be a number.")
                self.assertEqual(self.adapter.pause_calls, [])

    async def test_resume_passthrough(self) -> None:
        payload = json.loads(await meshtastic_tools.handle_mesh_resume({}))
        self.assertEqual(self.adapter.resume_calls, 1)
        self.assertEqual(payload["paused"], False)
        self.assertIn("note", payload)


class TestMeshToolsNoAdapter(unittest.IsolatedAsyncioTestCase):
    """Every handler degrades to a JSON error when no adapter is wired.

    This pins the graceful-degradation contract the gateway relies on: tools
    must return ``{"error": ...}`` instead of raising when the singleton is
    cleared (e.g. the adapter never connected).
    """

    _CASES = [
        ("handle_mesh_list_channels", {}),
        ("handle_mesh_list_nodes", {}),
        ("handle_mesh_node_info", {"node_id": "!aaaa1111"}),
        ("handle_mesh_signal_quality", {"node_id": "!aaaa1111"}),
        ("handle_mesh_send_dm", {"node_id": "!aaaa1111", "message": "hi"}),
        ("handle_mesh_send_broadcast", {"message": "hi"}),
        ("handle_mesh_telemetry", {"node_id": "!aaaa1111"}),
        ("handle_mesh_telemetry_history", {"node_id": "!aaaa1111"}),
        ("handle_mesh_request_telemetry", {"node_id": "!aaaa1111"}),
        ("handle_mesh_request_position", {"node_id": "!aaaa1111"}),
        ("handle_mesh_traceroute", {"node_id": "!aaaa1111"}),
        ("handle_mesh_pause", {}),
        ("handle_mesh_resume", {}),
    ]

    def setUp(self) -> None:
        meshtastic_tools.set_adapter(None)

    def tearDown(self) -> None:
        meshtastic_tools.set_adapter(None)

    async def test_handlers_return_error_without_adapter(self) -> None:
        for name, args in self._CASES:
            with self.subTest(handler=name):
                handler = getattr(meshtastic_tools, name)
                payload = json.loads(await handler(args))
                self.assertIn("error", payload)
                self.assertIn("not connected", payload["error"])

    async def test_signal_quality_no_adapter_error_text(self) -> None:
        payload = json.loads(
            await meshtastic_tools.handle_mesh_signal_quality({"node_id": "!aaaa1111"})
        )
        self.assertEqual(payload["error"], "Meshtastic platform adapter is not connected.")


class TestSchemaContracts(unittest.TestCase):
    """The 12 tool schemas are a machine-readable contract (C11).

    Pins the HIGH finding's fix: the schemas were pre-wrapped in the OpenAI
    ``{"type": "function", "function": {...}}`` envelope, which Hermes'
    registry re-wraps at definition time — the model saw tools with no
    description and no parameters, and ``model_tools.coerce_tool_args`` (which
    reads ``schema["parameters"]`` off the raw registered schema) never ran.
    They are now defined in the inner form the registry expects, matching the
    spotify plugin convention.
    """

    SCHEMA_CONSTANTS = [
        "MESH_LIST_CHANNELS_SCHEMA",
        "MESH_LIST_NODES_SCHEMA",
        "MESH_NODE_INFO_SCHEMA",
        "MESH_SIGNAL_QUALITY_SCHEMA",
        "MESH_SEND_DM_SCHEMA",
        "MESH_SEND_BROADCAST_SCHEMA",
        "MESH_TELEMETRY_SCHEMA",
        "MESH_TELEMETRY_HISTORY_SCHEMA",
        "MESH_REQUEST_TELEMETRY_SCHEMA",
        "MESH_REQUEST_POSITION_SCHEMA",
        "MESH_TRACEROUTE_SCHEMA",
        "MESH_PAUSE_SCHEMA",
        "MESH_RESUME_SCHEMA",
    ]

    def _schemas(self) -> list[tuple[str, dict]]:
        return [(const, getattr(meshtastic_tools, const)) for const in self.SCHEMA_CONSTANTS]

    def _register_all(self) -> None:
        for _, schema in self._schemas():
            registry.register(
                name=schema["name"],
                toolset="meshtastic",
                schema=schema,
                handler=lambda args, **kw: "{}",
                is_async=True,
            )

    def test_schemas_use_inner_form_not_the_openai_envelope(self) -> None:
        for const, schema in self._schemas():
            with self.subTest(schema=const):
                self.assertNotIn("type", schema, "outer 'type': 'function' envelope leaked")
                self.assertNotIn("function", schema, "nested 'function' wrapper leaked")
                self.assertIn("name", schema)
                self.assertTrue(schema["description"].strip())
                params = schema["parameters"]
                self.assertEqual(params["type"], "object")
                self.assertIsInstance(params["properties"], dict)
                self.assertFalse(params["additionalProperties"])

    def test_schema_names_match_handler_names(self) -> None:
        for const, schema in self._schemas():
            with self.subTest(schema=const):
                handler_name = "handle_" + schema["name"]
                handler = getattr(meshtastic_tools, handler_name, None)
                self.assertTrue(callable(handler), f"{const} has no {handler_name} handler")

    def test_required_is_subset_of_properties(self) -> None:
        for const, schema in self._schemas():
            with self.subTest(schema=const):
                props = schema["parameters"].get("properties", {})
                for req in schema["parameters"].get("required", []):
                    self.assertIn(req, props, f"{const} requires undeclared property {req}")

    def test_numeric_properties_declare_handler_bounds(self) -> None:
        """Schema min/max/default must match the clamps the handlers enforce
        (history caps, request-timeout clamps, hop limit, pause cap) so the
        description, the machine-readable bounds, and the code can't drift."""

        def prop(const: str, name: str) -> dict:
            return getattr(meshtastic_tools, const)["parameters"]["properties"][name]

        since_hours = prop("MESH_TELEMETRY_HISTORY_SCHEMA", "since_hours")
        self.assertGreater(since_hours["minimum"], 0)  # handler rejects <= 0
        self.assertEqual(since_hours["maximum"], mesh_helpers.HISTORY_MAX_WINDOW_HOURS)

        limit = prop("MESH_TELEMETRY_HISTORY_SCHEMA", "limit")
        self.assertEqual(limit["minimum"], 1)
        self.assertEqual(limit["maximum"], mesh_helpers.HISTORY_WINDOW_ROW_CAP)
        self.assertEqual(limit["default"], 10)

        for const, default in (
            ("MESH_REQUEST_TELEMETRY_SCHEMA", 45),
            ("MESH_REQUEST_POSITION_SCHEMA", 45),
            ("MESH_TRACEROUTE_SCHEMA", 60),
        ):
            timeout = prop(const, "timeout")
            self.assertEqual((timeout["minimum"], timeout["maximum"]), (5, 120))
            self.assertEqual(timeout["default"], default)

        hop = prop("MESH_TRACEROUTE_SCHEMA", "hop_limit")
        self.assertEqual((hop["minimum"], hop["maximum"], hop["default"]), (1, 7, 5))

        minutes = prop("MESH_PAUSE_SCHEMA", "minutes")
        self.assertGreater(minutes["minimum"], 0)  # handler rejects <= 0
        self.assertEqual(minutes["maximum"], PAUSE_MAX_MINUTES)

    def test_registry_round_trip_keeps_description_and_parameters(self) -> None:
        """The double-wrap bug meant get_definitions exposed tools with a
        missing description and empty parameters. After the format fix every
        provider-facing definition carries both, with no nested wrapper."""
        self._register_all()
        defs = {
            d["function"]["name"]: d["function"]
            for d in registry.get_definitions({schema["name"] for _, schema in self._schemas()})
        }
        self.assertEqual(len(defs), len(self.SCHEMA_CONSTANTS))
        for const, schema in self._schemas():
            with self.subTest(schema=const):
                fn = defs[schema["name"]]
                self.assertNotIn("function", fn, "registry double-wrapped the schema")
                self.assertTrue(fn["description"], "model-facing description lost")
                self.assertEqual(
                    fn["parameters"].get("properties", {}),
                    schema["parameters"]["properties"],
                    "model-facing parameters lost",
                )

    def test_coerce_tool_args_coerces_numeric_strings(self) -> None:
        """coerce_tool_args reads schema['parameters'] off the raw registered
        schema; under the old envelope that key was absent, so the standard
        string->number coercion for timeout/limit/since_hours never ran."""
        self._register_all()
        model_tools = importlib.import_module("model_tools")
        args = model_tools.coerce_tool_args("mesh_request_telemetry", {"timeout": "45"})
        self.assertIsInstance(args["timeout"], (int, float))
        self.assertEqual(args["timeout"], 45)
        self.assertIsInstance(
            model_tools.coerce_tool_args("mesh_telemetry_history", {"since_hours": "72"}),
            dict,
        )

    def test_schema_bounds_are_advisory_handler_clamps_authoritative(self) -> None:
        """Document the framework-doesn't-validate invariant (C11 coverage gap).

        ``model_tools.coerce_tool_args`` only coerces types; it never enforces
        ``minimum``/``maximum``/``enum``/``required``/``additionalProperties``.
        So every numeric bound in these schemas is an *advisory* model-prompt
        contract — the handlers re-clamp independently (``mesh_helpers.clamp``,
        ``history_window_params``, ``requested_node``). This test pins that
        invariant so a future contributor does not assume ``maximum: 120``
        rejects ``999`` at the framework layer, and does not remove a handler
        clamp believing the schema will catch the out-of-range value.
        """
        # Schemas DO carry advisory bounds (sanity-check the premise).
        timeout = meshtastic_tools.MESH_REQUEST_TELEMETRY_SCHEMA["parameters"]["properties"][
            "timeout"
        ]
        self.assertEqual(timeout["maximum"], 120)

        # coerce_tool_args must NOT reject an out-of-range value — it only
        # coerces types. If this ever starts raising/clamping, the handler
        # clamps would become redundant and the advisory contract would become
        # an enforced one (a behaviour change that must be a conscious choice).
        self._register_all()
        model_tools = importlib.import_module("model_tools")
        out_of_range = model_tools.coerce_tool_args("mesh_request_telemetry", {"timeout": 99999})
        self.assertEqual(out_of_range["timeout"], 99999, "framework must not clamp")
        # Unknown keys are passed through (additionalProperties is advisory too).
        with_extra = model_tools.coerce_tool_args(
            "mesh_request_telemetry", {"node_id": "!da1b1613", "rogue": 1}
        )
        self.assertIn("rogue", with_extra, "framework must not strip unknown keys")


class TestInitShim(unittest.TestCase):
    """The real plugin entry (__init__.py ``register``) is CI-executed here.

    The repo dir is hyphenated (``hermes-meshtastic-adapter``), an invalid
    package name, so ``__init__.py`` is excluded from pyrefly's project-includes
    and the flat test layout never imports it. This loads it the way the Hermes
    plugin loader does (``hermes_plugins.meshtastic``) and runs ``register``
    against a fake ctx — the 13-schema / 13-handler import list is resolved
    against the real ``mesh_tools`` module, so a renamed handler/schema that
    diverges from the shim fails the suite instead of silently registering
    nothing at plugin-load time (the loader wraps the failure).
    """

    def test_register_forwards_platform_and_all_13_tools(self) -> None:
        import types

        plugin_dir = os.path.dirname(os.path.abspath(__file__))
        if "hermes_plugins" not in sys.modules:
            ns_pkg = types.ModuleType("hermes_plugins")
            ns_pkg.__path__ = []
            ns_pkg.__package__ = "hermes_plugins"
            sys.modules["hermes_plugins"] = ns_pkg

        # Mirror _deps/hermes-agent/hermes_cli/plugins.py:_load_directory_module
        # exactly: the shim is its own package, so its relative imports resolve
        # to FRESH copies of the sibling modules loaded from this repo dir — a
        # stale import in the shim's mesh_tools list then fails exec_module.
        module_name = "hermes_plugins.meshtastic"
        spec = importlib.util.spec_from_file_location(
            module_name,
            os.path.join(plugin_dir, "__init__.py"),
            submodule_search_locations=[plugin_dir],
        )
        shim = importlib.util.module_from_spec(spec)
        shim.__package__ = module_name
        shim.__path__ = [plugin_dir]
        sys.modules[module_name] = shim
        spec.loader.exec_module(shim)
        tools_module = sys.modules[f"{module_name}.mesh_tools"]

        platform_kwargs: dict = {}
        tools: list[dict] = []

        class FakeCtx:
            def register_platform(self, **kwargs: object) -> None:
                platform_kwargs.update(kwargs)

            def register_tool(self, **kwargs: object) -> None:
                tools.append(kwargs)

        shim.register(FakeCtx())

        self.assertEqual(platform_kwargs["name"], "meshtastic")
        self.assertEqual(platform_kwargs["max_message_length"], 233)
        names = [tool["name"] for tool in tools]
        self.assertEqual(len(names), 13)
        self.assertEqual(len(set(names)), 13)
        for tool in tools:
            with self.subTest(tool=tool["name"]):
                self.assertEqual(tool["toolset"], "meshtastic")
                self.assertIs(tool["handler"], getattr(tools_module, "handle_" + tool["name"]))
                schema = getattr(tools_module, tool["name"].upper() + "_SCHEMA")
                self.assertIs(tool["schema"], schema)
                self.assertEqual(schema["name"], tool["name"])
                self.assertNotIn("type", schema, "outer 'type': 'function' envelope leaked")
                self.assertNotIn("function", schema, "nested 'function' wrapper leaked")


class StubNormalizeDriftCheck(unittest.TestCase):
    """Guard against the ``_StubAdapter._normalize_node_id`` mirror drifting
    from production. The mesh_tools handler tests deliberately stub the adapter
    rather than assembling it, so this cross-check runs only when the adapter is
    importable and fails loudly if the two canonicalize differently."""

    def test_stub_normalize_matches_production(self) -> None:
        try:
            from adapter import MeshtasticAdapter
        except Exception:  # pragma: no cover - adapter not importable in this env
            self.skipTest("adapter not importable; cross-check skipped")
        matrix = [
            None,
            True,
            False,
            0,
            0xAB12CD34,
            -1,
            2**40,  # out-of-range -> None
            "!AB12CD34",
            "ab12cd34",
            "!zzzzzzzz",  # non-hex
            "nobody",
            "  !ab12cd34  ",
            "",
            "!abcd",  # too short
        ]
        for value in matrix:
            with self.subTest(value=value):
                self.assertEqual(
                    _StubAdapter._normalize_node_id(value),
                    MeshtasticAdapter._normalize_node_id(value),
                )


if __name__ == "__main__":
    unittest.main()
