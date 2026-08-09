"""Unit tests for the pure helper functions in mesh_helpers.py.

The helpers were extracted from mesh_tools.py (P3.4) so they can be exercised
without a stub adapter or a live Meshtastic connection: telemetry_db is patched
at the module level (mesh_helpers holds a module reference to it, so patching
its attributes is all that is needed — no real sqlite file is ever created).
Handler-level behavior stays pinned in test_mesh_tools.py / test_meshtastic.py,
which import these helpers back through mesh_tools.
"""

import time
import unittest
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import mesh_helpers


def _make_iface(nodes: dict) -> SimpleNamespace:
    """Stub library interface carrying a nodes dict like the real SerialInterface."""
    return SimpleNamespace(nodes=nodes)


def _make_node(
    nid: str,
    long_name: str,
    short_name: str,
    *,
    user_extra: dict | None = None,
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
        "position": {},
        "lastHeard": 0,
    }
    node.update(fields)
    return node


class _StubAdapter:
    """The slice of MeshtasticAdapter that resolve_node / node_display_name use."""

    def __init__(self) -> None:
        self.interfaces: list[Any] = []

    def get_interfaces(self) -> list[Any]:
        return self.interfaces


class TestResolveNode(unittest.TestCase):
    """resolve_node is the shared node lookup under node_info / send_dm / the
    solicited requests; pin the matching rules here (moved from
    test_mesh_tools.py with the helper)."""

    def setUp(self) -> None:
        self.adapter = _StubAdapter()
        self.adapter.interfaces = [
            _make_iface(
                {
                    "!aaaa1111": _make_node("!aaaa1111", "Alpha Node", "ALPH"),
                    "!bbbb2222": _make_node(
                        "!bbbb2222", "Beta Node", "BETA", user_extra={"publicKey": "0x1"}
                    ),
                }
            )
        ]

    def test_resolve_by_id_with_and_without_bang(self) -> None:
        iface, info = mesh_helpers.resolve_node("!aaaa1111", self.adapter)
        self.assertEqual(info["user"]["id"], "!aaaa1111")
        _, info = mesh_helpers.resolve_node("aaaa1111", self.adapter)
        self.assertEqual(info["user"]["id"], "!aaaa1111")

    def test_resolve_by_name_and_num(self) -> None:
        _, info = mesh_helpers.resolve_node("beta node", self.adapter)
        self.assertEqual(info["user"]["id"], "!bbbb2222")
        _, info = mesh_helpers.resolve_node("ALPH", self.adapter)
        self.assertEqual(info["user"]["id"], "!aaaa1111")
        _, info = mesh_helpers.resolve_node(str(0xAAAA1111), self.adapter)
        self.assertEqual(info["user"]["id"], "!aaaa1111")

    def test_resolve_miss_returns_none(self) -> None:
        iface, info = mesh_helpers.resolve_node("!zzzz9999", self.adapter)
        self.assertIsNone(iface)
        self.assertIsNone(info)
        iface, info = mesh_helpers.resolve_node("", self.adapter)
        self.assertIsNone(iface)
        self.assertIsNone(info)

    def test_resolve_coerces_non_string_queries(self) -> None:
        """A numeric/bool node_id from the model must degrade to a clean miss,
        not an AttributeError on .strip()."""
        for query in (123, True, 3.14):
            with self.subTest(query=query):
                iface, info = mesh_helpers.resolve_node(query, self.adapter)
                self.assertIsNone(iface)
                self.assertIsNone(info)

    def test_resolve_ambiguous_name_refuses_to_pick(self) -> None:
        """A name shared by two nodes must not silently resolve to whoever
        appeared first: a hostile node can squat a trusted node's longName,
        so the only safe answer is a miss the callers surface as ambiguous."""
        self.adapter.interfaces = [
            _make_iface(
                {
                    "!aaaa1111": _make_node("!aaaa1111", "Shared Name", "SH1"),
                    "!bbbb2222": _make_node("!bbbb2222", "Shared Name", "SH2"),
                }
            )
        ]
        iface, info = mesh_helpers.resolve_node("shared name", self.adapter)
        self.assertIsNone(iface)
        self.assertIsNone(info)
        # By id the same two nodes still resolve unambiguously.
        _, info = mesh_helpers.resolve_node("!aaaa1111", self.adapter)
        self.assertEqual(info["user"]["id"], "!aaaa1111")

    def test_resolve_same_node_on_two_interfaces_is_not_a_collision(self) -> None:
        """The same node seen on two interfaces (keyless/keyed copies) counts
        once — a name lookup must not look ambiguous because of duplicates."""
        self.adapter.interfaces = [
            _make_iface({"!aaaa1111": _make_node("!aaaa1111", "Alpha Node", "ALPH")}),
            _make_iface({"!aaaa1111": _make_node("!aaaa1111", "Alpha Node", "ALPH")}),
        ]
        _, info = mesh_helpers.resolve_node("alpha node", self.adapter)
        self.assertEqual(info["user"]["id"], "!aaaa1111")

    def test_resolve_survives_int_keyed_nodes(self) -> None:
        """One stray int node key (as mesh_list_nodes iterations can see) must
        not crash every resolve_node-based handler with an AttributeError."""
        self.adapter.interfaces = [
            _make_iface(
                {
                    0xAAAA1111: _make_node("!aaaa1111", "Alpha Node", "ALPH"),
                    "!bbbb2222": _make_node("!bbbb2222", "Beta Node", "BETA"),
                }
            )
        ]
        _, info = mesh_helpers.resolve_node("!aaaa1111", self.adapter)
        self.assertEqual(info["user"]["id"], "!aaaa1111")
        _, info = mesh_helpers.resolve_node("aaaa1111", self.adapter)
        self.assertEqual(info["user"]["id"], "!aaaa1111")
        _, info = mesh_helpers.resolve_node("beta node", self.adapter)
        self.assertEqual(info["user"]["id"], "!bbbb2222")

    def test_resolve_bang_prefixed_name(self) -> None:
        """requested_node must still match a name when the query is '!'-prefixed:
        the id search strips the bang, so the name search must too."""
        _, info = mesh_helpers.resolve_node("!alpha node", self.adapter)
        self.assertEqual(info["user"]["id"], "!aaaa1111")

    def test_resolve_node_iterates_snapshot_of_live_nodes(self) -> None:
        """resolve_node / _name_matches iterate a snapshot of iface.nodes: the
        meshtastic library mutates that dict on its background receive thread,
        so a live view can raise 'dictionary changed size during iteration'
        between two steps. A value whose .get() mutates the parent dict
        reproduces that race deterministically — without the list() snapshot the
        view's next __next__ raises; with it the lookup is race-free."""

        nodes: dict[str, dict] = {}

        class _RacyInfo(dict):
            def get(self, key, default=None):
                if "!cc3333" not in nodes:
                    nodes["!cc3333"] = {"user": {"id": "!cc3333", "longName": "Inflight"}}
                return super().get(key, default)

        nodes["!aaaa1111"] = _RacyInfo(_make_node("!aaaa1111", "Alpha Node", "ALPH"))
        adapter = _StubAdapter()
        adapter.interfaces = [_make_iface(nodes)]
        # Resolve by name so _name_matches runs (its body calls info.get('user')).
        _, info = mesh_helpers.resolve_node("alpha node", adapter)
        self.assertEqual(info["user"]["id"], "!aaaa1111")


class TestMeshHelpersPure(unittest.TestCase):
    """Tiny pure helpers — pinned here so the threshold constants can't drift
    without a test noticing (moved from test_mesh_tools.py with the helpers)."""

    def test_assess_signal_quality_thresholds(self) -> None:
        cases = [
            (None, "Unknown"),
            (8.0, "Excellent"),
            (7.9, "Good"),
            (3.0, "Good"),
            (2.9, "Fair"),
            (-3.0, "Fair"),
            (-3.1, "Poor"),
            (-12.0, "Poor"),
            (-12.1, "No signal"),
        ]
        for snr, expected in cases:
            with self.subTest(snr=snr):
                self.assertEqual(mesh_helpers.assess_signal_quality(snr), expected)

    def test_assess_signal_quality_rejects_non_numeric(self) -> None:
        """A string / bool / NaN snr must degrade to 'Unknown', never a
        TypeError from comparing str against a float."""
        for bad in ("3", "loud", True, False, float("nan"), float("inf"), None):
            with self.subTest(bad=bad):
                self.assertEqual(mesh_helpers.assess_signal_quality(bad), "Unknown")

    def test_first_not_none_keeps_zero(self) -> None:
        self.assertIsNone(mesh_helpers.first_not_none(None, None))
        self.assertEqual(mesh_helpers.first_not_none(None, 0), 0)
        self.assertEqual(mesh_helpers.first_not_none(None, "", 1), "")

    def test_device_uptime_prefers_uptime_seconds(self) -> None:
        self.assertIsNone(mesh_helpers.device_uptime({}))
        self.assertEqual(mesh_helpers.device_uptime({"uptime": 5}), 5)
        self.assertEqual(mesh_helpers.device_uptime({"uptimeSeconds": 10, "uptime": 5}), 10)

    def test_clamp_coerces_and_bounds(self) -> None:
        self.assertEqual(mesh_helpers.clamp(30, 45.0, 5.0, 120.0), 30.0)
        self.assertEqual(mesh_helpers.clamp(9999, 45.0, 5.0, 120.0), 120.0)
        self.assertEqual(mesh_helpers.clamp(1, 45.0, 5.0, 120.0), 5.0)
        self.assertEqual(mesh_helpers.clamp("abc", 45.0, 5.0, 120.0), 45.0)

    def test_clamp_non_finite_falls_back_to_default(self) -> None:
        """NaN/±inf must not silently normalize to the high bound: min(high, nan)
        compares False and returns high, which would turn timeout: nan into the
        max wait."""
        for bad in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(bad=bad):
                self.assertEqual(mesh_helpers.clamp(bad, 45.0, 5.0, 120.0), 45.0)

    def test_signal_source_for_hops_bands(self) -> None:
        self.assertEqual(mesh_helpers.signal_source_for_hops(0), "direct")
        self.assertEqual(mesh_helpers.signal_source_for_hops(2), "relayed")
        self.assertEqual(mesh_helpers.signal_source_for_hops(None), "unknown")


class TestPositionAge(unittest.TestCase):
    """position_age dates a fix so an old one can't pass for current (moved from
    test_mesh_tools.py with the helper)."""

    def test_position_age_flags_stale_fixes(self) -> None:
        now = time.time()
        fresh = mesh_helpers.position_age({"time": now - 600}, "!aaaa1111")
        self.assertFalse(fresh["position_is_stale"])
        self.assertAlmostEqual(fresh["position_age_hours"], 0.2, places=1)
        stale = mesh_helpers.position_age({"time": now - 7 * 3600}, "!aaaa1111")
        self.assertTrue(stale["position_is_stale"])
        with patch.object(mesh_helpers.telemetry_db, "get_position_history", return_value=[]):
            no_time = mesh_helpers.position_age({}, "!aaaa1111")
        self.assertIsNone(no_time["position_time"])
        self.assertIsNone(no_time["position_is_stale"])

    def test_position_age_falls_back_to_db_history(self) -> None:
        now = time.time()
        with patch.object(
            mesh_helpers.telemetry_db,
            "get_position_history",
            return_value=[{"timestamp": now - 300}],
        ):
            age = mesh_helpers.position_age({}, "!aaaa1111")
        self.assertAlmostEqual(age["position_age_hours"], 300 / 3600, places=1)
        self.assertFalse(age["position_is_stale"])

    def test_position_age_prefers_position_time_over_timestamp(self) -> None:
        now = time.time()
        age = mesh_helpers.position_age({"time": now - 600, "timestamp": now - 60}, "!aaaa1111")
        self.assertAlmostEqual(age["position_age_hours"], 0.2, places=1)

    def test_position_age_fast_path_makes_no_db_call(self) -> None:
        """The timestamped path must not hit telemetry_db: position_age is a
        documented-I/O helper, and the caller off-loads it, so the fast path
        must stay pure (no blocking read on the event loop)."""
        now = time.time()
        with patch.object(mesh_helpers.telemetry_db, "get_position_history") as getter:
            age = mesh_helpers.position_age({"time": now - 600}, "!aaaa1111")
        getter.assert_not_called()
        self.assertFalse(age["position_is_stale"])

    def test_position_age_empty_pos_no_db_returns_nulls(self) -> None:
        with patch.object(mesh_helpers.telemetry_db, "get_position_history", return_value=[]):
            age = mesh_helpers.position_age(None, "")
        self.assertEqual(
            age, {"position_time": None, "position_age_hours": None, "position_is_stale": None}
        )

    def test_position_age_non_numeric_time_does_not_crash(self) -> None:
        """A truthy non-numeric `time` (a hostile/malformed Position payload)
        survives the truthiness gate and must not crash float() — position_age
        applies the same finite filter numeric_epoch gives lastHeard and returns
        the nulls dict. The DB fallback is patched to [] so the fast path is the
        one under test."""
        with patch.object(mesh_helpers.telemetry_db, "get_position_history", return_value=[]):
            age = mesh_helpers.position_age({"time": "not-a-number"}, "!aaaa1111")
        self.assertEqual(
            age, {"position_time": None, "position_age_hours": None, "position_is_stale": None}
        )


class TestLinkFacts(unittest.TestCase):
    """link_facts is the single place that answers "how far is this node, and
    does this reading describe it". Every node-reporting tool goes through it."""

    def test_live_observation_asserts_direct(self) -> None:
        # Live observations are recorded off 0-hop packets only; they win over
        # every other source, and assert direct range on their own.
        now = time.time()
        info = {"snr": 3.0, "rssi": -90, "hopsAway": 2}
        obs = {"snr": 9.5, "rssi": -65, "hops_away": 0, "last_heard": now - 60}
        latest = {"snr": 1.0, "rssi": -95, "hop_count": 3}
        link = mesh_helpers.link_facts(info, obs, latest)
        self.assertEqual(link["snr"], 9.5)
        self.assertEqual(link["signal_source"], "direct")
        self.assertEqual(link["hops_away"], 0)
        self.assertTrue(link["heard_directly"])

    def test_latest_direct_used_when_obs_empty(self) -> None:
        # A persisted 0-hop row counts while inside the direct-range window.
        now = time.time()
        latest_direct = {"snr": 5.0, "rssi": -70, "timestamp": now - 3600, "hop_count": 0}
        link = mesh_helpers.link_facts({}, {}, None, latest_direct)
        self.assertEqual(link["signal_source"], "direct")
        self.assertEqual(link["snr"], 5.0)
        self.assertTrue(link["heard_directly"])
        self.assertIsNotNone(link["last_direct_heard"])

    def test_node_db_reading_attributed_by_db_hops(self) -> None:
        link = mesh_helpers.link_facts({"snr": 5.0, "rssi": -80, "hopsAway": 2}, {})
        self.assertEqual(link["snr"], 5.0)
        self.assertEqual(link["signal_source"], "relayed")
        self.assertEqual(link["hops_away"], 2)
        self.assertFalse(link["heard_directly"])

        direct = mesh_helpers.link_facts({"snr": 5.0, "rssi": -80, "hopsAway": 0}, {})
        self.assertEqual(direct["signal_source"], "direct")
        self.assertTrue(direct["heard_directly"])

    def test_history_supplies_signal_and_its_own_provenance(self) -> None:
        now = time.time()
        latest = {"snr": 4.0, "rssi": -75, "hop_count": 1, "timestamp": now - 7200}
        link = mesh_helpers.link_facts({}, {}, latest)
        self.assertEqual(link["snr"], 4.0)
        self.assertEqual(link["signal_source"], "relayed")
        self.assertEqual(link["hops_away"], 1)

    def test_history_signal_without_hops_is_unknown(self) -> None:
        now = time.time()
        latest = {"snr": 4.0, "rssi": -75, "timestamp": now - 7200}
        link = mesh_helpers.link_facts({}, {}, latest)
        self.assertEqual(link["snr"], 4.0)
        self.assertEqual(link["signal_source"], "unknown")

    def test_no_readings_anywhere_is_unknown(self) -> None:
        link = mesh_helpers.link_facts({}, {})
        self.assertIsNone(link["snr"])
        self.assertIsNone(link["rssi"])
        self.assertEqual(link["signal_source"], "unknown")
        self.assertFalse(link["heard_directly"])

    # --- A3: the node DB and persisted history can disagree on provenance ---

    def test_a3_history_signal_not_attributed_to_db_hops(self) -> None:
        # obs empty, hopsAway present (2, node DB) but NO DB signal; the signal
        # comes from a persisted DIRECT row. The old code labelled it "relayed"
        # against the DB's hops — attribution must follow the row that supplied
        # the reading (AUG-TODO A3).
        now = time.time()
        info = {"hopsAway": 2}
        latest = {"snr": 5.0, "rssi": -70, "hop_count": 0, "timestamp": now - 1800}
        link = mesh_helpers.link_facts(info, {}, latest)
        self.assertEqual(link["snr"], 5.0)
        self.assertEqual(link["signal_source"], "direct")
        # hops_away stays the DB's latest distance (that is its contract).
        self.assertEqual(link["hops_away"], 2)
        # A persisted row cannot assert direct range on its own; there is no
        # latest_direct here (the latest row is the 2-hop one).
        self.assertFalse(link["heard_directly"])

    def test_a3_db_signal_not_attributed_to_history_hops(self) -> None:
        # obs empty, the DB supplies the signal but has NO hopsAway; the
        # persisted row claims relayed. The old code borrowed the history
        # row's hops and called the DB reading "relayed" — attribution is
        # "unknown" when the supplying row carries no hops of its own.
        info = {"snr": 6.0, "rssi": -60}
        latest = {"snr": 9.0, "rssi": -50, "hop_count": 1}
        link = mesh_helpers.link_facts(info, {}, latest)
        self.assertEqual(link["snr"], 6.0)
        self.assertEqual(link["signal_source"], "unknown")

    def test_a3_both_sources_agree_direct(self) -> None:
        now = time.time()
        info = {"snr": 6.0, "rssi": -60, "hopsAway": 0}
        latest = {"snr": 5.0, "rssi": -70, "hop_count": 0, "timestamp": now - 1800}
        link = mesh_helpers.link_facts(info, {}, latest)
        self.assertEqual(link["signal_source"], "direct")
        self.assertEqual(link["hops_away"], 0)
        self.assertTrue(link["heard_directly"])

    def test_a3_both_sources_disagree_db_signal(self) -> None:
        # DB reading with its own hopsAway=2: relayed by the DB row's own hops,
        # even though the persisted row was direct.
        now = time.time()
        info = {"snr": 6.0, "rssi": -60, "hopsAway": 2}
        latest = {"snr": 5.0, "rssi": -70, "hop_count": 0, "timestamp": now - 1800}
        link = mesh_helpers.link_facts(info, {}, latest)
        self.assertEqual(link["signal_source"], "relayed")
        self.assertEqual(link["hops_away"], 2)

    def test_a3_history_row_without_signal_claims_nothing(self) -> None:
        # A persisted row that carries only a hop count supplies no reading, so
        # no provenance is claimed (source stays unknown) — the reverse mix.
        latest = {"snr": None, "rssi": None, "hop_count": 0}
        link = mesh_helpers.link_facts({}, {}, latest)
        self.assertIsNone(link["snr"])
        self.assertEqual(link["signal_source"], "unknown")

    # --- direct-range expiry window ---

    def test_direct_range_expiry_window(self) -> None:
        now = time.time()
        recent = {"snr": 5.0, "rssi": -70, "timestamp": now - 3600}
        link = mesh_helpers.link_facts({}, {}, None, recent)
        self.assertTrue(link["heard_directly"])
        self.assertEqual(link["last_direct_heard_age_hours"], 1.0)

        old = {"snr": 5.0, "rssi": -70, "timestamp": now - 25 * 3600}
        link = mesh_helpers.link_facts({}, {}, None, old)
        self.assertFalse(link["heard_directly"])
        self.assertEqual(link["last_direct_heard_age_hours"], 25.0)

    def test_live_zero_hop_asserts_range_without_history(self) -> None:
        # A *current* 0-hop reading needs no window — both describe now.
        link = mesh_helpers.link_facts({"hopsAway": 0}, {}, None, None)
        self.assertTrue(link["heard_directly"])
        self.assertIsNone(link["last_direct_heard"])

    # --- signal rendered through the finite filter at the boundary ---

    def test_non_finite_signal_drops_field_not_nan(self) -> None:
        """A NaN snr from the overlay (representable in the protobuf float
        field) must render as None, never a literal NaN, and a string must not
        crash assess_signal_quality."""
        now = time.time()
        link = mesh_helpers.link_facts(
            {}, {"snr": float("nan"), "rssi": -70, "last_heard": now - 60}
        )
        self.assertIsNone(link["snr"])
        self.assertEqual(link["rssi"], -70.0)
        self.assertEqual(mesh_helpers.assess_signal_quality(link["snr"]), "Unknown")

        stringy = mesh_helpers.link_facts({}, {"snr": "loud", "rssi": -70, "last_heard": now - 60})
        self.assertIsNone(stringy["snr"])
        self.assertEqual(stringy["rssi"], -70.0)

        db_nan = mesh_helpers.link_facts({"snr": float("nan"), "rssi": -80}, {})
        self.assertIsNone(db_nan["snr"])
        self.assertEqual(db_nan["rssi"], -80.0)

    # --- in-session obs assertions expire like the persisted ones ---

    def test_stale_in_session_observation_does_not_assert_direct(self) -> None:
        """The overlay keeps values for the whole session with no expiry of its
        own, so a direct observation (or 0-hop hops_away) recorded >24h ago per
        its last_heard must not keep reporting 'in direct range'."""
        now = time.time()
        old = {"snr": 9.5, "rssi": -65, "hops_away": 0, "last_heard": now - 25 * 3600}
        link = mesh_helpers.link_facts({}, old)
        self.assertFalse(link["heard_directly"])

        fresh = {"snr": 9.5, "rssi": -65, "hops_away": 0, "last_heard": now - 60}
        link = mesh_helpers.link_facts({}, fresh)
        self.assertTrue(link["heard_directly"])

        # hops-only obs, no signal: still expires by last_heard.
        old_hops = {"hops_away": 0, "last_heard": now - 25 * 3600}
        self.assertFalse(mesh_helpers.link_facts({}, old_hops)["heard_directly"])

        # The library node DB's hopsAway asserts without a window (it keeps it
        # current) — but a stale overlay hops_away shadows it, so only the
        # obs-free path asserts.
        self.assertTrue(mesh_helpers.link_facts({"hopsAway": 0}, {})["heard_directly"])
        self.assertFalse(mesh_helpers.link_facts({"hopsAway": 0}, old_hops)["heard_directly"])

    def test_obs_is_recent_tolerates_unparseable_last_heard(self) -> None:
        """A garbage (non-numeric) last_heard must not crash the expiry window;
        the defensive fallback treats the entry as current."""
        now = time.time()
        self.assertTrue(mesh_helpers._obs_is_recent({"last_heard": "garbage"}, now))
        # And the same entry still asserts direct range through link_facts.
        link = mesh_helpers.link_facts(
            {}, {"snr": 9.5, "rssi": -65, "hops_away": 0, "last_heard": "garbage"}
        )
        self.assertTrue(link["heard_directly"])


class TestNodeDisplayNameAndRoute(unittest.TestCase):
    """node_display_name / format_route pair route hops with readable names."""

    def setUp(self) -> None:
        self.adapter = _StubAdapter()
        self.adapter.interfaces = [
            _make_iface({"!0a1b2c3d": _make_node("!0a1b2c3d", "Relay One", "RLY1")})
        ]

    def test_node_display_name_prefers_long_name(self) -> None:
        self.assertEqual(mesh_helpers.node_display_name(self.adapter, "!0a1b2c3d"), "Relay One")

    def test_node_display_name_falls_back_to_id(self) -> None:
        self.assertEqual(mesh_helpers.node_display_name(self.adapter, "!11111111"), "!11111111")
        self.assertEqual(mesh_helpers.node_display_name(self.adapter, ""), "")

    def test_format_route_names_and_scales_snr(self) -> None:
        route = mesh_helpers.format_route([0x0A1B2C3D], [80], self.adapter)
        self.assertEqual(route, [{"name": "Relay One", "node_id": "!0a1b2c3d", "snr": 20.0}])

        back = mesh_helpers.format_route([0x11111111], [-128], self.adapter)
        # -128 is the firmware's "unknown" sentinel — dropped at the render
        # boundary like the handler's segment lists do.
        self.assertEqual(back[0]["name"], "!11111111")
        self.assertIsNone(back[0]["snr"])

    def test_format_route_masks_negative_ints_and_filters_bad_snr(self) -> None:
        # A negative route int must not render a `!-000005` id that can never
        # resolve; it is masked to the unsigned 32-bit space.
        route = mesh_helpers.format_route([-5], [80], self.adapter)
        self.assertEqual(route[0]["node_id"], "!fffffffb")
        self.assertEqual(route[0]["snr"], 20.0)

        # NaN / non-numeric hop SNR renders as None (dropped), never NaN.
        bad = mesh_helpers.format_route([0x0A1B2C3D], [float("nan")], self.adapter)
        self.assertEqual(bad[0]["node_id"], "!0a1b2c3d")
        self.assertIsNone(bad[0]["snr"])
        nonnum = mesh_helpers.format_route([0x0A1B2C3D], ["loud"], self.adapter)
        self.assertNotIn("snr", nonnum[0])

    def test_format_route_handles_missing_snr(self) -> None:
        route = mesh_helpers.format_route([0x0A1B2C3D], [], self.adapter)
        self.assertEqual(route, [{"name": "Relay One", "node_id": "!0a1b2c3d"}])

    def test_format_route_non_int_hop_rendered_as_is(self) -> None:
        """A non-int hop (defensive leg) renders str(num), not a masked !-hex id."""
        route = mesh_helpers.format_route(["gateway"], [], self.adapter)
        self.assertEqual(route[0]["node_id"], "gateway")
        self.assertEqual(route[0]["name"], "gateway")


class TestRequestedNode(unittest.TestCase):
    """requested_node is the shared target resolution for the solicited tools."""

    def setUp(self) -> None:
        self.adapter = _StubAdapter()
        self.adapter.interfaces = [
            _make_iface({"!aaaa1111": _make_node("!aaaa1111", "Alpha Node", "ALPH")})
        ]

    def test_requires_node_id(self) -> None:
        node_id, err = mesh_helpers.requested_node({}, self.adapter)
        self.assertIsNone(node_id)
        self.assertIn("node_id is required", err)

    def test_resolves_name_to_id(self) -> None:
        node_id, err = mesh_helpers.requested_node({"node_id": "alpha node"}, self.adapter)
        self.assertIsNone(err)
        self.assertEqual(node_id, "!aaaa1111")

    def test_resolves_bang_prefixed_name(self) -> None:
        """A '!'-prefixed name must still resolve: the id search strips the
        bang, and so must the name search."""
        node_id, err = mesh_helpers.requested_node({"node_id": "!alpha node"}, self.adapter)
        self.assertIsNone(err)
        self.assertEqual(node_id, "!aaaa1111")

    def test_ambiguous_name_asks_for_exact_id(self) -> None:
        """Two nodes sharing a name must not silently deliver to whichever was
        discovered first — requested_node surfaces the collision for the model
        to disambiguate by id."""
        self.adapter.interfaces = [
            _make_iface(
                {
                    "!aaaa1111": _make_node("!aaaa1111", "Shared Name", "SH1"),
                    "!bbbb2222": _make_node("!bbbb2222", "Shared Name", "SH2"),
                }
            )
        ]
        node_id, err = mesh_helpers.requested_node({"node_id": "shared name"}, self.adapter)
        self.assertIsNone(node_id)
        self.assertIn("matches multiple nodes", err)
        self.assertIn("exact node ID", err)

    def test_explicit_id_passes_through_when_unknown(self) -> None:
        node_id, err = mesh_helpers.requested_node({"node_id": "!9f001122"}, self.adapter)
        self.assertIsNone(err)
        self.assertEqual(node_id, "!9f001122")

    def test_requested_node_strips_and_lowercases_explicit_id(self) -> None:
        """An unknown explicit '!'-id with surrounding whitespace and uppercase
        hex is normalized to the canonical lowercase !hex form rather than
        rejected (the validate stage and resolve_node must agree), and returned
        verbatim (no upper-case leak into the send path)."""
        node_id, err = mesh_helpers.requested_node({"node_id": "!9F001122 "}, self.adapter)
        self.assertIsNone(err)
        self.assertEqual(node_id, "!9f001122")

    def test_malformed_bang_id_errors_cleanly(self) -> None:
        """A short/garbage '!' id is rejected up front: the library would call
        sys.exit (SystemExit) trying to resolve it, not a clean error."""
        for bad in ("!abcd", "!zzzz9999", "!1234567", "!123456789"):
            node_id, err = mesh_helpers.requested_node({"node_id": bad}, self.adapter)
            self.assertIsNone(node_id, bad)
            self.assertIn("not a valid node ID", err)

    def test_unresolved_non_id_errors(self) -> None:
        node_id, err = mesh_helpers.requested_node({"node_id": "nobody"}, self.adapter)
        self.assertIsNone(node_id)
        self.assertIn("No node matched 'nobody'", err)


class TestCoerceAndPositionHelpers(unittest.TestCase):
    """Finite-numeric coercion and position normalization shared by the
    solicited tool handlers and the inbound periodic path."""

    def test_coerce_float_rejects_non_finite_and_garbage(self) -> None:
        self.assertEqual(mesh_helpers.coerce_float(3.91), 3.91)
        self.assertEqual(mesh_helpers.coerce_float("3.5"), 3.5)
        self.assertIsNone(mesh_helpers.coerce_float(float("nan")))
        self.assertIsNone(mesh_helpers.coerce_float(float("inf")))
        self.assertIsNone(mesh_helpers.coerce_float(float("-inf")))
        self.assertIsNone(mesh_helpers.coerce_float("north"))
        self.assertIsNone(mesh_helpers.coerce_float(None))
        self.assertIsNone(mesh_helpers.coerce_float(True))

    def test_coerce_float_rejects_huge_int_without_overflow(self) -> None:
        """A hostile huge-int field (10**400) overflows float() with
        OverflowError (an ArithmeticError, not a ValueError); the inbound-path
        coerce must drop it to None instead of crashing the caller. Mirrors
        node_freshness._coerce_float / chunking._effective_chunk_bytes."""
        self.assertIsNone(mesh_helpers.coerce_float(10**400))
        self.assertIsNone(mesh_helpers.coerce_float(-(10**400)))
        # A large-but-representable int is still a valid finite float.
        self.assertEqual(mesh_helpers.coerce_float(10**18), 1e18)

    def test_numeric_epoch_guards_type_before_max(self) -> None:
        """A truthy non-numeric lastHeard must not crash max(); it coerces to 0."""
        self.assertEqual(mesh_helpers.numeric_epoch(1700000000), 1700000000.0)
        self.assertEqual(mesh_helpers.numeric_epoch("1700000000"), 0.0)  # string, not parsed
        self.assertEqual(mesh_helpers.numeric_epoch("junk"), 0.0)
        self.assertEqual(mesh_helpers.numeric_epoch(None), 0.0)
        self.assertEqual(mesh_helpers.numeric_epoch(True), 0.0)  # bool is not numeric here
        self.assertEqual(mesh_helpers.numeric_epoch(float("nan")), 0.0)
        self.assertEqual(mesh_helpers.numeric_epoch(float("inf")), 0.0)
        # max() over a guarded bad value and a good one keeps the good one.
        self.assertEqual(max(mesh_helpers.numeric_epoch("bad"), mesh_helpers.numeric_epoch(5)), 5.0)

    def test_decode_snr_value_filters_non_numeric_before_division(self) -> None:
        """Non-numeric SNR entries filter to None rather than raising TypeError
        on the division; -128 is the firmware unknown sentinel; valid ints
        divide by 4."""
        self.assertEqual(mesh_helpers.decode_snr_value(80), 20.0)
        self.assertEqual(mesh_helpers.decode_snr_value(-128), None)
        self.assertEqual(mesh_helpers.decode_snr_value(0), 0.0)
        self.assertIsNone(mesh_helpers.decode_snr_value("loud"))
        self.assertIsNone(mesh_helpers.decode_snr_value(None))
        self.assertIsNone(mesh_helpers.decode_snr_value(True))
        self.assertIsNone(mesh_helpers.decode_snr_value([1, 2]))
        self.assertIsNone(mesh_helpers.decode_snr_value(float("nan")))

    def test_normalize_position_payload_accepts_camel_case_and_legacy(self) -> None:
        hardware = mesh_helpers.normalize_position_payload(
            {"latitudeI": 551885155, "longitudeI": 613386332, "altitude": 210}
        )
        self.assertAlmostEqual(hardware["latitude"], 55.1885155)
        self.assertAlmostEqual(hardware["longitude"], 61.3386332)
        self.assertEqual(hardware["altitude"], 210)

        legacy = mesh_helpers.normalize_position_payload(
            {"latitude": 551885155, "longitude": 613386332}
        )
        self.assertAlmostEqual(legacy["latitude"], 55.1885155)
        self.assertAlmostEqual(legacy["longitude"], 61.3386332)

        decimal = mesh_helpers.normalize_position_payload({"latitude": 55.75, "longitude": 37.61})
        self.assertEqual(decimal["latitude"], 55.75)
        self.assertEqual(decimal["longitude"], 37.61)

    def test_normalize_position_payload_filters_hostile_values(self) -> None:
        hostile = mesh_helpers.normalize_position_payload(
            {"latitudeI": float("nan"), "longitudeI": float("inf"), "altitude": "hundreds"}
        )
        self.assertIsNone(hostile["latitude"])
        self.assertIsNone(hostile["longitude"])
        self.assertIsNone(hostile["altitude"])

        absurd = mesh_helpers.normalize_position_payload({"latitude": 1e30, "longitude": -1e30})
        self.assertEqual(absurd["latitude"], 90.0)
        self.assertEqual(absurd["longitude"], -180.0)

    def test_normalize_position_payload_non_dict_is_null_safe(self) -> None:
        result = mesh_helpers.normalize_position_payload(None)
        self.assertEqual(result, {"latitude": None, "longitude": None, "altitude": None})

    def test_normalize_position_payload_scale_decided_per_axis(self) -> None:
        """One out-of-range axis must not flip the other into protobuf scale: a
        decimal payload with a bad latitude keeps its valid longitude."""
        mixed = mesh_helpers.normalize_position_payload({"latitude": 95.0, "longitude": 37.61})
        self.assertEqual(mixed["latitude"], 90.0)
        self.assertEqual(mixed["longitude"], 37.61)

        # A protobuf-scale axis divides independently of the other.
        per_axis = mesh_helpers.normalize_position_payload(
            {"latitudeI": 551885155, "longitude": 37.61}
        )
        self.assertAlmostEqual(per_axis["latitude"], 55.1885155)
        self.assertEqual(per_axis["longitude"], 37.61)

        # Decimal degrees at the valid boundary are left alone.
        boundary = mesh_helpers.normalize_position_payload({"latitude": 90.0, "longitude": 180.0})
        self.assertEqual(boundary["latitude"], 90.0)
        self.assertEqual(boundary["longitude"], 180.0)

    def test_normalize_position_payload_none_camel_case_falls_back(self) -> None:
        """`latitudeI: None` with a decimal `latitude` beside it must parse the
        decimal value (the old `.get(key, fallback)` only covered an absent key)."""
        result = mesh_helpers.normalize_position_payload(
            {"latitudeI": None, "latitude": 55.0, "longitudeI": None, "longitude": 37.61}
        )
        self.assertEqual(result["latitude"], 55.0)
        self.assertEqual(result["longitude"], 37.61)


class _ObservingAdapter:
    """The slice of MeshtasticAdapter that format_node_summary uses."""

    def __init__(self, observed: dict) -> None:
        self.observed = observed

    def get_observed_node(self, node_id: str) -> dict:
        return self.observed.get(node_id, {})


class TestFormatNodeSummary(unittest.TestCase):
    """format_node_summary builds one node's mesh_list_nodes entry (extracted
    from the handler so the per-node snapshot is testable standalone)."""

    def setUp(self) -> None:
        self.adapter = _ObservingAdapter({})

    def test_observed_overlay_wins_for_last_heard_and_signal(self) -> None:
        now = time.time()
        node = _make_node(
            "!aaaa1111",
            "Alpha Node",
            "ALPH",
            last_heard=now - 600,
            hopsAway=0,
            deviceMetrics={"batteryLevel": 87},
        )
        adapter = _ObservingAdapter(
            {"!aaaa1111": {"snr": 9.5, "rssi": -70, "last_heard": now - 60}}
        )
        entry = mesh_helpers.format_node_summary(adapter, "!aaaa1111", node, {}, {})
        self.assertEqual(entry["long_name"], "Alpha Node")
        self.assertEqual(entry["short_name"], "ALPH")
        self.assertEqual(entry["hw_model"], "TBEAM")
        self.assertEqual(entry["battery_level"], 87)
        self.assertEqual(entry["snr"], 9.5)
        self.assertEqual(entry["signal_quality"], "Excellent")
        self.assertEqual(entry["signal_source"], "direct")
        self.assertEqual(entry["hops_away"], 0)
        self.assertTrue(entry["heard_directly"])
        # last_heard is the freshest of the node DB and the observation.
        self.assertEqual(
            entry["last_heard"],
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now - 60)),
        )

    def test_node_db_fallback_when_obs_empty(self) -> None:
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH", snr=5.0, rssi=-80, hopsAway=2)
        entry = mesh_helpers.format_node_summary(self.adapter, "!aaaa1111", node, {}, {})
        self.assertEqual(entry["snr"], 5.0)
        self.assertEqual(entry["signal_source"], "relayed")
        self.assertEqual(entry["hops_away"], 2)
        self.assertFalse(entry["heard_directly"])
        self.assertEqual(entry["last_heard"], "Never")
        self.assertIsNone(entry["last_direct_heard"])

    def test_history_row_fills_signal_when_db_silent(self) -> None:
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH")
        latest = {"!aaaa1111": {"snr": 4.0, "rssi": -75, "hop_count": 1}}
        entry = mesh_helpers.format_node_summary(self.adapter, "!aaaa1111", node, latest, {})
        self.assertEqual(entry["snr"], 4.0)
        self.assertEqual(entry["signal_source"], "relayed")
        self.assertEqual(entry["hops_away"], 1)

    def test_format_node_summary_tolerates_non_numeric_last_heard(self) -> None:
        """A hostile/malformed NodeInfo with a truthy non-numeric lastHeard must
        not crash max() — it falls back to the observed value or 'Never'."""
        node = _make_node("!aaaa1111", "Alpha Node", "ALPH", last_heard="not-a-number")
        entry = mesh_helpers.format_node_summary(self.adapter, "!aaaa1111", node, {}, {})
        self.assertEqual(entry["last_heard"], "Never")


class TestHistoryWindowParams(unittest.TestCase):
    """history_window_params validates the mesh_telemetry_history args
    (moved from mesh_tools._history_window_params with the helper)."""

    def test_count_mode_defaults(self) -> None:
        limit, since, err = mesh_helpers.history_window_params({})
        self.assertIsNone(err)
        self.assertEqual(limit, 10)
        self.assertIsNone(since)

    def test_since_hours_validation(self) -> None:
        for bad in ("abc", "12x"):
            with self.subTest(bad=bad):
                limit, since, err = mesh_helpers.history_window_params({"since_hours": bad})
                self.assertEqual(err, "Parameter 'since_hours' must be a number.")
                self.assertIsNone(since)
        for non_positive in (0, -5):
            with self.subTest(non_positive=non_positive):
                limit, since, err = mesh_helpers.history_window_params(
                    {"since_hours": non_positive}
                )
                self.assertEqual(err, "Parameter 'since_hours' must be positive.")
                self.assertIsNone(since)

    def test_since_hours_rejects_non_finite(self) -> None:
        """NaN/Inf since_hours would poison `since` and crash time.localtime(nan)
        in the caller — reject them like any other non-number."""
        for bad in ("nan", "inf", "-inf", float("nan")):
            with self.subTest(bad=bad):
                limit, since, err = mesh_helpers.history_window_params({"since_hours": bad})
                self.assertEqual(err, "Parameter 'since_hours' must be a number.")
                self.assertIsNone(since)

    def test_since_hours_and_limit_reject_bools(self) -> None:
        """float(True) == 1.0, so `since_hours: true` would silently ask for a
        1-hour window — reject it; `limit: true` is rejected too, consistently
        (previously it fell back to the default with no feedback)."""
        limit, since, err = mesh_helpers.history_window_params({"since_hours": True})
        self.assertEqual(err, "Parameter 'since_hours' must be a number.")
        self.assertIsNone(since)

        limit, since, err = mesh_helpers.history_window_params({"limit": True})
        self.assertEqual(err, "Parameter 'limit' must be a number.")
        self.assertIsNone(since)
        limit, since, err = mesh_helpers.history_window_params({"limit": True, "since_hours": 1})
        self.assertEqual(err, "Parameter 'limit' must be a number.")

    def test_since_hours_clamped_to_retention_and_raises_cap(self) -> None:
        limit, since, err = mesh_helpers.history_window_params({"since_hours": 99999})
        self.assertIsNone(err)
        self.assertAlmostEqual(
            since, time.time() - mesh_helpers.HISTORY_MAX_WINDOW_HOURS * 3600, delta=5
        )
        # A time window is the ask, so the row cap goes up to the full window.
        self.assertEqual(limit, mesh_helpers.HISTORY_WINDOW_ROW_CAP)

    def test_limit_clamped_to_row_caps(self) -> None:
        limit, _, err = mesh_helpers.history_window_params({"limit": 99999})
        self.assertIsNone(err)
        self.assertEqual(limit, 100)
        limit, _, _ = mesh_helpers.history_window_params({"limit": 99999, "since_hours": 1})
        self.assertEqual(limit, mesh_helpers.HISTORY_WINDOW_ROW_CAP)
        # Numeric strings still coerce; a non-numeric limit errors like
        # since_hours does, instead of silently degrading to the default.
        limit, _, err = mesh_helpers.history_window_params({"limit": "50"})
        self.assertIsNone(err)
        self.assertEqual(limit, 50)
        for bad in ("x", "abc"):
            with self.subTest(bad=bad):
                limit, since, err = mesh_helpers.history_window_params({"limit": bad})
                self.assertEqual(err, "Parameter 'limit' must be a number.")
                self.assertIsNone(since)


class TestFetchHistoryRows(unittest.TestCase):
    """fetch_history_rows dispatches a history request to the right
    telemetry_db table; telemetry_db is patched at the module level."""

    def test_dispatches_metric_types(self) -> None:
        now = time.time()
        seen: list[tuple] = []

        def record_factory(name: str):
            def fn(node_id: str, *, limit: int, since: float | None) -> list:
                seen.append((name, node_id, limit, since))
                return [{"timestamp": now, "temperature": 20.0}]

            return fn

        with (
            patch.object(
                mesh_helpers.telemetry_db,
                "get_telemetry_history",
                side_effect=record_factory("telemetry"),
            ),
            patch.object(
                mesh_helpers.telemetry_db,
                "get_position_history",
                side_effect=record_factory("positions"),
            ),
            patch.object(
                mesh_helpers.telemetry_db,
                "get_signal_history",
                side_effect=record_factory("signal"),
            ),
        ):
            telemetry = mesh_helpers.fetch_history_rows("telemetry", "!aaaa1111", 11, now - 1000)
            positions = mesh_helpers.fetch_history_rows("positions", "!aaaa1111", 5, None)
            signal = mesh_helpers.fetch_history_rows("signal_quality", "!aaaa1111", 3, now - 500)
        self.assertEqual(len(telemetry), 1)
        self.assertEqual(len(positions), 1)
        self.assertEqual(len(signal), 1)
        self.assertEqual(
            seen,
            [
                ("telemetry", "!aaaa1111", 11, now - 1000),
                ("positions", "!aaaa1111", 5, None),
                ("signal", "!aaaa1111", 3, now - 500),
            ],
        )

    def test_unknown_metric_type_returns_none_without_calling_db(self) -> None:
        with patch.object(mesh_helpers.telemetry_db, "get_telemetry_history") as getter:
            result = mesh_helpers.fetch_history_rows("bogus", "!aaaa1111", 10, None)
        self.assertIsNone(result)
        getter.assert_not_called()


if __name__ == "__main__":
    unittest.main()
