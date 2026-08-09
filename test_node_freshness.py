"""Pure-function tests for the node-freshness overlay module."""

import math
import threading
import time
import unittest

from node_freshness import NodeFreshness, _coerce_float, _coerce_hop_count


class TestNodeFreshness(unittest.TestCase):
    def test_update_observed_last_heard_and_direct_signal(self):
        """last_heard tracks rx_time; snr/rssi only from direct (0-hop) packets."""
        nf = NodeFreshness()
        nf.update("!aaaa1111", 1_700_000_000, 5.0, -80, 0)
        obs = nf.get("!aaaa1111")
        self.assertEqual(obs["last_heard"], 1_700_000_000)
        self.assertEqual(obs["snr"], 5.0)
        self.assertEqual(obs["rssi"], -80)
        self.assertEqual(obs["hops_away"], 0)

    def test_update_observed_relayed_packet_skips_signal(self):
        """A relayed (hop>0) packet bumps last_heard but not snr/rssi."""
        nf = NodeFreshness()
        nf.update("!bbbb2222", None, 3.0, -90, 2)
        obs = nf.get("!bbbb2222")
        self.assertGreater(obs["last_heard"], 0)
        self.assertEqual(obs["hops_away"], 2)
        self.assertNotIn("snr", obs)  # relay metrics belong to the last hop
        self.assertNotIn("rssi", obs)

    def test_update_observed_future_rxtime_clamped(self):
        """A future rx_time (clock skew) is clamped to now."""
        nf = NodeFreshness()
        nf.update("!cccc3333", time.time() + 10_000, None, None, None)
        self.assertLessEqual(nf.get("!cccc3333")["last_heard"], time.time() + 1)

    def test_observed_overlay_is_size_bounded(self):
        """The observed overlay evicts the stalest entry past its cap."""
        nf = NodeFreshness(limit=3)
        for i in range(10):
            nf.update(f"!n{i:07d}", 1_700_000_000 + i, None, None, None)
        self.assertLessEqual(len(nf._observed), 3)
        self.assertIn("!n0000009", nf._observed)  # newest kept
        self.assertNotIn("!n0000000", nf._observed)  # stalest evicted

    def test_concurrent_update_and_get_are_safe(self):
        """Concurrent writes (with eviction pressure) and reads never tear.

        mesh_* tool handlers are not guaranteed to run on the writer's thread,
        so get()/update() must tolerate cross-thread access without raising or
        returning a torn snapshot.
        """
        nf = NodeFreshness(limit=50)
        stop = threading.Event()
        errors: list[BaseException] = []

        def writer() -> None:
            i = 0
            while not stop.is_set():
                nf.update(f"!n{i:07d}", 1_700_000_000 + i, 5.0, -80, 0)
                i += 1

        def reader() -> None:
            try:
                while not stop.is_set():
                    nf.get("!n0000009")
                    nf.get("!missing")
            except Exception as e:  # pragma: no cover - failure only
                errors.append(e)

        w1, w2, r = (
            threading.Thread(target=writer),
            threading.Thread(target=writer),
            threading.Thread(target=reader),
        )
        for t in (w1, w2, r):
            t.start()
        time.sleep(0.05)
        stop.set()
        for t in (w1, w2, r):
            t.join()

        self.assertEqual(errors, [])
        self.assertLessEqual(len(nf._observed), 50)
        for value in nf.get("!n0000009").values():
            self.assertNotIsInstance(value, dict)  # no torn nested snapshot

    def test_huge_rxtime_does_not_raise_and_records_now(self):
        """A huge rx_time (float() overflow) must not abort the receive path."""
        nf = NodeFreshness()
        nf.update("!x", 10**400, None, None, None)  # OverflowError -> now
        self.assertAlmostEqual(nf.get("!x")["last_heard"], time.time(), delta=5)

    def test_nan_rxtime_first_packet_records_now_not_zero(self):
        """A NaN rx_time on a node's first packet must record now, not epoch 0."""
        nf = NodeFreshness()
        nf.update("!x", float("nan"), None, None, None)
        heard = nf.get("!x")["last_heard"]
        self.assertAlmostEqual(heard, time.time(), delta=5)
        self.assertFalse(math.isnan(heard))

    def test_inf_rxtime_falls_back_to_now(self):
        """An infinite rx_time cannot be persisted as a future/stale sentinel."""
        nf = NodeFreshness()
        nf.update("!x", float("inf"), None, None, None)
        self.assertAlmostEqual(nf.get("!x")["last_heard"], time.time(), delta=5)

    def test_non_numeric_rxtime_falls_back_to_now(self):
        """A non-numeric rx_time falls back to now instead of raising."""
        nf = NodeFreshness()
        nf.update("!x", "garbage", None, None, None)
        self.assertAlmostEqual(nf.get("!x")["last_heard"], time.time(), delta=5)

    def test_zero_rxtime_records_now(self):
        """A falsy rx_time == 0 is a missing timestamp, not epoch."""
        nf = NodeFreshness()
        nf.update("!x", 0, None, None, None)
        self.assertAlmostEqual(nf.get("!x")["last_heard"], time.time(), delta=5)

    def test_zero_limit_behaves_like_bounded_store(self):
        """limit=0 must not crash or grow unbounded; it clamps to 1."""
        nf = NodeFreshness(limit=0)
        nf.update("!a", 1_700_000_000, None, None, None)  # no raise
        nf.update("!b", 1_700_000_001, None, None, None)
        nf.update("!c", 1_700_000_002, None, None, None)
        self.assertLessEqual(len(nf._observed), 1)
        self.assertIn("!c", nf._observed)

    def test_limit_one_evicts_on_every_new_node(self):
        """limit=1 exercises the evict-on-every-new-node path."""
        nf = NodeFreshness(limit=1)
        nf.update("!a", 1_700_000_000, None, None, None)
        nf.update("!b", 1_700_000_001, None, None, None)
        self.assertEqual(list(nf._observed), ["!b"])

    def test_direct_then_relayed_keeps_signal(self):
        """After a direct->relayed transition, snr/rssi persist and hops updates."""
        nf = NodeFreshness()
        nf.update("!aaaa1111", 1_700_000_000, 5.0, -80, 0)
        nf.update("!aaaa1111", 1_700_000_001, 3.0, -90, 2)
        obs = nf.get("!aaaa1111")
        self.assertEqual(obs["snr"], 5.0)  # from the direct packet
        self.assertEqual(obs["rssi"], -80)
        self.assertEqual(obs["hops_away"], 2)
        self.assertEqual(obs["last_heard"], 1_700_000_001)

    def test_signal_coerced_at_store_boundary(self):
        """NaN/string snr must not land in the overlay; numeric strings coerce."""
        nf = NodeFreshness()
        nf.update("!aaaa1111", None, float("nan"), -80, 0)
        obs = nf.get("!aaaa1111")
        self.assertNotIn("snr", obs)  # NaN dropped at the boundary
        self.assertEqual(obs["rssi"], -80)
        self.assertFalse(math.isnan(obs["rssi"]))

        nf.update("!bbbb2222", None, "5.0", "-90", 0)
        obs = nf.get("!bbbb2222")
        self.assertEqual(obs["snr"], 5.0)  # coerced to a finite float
        self.assertEqual(obs["rssi"], -90)

        nf.update("!cccc3333", None, "not-a-number", -80, 0)
        obs = nf.get("!cccc3333")
        self.assertNotIn("snr", obs)  # non-numeric string dropped
        self.assertEqual(obs["rssi"], -80)

        nf.update("!dddd4444", None, True, -80, 0)
        obs = nf.get("!dddd4444")
        self.assertNotIn("snr", obs)  # bools are not signal values
        self.assertEqual(obs["rssi"], -80)

    def test_eviction_prefers_entry_without_last_heard(self):
        """The eviction key defaults missing last_heard to 0.0 (stalest first)."""
        nf = NodeFreshness(limit=2)
        nf.update("!a", 1_700_000_000, None, None, None)
        nf.update("!b", 1_700_000_001, None, None, None)
        nf._observed["!a"] = {"hops_away": 0}  # entry lacking last_heard
        nf.update("!c", 1_700_000_002, None, None, None)
        self.assertNotIn("!a", nf._observed)  # 0.0 default evicts it first
        self.assertIn("!c", nf._observed)
        self.assertLessEqual(len(nf._observed), 2)

    def test_huge_int_signal_does_not_raise(self):
        """A hostile huge-int snr/rssi must not abort the freshness update.

        ``float(10**400)`` raises OverflowError; ``_coerce_float`` must catch it
        so the signal fields are dropped but last_heard/hops_away still land.
        """
        nf = NodeFreshness()
        before = time.time()
        nf.update("!x", 1_700_000_000, 10**400, 10**400, 0)  # no raise
        after = time.time()
        obs = nf.get("!x")
        self.assertEqual(obs["last_heard"], 1_700_000_000)  # rx_time survived
        self.assertEqual(obs["hops_away"], 0)  # hop_count survived
        self.assertNotIn("snr", obs)  # huge-int snr dropped at the boundary
        self.assertNotIn("rssi", obs)  # huge-int rssi dropped at the boundary
        self.assertGreaterEqual(obs["last_heard"], 0)
        self.assertLessEqual(obs["last_heard"], after + 1)
        self.assertGreaterEqual(before, obs["last_heard"] - 1)

    def test_bool_and_negative_hop_count_not_stored_verbatim(self):
        """A bool/negative hop_count is coerced, not persisted as-is.

        ``False`` must not masquerade as a 0-hop direct packet (no signal
        synthesized); a negative is dropped as nonsensical.
        """
        nf = NodeFreshness()
        # False: previously stored hops_away=False AND fired the signal branch.
        nf.update("!b", None, 5.0, -80, False)
        obs_b = nf.get("!b")
        self.assertNotIn("hops_away", obs_b)  # bool rejected, not stored
        self.assertNotIn("snr", obs_b)  # no direct-signal record synthesized
        self.assertNotIn("rssi", obs_b)
        # Negative: previously stored hops_away=-1.
        nf.update("!n", None, 5.0, -80, -1)
        obs_n = nf.get("!n")
        self.assertNotIn("hops_away", obs_n)  # negative rejected
        self.assertNotIn("snr", obs_n)
        # String hop_count rejected too (a "0" string must not mis-classify).
        nf.update("!s", None, 5.0, -80, "0")
        obs_s = nf.get("!s")
        self.assertNotIn("hops_away", obs_s)
        self.assertNotIn("snr", obs_s)
        # A real 0-hop int still works end-to-end.
        nf.update("!ok", None, 5.0, -80, 0)
        obs_ok = nf.get("!ok")
        self.assertEqual(obs_ok["hops_away"], 0)
        self.assertEqual(obs_ok["snr"], 5.0)

    def test_bool_rx_time_falls_back_to_now(self):
        """A bool rx_time (True == 1.0) is treated as missing, not epoch+1s."""
        nf = NodeFreshness()
        nf.update("!x", True, None, None, None)
        heard = nf.get("!x")["last_heard"]
        self.assertNotAlmostEqual(heard, 1.0, delta=5)  # not epoch+1s
        self.assertAlmostEqual(heard, time.time(), delta=5)  # falls back to now
        # False is falsy -> also now.
        nf.update("!y", False, None, None, None)
        self.assertAlmostEqual(nf.get("!y")["last_heard"], time.time(), delta=5)

    def test_oversized_node_id_is_truncated(self):
        """A node id longer than the cap is truncated at the store boundary."""
        nf = NodeFreshness()
        long_id = "!" + "a" * 500
        nf.update(long_id, 1_700_000_000, None, None, None)
        # The full key must not be present; the truncated prefix must.
        self.assertNotIn(long_id, nf._observed)
        truncated = long_id[:128]
        self.assertIn(truncated, nf._observed)
        self.assertEqual(nf.get(truncated)["last_heard"], 1_700_000_000)
        # A normal (short) id is untouched.
        nf.update("!aaaa1111", 1_700_000_001, None, None, None)
        self.assertIn("!aaaa1111", nf._observed)

    def test_coerce_float_helper_matrix(self):
        """Direct unit test of _coerce_float for the bool/NaN/inf/string matrix."""
        self.assertIsNone(_coerce_float(None))
        self.assertIsNone(_coerce_float(True))
        self.assertIsNone(_coerce_float(False))
        self.assertIsNone(_coerce_float(float("nan")))
        self.assertIsNone(_coerce_float(float("inf")))
        self.assertIsNone(_coerce_float(float("-inf")))
        self.assertIsNone(_coerce_float("not-a-number"))
        self.assertIsNone(_coerce_float(10**400))  # huge int -> OverflowError
        self.assertEqual(_coerce_float(5.0), 5.0)
        self.assertEqual(_coerce_float(5), 5.0)  # int coerces
        self.assertEqual(_coerce_float("5.0"), 5.0)  # numeric string coerces
        self.assertEqual(_coerce_float(-80), -80.0)

    def test_coerce_hop_count_helper_matrix(self):
        """Direct unit test of _coerce_hop_count for the bool/string/negative matrix."""
        self.assertIsNone(_coerce_hop_count(None))
        self.assertIsNone(_coerce_hop_count(True))
        self.assertIsNone(_coerce_hop_count(False))
        self.assertIsNone(_coerce_hop_count(-1))
        self.assertIsNone(_coerce_hop_count("0"))  # string rejected
        self.assertIsNone(_coerce_hop_count(0.0))  # float rejected
        self.assertEqual(_coerce_hop_count(0), 0)
        self.assertEqual(_coerce_hop_count(2), 2)


if __name__ == "__main__":
    unittest.main()
