"""Pure-function tests for the node-freshness overlay module."""

import time
import unittest

from node_freshness import NodeFreshness


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


if __name__ == "__main__":
    unittest.main()
