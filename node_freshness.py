"""Per-node live-observed overlay learned from the packet stream.

``iface.nodes[x]["lastHeard"]`` from the meshtastic library only refreshes from
periodic NodeInfo packets, so it lags actual transmissions. The adapter feeds
every received packet's ``rxTime`` / SNR / RSSI into a :class:`NodeFreshness`
instance and the ``mesh_list_nodes`` / ``mesh_node_info`` /
``mesh_signal_quality`` tools layer it over the library node DB.
"""

import time
from typing import Any

# Upper bound on the per-node "observed" overlay (live last_heard / signal
# learned from the packet stream). Stalest entry evicts first on overflow.
OBSERVED_NODE_LIMIT = 2048


class NodeFreshness:
    """Per-node live-observed overlay (last_heard / signal) learned from the
    packet stream, layered over the library's node DB by the mesh_* tools.

    Mirrors the official Meshtastic client: last_heard refreshes from each
    packet's rxTime (clamped to now); snr/rssi only from direct (0-hop) packets.
    Runs on the loop thread (same thread as the mesh_* tools that read it), so
    no locking is needed.
    """

    def __init__(self, limit: int = OBSERVED_NODE_LIMIT) -> None:
        self._observed: dict[str, dict[str, Any]] = {}
        self._limit = limit

    def update(
        self,
        node_id: str,
        rx_time: Any,
        snr: Any,
        rssi: Any,
        hop_count: int | None,
    ) -> None:
        """Record live packet observations for a node, keyed by node id.

        Mirrors the official Meshtastic client: ``last_heard`` is refreshed from
        the packet's ``rxTime`` on every received packet (clamped to now, so a
        skewed clock can't push it into the future); ``snr``/``rssi`` are
        refreshed only from **direct** (0-hop) packets, since a relayed packet's
        link metrics belong to the last hop, not the origin node.

        Runs on the loop thread (via the incoming-queue consumer), same as the
        mesh_* tools that read it, so no locking is needed.
        """
        now = time.time()
        try:
            last_heard = min(float(rx_time), now) if rx_time else now
        except (TypeError, ValueError):
            last_heard = now

        obs = self._observed.get(node_id)
        if obs is None:
            if len(self._observed) >= self._limit:
                stalest = min(
                    self._observed,
                    key=lambda k: self._observed[k].get("last_heard", 0.0),
                )
                self._observed.pop(stalest, None)
            obs = {}
            self._observed[node_id] = obs

        obs["last_heard"] = max(obs.get("last_heard", 0.0), last_heard)
        if hop_count is not None:
            obs["hops_away"] = hop_count
        if hop_count == 0:  # direct packet: link metrics describe this node
            if snr is not None:
                obs["snr"] = snr
            if rssi is not None:
                obs["rssi"] = rssi

    def get(self, node_id: str) -> dict[str, Any]:
        """Return the live-observed overlay for a node id ({} if never heard)."""
        obs = self._observed.get(node_id)
        return dict(obs) if obs else {}
