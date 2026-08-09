"""Per-node live-observed overlay learned from the packet stream.

``iface.nodes[x]["lastHeard"]`` from the meshtastic library only refreshes from
periodic NodeInfo packets, so it lags actual transmissions. The adapter feeds
every received packet's ``rxTime`` / SNR / RSSI into a :class:`NodeFreshness`
instance and the ``mesh_list_nodes`` / ``mesh_node_info`` /
``mesh_signal_quality`` tools layer it over the library node DB.
"""

import math
import threading
import time
from typing import Any

# Upper bound on the per-node "observed" overlay (live last_heard / signal
# learned from the packet stream). Stalest entry evicts first on overflow.
OBSERVED_NODE_LIMIT = 2048

# Defense-in-depth cap on a single node-id key length (the count of keys is
# already bounded by OBSERVED_NODE_LIMIT). Production ids are 9 chars.
_NODE_ID_MAX_LEN = 128


def _coerce_float(value: Any) -> float | None:
    """Coerce an untrusted signal field to a finite float (None if unusable).

    Non-numeric strings, bools, and NaN/inf are rejected so a hostile/malformed
    envelope can never land in the overlay as a non-standard JSON literal —
    ``get()`` stays safe for any consumer, not just ``link_facts``.
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        num = float(value)
    except (TypeError, ValueError, OverflowError):
        # A huge int (e.g. 10**400) overflows float() with OverflowError; catch
        # it here so a hostile huge-int snr/rssi can never abort the freshness
        # update (mirrors the rx_time path's OverflowError handling below).
        return None
    return num if math.isfinite(num) else None


def _coerce_hop_count(value: Any) -> int | None:
    """Coerce an untrusted hop count to a non-negative int (None if unusable).

    Only real ints are accepted: bools are rejected (a ``False`` must not
    masquerade as a 0-hop direct packet and synthesize a signal record) and
    strings/floats are rejected so the direct-signal gate stays exact at the
    store boundary. Negatives are dropped as nonsensical. Production always
    supplies a non-negative int or ``None`` (``max(0, hop_start - hop_limit)``).
    """
    if not isinstance(value, int) or isinstance(value, bool):
        return None
    return value if value >= 0 else None


class NodeFreshness:
    """Per-node live-observed overlay (last_heard / signal) learned from the
    packet stream, layered over the library's node DB by the mesh_* tools.

    Mirrors the official Meshtastic client: last_heard refreshes from each
    packet's rxTime (clamped to now); snr/rssi only from direct (0-hop) packets.

    Writes normally happen on the platform loop and reads on the mesh_* tool
    handlers, which are NOT guaranteed to be the same thread — so both sides are
    serialized by a small lock. The lock is held only around the short dict
    operations (hot-path cost is a cheap uncontended acquire).
    """

    def __init__(self, limit: int = OBSERVED_NODE_LIMIT) -> None:
        self._observed: dict[str, dict[str, Any]] = {}
        # Clamp so a config of 0 (or negative) can't produce an unbounded store
        # or crash the eviction scan on an empty dict.
        self._limit = max(1, limit)
        self._lock = threading.Lock()

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
        link metrics belong to the last hop, not the origin node. ``snr``/``rssi``
        are finite-filtered at the store boundary (see :func:`_coerce_float`);
        ``hops_away`` is coerced to a non-negative int at the store boundary
        (see :func:`_coerce_hop_count`).
        """
        now = time.time()
        # Defense-in-depth: bound the key length so a future caller that forgets
        # the inbound truncation can't plant unbounded keys (count is already
        # capped by _limit). Production node ids are "!"+8 hex (9 chars).
        node_id = node_id[:_NODE_ID_MAX_LEN]
        try:
            last_heard = float(rx_time) if (rx_time and not isinstance(rx_time, bool)) else now
        except (TypeError, ValueError, OverflowError):
            # A huge int (e.g. 10**400) overflows float(); NaN/inf parse fine but
            # must not be persisted — both fall back to now. A bool rx_time
            # (True == 1.0) is treated as a missing timestamp, not epoch+1s, for
            # consistency with _coerce_float's bool rejection. A malformed
            # rx_time must never drop the whole packet's receive path.
            last_heard = now
        if math.isfinite(last_heard):
            last_heard = min(last_heard, now)
        else:
            last_heard = now

        with self._lock:
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
            hops = _coerce_hop_count(hop_count)
            if hops is not None:
                obs["hops_away"] = hops
            if hops == 0:  # direct packet: link metrics describe this node
                snr_f = _coerce_float(snr)
                rssi_f = _coerce_float(rssi)
                if snr_f is not None:
                    obs["snr"] = snr_f
                if rssi_f is not None:
                    obs["rssi"] = rssi_f

    def get(self, node_id: str) -> dict[str, Any]:
        """Return the live-observed overlay for a node id ({} if never heard)."""
        with self._lock:
            obs = self._observed.get(node_id)
            return dict(obs) if obs else {}
