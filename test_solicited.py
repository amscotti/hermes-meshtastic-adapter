"""Unit tests for the solicited-request response-waiter tracker (solicited).

Pure-logic tests that don't need an assembled adapter: the tracker takes all
adapter-specific pieces (node-id normalization, interface fetch, transport
executor, the link-lost exception class) as constructor-injected callables, so
this module stubs them directly. Integration coverage (the ``request_*``
adapter delegates, pubsub-driven resolution through ``_on_receive``, the
disconnect/connection-lost abandon wiring) remains in test_send.py /
test_meshtastic.py.

Pinned behaviors:

* registry lifecycle: register -> resolve pops waiters; multiple waiters per
  key all resolve; discard removes only the given waiter; abandon clears all.
* timeout path: ``solicit`` returns the "did not answer" dict and discards the
  waiter (no registry leak).
* A8: ``solicit`` re-raises ``asyncio.CancelledError`` after discarding the
  waiter on task cancellation.
* ``maybe_resolve`` payload matching: TELEMETRY_APP / POSITION_APP /
  TRACEROUTE_APP shapes (string and numeric portnums), unknown payloads are
  a no-op, and node ids match after normalization.
"""

import asyncio
import threading
import unittest
from concurrent.futures import Future as ConcurrentFuture
from concurrent.futures import InvalidStateError as ConcurrentInvalidStateError
from typing import Any
from unittest.mock import patch

import solicited
from solicited import SolicitedRequestTracker
from transport import _DaemonTransportExecutor

DEST = "!ab12cd34"


class _LinkLost(Exception):
    """Stand-in for ``adapter.MeshLinkLost`` (injected, never imported)."""


def _normalize_node_id(node_id: Any) -> str | None:
    """Mirror of adapter.MeshtasticAdapter._normalize_node_id.

    Kept as a hand-reimplemented mirror so this module stays free of an adapter
    import (the tracker takes normalization as an injected callable). Drift is
    caught by ``test_stub_normalize_matches_production`` below, which cross-checks
    this mirror against the real helper whenever the adapter is importable.
    """
    if node_id is None:
        return None
    if isinstance(node_id, bool):
        return str(node_id).lower()
    if isinstance(node_id, int):
        if 0 <= node_id < 2**32:  # unsigned 32-bit; reject out-of-range like production
            return f"!{node_id:08x}"
        return None
    text = str(node_id).strip()
    if not text:
        return None
    low = text.lower()
    bare = low[1:] if low.startswith("!") else low
    if len(bare) == 8 and all(c in "0123456789abcdef" for c in bare):
        return f"!{bare}"
    return low


class _FakeExecutor:
    """Runs submitted calls inline, returning a settled ConcurrentFuture."""

    def __init__(self, fail: BaseException | None = None) -> None:
        self.fail = fail

    def submit(self, fn) -> ConcurrentFuture:
        future: ConcurrentFuture = ConcurrentFuture()
        if self.fail is not None:
            future.set_exception(self.fail)
        else:
            future.set_result(fn())
        return future


class _TrackerHarness:
    """Builds a tracker with stubbed deps; exposes the fake iface for asserts."""

    def __init__(self, executor: _FakeExecutor | None = None) -> None:
        self.iface = object()
        self.sent: list[tuple[str, Any]] = []
        self.executor = executor or _FakeExecutor()
        self.tracker = SolicitedRequestTracker(
            normalize_node_id=_normalize_node_id,
            interfaces_provider=lambda: [self.iface],
            executor_provider=lambda: self.executor,
            link_lost_exc=_LinkLost,
        )

    def sender(self, kind: str):
        def send(iface: Any) -> None:
            self.sent.append((kind, iface))

        return send


class SolicitedTrackerRegistryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.harness = _TrackerHarness()
        self.tracker = self.harness.tracker

    def test_register_append_and_resolve_pops_all_waiters(self) -> None:
        first = self.tracker.register_waiter("telemetry", DEST)
        second = self.tracker.register_waiter("telemetry", DEST)
        self.assertIn(("telemetry", DEST), self.tracker._response_waiters)
        self.assertEqual(len(self.tracker._response_waiters[("telemetry", DEST)]), 2)

        self.tracker.resolve_waiters("telemetry", DEST, {"batteryLevel": 64})

        self.assertTrue(first.done())
        self.assertTrue(second.done())
        self.assertEqual(first.result(), {"batteryLevel": 64})
        self.assertEqual(second.result(), {"batteryLevel": 64})
        # The key is popped wholesale once resolved.
        self.assertNotIn(("telemetry", DEST), self.tracker._response_waiters)

    def test_resolve_missing_key_is_a_noop(self) -> None:
        # Must not raise or create registry entries.
        self.tracker.resolve_waiters("position", DEST, {"latitude": 1})
        self.assertEqual(self.tracker._response_waiters, {})

    def test_kinds_are_independent_registry_keys(self) -> None:
        telemetry = self.tracker.register_waiter("telemetry", DEST)
        position = self.tracker.register_waiter("position", DEST)
        self.tracker.resolve_waiters("telemetry", DEST, {"batteryLevel": 1})
        self.assertTrue(telemetry.done())
        self.assertFalse(position.done())
        self.assertNotIn(("telemetry", DEST), self.tracker._response_waiters)
        self.assertIn(("position", DEST), self.tracker._response_waiters)

    def test_resolve_skips_already_done_future(self) -> None:
        future = self.tracker.register_waiter("telemetry", DEST)
        future.set_result({"early": True})
        self.tracker.resolve_waiters("telemetry", DEST, {"batteryLevel": 1})
        self.assertEqual(future.result(), {"early": True})

    def test_resolve_waiters_mixed_predating_and_post_request(self) -> None:
        """A single resolve can split a key: post-request replies resolve, and
        predating packets keep their waiter registered (re-added under lock)."""
        first = self.tracker.register_waiter("telemetry", DEST)
        second = self.tracker.register_waiter("telemetry", DEST)
        self.tracker._waiter_sent_at[first] = 500.0
        self.tracker._waiter_sent_at[second] = 1000.0
        # rx_time (700) is after first's request but before second's: first is a
        # reply, second is still a pre-armed broadcast.
        self.tracker.resolve_waiters("telemetry", DEST, {"batteryLevel": 5}, rx_time=700.0)
        self.assertTrue(first.done())
        self.assertEqual(first.result(), {"batteryLevel": 5})
        self.assertFalse(second.done())
        self.assertEqual(self.tracker._response_waiters[("telemetry", DEST)], [second])

    def test_resolve_predating_waiter_visible_to_abandon_all(self) -> None:
        """A predating waiter is re-registered atomically, so a link-drop
        abandon_all can still fail it fast instead of leaving it for timeout."""
        future = self.tracker.register_waiter("telemetry", DEST)
        sent_at = self.tracker._waiter_sent_at[future]
        self.tracker.resolve_waiters("telemetry", DEST, {"batteryLevel": 1}, rx_time=sent_at - 60)
        self.assertFalse(future.done())
        self.assertIn(("telemetry", DEST), self.tracker._response_waiters)

        self.tracker.abandon_all("connection lost")

        self.assertTrue(future.done())
        with self.assertRaises(_LinkLost) as caught:
            future.result()
        self.assertEqual(str(caught.exception), "connection lost")
        self.assertEqual(self.tracker._response_waiters, {})

    def test_resolve_does_not_rereregister_done_predating_waiter(self) -> None:
        """A predating waiter already settled (e.g. the abandon_all race) must
        not be re-registered — a done waiter is never re-armed for a later
        resolve."""
        future = self.tracker.register_waiter("telemetry", DEST)
        sent_at = self.tracker._waiter_sent_at[future]
        future.set_result({"settled": True})  # lost the settle race to abandon_all
        self.tracker.resolve_waiters("telemetry", DEST, {"batteryLevel": 1}, rx_time=sent_at - 60)
        self.assertEqual(self.tracker._response_waiters, {})
        self.assertEqual(future.result(), {"settled": True})

    def test_discard_removes_only_the_given_waiter(self) -> None:
        first = self.tracker.register_waiter("telemetry", DEST)
        second = self.tracker.register_waiter("telemetry", DEST)
        self.tracker.discard_waiter("telemetry", DEST, first)
        # The discarded waiter settles (cancelled) instead of lingering PENDING.
        self.assertTrue(first.cancelled())
        self.assertFalse(second.done())
        self.assertEqual(self.tracker._response_waiters[("telemetry", DEST)], [second])

    def test_discard_last_waiter_removes_the_key_and_cancels(self) -> None:
        future = self.tracker.register_waiter("telemetry", DEST)
        self.tracker.discard_waiter("telemetry", DEST, future)
        self.assertEqual(self.tracker._response_waiters, {})
        self.assertTrue(future.cancelled())

    def test_discard_unknown_waiter_is_a_noop(self) -> None:
        future = self.tracker.register_waiter("telemetry", DEST)
        stranger = ConcurrentFuture()
        self.tracker.discard_waiter("telemetry", DEST, stranger)
        self.tracker.discard_waiter("position", DEST, future)
        self.assertEqual(self.tracker._response_waiters[("telemetry", DEST)], [future])
        # The no-op discards did not settle anything.
        self.assertFalse(future.done())
        self.assertFalse(stranger.done())


class SolicitedTrackerAbandonTests(unittest.TestCase):
    def setUp(self) -> None:
        self.harness = _TrackerHarness()
        self.tracker = self.harness.tracker

    def test_abandon_all_settles_futures_with_reason_and_clears(self) -> None:
        futures = [
            self.tracker.register_waiter("telemetry", DEST),
            self.tracker.register_waiter("telemetry", DEST),
            self.tracker.register_waiter("traceroute", "!cafe1234"),
        ]
        self.tracker.abandon_all("connection lost")

        for future in futures:
            self.assertTrue(future.done())
            with self.assertRaises(_LinkLost) as caught:
                future.result()
            self.assertEqual(str(caught.exception), "connection lost")
        self.assertEqual(self.tracker._response_waiters, {})

    def test_abandon_all_is_idempotent_and_noop_when_empty(self) -> None:
        self.tracker.abandon_all("disconnect")  # empty registry: quiet no-op
        self.tracker.abandon_all("disconnect")
        self.assertEqual(self.tracker._response_waiters, {})

    def test_abandon_all_skips_done_futures(self) -> None:
        future = self.tracker.register_waiter("telemetry", DEST)
        future.set_result({"already": True})
        self.tracker.abandon_all("disconnect")
        self.assertEqual(future.result(), {"already": True})

    def test_abandon_all_swallows_waiter_settled_mid_loop(self) -> None:
        """A waiter settling between the done() check and set_exception must not
        raise: abandon_all tolerates the race where another thread wins."""
        future = self.tracker.register_waiter("telemetry", DEST)

        def racy_set_exception(exc):
            raise ConcurrentInvalidStateError("future already finished")

        with patch.object(future, "set_exception", racy_set_exception):
            self.tracker.abandon_all("connection lost")  # no InvalidStateError escapes
        # The registry is still cleared regardless of the lost settle race.
        self.assertEqual(self.tracker._response_waiters, {})

    def test_set_future_result_swallows_double_set(self) -> None:
        """_set_future_result on an already-settled waiter is a silent no-op
        (the losing racer), never an InvalidStateError."""
        future = self.tracker.register_waiter("telemetry", DEST)
        future.set_result({"first": True})
        SolicitedRequestTracker._set_future_result(future, {"second": True})
        self.assertEqual(future.result(), {"first": True})


class SolicitedTrackerMaybeResolveTests(unittest.TestCase):
    def setUp(self) -> None:
        self.harness = _TrackerHarness()
        self.tracker = self.harness.tracker

    def test_telemetry_payload_shape(self) -> None:
        future = self.tracker.register_waiter("telemetry", DEST)
        payload = {"deviceMetrics": {"batteryLevel": 64, "voltage": 3.91}}
        self.tracker.maybe_resolve(DEST, {"portnum": "TELEMETRY_APP", "telemetry": payload})
        self.assertTrue(future.done())
        self.assertEqual(future.result(), payload)

    def test_telemetry_numeric_portnum(self) -> None:
        future = self.tracker.register_waiter("telemetry", DEST)
        self.tracker.maybe_resolve(DEST, {"portnum": 67, "telemetry": {"x": 1}})
        self.assertTrue(future.done())
        self.assertEqual(future.result(), {"x": 1})

    def test_position_payload_shape(self) -> None:
        future = self.tracker.register_waiter("position", DEST)
        payload = {"latitude": 551885155, "longitude": 613386332, "altitude": 210}
        self.tracker.maybe_resolve(DEST, {"portnum": "POSITION_APP", "position": payload})
        self.assertTrue(future.done())
        self.assertEqual(future.result(), payload)

    def test_position_numeric_portnum(self) -> None:
        future = self.tracker.register_waiter("position", DEST)
        self.tracker.maybe_resolve(DEST, {"portnum": 3, "position": {"altitude": 5}})
        self.assertTrue(future.done())

    def test_traceroute_payload_shape(self) -> None:
        future = self.tracker.register_waiter("traceroute", DEST)
        payload = {"route": [0x9E77EDEC], "snrTowards": [24, -18]}
        self.tracker.maybe_resolve(DEST, {"portnum": "TRACEROUTE_APP", "traceroute": payload})
        self.assertTrue(future.done())
        self.assertEqual(future.result(), payload)

    def test_traceroute_route_discovery_fallback(self) -> None:
        future = self.tracker.register_waiter("traceroute", DEST)
        route = {"route": [0x9E77EDEC]}
        self.tracker.maybe_resolve(DEST, {"portnum": 70, "routeDiscovery": route})
        self.assertTrue(future.done())
        self.assertEqual(future.result(), route)

    def test_unknown_payload_is_a_noop(self) -> None:
        future = self.tracker.register_waiter("telemetry", DEST)
        self.tracker.maybe_resolve(DEST, {"portnum": "TEXT_APP", "text": "hello"})
        self.assertFalse(future.done())
        self.assertIn(("telemetry", DEST), self.tracker._response_waiters)

    def test_maybe_resolve_falls_back_to_top_level_payload(self) -> None:
        """Telemetry/position without a ``telemetry``/``position`` sub-key falls
        back to the whole decoded payload (older envelope shapes)."""
        telemetry_future = self.tracker.register_waiter("telemetry", DEST)
        self.tracker.maybe_resolve(DEST, {"portnum": 67, "batteryLevel": 1})
        self.assertTrue(telemetry_future.done())
        self.assertEqual(telemetry_future.result(), {"portnum": 67, "batteryLevel": 1})

        position_future = self.tracker.register_waiter("position", DEST)
        self.tracker.maybe_resolve(DEST, {"portnum": 3, "altitude": 5})
        self.assertTrue(position_future.done())
        self.assertEqual(position_future.result(), {"portnum": 3, "altitude": 5})

    def test_non_dict_decoded_is_a_noop(self) -> None:
        future = self.tracker.register_waiter("telemetry", DEST)
        decoded: Any = None
        self.tracker.maybe_resolve(DEST, decoded)
        self.assertFalse(future.done())

    def test_normalized_id_matching(self) -> None:
        future = self.tracker.register_waiter("telemetry", DEST)
        # Uppercase and bare (no bang) sender ids normalize to the registered key.
        self.tracker.maybe_resolve("AB12CD34", {"portnum": "TELEMETRY_APP", "telemetry": {"x": 1}})
        self.assertTrue(future.done())

    def test_maybe_resolve_rejects_packet_predating_request(self) -> None:
        """A pre-armed broadcast (rxTime < request time) is not a reply."""
        future = self.tracker.register_waiter("telemetry", DEST)
        sent_at = self.tracker._waiter_sent_at[future]

        self.tracker.maybe_resolve(
            DEST,
            {"portnum": "TELEMETRY_APP", "telemetry": {"batteryLevel": 10}},
            rx_time=sent_at - 60,
        )
        self.assertFalse(future.done())
        self.assertIn(("telemetry", DEST), self.tracker._response_waiters)

        # The genuine post-request reply resolves it.
        self.tracker.maybe_resolve(
            DEST,
            {"portnum": "TELEMETRY_APP", "telemetry": {"batteryLevel": 20}},
            rx_time=sent_at + 1,
        )
        self.assertTrue(future.done())
        self.assertEqual(future.result(), {"batteryLevel": 20})
        self.assertNotIn(("telemetry", DEST), self.tracker._response_waiters)

    def test_maybe_resolve_missing_or_garbage_rx_time_never_rejects(self) -> None:
        """Absent/unparseable rx_time is backward compatible: never reject."""
        future = self.tracker.register_waiter("telemetry", DEST)
        self.tracker.maybe_resolve(DEST, {"portnum": 67, "telemetry": {"x": 1}}, rx_time=None)
        self.assertTrue(future.done())

        second = self.tracker.register_waiter("telemetry", DEST)
        self.tracker.maybe_resolve(
            DEST, {"portnum": 67, "telemetry": {"x": 1}}, rx_time="not-a-number"
        )
        self.assertTrue(second.done())

        zero = self.tracker.register_waiter("telemetry", DEST)
        # Firmware's proto3 default/absent timestamp is 0 — not evidence the
        # packet predates the request, so it must never reject a genuine reply.
        self.tracker.maybe_resolve(DEST, {"portnum": 67, "telemetry": {"x": 1}}, rx_time=0)
        self.assertTrue(zero.done())

        float_zero = self.tracker.register_waiter("telemetry", DEST)
        self.tracker.maybe_resolve(DEST, {"portnum": 67, "telemetry": {"x": 1}}, rx_time=0.0)
        self.assertTrue(float_zero.done())

    def test_predates_never_rejects_cross_clock_domain_timestamps(self) -> None:
        """rxTime is the gateway's clock, not the host's: far-apart values must
        never classify a packet as predating (the genuine reply is kept)."""
        sent_at = 1_700_000_000.0
        # A timestamp far before the request (beyond the skew bound) is a
        # different clock domain — e.g. a gateway that synced from GPS or a
        # leading clock — so it is not treated as a pre-armed broadcast.
        self.assertFalse(self.tracker._predates_request(sent_at - 10 * 3600, sent_at))
        # A gateway without wall-clock sync stamps boot-seconds (uptime-sized).
        self.assertFalse(self.tracker._predates_request(1234, sent_at))
        self.assertFalse(self.tracker._predates_request(1, sent_at))
        # Future timestamps beyond the bound (host clock behind the gateway).
        self.assertFalse(self.tracker._predates_request(sent_at + 10 * 3600, sent_at))

    def test_predates_accepts_synced_clock_within_skew_bound(self) -> None:
        """On a synced gateway, a small clock skew still guards stale packets."""
        sent_at = 1_700_000_000.0
        self.assertTrue(self.tracker._predates_request(sent_at - 60, sent_at))
        self.assertFalse(self.tracker._predates_request(sent_at + 60, sent_at))
        # Boundary: exactly at the skew bound is not "beyond" it.
        bound = solicited._RX_TIME_SKEW_BOUND_SECS
        self.assertTrue(self.tracker._predates_request(sent_at - bound, sent_at))
        self.assertFalse(self.tracker._predates_request(sent_at - bound - 1, sent_at))


class SolicitedTrackerSolicitTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.harness = _TrackerHarness()
        self.tracker = self.harness.tracker

    async def test_solicit_returns_reply_payload(self) -> None:
        task = asyncio.create_task(
            self.tracker.solicit("telemetry", DEST, self.harness.sender("telemetry"), 5.0)
        )
        for _ in range(100):
            if self.tracker._response_waiters:
                break
            await asyncio.sleep(0.01)
        payload = {"deviceMetrics": {"batteryLevel": 64}}
        self.tracker.maybe_resolve(DEST, {"portnum": "TELEMETRY_APP", "telemetry": payload})
        out = await task
        self.assertTrue(out["ok"])
        self.assertEqual(out["data"], payload)
        self.assertEqual(self.harness.sent, [("telemetry", self.harness.iface)])
        self.assertEqual(self.tracker._response_waiters, {})

    async def test_solicit_timeout_returns_did_not_answer_and_discards(self) -> None:
        out = await self.tracker.solicit("position", DEST, self.harness.sender("position"), 0.05)
        self.assertFalse(out["ok"])
        self.assertIn("did not answer", out["error"])
        self.assertIn(DEST, out["error"])
        self.assertEqual(self.tracker._response_waiters, {})

    async def test_solicit_cancelled_reraises_and_discards(self) -> None:
        """A8 pin: CancelledError propagates, and the waiter is not leaked."""
        task = asyncio.create_task(
            self.tracker.solicit("telemetry", DEST, self.harness.sender("telemetry"), 30.0)
        )
        for _ in range(100):
            if self.tracker._response_waiters:
                break
            await asyncio.sleep(0.01)
        self.assertTrue(self.tracker._response_waiters)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.tracker._response_waiters, {})

    async def test_solicit_send_failure_discards_and_reports(self) -> None:
        self.harness = _TrackerHarness(executor=_FakeExecutor(fail=RuntimeError("boom")))
        self.tracker = self.harness.tracker
        out = await self.tracker.solicit("telemetry", DEST, self.harness.sender("telemetry"), 5.0)
        self.assertFalse(out["ok"])
        self.assertIn("Could not send telemetry request", out["error"])
        self.assertEqual(self.tracker._response_waiters, {})

    async def test_solicit_send_base_exception_discards_waiter_and_reraises(self) -> None:
        """A SystemExit from the transport (the library calls our_exit when a
        destination can't be resolved) must not leak the waiter: discard, then
        propagate the BaseException like the CancelledError branch."""
        for exc in (SystemExit("bad id"), KeyboardInterrupt()):
            harness = _TrackerHarness(executor=_FakeExecutor(fail=exc))
            tracker = harness.tracker
            with self.assertRaises(type(exc)):
                await tracker.solicit("telemetry", DEST, harness.sender("telemetry"), 5.0)
            self.assertEqual(tracker._response_waiters, {})
            self.assertEqual(tracker._waiter_sent_at, {})

    async def test_solicit_no_interfaces_reports_error(self) -> None:
        tracker = SolicitedRequestTracker(
            normalize_node_id=_normalize_node_id,
            interfaces_provider=lambda: [],
            executor_provider=lambda: self.harness.executor,
            link_lost_exc=_LinkLost,
        )
        out = await tracker.solicit("telemetry", DEST, self.harness.sender("telemetry"), 5.0)
        self.assertFalse(out["ok"])
        self.assertIn("No active Meshtastic interfaces", out["error"])
        self.assertEqual(tracker._response_waiters, {})

    async def test_solicit_none_executor_reports_error(self) -> None:
        tracker = SolicitedRequestTracker(
            normalize_node_id=_normalize_node_id,
            interfaces_provider=lambda: [object()],
            executor_provider=lambda: None,
            link_lost_exc=_LinkLost,
        )
        out = await tracker.solicit("telemetry", DEST, self.harness.sender("telemetry"), 5.0)
        self.assertFalse(out["ok"])
        self.assertIn("No active Meshtastic interfaces", out["error"])
        self.assertEqual(tracker._response_waiters, {})

    async def test_solicit_immediate_timeout_reports_did_not_answer(self) -> None:
        """A timeout <= 0 never waits — the waiter is discarded right away."""
        out = await self.tracker.solicit("telemetry", DEST, self.harness.sender("telemetry"), 0)
        self.assertFalse(out["ok"])
        self.assertIn("did not answer", out["error"])
        self.assertEqual(self.tracker._response_waiters, {})

    async def test_solicit_abandoned_via_abandon_all(self) -> None:
        """A9 pin: abandon_all fails the in-flight solicit fast with the link reason."""
        task = asyncio.create_task(
            self.tracker.solicit("traceroute", DEST, self.harness.sender("traceroute"), 30.0)
        )
        for _ in range(100):
            if self.tracker._response_waiters:
                break
            await asyncio.sleep(0.01)
        self.tracker.abandon_all("connection lost")
        out = await task
        self.assertFalse(out["ok"])
        self.assertIn("link dropped", out["error"])
        self.assertIn("connection lost", out["error"])
        self.assertEqual(self.tracker._response_waiters, {})

    async def test_solicit_uses_normalized_registry_key(self) -> None:
        """The waiter is registered under the normalized id (A9 matching contract)."""
        task = asyncio.create_task(
            self.tracker.solicit("telemetry", "AB12CD34", self.harness.sender("telemetry"), 5.0)
        )
        for _ in range(100):
            if self.tracker._response_waiters:
                break
            await asyncio.sleep(0.01)
        self.assertIn(("telemetry", DEST), self.tracker._response_waiters)
        self.tracker.maybe_resolve(DEST, {"portnum": "TELEMETRY_APP", "telemetry": {"x": 1}})
        out = await task
        self.assertTrue(out["ok"])
        self.assertEqual(out["data"], {"x": 1})

    async def test_solicit_cancel_during_send_discards_waiter(self) -> None:
        """A8 pin: cancellation while the transmit await is pending (send still
        running on the transport worker) must not leak the waiter."""
        executor = _DaemonTransportExecutor(name="solicit-cancel-test")
        try:
            harness = _TrackerHarness(executor=executor)
            tracker = harness.tracker
            started = threading.Event()
            release = threading.Event()

            def send(iface) -> None:
                started.set()
                release.wait(timeout=5)
                return None

            task = asyncio.create_task(tracker.solicit("telemetry", DEST, send, 30.0))
            self.assertTrue(await asyncio.to_thread(started.wait, 5.0))
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertEqual(tracker._response_waiters, {})
        finally:
            release.set()
            executor.shutdown(wait=True, timeout=5)


class SolicitedTrackerRaceTests(unittest.TestCase):
    """Concurrency pins: settle-once semantics under racing registry access."""

    def setUp(self) -> None:
        self.harness = _TrackerHarness()
        self.tracker = self.harness.tracker

    def test_resolve_vs_abandon_race_settles_each_waiter_exactly_once(self) -> None:
        """resolve_waiters and abandon_all may race on the same key.

        Every waiter must settle exactly once — either with the payload or
        with the link-lost exception — no InvalidStateError may escape, and
        nothing may stay unresolved or registered.
        """
        errors: list[BaseException] = []

        def worker() -> None:
            for _ in range(30):
                self.tracker.resolve_waiters("telemetry", DEST, {"batteryLevel": 1})
                self.tracker.abandon_all("race")

        for _ in range(150):
            futures = [
                self.tracker.register_waiter("telemetry", DEST),
                self.tracker.register_waiter("telemetry", DEST),
            ]
            threads = [threading.Thread(target=worker) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=5)
            self.assertFalse(any(thread.is_alive() for thread in threads))
            self.assertEqual(self.tracker._response_waiters, {})
            for future in futures:
                self.assertTrue(future.done(), "waiter left unresolved")
                try:
                    future.result()
                except _LinkLost:
                    pass
                except Exception as exc:
                    errors.append(exc)
        self.assertEqual(errors, [])


class StubNormalizeDriftCheck(unittest.TestCase):
    """Guard against the hand-reimplemented ``_normalize_node_id`` mirror
    drifting from production. The tracker tests deliberately do not import the
    adapter, so this cross-check runs only when the adapter is importable and
    fails loudly if the two canonicalize differently over the tricky matrix."""

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
                    _normalize_node_id(value),
                    MeshtasticAdapter._normalize_node_id(value),
                )


if __name__ == "__main__":
    unittest.main()
