"""Unit tests for the ACK/NACK state machine (ack_state).

Pure-logic tests that don't need an assembled adapter. Integration coverage of
the tracker (lock ordering, lifecycle checks, send()-level ACK waits, pruning)
remains in test_meshtastic.py and exercises AckTracker through the adapter's
thin delegates.

Beyond the pure ``is_retriable_failure`` / callback-naming pins, this module
pins the decision tables of the two complex tracker functions that used to be
covered only through the 3,800-line integration test (P3.2, before the P3.3
refactor lands):

* ``_track_pending_ack`` (cc 28): ACK-window classification (early / implicit /
  real), waiter registration (tokens, dest, bytes, sent_at), packet-id
  collision handling, the not-running settle path, and the pubsub upgrade
  interaction via ``_maybe_record_pubsub_ack``.
* ``_record_ack_response`` (cc 41): the real-vs-implicit-vs-NAK verdict table,
  malformed packets, lifecycle-staleness, and send-token defense.
* ``_maybe_record_pubsub_ack``: the pubsub path that upgrades an IMPLICIT_ACK
  record to a real ACK.

Tests run against a minimal adapter stub (``_StubAdapter``) that satisfies only
the attributes AckTracker actually dereferences — evidence for the R4 lifecycle
Protocol refactor. Locks are real ``threading.Lock`` instances so the
documented lifecycle_lock → ack_lock ExitStack ordering is exercised, not
mocked.
"""

import asyncio
import os
import sys
import threading
import unittest
from concurrent.futures import Future as ConcurrentFuture
from typing import Any
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
hermes_agent_path = os.getenv("HERMES_AGENT_PATH", os.path.expanduser("~/.hermes/hermes-agent"))
if os.path.isdir(hermes_agent_path):
    sys.path.append(hermes_agent_path)

from gateway.platforms.base import SendResult

import ack_state
from ack_state import (
    ACK_RECORD_LIMIT,
    INTERNAL_NAK_DUPLICATE_PACKET_ID,
    PERMANENT_NAK_REASONS,
    AckStatus,
)

DEST = "!deadbeef"  # ! + 8 lowercase hex, the canonical node id form
RELAY = "!aaaa1111"


class _StubAdapter:
    """Minimal stand-in for ``MeshtasticAdapter`` (the AckTracker back-ref).

    Pins the exact adapter surface ``AckTracker`` dereferences today — the
    evidence base for a narrow LifecycleHost Protocol on the tracker. Grep of
    ack_state.py's ``self._adapter.`` accesses shows these methods read only:

    * ``_track_pending_ack``: ``loop``, ``_cross_loop_send_logged``,
      ``_running``, ``ACK_RECORD_LIMIT`` (via ``_prune_ack_history_locked``).
    * ``_record_ack_response``: ``_normalize_node_id``, ``_lifecycle_lock``,
      ``_lifecycle_id``, ``_running``, ``ACK_RECORD_LIMIT``.

    Nothing else on the adapter is touched. The locks are real so the
    documented ``_lifecycle_lock`` → ``_ack_lock`` ordering stays real.
    """

    def __init__(self) -> None:
        self.loop: asyncio.AbstractEventLoop | None = None
        self._running = True
        self._cross_loop_send_logged = False
        self.ACK_RECORD_LIMIT = ACK_RECORD_LIMIT
        self._lifecycle_lock = threading.Lock()
        self._lifecycle_id = 0

    @staticmethod
    def _normalize_node_id(node_id: Any) -> str | None:
        """Mirror of adapter.MeshtasticAdapter._normalize_node_id (adapter.py:808)."""
        if node_id is None:
            return None
        if isinstance(node_id, bool):
            return str(node_id).lower()
        if isinstance(node_id, int):
            return f"!{node_id:08x}"
        text = str(node_id).strip()
        if not text:
            return None
        low = text.lower()
        bare = low[1:] if low.startswith("!") else low
        if len(bare) == 8 and all(c in "0123456789abcdef" for c in bare):
            return f"!{bare}"
        return low


class _TrackerTestCase(unittest.TestCase):
    """Shared stub/tracker/packet plumbing (underscore: never collected)."""

    def setUp(self) -> None:
        self.adapter = _StubAdapter()
        self.tracker = ack_state.AckTracker(self.adapter)

    def packet(
        self, request_id, from_id=None, to_id=None, error_reason=None, request_key="requestId"
    ):
        """Build a pubsub-style routing packet for a given request id."""
        decoded = {"routing": {}}
        if error_reason is not None:
            decoded["routing"]["errorReason"] = error_reason
        decoded[request_key] = request_id
        pkt = {"id": f"wire-{request_id}", "decoded": decoded}
        if from_id is not None:
            pkt["fromId"] = from_id
        if to_id is not None:
            pkt["toId"] = to_id
        return pkt


class TestTrackPendingAck(_TrackerTestCase):
    """Decision table for ``_track_pending_ack`` (cc 28)."""

    def test_empty_packet_id_registers_nothing(self):
        self.assertIsNone(self.tracker._track_pending_ack(None, DEST, "hi"))
        self.assertIsNone(self.tracker._track_pending_ack("", DEST, "hi"))
        self.assertEqual(self.tracker._pending_acks, {})

    def test_registers_pending_record_without_future(self):
        # Fire-and-forget sends (no waiter) still get bookkeeping: dest, bytes,
        # sent_at and the token. Registration touches no interface/transport
        # state — pure ACK-dict bookkeeping.
        token = object()
        fut = self.tracker._track_pending_ack("1001", DEST, "hello", send_token=token)
        self.assertIsNone(fut)
        rec = self.tracker.get_ack_status("1001")
        self.assertEqual(rec["status"], AckStatus.PENDING)
        self.assertEqual(rec["dest"], DEST)
        self.assertEqual(rec["bytes"], len(b"hello"))
        self.assertIn("sent_at", rec)
        self.assertIs(self.tracker._ack_tokens["1001"], token)
        self.assertNotIn("1001", self.tracker._ack_futures)

    def test_create_future_registers_waiter(self):
        fut = self.tracker._track_pending_ack("1002", DEST, "hi", create_future=True)
        self.assertIsNotNone(fut)
        self.assertFalse(fut.done())
        self.assertIs(self.tracker._ack_futures["1002"], fut)
        self.assertEqual(self.tracker.get_ack_status("1002")["status"], AckStatus.PENDING)

    def test_early_real_ack_resolves_new_waiter_immediately(self):
        # Early ACK window: the definitive verdict landed before the waiter
        # existed; the new future must settle immediately with that record.
        self.tracker._record_ack_response(self.packet("1003", from_id=DEST, to_id=DEST), DEST, "hi")
        fut = self.tracker._track_pending_ack("1003", DEST, "hi", create_future=True)
        self.assertIsNotNone(fut)
        self.assertTrue(fut.done())
        self.assertEqual(fut.result()["status"], AckStatus.ACK)

    def test_early_implicit_ack_keeps_waiter_open(self):
        # Implicit ACK window: a relay confirmation is NOT definitive, so the
        # waiter stays open for a real ACK (or timeout) to decide.
        self.tracker._record_ack_response(self.packet("1004", from_id=RELAY), DEST, "hi")
        fut = self.tracker._track_pending_ack("1004", DEST, "hi", create_future=True)
        self.assertIsNotNone(fut)
        self.assertFalse(fut.done())
        self.assertIs(self.tracker._ack_futures["1004"], fut)
        self.assertEqual(self.tracker.get_ack_status("1004")["status"], AckStatus.IMPLICIT_ACK)

    def test_active_waiter_collision_settles_old_waiter(self):
        # Retry-classification row: a reused packet id against a live waiter is
        # a definitive NAK (DUPLICATE_PACKET_ID) and is never retried — the
        # chunk was already transmitted, a retry would duplicate it on-air.
        old_token, new_token = object(), object()
        old_fut = self.tracker._track_pending_ack(
            "1005", DEST, "old", create_future=True, send_token=old_token
        )
        with self.assertLogs("ack_state", level="WARNING") as cm:
            result = self.tracker._track_pending_ack("1005", DEST, "new", send_token=new_token)
        self.assertIsNone(result)
        self.assertTrue(old_fut.done())
        self.assertEqual(old_fut.result()["status"], AckStatus.NAK)
        rec = self.tracker.get_ack_status("1005")
        self.assertEqual(rec["error_reason"], INTERNAL_NAK_DUPLICATE_PACKET_ID)
        self.assertIs(self.tracker._ack_tokens["1005"], new_token)
        self.assertIn("collision", cm.output[0])
        self.assertFalse(
            ack_state.is_retriable_failure(SendResult(success=False, raw_response={"ack": rec}))
        )

    def test_collision_with_new_waiter_fails_both_generations(self):
        # Both senders create waiters and the id is reused mid-flight: the old
        # waiter is settled with the collision AND the new waiter fails fast,
        # with the id poisoned so neither generation's ACK can overwrite it.
        old_token, new_token = object(), object()
        old_fut = self.tracker._track_pending_ack(
            "1006", DEST, "old", create_future=True, send_token=old_token
        )
        new_fut = self.tracker._track_pending_ack(
            "1006", DEST, "new", create_future=True, send_token=new_token
        )
        self.assertIsNotNone(new_fut)
        self.assertTrue(old_fut.done())
        self.assertTrue(new_fut.done())
        self.assertEqual(old_fut.result()["error_reason"], INTERNAL_NAK_DUPLICATE_PACKET_ID)
        self.assertEqual(new_fut.result()["error_reason"], INTERNAL_NAK_DUPLICATE_PACKET_ID)
        rec = self.tracker.get_ack_status("1006")
        self.assertEqual(rec["status"], AckStatus.NAK)
        # Poisoned: neither the old nor the new token is the owner anymore.
        with self.assertLogs("ack_state", level="DEBUG") as cm:
            self.tracker._record_ack_response(
                self.packet("1006", from_id=DEST), DEST, "old", send_token=old_token
            )
            self.tracker._record_ack_response(
                self.packet("1006", from_id=DEST), DEST, "new", send_token=new_token
            )
        self.assertEqual(
            self.tracker.get_ack_status("1006")["error_reason"], INTERNAL_NAK_DUPLICATE_PACKET_ID
        )
        self.assertEqual(sum(1 for m in cm.output if "stale ACK callback" in m), 2)

    def test_prune_evicts_oldest_completed_records(self):
        # _prune_ack_history_locked bounds growth to ACK_RECORD_LIMIT, evicting
        # the oldest non-waited records (and their tokens) first.
        self.adapter.ACK_RECORD_LIMIT = 5
        for i in range(6):
            token = object()
            self.tracker._track_pending_ack(str(i), DEST, "hi", send_token=token)
        self.assertIsNone(self.tracker.get_ack_status("0"))
        self.assertNotIn("0", self.tracker._ack_tokens)
        for i in range(1, 6):
            self.assertIsNotNone(self.tracker.get_ack_status(str(i)), f"{i} should survive")

    def test_reused_id_with_new_waiter_fails_fast(self):
        # Token-reuse collision: the older send's id is still tracked, so the
        # new waiter is failed with a collision rather than risking a
        # misattributed delayed wire ACK from the old generation.
        old_token, new_token = object(), object()
        old_fut = self.tracker._track_pending_ack(
            "1006", DEST, "old", create_future=True, send_token=old_token
        )
        self.tracker._record_ack_response(
            self.packet("1006", from_id=DEST, to_id=DEST), DEST, "old", send_token=old_token
        )
        self.assertTrue(old_fut.done())
        new_fut = self.tracker._track_pending_ack(
            "1006", DEST, "new", create_future=True, send_token=new_token
        )
        self.assertIsNotNone(new_fut)
        self.assertTrue(new_fut.done())
        rec = new_fut.result()
        self.assertEqual(rec["status"], AckStatus.NAK)
        self.assertEqual(rec["error_reason"], INTERNAL_NAK_DUPLICATE_PACKET_ID)
        # The id is poisoned: a delayed wire ACK from the old generation is stale.
        with self.assertLogs("ack_state", level="DEBUG") as cm:
            self.tracker._record_ack_response(
                self.packet("1006", from_id=DEST), DEST, "old", send_token=old_token
            )
        self.assertEqual(
            self.tracker.get_ack_status("1006")["error_reason"], INTERNAL_NAK_DUPLICATE_PACKET_ID
        )
        self.assertTrue(any("stale ACK callback" in m for m in cm.output))

    def test_not_running_settles_new_waiter_as_disconnected(self):
        self.adapter._running = False
        fut = self.tracker._track_pending_ack("1007", DEST, "hi", create_future=True)
        self.assertIsNotNone(fut)
        self.assertTrue(fut.done())
        rec = fut.result()
        self.assertEqual(rec["status"], AckStatus.TIMEOUT)
        self.assertEqual(rec["error_reason"], "DISCONNECTED")
        self.assertNotIn("1007", self.tracker._ack_futures)
        self.assertFalse(
            ack_state.is_retriable_failure(SendResult(success=False, raw_response={"ack": rec}))
        )

    def test_not_running_preserves_early_definitive_verdict(self):
        self.tracker._record_ack_response(self.packet("1008", from_id=DEST, to_id=DEST), DEST, "hi")
        self.adapter._running = False
        fut = self.tracker._track_pending_ack("1008", DEST, "hi", create_future=True)
        self.assertIsNotNone(fut)
        self.assertTrue(fut.done())
        self.assertEqual(fut.result()["status"], AckStatus.ACK)

    def test_not_running_with_early_implicit_settles_disconnected(self):
        # An early IMPLICIT_ACK (relay confirmation) followed by an immediate
        # disconnect before waiter registration is rewritten to
        # TIMEOUT/DISCONNECTED — not preserved as IMPLICIT_ACK. The outcome is
        # correct (DISCONNECTED is non-retriable so a carried packet is not
        # duplicated), at the cost of losing the "relay confirmed" label.
        # This pins the documented behavior (see _settle_not_running_record).
        self.tracker._record_ack_response(self.packet("1008b", from_id=RELAY), DEST, "hi")
        self.adapter._running = False
        fut = self.tracker._track_pending_ack("1008b", DEST, "hi", create_future=True)
        self.assertIsNotNone(fut)
        self.assertTrue(fut.done())
        rec = fut.result()
        self.assertEqual(rec["status"], AckStatus.TIMEOUT)
        self.assertEqual(rec["error_reason"], "DISCONNECTED")
        self.assertFalse(
            ack_state.is_retriable_failure(SendResult(success=False, raw_response={"ack": rec}))
        )

    def test_cross_loop_send_logged_once(self):
        # Cross-loop send: a waiter created on a different event loop than the
        # platform loop logs INFO exactly once per adapter instance.
        self.adapter.loop = asyncio.new_event_loop()
        try:

            async def scenario():
                with self.assertLogs("ack_state", level="INFO") as cm:
                    fut = self.tracker._track_pending_ack("1009", DEST, "hi", create_future=True)
                    self.tracker._track_pending_ack("1010", DEST, "hi", create_future=True)
                self.assertTrue(self.adapter._cross_loop_send_logged)
                self.assertEqual(sum(1 for m in cm.output if "different event loop" in m), 1)
                self.assertIsNotNone(fut)
                self.assertFalse(fut.done())

            asyncio.run(scenario())
        finally:
            self.adapter.loop.close()

    def test_fail_pending_acks_preserves_existing_timeout_reason(self):
        # A waiter already stamped TIMEOUT within the timeout->finally gap must
        # keep its original reason (ACK_TIMEOUT), not be rewritten to DISCONNECTED.
        pkt_id = "timeout-preserve-1"
        fut = ConcurrentFuture()
        with self.tracker._ack_lock:
            self.tracker._pending_acks[pkt_id] = {
                "status": AckStatus.TIMEOUT,
                "error_reason": "ACK_TIMEOUT",
                "dest": DEST,
            }
            self.tracker._ack_futures[pkt_id] = fut
        self.tracker._fail_pending_acks()
        self.assertEqual(fut.result(timeout=0.1)["error_reason"], "ACK_TIMEOUT")
        rec = self.tracker.get_ack_status(pkt_id)
        self.assertEqual(rec["status"], AckStatus.TIMEOUT)
        self.assertEqual(rec["error_reason"], "ACK_TIMEOUT")

    def test_fail_pending_acks_pending_becomes_disconnected(self):
        # A still-PENDING waiter is still settled as DISCONNECTED (not retriable).
        pkt_id = "pending-disc-1"
        fut = ConcurrentFuture()
        with self.tracker._ack_lock:
            self.tracker._pending_acks[pkt_id] = {"status": AckStatus.PENDING, "dest": DEST}
            self.tracker._ack_futures[pkt_id] = fut
        self.tracker._fail_pending_acks()
        record = fut.result(timeout=0.1)
        self.assertEqual(record["status"], AckStatus.TIMEOUT)
        self.assertEqual(record["error_reason"], "DISCONNECTED")
        self.assertFalse(
            ack_state.is_retriable_failure(
                SendResult(success=False, raw_response={"ack": self.tracker.get_ack_status(pkt_id)})
            )
        )


class TestRecordAckResponse(_TrackerTestCase):
    """Decision table for ``_record_ack_response`` (cc 41)."""

    def test_plain_ack_from_destination_resolves_waiter(self):
        fut = self.tracker._track_pending_ack("2001", DEST, "hi", create_future=True)
        self.tracker._record_ack_response(self.packet("2001", from_id=DEST, to_id=DEST), DEST, "hi")
        self.assertTrue(fut.done())
        rec = self.tracker.get_ack_status("2001")
        self.assertEqual(rec["status"], AckStatus.ACK)
        self.assertEqual(rec["dest"], DEST)
        self.assertEqual(rec["ack_from"], DEST)
        self.assertEqual(rec["error_reason"], None)
        self.assertEqual(rec["response"]["request_id"], "2001")
        self.assertEqual(rec["response"]["to_id"], DEST)
        self.assertNotIn("2001", self.tracker._ack_futures)

    def test_ack_without_sender_counts_as_real(self):
        # Backward-compat row: a missing sender (ack_from None) still ACKs.
        fut = self.tracker._track_pending_ack("2002", DEST, "hi", create_future=True)
        self.tracker._record_ack_response(self.packet("2002", to_id=DEST), DEST, "hi")
        self.assertTrue(fut.done())
        self.assertEqual(fut.result()["status"], AckStatus.ACK)
        self.assertIsNone(fut.result()["ack_from"])

    def test_ack_from_fallback_key(self):
        # ``from`` is accepted as a fallback when ``fromId`` is absent.
        fut = self.tracker._track_pending_ack("2003", DEST, "hi", create_future=True)
        packet = self.packet("2003", to_id=DEST)
        packet["from"] = DEST
        self.tracker._record_ack_response(packet, DEST, "hi")
        self.assertEqual(fut.result()["ack_from"], DEST)

    def test_relayed_ack_is_implicit_and_keeps_waiter_open(self):
        fut = self.tracker._track_pending_ack("2004", DEST, "hi", create_future=True)
        self.tracker._record_ack_response(self.packet("2004", from_id=RELAY), DEST, "hi")
        rec = self.tracker.get_ack_status("2004")
        self.assertEqual(rec["status"], AckStatus.IMPLICIT_ACK)
        self.assertEqual(rec["ack_from"], RELAY)
        self.assertFalse(fut.done())  # relay confirmation is not delivery
        self.assertIs(self.tracker._ack_futures["2004"], fut)

    def test_implicit_never_downgrades_definitive_verdict(self):
        fut = self.tracker._track_pending_ack("2005", DEST, "hi", create_future=True)
        self.tracker._record_ack_response(self.packet("2005", from_id=DEST, to_id=DEST), DEST, "hi")
        self.assertTrue(fut.done())
        with self.assertLogs("ack_state", level="DEBUG") as cm:
            self.tracker._record_ack_response(self.packet("2005", from_id=RELAY), DEST, "hi")
        rec = self.tracker.get_ack_status("2005")
        self.assertEqual(rec["status"], AckStatus.ACK)
        self.assertEqual(rec["ack_from"], DEST)  # original verdict preserved
        self.assertTrue(any("ignored after definitive" in m for m in cm.output))

    def test_non_node_dest_always_acks(self):
        # The real-vs-implicit split applies to DMs only (dest is a !node id).
        fut = self.tracker._track_pending_ack(
            "2006", "meshtastic:channel:0", "hi", create_future=True
        )
        self.tracker._record_ack_response(
            self.packet("2006", from_id=RELAY), "meshtastic:channel:0", "hi"
        )
        self.assertTrue(fut.done())
        self.assertEqual(fut.result()["status"], AckStatus.ACK)

    def test_nak_verdict_for_permanent_and_transient_reasons(self):
        # The verdict is NAK for ANY errorReason; permanence only matters for
        # the retry decision (pinned in TestRetriabilityClassification).
        for reason in ("TOO_LARGE", "NOT_AUTHORIZED", "NO_ROUTE", "MAX_RETRANSMIT"):
            with self.subTest(reason=reason):
                tracker = ack_state.AckTracker(_StubAdapter())
                fut = tracker._track_pending_ack("2101", DEST, "hi", create_future=True)
                tracker._record_ack_response(
                    self.packet("2101", from_id=DEST, to_id=DEST, error_reason=reason), DEST, "hi"
                )
                self.assertTrue(fut.done())
                rec = tracker.get_ack_status("2101")
                self.assertEqual(rec["status"], AckStatus.NAK)
                self.assertEqual(rec["error_reason"], reason)
                self.assertNotIn("2101", tracker._ack_futures)

    def test_error_reason_none_token_is_not_a_nak(self):
        fut = self.tracker._track_pending_ack("2007", DEST, "hi", create_future=True)
        self.tracker._record_ack_response(
            self.packet("2007", from_id=DEST, to_id=DEST, error_reason="NONE"), DEST, "hi"
        )
        self.assertTrue(fut.done())
        self.assertEqual(self.tracker.get_ack_status("2007")["status"], AckStatus.ACK)

    def test_malformed_packet_without_request_id_is_dropped(self):
        # No decoded/routing/requestId: the ACK cannot be tied to an outbound
        # packet, so it must not persist an orphan "unknown" record that a
        # forged id-less routing packet could inflate. Log-only.
        with self.assertLogs("ack_state", level="DEBUG") as cm:
            self.tracker._record_ack_response({"id": "garbage"}, DEST, "")
        self.assertIsNone(self.tracker.get_ack_status("unknown"))
        self.assertNotIn("unknown", self.tracker._pending_acks)
        self.assertNotIn("unknown", self.tracker._ack_responses)
        self.assertTrue(any("without a request id" in m for m in cm.output))

    def test_lifecycle_stale_callback_ignored(self):
        fut = self.tracker._track_pending_ack("2008", DEST, "hi", create_future=True)
        self.adapter._lifecycle_id = 1
        with self.assertLogs("ack_state", level="DEBUG") as cm:
            self.tracker._record_ack_response(
                self.packet("2008", from_id=DEST), DEST, "hi", lifecycle_id=0
            )
        self.assertFalse(fut.done())
        self.assertEqual(self.tracker.get_ack_status("2008")["status"], AckStatus.PENDING)
        self.assertTrue(any("stale lifecycle" in m for m in cm.output))

    def test_lifecycle_not_running_callback_ignored(self):
        fut = self.tracker._track_pending_ack("2009", DEST, "hi", create_future=True)
        self.adapter._running = False
        self.tracker._record_ack_response(
            self.packet("2009", from_id=DEST), DEST, "hi", lifecycle_id=0
        )
        self.assertFalse(fut.done())
        self.assertEqual(self.tracker.get_ack_status("2009")["status"], AckStatus.PENDING)

    def test_lifecycle_current_callback_applied(self):
        fut = self.tracker._track_pending_ack("2010", DEST, "hi", create_future=True)
        self.tracker._record_ack_response(
            self.packet("2010", from_id=DEST, to_id=DEST), DEST, "hi", lifecycle_id=0
        )
        self.assertTrue(fut.done())
        self.assertEqual(self.tracker.get_ack_status("2010")["status"], AckStatus.ACK)

    def test_stale_send_token_ignored(self):
        old_token, wrong_token = object(), object()
        fut = self.tracker._track_pending_ack(
            "2011", DEST, "hi", create_future=True, send_token=old_token
        )
        with self.assertLogs("ack_state", level="DEBUG") as cm:
            self.tracker._record_ack_response(
                self.packet("2011", from_id=DEST), DEST, "hi", send_token=wrong_token
            )
        self.assertFalse(fut.done())
        self.assertEqual(self.tracker.get_ack_status("2011")["status"], AckStatus.PENDING)
        self.assertTrue(any("stale ACK callback" in m for m in cm.output))

    def test_matching_send_token_recorded_with_response_token(self):
        token = object()
        self.tracker._track_pending_ack("2012", DEST, "hi", send_token=token)
        self.tracker._record_ack_response(
            self.packet("2012", from_id=DEST), DEST, "hi", send_token=token
        )
        self.assertEqual(self.tracker.get_ack_status("2012")["status"], AckStatus.ACK)
        self.assertIs(self.tracker._ack_response_tokens["2012"], token)

    def test_inflight_token_stages_early_ack(self):
        # sendText can invoke onAckNak before returning the packet id; the
        # response is staged by send generation, not written to history.
        token = object()
        self.tracker._ack_inflight_tokens[token] = 5
        self.tracker._record_ack_response(
            self.packet("2013", from_id=DEST), DEST, "hi", send_token=token
        )
        self.assertNotIn("2013", self.tracker._pending_acks)
        staged = self.tracker._early_ack_packets[token]
        self.assertEqual(staged[0]["id"], "wire-2013")
        self.assertEqual(staged[1], DEST)
        self.assertEqual(staged[2], "hi")
        self.assertEqual(staged[3], 5)

    def test_inflight_token_lifecycle_mismatch_dropped(self):
        self.adapter._lifecycle_id = 7
        token = object()
        self.tracker._ack_inflight_tokens[token] = 5
        self.tracker._record_ack_response(
            self.packet("2014", from_id=DEST), DEST, "hi", send_token=token, lifecycle_id=7
        )
        self.assertNotIn(token, self.tracker._early_ack_packets)

    def test_tokenless_callback_dropped_after_lifecycle_turnover(self):
        # The tokenless _make_ack_callback shim synthesizes the current
        # lifecycle id, so a callback that fires after disconnect/reconnect is
        # dropped rather than written into the fresh lifecycle's stores.
        callback = self.tracker._make_ack_callback(DEST, "hi")
        self.adapter._running = False
        self.adapter._lifecycle_id = 1
        with self.assertLogs("ack_state", level="DEBUG") as cm:
            callback(self.packet("88099", from_id=DEST, to_id=DEST))
        self.assertIsNone(self.tracker.get_ack_status("88099"))
        self.assertNotIn("88099", self.tracker._ack_responses)
        self.assertTrue(any("stale lifecycle" in m for m in cm.output))

    def test_tokenless_callback_current_lifecycle_records(self):
        callback = self.tracker._make_ack_callback(DEST, "hi")
        callback(self.packet("88100", from_id=DEST, to_id=DEST))
        self.assertEqual(self.tracker.get_ack_status("88100")["status"], AckStatus.ACK)

    def test_record_ack_response_with_non_dict_decoded_does_not_raise(self):
        # Attacker-influenceable packet fields (decoded/routing) must not
        # raise AttributeError when non-dict truthy values surface off the
        # radio. The sibling _maybe_record_pubsub_ack guards both;
        # _record_ack_response must mirror that defense.
        self.tracker._track_pending_ack("2015", DEST, "hi", create_future=True)
        # Non-dict decoded (string): decoded defaults to {}, pkt_id becomes
        # "unknown", packet is dropped (log-only). No exception, no record.
        self.tracker._record_ack_response({"decoded": "garbage"}, DEST, "hi")
        self.assertEqual(self.tracker.get_ack_status("2015")["status"], AckStatus.PENDING)
        # decoded is a dict but routing is a non-dict truthy string: routing
        # defaults to {} so error_reason is None. The relay sender ≠ dest →
        # IMPLICIT_ACK (not definitive); waiter stays open, no exception.
        self.tracker._record_ack_response(
            {"decoded": {"requestId": "2015", "routing": "x"}, "fromId": RELAY}, DEST, "hi"
        )
        self.assertEqual(self.tracker.get_ack_status("2015")["status"], AckStatus.IMPLICIT_ACK)

    def test_second_definitive_does_not_overwrite_first(self):
        # First-definitive-wins: a late NAK after a real ACK (or vice versa)
        # for the same pkt_id must not flip the stored verdict. The first
        # definitive already resolved and popped the waiter, so this is purely
        # an observability/correctness guard on get_ack_status.
        fut = self.tracker._track_pending_ack("2016", DEST, "hi", create_future=True)
        self.tracker._record_ack_response(self.packet("2016", from_id=DEST, to_id=DEST), DEST, "hi")
        self.assertTrue(fut.done())
        self.assertEqual(self.tracker.get_ack_status("2016")["status"], AckStatus.ACK)
        # Late NAK for the same id — record must stay ACK.
        self.tracker._record_ack_response(
            self.packet("2016", from_id=DEST, to_id=DEST, error_reason="MAX_RETRANSMIT"),
            DEST,
            "hi",
        )
        rec = self.tracker.get_ack_status("2016")
        self.assertEqual(rec["status"], AckStatus.ACK)


class TestMaybeRecordPubsubAck(_TrackerTestCase):
    """Wires pubsub routing ACKs into the tracker (cc 10)."""

    def test_rejects_malformed_packets(self):
        self.assertFalse(self.tracker._maybe_record_pubsub_ack(None))
        self.assertFalse(self.tracker._maybe_record_pubsub_ack({}))
        self.assertFalse(self.tracker._maybe_record_pubsub_ack({"decoded": "nope"}))
        self.assertFalse(self.tracker._maybe_record_pubsub_ack({"decoded": {"requestId": 7}}))
        self.assertFalse(self.tracker._maybe_record_pubsub_ack({"decoded": {"routing": {}}}))

    def test_rejects_when_no_pending_record(self):
        self.assertFalse(self.tracker._maybe_record_pubsub_ack(self.packet("3001", from_id=DEST)))
        self.assertIsNone(self.tracker.get_ack_status("3001"))

    def test_rejects_pending_waiter(self):
        # The magic-named onAckNak callback is authoritative for a still-PENDING
        # waiter; the pubsub path is deliberately not used there.
        fut = self.tracker._track_pending_ack("3002", DEST, "hi", create_future=True)
        self.assertFalse(self.tracker._maybe_record_pubsub_ack(self.packet("3002", from_id=DEST)))
        self.assertFalse(fut.done())

    def test_upgrades_implicit_record_to_real_ack(self):
        # The documented fallback path: a DM whose first response was a relay
        # confirmation (IMPLICIT_ACK) gets upgraded to a real ACK when the
        # destination's own direct routing ACK arrives via pubsub.
        fut = self.tracker._track_pending_ack("3004", DEST, "hi", create_future=True)
        self.tracker._record_ack_response(self.packet("3004", from_id=RELAY), DEST, "hi")
        self.assertFalse(fut.done())
        direct = self.packet("3004", from_id=DEST, to_id=DEST)
        direct["hopStart"] = 1
        direct["hopLimit"] = 1
        self.assertTrue(self.tracker._maybe_record_pubsub_ack(direct))
        self.assertTrue(fut.done())
        rec = self.tracker.get_ack_status("3004")
        self.assertEqual(rec["status"], AckStatus.ACK)
        self.assertEqual(rec["ack_from"], DEST)
        self.assertNotIn("3004", self.tracker._ack_futures)

    def test_upgrade_accepts_snake_case_request_id(self):
        fut = self.tracker._track_pending_ack("3005", DEST, "hi", create_future=True)
        self.tracker._record_ack_response(self.packet("3005", from_id=RELAY), DEST, "hi")
        packet = self.packet("3005", from_id=DEST, request_key="request_id")
        packet["hopStart"] = 1
        packet["hopLimit"] = 1
        self.assertTrue(self.tracker._maybe_record_pubsub_ack(packet))
        self.assertTrue(fut.done())
        self.assertEqual(self.tracker.get_ack_status("3005")["status"], AckStatus.ACK)

    def test_upgrade_refused_for_relayed_routing_ack(self):
        """A routing ACK that itself arrived via a relay is not end-to-end.

        On a shared/PSK channel any node that observed the transmission can
        forge a routing ACK (our requestId + the destination's id); the hop
        envelope is the only per-packet provenance available, so a relayed
        packet must not upgrade an IMPLICIT_ACK to a real ACK. The record
        stays IMPLICIT_ACK — still a delivered outcome, just not upgraded.
        """
        fut = self.tracker._track_pending_ack("3006", DEST, "hi", create_future=True)
        self.tracker._record_ack_response(self.packet("3006", from_id=RELAY), DEST, "hi")
        self.assertFalse(fut.done())

        relayed = self.packet("3006", from_id=DEST, to_id=DEST)
        relayed["hopStart"] = 2
        relayed["hopLimit"] = 0
        self.assertFalse(self.tracker._maybe_record_pubsub_ack(relayed))
        self.assertFalse(fut.done())
        self.assertEqual(self.tracker.get_ack_status("3006")["status"], AckStatus.IMPLICIT_ACK)

        # A forged routing ACK that omits the hop envelope entirely must not
        # upgrade either — absence of the fields is not proof of a direct hop.
        no_hops = self.packet("3006", from_id=DEST, to_id=DEST)
        self.assertFalse(self.tracker._maybe_record_pubsub_ack(no_hops))
        self.assertFalse(fut.done())
        self.assertEqual(self.tracker.get_ack_status("3006")["status"], AckStatus.IMPLICIT_ACK)

        # A direct (0-hop) routing ACK still upgrades.
        direct = self.packet("3006", from_id=DEST, to_id=DEST)
        direct["hopStart"] = 1
        direct["hopLimit"] = 1
        self.assertTrue(self.tracker._maybe_record_pubsub_ack(direct))
        self.assertTrue(fut.done())
        self.assertEqual(self.tracker.get_ack_status("3006")["status"], AckStatus.ACK)

    def test_upgrades_implicit_without_live_waiter(self):
        # A fire-and-forget DM (no waiter) consumes the one-shot callback with
        # a relay confirmation; the destination's real direct routing ACK via
        # pubsub must still upgrade the record for observability.
        self.tracker._track_pending_ack("3007", DEST, "hi")
        self.tracker._record_ack_response(self.packet("3007", from_id=RELAY), DEST, "hi")
        self.assertEqual(self.tracker.get_ack_status("3007")["status"], AckStatus.IMPLICIT_ACK)
        direct = self.packet("3007", from_id=DEST, to_id=DEST)
        direct["hopStart"] = 1
        direct["hopLimit"] = 1
        self.assertTrue(self.tracker._maybe_record_pubsub_ack(direct))
        self.assertEqual(self.tracker.get_ack_status("3007")["status"], AckStatus.ACK)

    def test_upgrades_fire_and_forget_dm_with_send_token(self):
        # Same as above with a send token registered: the pubsub upgrade must
        # carry the record's own token, otherwise _record_ack_response would
        # treat it as a stale owner and drop it.
        token = object()
        self.tracker._track_pending_ack("3008", DEST, "hi", send_token=token)
        self.tracker._record_ack_response(
            self.packet("3008", from_id=RELAY), DEST, "hi", send_token=token
        )
        self.assertEqual(self.tracker.get_ack_status("3008")["status"], AckStatus.IMPLICIT_ACK)
        direct = self.packet("3008", from_id=DEST, to_id=DEST)
        direct["hopStart"] = 1
        direct["hopLimit"] = 1
        self.assertTrue(self.tracker._maybe_record_pubsub_ack(direct))
        self.assertEqual(self.tracker.get_ack_status("3008")["status"], AckStatus.ACK)

    def test_upgrade_refused_for_record_from_dead_lifecycle(self):
        """A delayed routing ACK for a pre-reconnect send must not upgrade.

        ACK records survive lifecycle turnover, so a pubsub packet arriving in
        the NEW lifecycle must not promote an IMPLICIT_ACK record created in an
        OLD one — that would mint a phantom "delivered" verdict in the fresh
        lifecycle's observability stores.
        """
        self.tracker._track_pending_ack("3009", DEST, "hi")
        self.tracker._record_ack_response(self.packet("3009", from_id=RELAY), DEST, "hi")
        self.assertEqual(self.tracker.get_ack_status("3009")["status"], AckStatus.IMPLICIT_ACK)

        self.adapter._lifecycle_id += 1  # reconnect bumps the lifecycle
        direct = self.packet("3009", from_id=DEST, to_id=DEST)
        direct["hopStart"] = 1
        direct["hopLimit"] = 1
        self.assertFalse(self.tracker._maybe_record_pubsub_ack(direct))
        self.assertEqual(self.tracker.get_ack_status("3009")["status"], AckStatus.IMPLICIT_ACK)

    def test_pubsub_upgrade_forwards_lifecycle_id(self):
        # Pin the TOCTOU contract: _maybe_record_pubsub_ack must capture the
        # lifecycle_id under the lock and forward it to _record_ack_response
        # so the ExitStack re-validates atomically. Without forwarding,
        # lifecycle_id defaults to None inside _record_ack_response and the
        # staleness re-check is skipped.
        self.tracker._track_pending_ack("3010", DEST, "hi")
        self.tracker._record_ack_response(self.packet("3010", from_id=RELAY), DEST, "hi")
        self.assertEqual(self.tracker.get_ack_status("3010")["status"], AckStatus.IMPLICIT_ACK)

        forwarded_ids: list[int | None] = []
        original = self.tracker._record_ack_response

        def capture(packet, dest, content, *, send_token=None, lifecycle_id=None):
            forwarded_ids.append(lifecycle_id)
            return original(packet, dest, content, send_token=send_token, lifecycle_id=lifecycle_id)

        self.tracker._record_ack_response = capture  # type: ignore[method-assign]
        try:
            direct = self.packet("3010", from_id=DEST, to_id=DEST)
            direct["hopStart"] = 1
            direct["hopLimit"] = 1
            self.assertTrue(self.tracker._maybe_record_pubsub_ack(direct))
        finally:
            self.tracker._record_ack_response = original  # type: ignore[method-assign]
        self.assertEqual(forwarded_ids, [self.adapter._lifecycle_id])
        self.assertEqual(self.tracker.get_ack_status("3010")["status"], AckStatus.ACK)


class TestRetriabilityClassification(unittest.TestCase):
    """Only ACK-observed transient failures are retriable.

    Moved from test_meshtastic.TestMeshtasticPlatform.test_is_retriable_failure_classification
    — ack_state.is_retriable_failure is a pure module-level function, so it is
    exercised directly without a tracker or an assembled adapter.
    """

    def _r(self, ack):
        return SendResult(success=False, raw_response={"ack": ack} if ack else None)

    def test_classification(self):
        self.assertTrue(ack_state.is_retriable_failure(self._r({"status": AckStatus.TIMEOUT})))
        self.assertTrue(
            ack_state.is_retriable_failure(
                self._r({"status": AckStatus.NAK, "error_reason": "NO_ROUTE"})
            )
        )
        self.assertFalse(
            ack_state.is_retriable_failure(
                self._r({"status": AckStatus.NAK, "error_reason": "TOO_LARGE"})
            )
        )
        # PKI / auth failures are permanent — re-sending can't fix a key problem.
        for reason in (
            "PKI_FAILED",
            "PKI_UNKNOWN_PUBKEY",
            "PKI_SEND_FAIL_PUBLIC_KEY",
            "ADMIN_PUBLIC_KEY_UNAUTHORIZED",
            "NOT_AUTHORIZED",
            "DUTY_CYCLE_LIMIT",
            "RATE_LIMIT_EXCEEDED",
        ):
            self.assertFalse(
                ack_state.is_retriable_failure(
                    self._r({"status": AckStatus.NAK, "error_reason": reason})
                ),
                f"{reason} should be permanent",
            )
        self.assertFalse(ack_state.is_retriable_failure(self._r({"status": AckStatus.ACK})))
        # MAX_RETRANSMIT is the firmware's own "reliable send failed" verdict —
        # evidence of non-delivery, so worth another attempt.
        self.assertTrue(
            ack_state.is_retriable_failure(
                self._r({"status": AckStatus.NAK, "error_reason": "MAX_RETRANSMIT"})
            )
        )
        # An implicit ACK is NOT retried: the mesh carried the packet, so
        # non-delivery isn't established, and _maybe_record_pubsub_ack can still
        # upgrade it to a real ACK from the destination's later routing packet.
        self.assertFalse(
            ack_state.is_retriable_failure(self._r({"status": AckStatus.IMPLICIT_ACK}))
        )
        # Plain strings still match (StrEnum + public JSON surface).
        self.assertTrue(ack_state.is_retriable_failure(self._r({"status": "timeout"})))
        self.assertFalse(ack_state.is_retriable_failure(self._r(None)))  # pre-send error
        # Disconnect-settled waiters must not spin retries against a closed radio.
        self.assertFalse(
            ack_state.is_retriable_failure(
                self._r({"status": AckStatus.TIMEOUT, "error_reason": "DISCONNECTED"})
            )
        )
        # Adapter-internal collision NAK: the chunk was already transmitted, so
        # retrying would duplicate it on-air.
        self.assertFalse(
            ack_state.is_retriable_failure(
                self._r({"status": AckStatus.NAK, "error_reason": INTERNAL_NAK_DUPLICATE_PACKET_ID})
            )
        )

        # The PERMANENT_NAK_REASONS set is the single source of truth in
        # ack_state; confirm membership matches the documented contract.
        self.assertIs(PERMANENT_NAK_REASONS, ack_state.PERMANENT_NAK_REASONS)
        self.assertEqual(ACK_RECORD_LIMIT, 1000)

    def test_forged_wire_nak_does_not_match_internal_tokens(self):
        """Wire reasons must not trigger the internal "don't retry" branches.

        The internal synthetic tokens are ``_``-prefixed (DUPLICATE_PACKET_ID)
        or gated on ``status == TIMEOUT`` (DISCONNECTED). A forged wire NAK
        carrying the unprefixed wire spellings must fall through to normal NAK
        retry classification — and since neither is in PERMANENT_NAK_REASONS,
        both are retriable.
        """
        # Forged wire NAK with errorReason="DUPLICATE_PACKET_ID" (wire spelling):
        # does not match the internal _-prefixed token, so it is retriable.
        self.assertTrue(
            ack_state.is_retriable_failure(
                self._r({"status": AckStatus.NAK, "error_reason": "DUPLICATE_PACKET_ID"})
            )
        )
        # Forged wire NAK with errorReason="DISCONNECTED": status is NAK (any
        # errorReason classifies as NAK), so the TIMEOUT-gated internal branch
        # does not match — retriable.
        self.assertTrue(
            ack_state.is_retriable_failure(
                self._r({"status": AckStatus.NAK, "error_reason": "DISCONNECTED"})
            )
        )
        # Sanity: the internal spellings are still non-retriable.
        self.assertFalse(
            ack_state.is_retriable_failure(
                self._r({"status": AckStatus.NAK, "error_reason": INTERNAL_NAK_DUPLICATE_PACKET_ID})
            )
        )
        self.assertFalse(
            ack_state.is_retriable_failure(
                self._r({"status": AckStatus.TIMEOUT, "error_reason": "DISCONNECTED"})
            )
        )


class TestAckHopInfo(unittest.TestCase):
    """``ack_hop_info`` extracts hops from a raw ACK packet.

    Pure function; the hops_away delta must never go negative (a packet that
    claims fewer hops than its limit is at distance 0, not a negative
    distance), and non-int hop fields must not crash it.
    """

    def test_hop_delta_is_clamped_at_zero(self):
        self.assertEqual(ack_state.ack_hop_info({"hopStart": 0, "hopLimit": 3}), (0, 0, 3))
        self.assertEqual(ack_state.ack_hop_info({"hopStart": 4, "hopLimit": 5}), (0, 4, 5))
        self.assertEqual(ack_state.ack_hop_info({"hopStart": 7, "hopLimit": 2}), (5, 7, 2))

    def test_non_int_hop_fields_yield_none_hops(self):
        self.assertEqual(
            ack_state.ack_hop_info({"hopStart": "0", "hopLimit": 3}),
            (None, "0", 3),
        )
        self.assertEqual(
            ack_state.ack_hop_info({"hopStart": 0, "hopLimit": None}),
            (None, 0, None),
        )
        self.assertEqual(
            ack_state.ack_hop_info({"hopStart": 0}),
            (None, 0, None),
        )


class TestWaitForAck(_TrackerTestCase):
    """``_wait_for_ack`` timeout stamping and return-value isolation."""

    def test_timeout_returns_isolated_snapshot(self):
        # The TIMEOUT return value must be a snapshot, not the live shared
        # record: a late ACK/NAK on the pubsub thread between the stamp and the
        # caller's read must not flip the verdict _send_immediate reports.
        pkt_id = "snapshot-timeout-1"
        fut = self.tracker._track_pending_ack(pkt_id, DEST, "hi", create_future=True)

        async def scenario():
            record = await self.tracker._wait_for_ack(pkt_id, fut, 0.05)
            self.assertEqual(record["status"], AckStatus.TIMEOUT)
            self.assertEqual(record["error_reason"], "ACK_TIMEOUT")
            # A late real ACK mutates the shared store after the snapshot froze.
            self.tracker._record_ack_response(
                self.packet(pkt_id, from_id=DEST, to_id=DEST), DEST, "hi"
            )
            return record

        record = asyncio.run(scenario())
        self.assertEqual(record["status"], AckStatus.TIMEOUT)
        self.assertEqual(record["error_reason"], "ACK_TIMEOUT")
        # The live store reflects the later ACK; the returned snapshot does not.
        self.assertEqual(self.tracker.get_ack_status(pkt_id)["status"], AckStatus.ACK)

    def test_timeout_keeps_concurrent_definitive_verdict(self):
        # A definitive verdict stamped before the timeout's lock acquisition
        # must not be overwritten by the TIMEOUT stamp.
        pkt_id = "timeout-race-1"
        fut = self.tracker._track_pending_ack(pkt_id, DEST, "hi", create_future=True)

        async def scenario():
            with self.tracker._ack_lock:
                rec = self.tracker._pending_acks[pkt_id]
                rec["status"] = AckStatus.ACK
                rec["error_reason"] = None
            return await self.tracker._wait_for_ack(pkt_id, fut, 0.05)

        record = asyncio.run(scenario())
        self.assertEqual(record["status"], AckStatus.ACK)
        self.assertNotEqual(record.get("error_reason"), "ACK_TIMEOUT")


class TestAckWaitConfig(unittest.TestCase):
    """``ack_wait_config`` metadata overrides (bool/timeout coercion).

    ``meshtastic_wait_for_ack`` arrives as a JSON string on tool-call paths;
    a bare ``bool()`` would treat ``"false"``/``"0"``/``"no"`` as truthy and
    force every send to wait 30s for an ACK. The env default is pinned to 0
    so these cases exercise the metadata-override branch only.
    """

    def test_string_false_values_are_not_truthy(self):
        with mock.patch.dict(os.environ, {"MESHTASTIC_ACK_TIMEOUT": "0"}):
            for raw in ("false", "0", "no", "off", ""):
                with self.subTest(raw=raw):
                    self.assertEqual(
                        ack_state.ack_wait_config({"meshtastic_wait_for_ack": raw}),
                        (False, 0.0),
                    )
            self.assertEqual(
                ack_state.ack_wait_config({"meshtastic_wait_for_ack": 0}), (False, 0.0)
            )
            self.assertEqual(
                ack_state.ack_wait_config({"meshtastic_wait_for_ack": False}), (False, 0.0)
            )

    def test_true_values_enable_wait_and_default_timeout(self):
        with mock.patch.dict(os.environ, {"MESHTASTIC_ACK_TIMEOUT": "0"}):
            for raw in ("true", "1", "yes", "on"):
                with self.subTest(raw=raw):
                    self.assertEqual(
                        ack_state.ack_wait_config({"meshtastic_wait_for_ack": raw}),
                        (True, 30.0),
                    )
            self.assertEqual(
                ack_state.ack_wait_config({"meshtastic_wait_for_ack": 1}), (True, 30.0)
            )
            self.assertEqual(
                ack_state.ack_wait_config({"meshtastic_wait_for_ack": True}), (True, 30.0)
            )

    def test_metadata_timeout_still_wins_when_waiting(self):
        with mock.patch.dict(os.environ, {"MESHTASTIC_ACK_TIMEOUT": "0"}):
            self.assertEqual(
                ack_state.ack_wait_config(
                    {"meshtastic_wait_for_ack": "true", "meshtastic_ack_timeout": "7"}
                ),
                (True, 7.0),
            )
            # A garbage metadata timeout falls back to 0, then the explicit
            # wait flag forces the 30s default.
            self.assertEqual(
                ack_state.ack_wait_config(
                    {"meshtastic_wait_for_ack": "true", "meshtastic_ack_timeout": "bogus"}
                ),
                (True, 30.0),
            )

    def test_env_default_decides_when_no_wait_flag(self):
        with mock.patch.dict(os.environ, {"MESHTASTIC_ACK_TIMEOUT": "2.5"}):
            self.assertEqual(ack_state.ack_wait_config(None), (True, 2.5))
            self.assertEqual(
                ack_state.ack_wait_config({"meshtastic_wait_for_ack": "false"}),
                (False, 2.5),
            )


class TestAckCallbackNaming(unittest.TestCase):
    """The pubsub ACK callback must be literally named ``onAckNak``.

    The meshtastic library only delivers plain ACKs to a callback whose
    ``__name__`` is exactly ``onAckNak`` (magic-name check in
    ``mesh_interface.py``). Mock tests invoke callbacks directly, so a rename
    would silently break real hardware while tests stay green. The factory is
    synchronous, so it is constructed directly (``AckTracker.__init__`` only
    stores the adapter back-reference; it is never dereferenced here).
    """

    def test_make_ack_callback_for_send_is_named_on_ack_nak(self):
        tracker = ack_state.AckTracker(None)
        callback = tracker._make_ack_callback_for_send("!deadbeef", "hello", None)
        self.assertEqual(callback.__name__, "onAckNak")


if __name__ == "__main__":
    unittest.main()
