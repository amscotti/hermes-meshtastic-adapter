"""
Unit tests for the pure send-path decision helpers in send_path.py.

The outbound pipeline's decisions were extracted from adapter.send /
_send_immediate / _send_text_serialized so they can be tested decision-table
style without an adapter instance: retry eligibility and budget, backoff /
pacing values, transport-error mapping, ACK-outcome classification, DM node
selection and channel resolution. Everything here is synchronous and pure.
"""

import os
import unittest
from concurrent.futures import Future as ConcurrentFuture
from types import SimpleNamespace
from unittest.mock import patch

import inbound
import send_path
from send_path import AckStatus


def _channel(name, index):
    """A dict-shaped channel like the mock interface exposes."""
    return {"name": name, "index": index}


def _pubkey_node(public_key=b"k"):
    return {"user": {"publicKey": public_key}}


class TestChatIdParsing(unittest.TestCase):
    def test_dest_from_chat_id(self):
        self.assertEqual(send_path.dest_from_chat_id("meshtastic:!ab12cd34"), "!ab12cd34")
        self.assertEqual(send_path.dest_from_chat_id("meshtastic:channel:0"), "channel")
        self.assertEqual(send_path.dest_from_chat_id("meshtastic:channel:Primary"), "channel")
        self.assertEqual(send_path.dest_from_chat_id("no-colon-here"), "")
        self.assertEqual(send_path.dest_from_chat_id(""), "")


class TestRetryConfig(unittest.TestCase):
    """Decision tables for the retry/attempt-budget computations."""

    def test_retry_implies_ack_wait_unchanged_when_not_retrying(self):
        self.assertEqual(send_path.retry_implies_ack_wait(0, True, False, 0.0), (False, 0.0))
        self.assertEqual(send_path.retry_implies_ack_wait(0, True, True, 5.0), (True, 5.0))

    def test_retry_implies_ack_wait_forces_wait_on_dm(self):
        # Retries=1 on a DM without an ACK wait forces waiting with the 30s default.
        self.assertEqual(send_path.retry_implies_ack_wait(1, True, False, 0.0), (True, 30.0))
        # An existing shorter timeout is kept, only waiting is forced on.
        self.assertEqual(send_path.retry_implies_ack_wait(1, True, False, 2.5), (True, 2.5))
        # Already waiting keeps its own config untouched.
        self.assertEqual(send_path.retry_implies_ack_wait(1, True, True, 0.0), (True, 0.0))

    def test_retry_implies_ack_wait_does_not_force_wait_on_broadcast(self):
        # Retrying a broadcast is meaningless (no per-recipient ACK): no upgrade.
        self.assertEqual(send_path.retry_implies_ack_wait(3, False, False, 0.0), (False, 0.0))

    def test_max_send_attempts_decision_table(self):
        cases = [
            # (retries, wait_for_ack, is_dm, expected)
            (0, False, False, 1),
            (0, True, True, 1),  # no retries configured
            (2, True, True, 3),  # 1 + retries
            (3, True, True, 4),
            (2, False, True, 1),  # retry needs ACK-waiting
            (2, True, False, 1),  # broadcast never retries
            (2, False, False, 1),
        ]
        for retries, wait, is_dm, expected in cases:
            with self.subTest(retries=retries, wait=wait, is_dm=is_dm):
                self.assertEqual(send_path.max_send_attempts(retries, wait, is_dm), expected)


class TestChunkSendResult(unittest.TestCase):
    """chunk_send_result: empty / over-cap content fails explicitly."""

    def test_empty_and_whitespace_content_fail(self):
        for empty in ("", "   ", "\n\t "):
            chunks, err = send_path.chunk_send_result(empty, lambda c: [c])
            self.assertEqual(chunks, [])
            self.assertEqual(err, "message content is empty")

    def test_cap_raise_becomes_error(self):
        def exploding(_content):
            raise ValueError("message too long: 61 chunks would exceed the 60-chunk limit")

        chunks, err = send_path.chunk_send_result("x" * 1000, exploding)
        self.assertEqual(chunks, [])
        self.assertIn("chunk", err)

    def test_normal_content_chunks(self):
        chunks, err = send_path.chunk_send_result("hello", lambda c: [c])
        self.assertEqual(chunks, ["hello"])
        self.assertIsNone(err)


class TestPacingAndBackoff(unittest.TestCase):
    def test_chunk_pacing_delay_default_and_override(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(send_path.chunk_pacing_delay(), 4.0)
        with patch.dict(os.environ, {"MESHTASTIC_CHUNK_DELAY": "2.5"}):
            self.assertEqual(send_path.chunk_pacing_delay(), 2.5)
        with patch.dict(os.environ, {"MESHTASTIC_CHUNK_DELAY": "0"}):
            self.assertEqual(send_path.chunk_pacing_delay(), 0.0)

    def test_chunk_pacing_delay_garbage_raises(self):
        # Exact semantics of the original inline float(os.getenv(...)): a
        # misconfiguration surfaces instead of silently disabling pacing.
        with patch.dict(os.environ, {"MESHTASTIC_CHUNK_DELAY": "garbage"}):
            with self.assertRaises(ValueError):
                send_path.chunk_pacing_delay()

    def test_chunk_pacing_delay_empty_string_uses_default(self):
        # A set-but-empty value falls back to the default like every other env
        # reader, so a blank var cannot abort a multi-chunk send mid-stream.
        with patch.dict(os.environ, {"MESHTASTIC_CHUNK_DELAY": ""}):
            self.assertEqual(send_path.chunk_pacing_delay(), 4.0)

    def test_chunk_pacing_delay_rejects_non_finite(self):
        # ``inf`` would hang asyncio.sleep on the second chunk forever and
        # ``nan`` never fires the timer; the strict path surfaces them like any
        # other misconfiguration instead of silently self-DoSing pacing.
        for bad in ("inf", "-inf", "nan", "NaN", "INF"):
            with patch.dict(os.environ, {"MESHTASTIC_CHUNK_DELAY": bad}):
                with self.subTest(value=bad):
                    with self.assertRaises(ValueError):
                        send_path.chunk_pacing_delay()

    def test_safe_chunk_pacing_delay_falls_back_on_misconfiguration(self):
        # The drain loop's variant must never raise after an item was already
        # delivered (that would requeue a duplicate): garbage and non-finite
        # values both fall back to the default.
        for bad in ("garbage", "inf", "-inf", "nan"):
            with patch.dict(os.environ, {"MESHTASTIC_CHUNK_DELAY": bad}):
                with self.subTest(value=bad):
                    self.assertEqual(send_path.safe_chunk_pacing_delay(), 4.0)


class TestRetryDecision(unittest.TestCase):
    """should_retry_chunk decision table."""

    def test_success_stops_the_loop(self):
        for attempt in (1, 2, 5):
            with self.subTest(attempt=attempt):
                self.assertFalse(send_path.should_retry_chunk(True, attempt, 5, True))

    def test_retry_allowed_only_inside_budget_and_retriable(self):
        self.assertTrue(send_path.should_retry_chunk(False, 1, 3, True))
        self.assertTrue(send_path.should_retry_chunk(False, 2, 3, True))

    def test_attempt_budget_exhausted_blocks_retry(self):
        self.assertFalse(send_path.should_retry_chunk(False, 3, 3, True))
        self.assertFalse(send_path.should_retry_chunk(False, 4, 3, True))

    def test_non_retriable_failure_blocks_retry(self):
        self.assertFalse(send_path.should_retry_chunk(False, 1, 3, False))

    def test_single_attempt_budget(self):
        self.assertFalse(send_path.should_retry_chunk(False, 1, 1, True))


class TestDrainRetryDecision(unittest.TestCase):
    """drain_retry_decision: which failures a drained item is requeued for."""

    def test_no_interface_failure_is_retriable(self):
        self.assertTrue(send_path.drain_retry_decision("No active interfaces connected", 1, 3))
        self.assertTrue(send_path.drain_retry_decision("No active interfaces connected", 2, 3))
        self.assertFalse(send_path.drain_retry_decision("No active interfaces connected", 3, 3))

    def test_ack_wait_no_interface_failure_is_retriable(self):
        self.assertTrue(
            send_path.drain_retry_decision(
                "No active interfaces connected; cannot wait for ACK", 1, 3
            )
        )

    def test_transport_failure_token_is_retriable(self):
        # The classified transport-failure token is requeued within the budget
        # even though it is not the no-interface sentinel.
        self.assertTrue(send_path.drain_retry_decision("Meshtastic send failed", 1, 3))
        self.assertTrue(send_path.drain_retry_decision("Meshtastic send failed", 2, 3))
        self.assertFalse(send_path.drain_retry_decision("Meshtastic send failed", 3, 3))

    def test_permanent_failure_is_dropped(self):
        self.assertFalse(send_path.drain_retry_decision("Invalid chat_id format", 1, 3))
        self.assertFalse(send_path.drain_retry_decision("Target node !x has no public key", 1, 3))
        self.assertFalse(
            send_path.drain_retry_decision("Requested channel index is not available", 1, 3)
        )

    def test_send_raise_is_retriable(self):
        self.assertTrue(send_path.drain_retry_decision(None, 1, 3))
        self.assertFalse(send_path.drain_retry_decision(None, 3, 3))


class TestNormalizationAndErrorMapping(unittest.TestCase):
    def test_normalize_dm_dest(self):
        norm = lambda node: node.lower()  # noqa: E731
        self.assertEqual(send_path.normalize_dm_dest("!AB12CD34", norm), "!ab12cd34")
        self.assertEqual(send_path.normalize_dm_dest("!ab12cd34", norm), "!ab12cd34")
        # Non-DM destinations pass through untouched.
        self.assertEqual(send_path.normalize_dm_dest("channel", norm), "channel")

    def test_normalize_dm_dest_falls_back_to_input(self):
        def norm(_node):
            return None

        self.assertEqual(send_path.normalize_dm_dest("!ab12cd34", norm), "!ab12cd34")

    def test_is_executor_shutdown_error(self):
        self.assertTrue(
            send_path.is_executor_shutdown_error(
                RuntimeError("cannot schedule new futures after shutdown")
            )
        )
        self.assertTrue(
            send_path.is_executor_shutdown_error(
                RuntimeError("Cannot Schedule New Futures After Shutdown")
            )
        )
        self.assertFalse(send_path.is_executor_shutdown_error(RuntimeError("boom")))
        self.assertFalse(send_path.is_executor_shutdown_error(ValueError("cannot schedule")))

    def test_is_executor_shutdown_error_matches_typed_exception(self):
        # The typed TransportShutdownError raised by the executor's submit is
        # matched by type, not by the stdlib message string.
        import transport

        self.assertTrue(send_path.is_executor_shutdown_error(transport.TransportShutdownError()))
        self.assertTrue(
            send_path.is_executor_shutdown_error(
                transport.TransportShutdownError("any wording change still matches")
            )
        )

    def test_map_transport_error_tokens(self):
        self.assertEqual(
            send_path.map_transport_error("no_iface", "!ab12cd34"),
            "No active interfaces connected",
        )
        self.assertEqual(
            send_path.map_transport_error("no_pubkey", "!ab12cd34"),
            "Target node !ab12cd34 has no public key; direct message cannot be encrypted",
        )
        # No token (sendText ran) or an unknown token: not a pre-send failure.
        self.assertIsNone(send_path.map_transport_error(None, "!ab12cd34"))
        self.assertIsNone(send_path.map_transport_error("something-else", "!ab12cd34"))

    def test_map_transport_error_no_channel(self):
        self.assertEqual(
            send_path.map_transport_error("no_channel", "channel"),
            "Requested channel index is not available on any connected interface",
        )

    def test_no_interfaces_error_is_canonical_transient_token(self):
        """Every "no interfaces" spelling shares NO_INTERFACES_ERROR as a prefix,
        and the canonical token is the one TRANSIENT_TRANSPORT_ERRORS matches —
        so _send_chunk's requeue predicate never silently stops matching a
        future variant."""
        import ack_state

        base = send_path.NO_INTERFACES_ERROR
        # The adapter's longer variants are built from the base constant.
        variants = [
            base,
            f"{base}; cannot wait for ACK",
            f"{base} and queueing disabled",
        ]
        for v in variants:
            self.assertTrue(
                v.startswith(base),
                f"variant {v!r} does not share the canonical prefix",
            )
        # The bare token is the one map_transport_error emits and the set keys on.
        self.assertEqual(send_path.map_transport_error("no_iface", "!ab12cd34"), base)
        self.assertIn(base, ack_state.TRANSIENT_TRANSPORT_ERRORS)

    def test_is_node_dest(self):
        self.assertTrue(send_path.is_node_dest("!ab12cd34"))
        self.assertTrue(send_path.is_node_dest("!AB12CD34"))
        self.assertTrue(send_path.is_node_dest("0"))
        self.assertTrue(send_path.is_node_dest("2870135092"))
        self.assertTrue(send_path.is_node_dest(str(2**32 - 1)))
        # Group / named-channel destinations are not node ids.
        self.assertFalse(send_path.is_node_dest("channel"))
        self.assertFalse(send_path.is_node_dest("Primary"))
        self.assertFalse(send_path.is_node_dest(""))
        # Node numbers are unsigned 32-bit; out-of-range / non-numeric are not.
        self.assertFalse(send_path.is_node_dest(str(2**32)))
        self.assertFalse(send_path.is_node_dest("-1"))

    def test_is_node_dest_rejects_underscore_separators(self):
        # Python int() accepts underscore digit separators (int("1_000") ==
        # 1000); a malformed chat id must not route to an unintended DM target,
        # so the separator form is rejected. Surrounding whitespace is tolerated
        # via stripping (it cannot change the resolved node number).
        self.assertFalse(send_path.is_node_dest("1_000"))
        self.assertFalse(send_path.is_node_dest("2870_135092"))
        self.assertTrue(send_path.is_node_dest(" 123 "))
        self.assertTrue(send_path.is_node_dest(" 0 "))


class TestStaleLifecycleShaping(unittest.TestCase):
    def test_disconnect_ack_record(self):
        record = send_path.disconnect_ack_record("!ab12cd34", "hello")
        self.assertEqual(record["dest"], "!ab12cd34")
        self.assertEqual(record["bytes"], 5)
        self.assertEqual(record["status"], AckStatus.TIMEOUT)
        self.assertEqual(record["error_reason"], "DISCONNECTED")
        self.assertIn("response_at", record)

    def test_disconnect_error(self):
        self.assertEqual(
            send_path.disconnect_error(True, "12345"),
            "Meshtastic disconnected while waiting for ACK on packet 12345",
        )
        self.assertEqual(
            send_path.disconnect_error(False, "12345"),
            "Meshtastic disconnected while transport send was in progress",
        )
        self.assertEqual(
            send_path.disconnect_error(True, None),
            "Meshtastic disconnected while transport send was in progress",
        )

    def test_stale_send_raw_response_shape(self):
        record = {"status": AckStatus.TIMEOUT}
        resp = send_path.stale_send_raw_response("1", "!ab12cd34", True, 30.0, record)
        self.assertEqual(resp["packet_id"], "1")
        self.assertEqual(resp["dest"], "!ab12cd34")
        self.assertTrue(resp["ack_requested"])
        self.assertTrue(resp["ack_waited"])
        self.assertEqual(resp["ack_timeout"], 30.0)
        self.assertIs(resp["ack"], record)

    def test_outbound_raw_response_ack_timeout_only_when_waiting(self):
        waiting = send_path.outbound_raw_response("1", "!ab12cd34", True, 30.0, None)
        self.assertEqual(waiting["ack_timeout"], 30.0)
        fire_and_forget = send_path.outbound_raw_response("1", "!ab12cd34", False, 30.0, None)
        self.assertIsNone(fire_and_forget["ack_timeout"])
        self.assertEqual(fire_and_forget["ack_requested"], True)


class TestAckOutcomeClassification(unittest.TestCase):
    """classify_ack_outcome decision table."""

    def test_ack_is_delivered(self):
        success, error = send_path.classify_ack_outcome({"status": AckStatus.ACK}, "11")
        self.assertTrue(success)
        self.assertIsNone(error)

    def test_implicit_ack_is_delivered_but_distinct_status(self):
        success, error = send_path.classify_ack_outcome({"status": AckStatus.IMPLICIT_ACK}, "12")
        self.assertTrue(success)
        self.assertIsNone(error)

    def test_nak_fails_with_reason(self):
        success, error = send_path.classify_ack_outcome(
            {"status": AckStatus.NAK, "error_reason": "NO_ROUTE"}, "13"
        )
        self.assertFalse(success)
        self.assertEqual(error, "Meshtastic NAK for packet 13: NO_ROUTE")

    def test_nak_without_reason_says_unknown(self):
        success, error = send_path.classify_ack_outcome({"status": AckStatus.NAK}, "14")
        self.assertFalse(success)
        self.assertEqual(error, "Meshtastic NAK for packet 14: unknown")

    def test_timeout_without_reason_is_ack_timeout(self):
        success, error = send_path.classify_ack_outcome(
            {"status": AckStatus.TIMEOUT, "error_reason": "ACK_TIMEOUT"}, "15"
        )
        self.assertFalse(success)
        self.assertEqual(error, "Meshtastic ACK timeout for packet 15")

    def test_timeout_with_disconnect_reason_is_disconnect(self):
        success, error = send_path.classify_ack_outcome(
            {"status": AckStatus.TIMEOUT, "error_reason": "DISCONNECTED"}, "16"
        )
        self.assertFalse(success)
        self.assertEqual(error, "Meshtastic disconnected while waiting for ACK on packet 16")

    def test_unknown_status_lands_in_timeout(self):
        success, error = send_path.classify_ack_outcome({"status": "pending"}, "17")
        self.assertFalse(success)
        self.assertEqual(error, "Meshtastic ACK timeout for packet 17")

    def test_missing_status_key_lands_in_timeout(self):
        # A record with no "status" key at all (.get("status") -> None) defends
        # like an unknown status: the timeout verdict rather than a KeyError.
        success, error = send_path.classify_ack_outcome({}, "18")
        self.assertFalse(success)
        self.assertEqual(error, "Meshtastic ACK timeout for packet 18")

    def test_waitable_ack_wait_requires_id_and_future(self):
        future = ConcurrentFuture()
        self.assertEqual(send_path.waitable_ack_wait("1", future), ("1", future))
        self.assertIsNone(send_path.waitable_ack_wait(None, future))
        self.assertIsNone(send_path.waitable_ack_wait("1", None))
        self.assertIsNone(send_path.waitable_ack_wait(None, None))
        self.assertIsNone(send_path.waitable_ack_wait("", future))


class TestDmNodeResolution(unittest.TestCase):
    def test_exact_key_wins(self):
        nodes = {"!ab12cd34": _pubkey_node(), "!AA00BB11": _pubkey_node()}
        dest, info = send_path.resolve_dm_node("!ab12cd34", nodes)
        self.assertEqual(dest, "!ab12cd34")
        self.assertIs(info, nodes["!ab12cd34"])

    def test_case_insensitive_scan_rewrites_dest(self):
        nodes = {"!AA00BB11": _pubkey_node()}
        dest, info = send_path.resolve_dm_node("!aa00bb11", nodes)
        self.assertEqual(dest, "!AA00BB11")
        self.assertIs(info, nodes["!AA00BB11"])

    def test_numeric_node_keys(self):
        nodes = {2870135092: _pubkey_node()}
        dest, info = send_path.resolve_dm_node("2870135092", nodes)
        self.assertEqual(dest, "2870135092")
        self.assertIsNotNone(info)

    def test_miss_returns_dest_unchanged(self):
        nodes = {"!ab12cd34": _pubkey_node()}
        dest, info = send_path.resolve_dm_node("!deadbeef", nodes)
        self.assertEqual(dest, "!deadbeef")
        self.assertIsNone(info)

    def test_scan_uses_snapshot_not_live_items_view(self):
        # The node DB is the library's *live* dict, mutated on its reader
        # thread; iterating the bare ``.items()`` view can raise "dictionary
        # changed size during iteration" mid-send. ``dict(nodes)`` copies via
        # the C fast path WITHOUT invoking the live ``.items()``, so a DB whose
        # ``.items()`` raises cannot reach the scan. This distinguishes the
        # snapshot fix from the bare ``for ... in nodes.items()`` scan.
        base = {"!ab12cd34": _pubkey_node(), "!AA00BB11": _pubkey_node()}

        class LiveNodes(dict):
            def items(self):
                raise RuntimeError("dictionary changed size during iteration")

        nodes = LiveNodes(base)
        # Exact key resolves without scanning the live items() view.
        dest, info = send_path.resolve_dm_node("!ab12cd34", nodes)
        self.assertEqual(dest, "!ab12cd34")
        self.assertIs(info, base["!ab12cd34"])
        # Case-insensitive scan resolves via the snapshot, never the live view.
        dest, info = send_path.resolve_dm_node("!aa00bb11", nodes)
        self.assertEqual(dest, "!AA00BB11")
        self.assertIs(info, base["!AA00BB11"])
        # Miss path completes without raising.
        dest, info = send_path.resolve_dm_node("!deadbeef", nodes)
        self.assertEqual(dest, "!deadbeef")
        self.assertIsNone(info)


class TestDmSendTarget(unittest.TestCase):
    def test_send_goes_out_on_interface_owning_the_node(self):
        iface_a = SimpleNamespace(nodes={})
        iface_b = SimpleNamespace(nodes={"!ab12cd34": _pubkey_node()})
        iface, dest, ready = send_path.dm_send_target("!ab12cd34", [iface_a, iface_b])
        self.assertIs(iface, iface_b)
        self.assertEqual(dest, "!ab12cd34")
        self.assertTrue(ready)

    def test_scan_match_returns_library_key_form(self):
        owner = SimpleNamespace(nodes={"!AA00BB11": _pubkey_node()})
        iface, dest, ready = send_path.dm_send_target("!aa00bb11", [owner])
        self.assertIs(iface, owner)
        self.assertEqual(dest, "!AA00BB11")
        self.assertTrue(ready)

    def test_known_node_without_pubkey_is_not_sendable(self):
        owner = SimpleNamespace(nodes={"!ab12cd34": _pubkey_node(public_key="")})
        iface, dest, ready = send_path.dm_send_target("!ab12cd34", [owner])
        self.assertIs(iface, owner)
        self.assertFalse(ready)

    def test_keyless_owner_defers_to_later_keyful_owner(self):
        """A keyless first-match must not short-circuit when a later interface
        knows the SAME node with a public key (that owner is strictly more
        capable and would be the correct send target)."""
        keyless = SimpleNamespace(nodes={"!ab12cd34": _pubkey_node(public_key="")})
        keyful = SimpleNamespace(nodes={"!ab12cd34": _pubkey_node(public_key=b"k")})
        iface, dest, ready = send_path.dm_send_target("!ab12cd34", [keyless, keyful])
        self.assertIs(iface, keyful)
        self.assertEqual(dest, "!ab12cd34")
        self.assertTrue(ready)

    def test_keyless_owner_returned_when_every_owner_is_keyless(self):
        keyless_a = SimpleNamespace(nodes={"!ab12cd34": _pubkey_node(public_key="")})
        keyless_b = SimpleNamespace(nodes={"!ab12cd34": _pubkey_node(public_key=None)})
        iface, dest, ready = send_path.dm_send_target("!ab12cd34", [keyless_a, keyless_b])
        self.assertIs(iface, keyless_a)
        self.assertFalse(ready)

    def test_empty_interface_list_raises(self):
        with self.assertRaises(ValueError):
            send_path.dm_send_target("!ab12cd34", [])

    def test_unknown_node_attempted_on_first_interface(self):
        iface_a = SimpleNamespace(nodes={})
        iface_b = SimpleNamespace(nodes={"!ab12cd34": _pubkey_node()})
        iface, dest, ready = send_path.dm_send_target("!deadbeef", [iface_a, iface_b])
        self.assertIs(iface, iface_a)
        self.assertEqual(dest, "!deadbeef")
        self.assertTrue(ready)

    def test_interface_without_nodes_attribute_is_skipped(self):
        no_nodes = SimpleNamespace()
        owner = SimpleNamespace(nodes={"!ab12cd34": _pubkey_node()})
        iface, _, ready = send_path.dm_send_target("!ab12cd34", [no_nodes, owner])
        self.assertIs(iface, owner)
        self.assertTrue(ready)

    def test_non_dict_node_info_is_treated_as_unknown(self):
        # The node DB is mesh-influenced and may hold non-dict entries on odd
        # firmware; a malformed entry must not raise AttributeError on the
        # ``.get("user", {})`` lookup mid-send. It is skipped like an unknown
        # node, and a later well-formed owner (or the first interface) wins.
        malformed = SimpleNamespace(nodes={"!ab12cd34": "not-a-dict"})
        owner = SimpleNamespace(nodes={"!ab12cd34": _pubkey_node()})
        iface, dest, ready = send_path.dm_send_target("!ab12cd34", [malformed, owner])
        self.assertIs(iface, owner)
        self.assertEqual(dest, "!ab12cd34")
        self.assertTrue(ready)

    def test_non_dict_node_info_on_sole_interface_falls_back_to_first(self):
        # When every interface holds a malformed entry, the send falls back to
        # the first interface with the original dest (the library may resolve
        # it) instead of raising.
        malformed = SimpleNamespace(nodes={"!ab12cd34": 12345})
        iface, dest, ready = send_path.dm_send_target("!ab12cd34", [malformed])
        self.assertIs(iface, malformed)
        self.assertEqual(dest, "!ab12cd34")
        self.assertTrue(ready)


class TestChannelSendTarget(unittest.TestCase):
    def _channel_field(self, ch, key):
        return ch.get(key)

    def test_numeric_spec_maps_directly_on_first_interface(self):
        iface_a = SimpleNamespace(
            localNode=SimpleNamespace(channels=[_channel("Primary", 0), _channel("Diy", 1)])
        )
        iface_b = SimpleNamespace(localNode=SimpleNamespace(channels=[]))
        index, iface = send_path.channel_send_target(
            ["meshtastic", "channel", "1"], [iface_a, iface_b], self._channel_field
        )
        self.assertEqual(index, 1)
        self.assertIs(iface, iface_a)

    def test_numeric_spec_out_of_range_is_rejected(self):
        """A numeric spec exceeding every interface's channel table returns the
        reject sentinel, not an index handed to the radio unverified."""
        iface = SimpleNamespace(localNode=SimpleNamespace(channels=[_channel("Primary", 0)]))
        index, iface_out = send_path.channel_send_target(
            ["meshtastic", "channel", "2"], [iface], self._channel_field
        )
        self.assertIsNone(index)
        self.assertIsNone(iface_out)

    def test_negative_numeric_spec_is_rejected(self):
        """A ``"-1"`` spec must not silently fall through to channel 0."""
        iface = SimpleNamespace(localNode=SimpleNamespace(channels=[_channel("Primary", 0)]))
        index, _ = send_path.channel_send_target(
            ["meshtastic", "channel", "-1"], [iface], self._channel_field
        )
        self.assertIsNone(index)

    def test_numeric_spec_dispatched_to_owning_interface(self):
        """A numeric index present only on a later interface is dispatched to
        that owner, not to ``ifaces[0]`` which may lack the channel — channel
        indexes are per-radio, so handing the spec to a radio without that
        channel would silently misdeliver."""
        iface_a = SimpleNamespace(localNode=SimpleNamespace(channels=[_channel("Primary", 0)]))
        iface_b = SimpleNamespace(localNode=SimpleNamespace(channels=[_channel("Primary", 7)]))
        index, iface = send_path.channel_send_target(
            ["meshtastic", "channel", "7"], [iface_a, iface_b], self._channel_field
        )
        self.assertEqual(index, 7)
        self.assertIs(iface, iface_b)

    def test_numeric_spec_first_owner_wins_when_on_multiple_interfaces(self):
        # Index present on two interfaces: the FIRST owner dispatches it.
        iface_a = SimpleNamespace(localNode=SimpleNamespace(channels=[_channel("Primary", 5)]))
        iface_b = SimpleNamespace(localNode=SimpleNamespace(channels=[_channel("Primary", 5)]))
        index, iface = send_path.channel_send_target(
            ["meshtastic", "channel", "5"], [iface_a, iface_b], self._channel_field
        )
        self.assertEqual(index, 5)
        self.assertIs(iface, iface_a)

    def test_numeric_spec_unvalidatable_when_no_channel_table(self):
        """No interface exposes localNode.channels: pass the index through
        unchanged (nothing to validate against on such hardware)."""
        bare = SimpleNamespace(localNode=SimpleNamespace(channels=None))
        index, iface = send_path.channel_send_target(
            ["meshtastic", "channel", "3"], [bare], self._channel_field
        )
        self.assertEqual(index, 3)
        self.assertIs(iface, bare)

    def test_named_spec_resolves_on_owning_interface(self):
        iface_a = SimpleNamespace(localNode=SimpleNamespace(channels=[]))
        iface_b = SimpleNamespace(localNode=SimpleNamespace(channels=[_channel("Primary", 1)]))
        index, iface = send_path.channel_send_target(
            ["meshtastic", "channel", "Primary"], [iface_a, iface_b], self._channel_field
        )
        self.assertEqual(index, 1)
        self.assertIs(iface, iface_b)

    def test_named_spec_is_case_insensitive(self):
        iface = SimpleNamespace(localNode=SimpleNamespace(channels=[_channel("primary", 3)]))
        index, _ = send_path.channel_send_target(
            ["meshtastic", "channel", "PRIMARY"], [iface], self._channel_field
        )
        self.assertEqual(index, 3)

    def test_first_match_wins_across_interfaces(self):
        # Both interfaces expose the same channel name (case-insensitive):
        # the FIRST interface's channel wins, never a later one's.
        iface_a = SimpleNamespace(localNode=SimpleNamespace(channels=[_channel("LongFast", 0)]))
        iface_b = SimpleNamespace(localNode=SimpleNamespace(channels=[_channel("longfast", 9)]))
        index, iface = send_path.channel_send_target(
            ["meshtastic", "channel", "LongFast"], [iface_a, iface_b], self._channel_field
        )
        self.assertEqual(index, 0)
        self.assertIs(iface, iface_a)

    def test_second_named_channel_reachable_on_later_interface(self):
        iface_a = SimpleNamespace(localNode=SimpleNamespace(channels=[_channel("LongFast", 0)]))
        iface_b = SimpleNamespace(
            localNode=SimpleNamespace(channels=[_channel("longfast", 6), _channel("diy", 7)])
        )
        index, iface = send_path.channel_send_target(
            ["meshtastic", "channel", "Diy"], [iface_a, iface_b], self._channel_field
        )
        self.assertEqual(index, 7)
        self.assertIs(iface, iface_b)

    def test_unnamed_channel_defaults_to_index_zero(self):
        iface = SimpleNamespace(localNode=SimpleNamespace(channels=[_channel("Primary", None)]))
        index, _ = send_path.channel_send_target(
            ["meshtastic", "channel", "Primary"], [iface], self._channel_field
        )
        self.assertEqual(index, 0)

    def test_no_match_falls_back_to_channel_zero_first_interface(self):
        """No channel table anywhere: nothing to validate against, so the named
        spec falls back to the legacy channel 0 on the first interface."""
        bare = SimpleNamespace(localNode=SimpleNamespace(channels=None))
        index, iface = send_path.channel_send_target(
            ["meshtastic", "channel", "Nope"], [bare], self._channel_field
        )
        self.assertEqual(index, 0)
        self.assertIs(iface, bare)

    def test_unmatched_named_spec_is_rejected_when_table_exposed(self):
        """A name that matches no channel on a table-exposing interface must not
        silently broadcast on channel 0 while the caller thinks it reached the
        named channel — return the reject sentinel like an out-of-range index."""
        iface_a = SimpleNamespace(localNode=SimpleNamespace(channels=[_channel("Primary", 1)]))
        iface_b = SimpleNamespace(localNode=SimpleNamespace(channels=[_channel("Other", 2)]))
        index, iface = send_path.channel_send_target(
            ["meshtastic", "channel", "Nope"], [iface_a, iface_b], self._channel_field
        )
        self.assertIsNone(index)
        self.assertIsNone(iface)

    def test_empty_channel_table_exposes_validation(self):
        """An interface with an empty channels list still exposes the table, so
        an unmatched name is rejected rather than falling back to channel 0."""
        empty = SimpleNamespace(localNode=SimpleNamespace(channels=[]))
        index, iface = send_path.channel_send_target(
            ["meshtastic", "channel", "Nope"], [empty], self._channel_field
        )
        self.assertIsNone(index)
        self.assertIsNone(iface)

    def test_missing_local_node_or_channels_skips_interface(self):
        bare = SimpleNamespace()
        owner = SimpleNamespace(localNode=SimpleNamespace(channels=[_channel("Primary", 4)]))
        index, iface = send_path.channel_send_target(
            ["meshtastic", "channel", "Primary"], [bare, owner], self._channel_field
        )
        self.assertEqual(index, 4)
        self.assertIs(iface, owner)

    def test_missing_channel_spec_defaults_to_zero(self):
        iface = SimpleNamespace(localNode=SimpleNamespace(channels=[_channel("Primary", 0)]))
        index, _ = send_path.channel_send_target(
            ["meshtastic", "channel"], [iface], self._channel_field
        )
        self.assertEqual(index, 0)

    def test_empty_interface_list_raises(self):
        with self.assertRaises(ValueError):
            send_path.channel_send_target(["meshtastic", "channel", "1"], [], self._channel_field)

    def test_protobuf_shaped_channel_is_validated(self):
        """Hardware-style protobuf Channels (settings.name, no .get) work too."""

        class ProtoChannel:
            def __init__(self, name, index):
                self.settings = SimpleNamespace(name=name)
                self.index = index

        iface = SimpleNamespace(localNode=SimpleNamespace(channels=[ProtoChannel("Primary", 0)]))
        # Named match reads settings.name.
        index, iface_out = send_path.channel_send_target(
            ["meshtastic", "channel", "PRIMARY"], [iface], inbound.channel_field
        )
        self.assertEqual(index, 0)
        self.assertIs(iface_out, iface)
        # Out-of-range numeric spec is still rejected against a protobuf table.
        out_index, out_iface = send_path.channel_send_target(
            ["meshtastic", "channel", "9"], [iface], inbound.channel_field
        )
        self.assertIsNone(out_index)
        self.assertIsNone(out_iface)


if __name__ == "__main__":
    unittest.main()
