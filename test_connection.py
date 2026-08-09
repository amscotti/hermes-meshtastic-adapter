"""
Unit tests for the pure connection-lifecycle decision helpers in connection.py.

The reconnect/disconnect lifecycle's decisions were extracted from
adapter._reconnect_loop / _disconnect_impl so they can be tested
decision-table style without an adapter instance: backoff schedule, per-tick
reconnect steps, pause-state classification, close-wait policies, link-drop
classification, and teardown step planning. Everything here is synchronous
and pure.
"""

import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import connection


def _task(loop):
    """A duck-typed task owned by ``loop`` (get_loop is all we need)."""
    return SimpleNamespace(get_loop=lambda: loop)


class TestBackoffSchedule(unittest.TestCase):
    def test_next_backoff_doubles_from_initial(self):
        self.assertEqual(connection.next_backoff(connection.INITIAL_BACKOFF), 2.0)
        self.assertEqual(connection.next_backoff(2.0), 4.0)
        self.assertEqual(connection.next_backoff(4.0), 8.0)

    def test_next_backoff_capped_at_max(self):
        self.assertEqual(connection.next_backoff(30.0), 60.0)
        self.assertEqual(connection.next_backoff(60.0), 60.0)
        self.assertEqual(connection.next_backoff(10_000.0), 60.0)

    def test_next_backoff_respects_custom_cap(self):
        self.assertEqual(connection.next_backoff(40.0, cap=45.0), 45.0)
        self.assertEqual(connection.next_backoff(20.0, cap=45.0), 40.0)

    def test_next_backoff_cap_below_initial_returns_cap(self):
        # A cap below INITIAL_BACKOFF returns the cap (min of the doubled value
        # and the cap). Unreachable today (only the default MAX_BACKOFF is
        # passed) but documents the behavior if the cap is ever exposed.
        self.assertEqual(connection.next_backoff(1.0, cap=0.5), 0.5)
        self.assertEqual(connection.next_backoff(40.0, cap=0.5), 0.5)

    def test_reset_backoff_returns_initial(self):
        self.assertEqual(connection.reset_backoff(), connection.INITIAL_BACKOFF)
        self.assertEqual(connection.reset_backoff(), 1.0)

    def test_next_backoff_rejects_non_finite_and_non_positive(self):
        # NaN would propagate through min() and crash asyncio.sleep; Inf would
        # clamp to the cap; zero would make the reconnect loop spin hot.
        for bad in (float("nan"), float("inf"), float("-inf"), 0.0, -1.0):
            with self.subTest(backoff=bad):
                self.assertEqual(connection.next_backoff(bad), connection.INITIAL_BACKOFF)


class TestReconnectStep(unittest.TestCase):
    """reconnect_step decision matrix: (paused, has_interface) -> step."""

    def test_paused_with_interface_releases(self):
        self.assertEqual(connection.reconnect_step(True, True), connection.RELEASE)

    def test_paused_without_interface_waits(self):
        self.assertEqual(connection.reconnect_step(True, False), connection.WAIT)

    def test_unpaused_without_interface_connects(self):
        self.assertEqual(connection.reconnect_step(False, False), connection.CONNECT)

    def test_unpaused_with_interface_polls(self):
        self.assertEqual(connection.reconnect_step(False, True), connection.POLL)

    def test_full_decision_table(self):
        cases = [
            (True, True, connection.RELEASE),
            (True, False, connection.WAIT),
            (False, False, connection.CONNECT),
            (False, True, connection.POLL),
        ]
        for paused, has_iface, expected in cases:
            with self.subTest(paused=paused, has_iface=has_iface):
                self.assertEqual(connection.reconnect_step(paused, has_iface), expected)


class TestPollOutcome(unittest.TestCase):
    def test_alive_healthy(self):
        self.assertEqual(connection.poll_outcome(True), connection.HEALTHY)

    def test_dead_drop(self):
        self.assertEqual(connection.poll_outcome(False), connection.DROP)

    def test_target_changed_exit(self):
        # The probed interface is no longer the registered one.
        self.assertEqual(connection.poll_outcome(None), connection.EXIT)


class TestPauseClassification(unittest.TestCase):
    """pause_classify decision table: (paused, until, now) -> state."""

    def test_unpaused(self):
        self.assertEqual(connection.pause_classify(False, None, 1000.0), connection.NOT_PAUSED)
        self.assertEqual(connection.pause_classify(False, 500.0, 1000.0), connection.NOT_PAUSED)

    def test_untimed_pause(self):
        self.assertEqual(connection.pause_classify(True, None, 1000.0), connection.UNTIMED_PAUSE)

    def test_timed_pause_before_deadline(self):
        self.assertEqual(connection.pause_classify(True, 2000.0, 1000.0), connection.TIMED_PAUSE)

    def test_timed_pause_expired_at_and_after_deadline(self):
        self.assertEqual(connection.pause_classify(True, 1000.0, 1000.0), connection.PAUSE_EXPIRED)
        self.assertEqual(connection.pause_classify(True, 500.0, 1000.0), connection.PAUSE_EXPIRED)

    def test_full_decision_table(self):
        cases = [
            (False, None, 0.0, connection.NOT_PAUSED),
            (False, 100.0, 200.0, connection.NOT_PAUSED),
            (True, None, 0.0, connection.UNTIMED_PAUSE),
            (True, 200.0, 100.0, connection.TIMED_PAUSE),
            (True, 100.0, 100.0, connection.PAUSE_EXPIRED),
            (True, 100.0, 200.0, connection.PAUSE_EXPIRED),
        ]
        for paused, until, now, expected in cases:
            with self.subTest(paused=paused, until=until, now=now):
                self.assertEqual(connection.pause_classify(paused, until, now), expected)

    def test_nan_deadline_classified_expired(self):
        # A broken (NaN) deadline must auto-resume, not park the link in a timed
        # pause forever (now >= nan is always False).
        self.assertEqual(
            connection.pause_classify(True, float("nan"), 1000.0), connection.PAUSE_EXPIRED
        )

    def test_non_finite_now_classified_expired(self):
        # A broken (NaN/Inf) clock must auto-resume rather than wedging the link:
        # now >= deadline is always False for NaN, so an expiring timed pause
        # would otherwise stay TIMED_PAUSE forever. ``now`` is always time.time()
        # today, but the guard keeps the helper total.
        for bad in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(now=bad):
                self.assertEqual(
                    connection.pause_classify(True, 1000.0, bad), connection.PAUSE_EXPIRED
                )


class TestPauseFormatting(unittest.TestCase):
    def test_resumes_at_str_none_when_untimed(self):
        self.assertIsNone(connection.resumes_at_str(None))

    def test_resumes_at_str_none_for_non_finite_deadline(self):
        self.assertIsNone(connection.resumes_at_str(float("nan")))
        self.assertIsNone(connection.resumes_at_str(float("inf")))

    def test_resumes_at_str_formats_deadline(self):
        # 2001-02-03 04:05:06 UTC (local-tz dependent only on the host tz).
        stamped = connection.resumes_at_str(981173106.0)
        self.assertIsNotNone(stamped)
        self.assertRegex(stamped, r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")

    def test_resumes_at_str_survives_out_of_range_deadline(self):
        # A huge-but-finite deadline passes pause_link's finite guard and would
        # crash time.localtime — report None instead of raising for every
        # pause_state caller.
        self.assertIsNone(connection.resumes_at_str(1e19))
        self.assertIsNone(connection.resumes_at_str(1e18))

    def test_resumes_at_str_survives_localtime_oserror_and_valueerror(self):
        # The except also covers OSError/ValueError from time.localtime (all
        # three arms return None identically); drive them directly via a patch
        # so the arms are not coverage-dark.
        for exc in (OSError("boom"), ValueError("boom")):
            with self.subTest(exc=type(exc).__name__):
                with patch("connection.time.localtime", side_effect=exc):
                    self.assertIsNone(connection.resumes_at_str(1000.0))

    def test_resumes_in_minutes_none_when_untimed(self):
        self.assertIsNone(connection.resumes_in_minutes(None, 0.0))

    def test_resumes_in_minutes_none_for_non_finite_deadline(self):
        self.assertIsNone(connection.resumes_in_minutes(float("nan"), 0.0))
        self.assertIsNone(connection.resumes_in_minutes(float("inf"), 0.0))

    def test_resumes_in_minutes_computes_and_rounds(self):
        self.assertEqual(connection.resumes_in_minutes(60.0, 0.0), 1.0)
        self.assertEqual(connection.resumes_in_minutes(65.0, 5.0), 1.0)
        self.assertEqual(connection.resumes_in_minutes(30.0, 0.0), 0.5)

    def test_resumes_in_minutes_clamped_at_zero(self):
        self.assertEqual(connection.resumes_in_minutes(0.0, 60.0), 0.0)
        self.assertEqual(connection.resumes_in_minutes(-60.0, 60.0), 0.0)

    def test_resumes_in_minutes_non_finite_now_is_zero(self):
        # A broken clock can't compute remaining time (pause_until - nan is NaN);
        # report 0.0 — consistent with pause_classify treating a non-finite now
        # as expired — rather than propagating a misleading NaN.
        for bad in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(now=bad):
                self.assertEqual(connection.resumes_in_minutes(2000.0, bad), 0.0)


class TestCloseWaitPolicy(unittest.TestCase):
    def test_open_cancel_timeout_default(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(connection.open_cancel_timeout(), 5.0)

    def test_open_cancel_timeout_parsing(self):
        for raw, expected in (("2.5", 2.5), ("0", 0.0), ("-3", 0.0), ("bogus", 5.0), ("", 5.0)):
            with patch.dict(os.environ, {"MESHTASTIC_OPEN_CANCEL_TIMEOUT": raw}):
                self.assertEqual(connection.open_cancel_timeout(), expected, f"raw={raw!r}")

    def test_executor_shutdown_timeout_default(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(connection.executor_shutdown_timeout(), 5.0)

    def test_executor_shutdown_timeout_parsing(self):
        for raw, expected in (("1.5", 1.5), ("0", 0.0), ("-3", 0.0), ("bogus", 5.0), ("", 5.0)):
            with patch.dict(os.environ, {"MESHTASTIC_EXECUTOR_SHUTDOWN_TIMEOUT": raw}):
                self.assertEqual(connection.executor_shutdown_timeout(), expected, f"raw={raw!r}")

    def test_timeouts_are_independent_env_vars(self):
        with patch.dict(
            os.environ,
            {
                "MESHTASTIC_OPEN_CANCEL_TIMEOUT": "1.0",
                "MESHTASTIC_EXECUTOR_SHUTDOWN_TIMEOUT": "9.0",
            },
        ):
            self.assertEqual(connection.open_cancel_timeout(), 1.0)
            self.assertEqual(connection.executor_shutdown_timeout(), 9.0)

    def test_open_timeout_default(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(connection.open_timeout(), 20.0)

    def test_open_timeout_parsing(self):
        for raw, expected in (("15.0", 15.0), ("0", 0.0), ("-3", 0.0), ("bogus", 20.0), ("", 20.0)):
            with patch.dict(os.environ, {"MESHTASTIC_OPEN_TIMEOUT": raw}):
                self.assertEqual(connection.open_timeout(), expected, f"raw={raw!r}")

    def test_timeouts_reject_non_finite_env(self):
        # NaN/Inf parse as floats but must not silently disable a bound
        # (max(0.0, nan) -> 0.0) or abandon cancel waits — fall back to default.
        for raw in ("nan", "inf", "-inf", "Infinity", "-Infinity", "NaN"):
            for env_name, fn, default in (
                ("MESHTASTIC_OPEN_TIMEOUT", connection.open_timeout, 20.0),
                ("MESHTASTIC_OPEN_CANCEL_TIMEOUT", connection.open_cancel_timeout, 5.0),
                ("MESHTASTIC_EXECUTOR_SHUTDOWN_TIMEOUT", connection.executor_shutdown_timeout, 5.0),
            ):
                with self.subTest(raw=raw, env=env_name):
                    with patch.dict(os.environ, {env_name: raw}):
                        self.assertEqual(fn(), default, f"raw={raw!r}")


class TestDropClassification(unittest.TestCase):
    def test_short_outage_is_socket_reset(self):
        self.assertEqual(connection.classify_link_drop(0.1), connection.SOCKET_RESET)
        self.assertEqual(connection.classify_link_drop(5.9), connection.SOCKET_RESET)

    def test_threshold_boundary_counts_as_reset(self):
        self.assertEqual(connection.classify_link_drop(6.0), connection.SOCKET_RESET)

    def test_long_outage_is_node_absent(self):
        self.assertEqual(connection.classify_link_drop(6.1), connection.NODE_ABSENT)
        self.assertEqual(connection.classify_link_drop(120.0), connection.NODE_ABSENT)

    def test_custom_threshold(self):
        self.assertEqual(
            connection.classify_link_drop(30.0, threshold=60.0), connection.SOCKET_RESET
        )
        self.assertEqual(
            connection.classify_link_drop(90.0, threshold=60.0), connection.NODE_ABSENT
        )

    def test_negative_and_non_finite_outage_classifications(self):
        # NaN (NaN <= threshold is False) -> node_absent; a negative outage
        # (plausible via backward clock skew between drop and recovery) and 0.0
        # -> socket_reset.
        with self.subTest(outage=float("nan")):
            self.assertEqual(connection.classify_link_drop(float("nan")), connection.NODE_ABSENT)
        with self.subTest(outage=float("inf")):
            self.assertEqual(connection.classify_link_drop(float("inf")), connection.NODE_ABSENT)
        with self.subTest(outage=-5.0):
            self.assertEqual(connection.classify_link_drop(-5.0), connection.SOCKET_RESET)
        with self.subTest(outage=0.0):
            self.assertEqual(connection.classify_link_drop(0.0), connection.SOCKET_RESET)


class TestTeardownPlanning(unittest.TestCase):
    """teardown_owner_current gates: all three conditions must hold."""

    def test_owner_current_when_all_hold(self):
        completion = object()
        task = object()
        self.assertTrue(connection.teardown_owner_current(True, completion, task, completion, task))

    def test_not_owner_when_disconnect_flag_cleared(self):
        completion = object()
        task = object()
        self.assertFalse(
            connection.teardown_owner_current(False, completion, task, completion, task)
        )

    def test_not_owner_when_future_epoch_changed(self):
        completion = object()
        task = object()
        self.assertFalse(connection.teardown_owner_current(True, object(), task, completion, task))

    def test_not_owner_when_task_superseded(self):
        completion = object()
        task = object()
        self.assertFalse(
            connection.teardown_owner_current(True, completion, object(), completion, task)
        )
        self.assertFalse(
            connection.teardown_owner_current(True, completion, task, completion, object())
        )

    def test_task_list_assembly(self):
        tasks = {"a": _task("loop1"), "b": _task("loop1")}
        drain = _task("loop2")
        consumer = _task("loop3")

        self.assertEqual(connection.teardown_task_list(tasks, None, None), [tasks["a"], tasks["b"]])
        self.assertEqual(
            connection.teardown_task_list(tasks, drain, None),
            [tasks["a"], tasks["b"], drain],
        )
        self.assertEqual(
            connection.teardown_task_list(tasks, drain, consumer),
            [tasks["a"], tasks["b"], drain, consumer],
        )
        self.assertEqual(connection.teardown_task_list({}, None, None), [])

    def test_task_list_does_not_mutate_input(self):
        tasks = {"a": _task("loop1")}
        connection.teardown_task_list(tasks, _task("loop2"), None)
        self.assertEqual(list(tasks.keys()), ["a"])

    def test_tasks_on_loop_splits_by_owner(self):
        loop_a = object()
        loop_b = object()
        tasks = [_task(loop_a), _task(loop_b), _task(loop_a)]
        local = connection.tasks_on_loop(tasks, loop_a)
        self.assertEqual(local, [tasks[0], tasks[2]])
        self.assertEqual(connection.tasks_on_loop(tasks, loop_b), [tasks[1]])
        self.assertEqual(connection.tasks_on_loop([], loop_a), [])


if __name__ == "__main__":
    unittest.main()
