"""Unit tests for forwarding.py (DMs from non-allowlisted nodes → owner)."""

import unittest

import forwarding
from forwarding import ForwardConfig, ForwardGate


class _Clock:
    def __init__(self) -> None:
        self.now = 1_000_000.0

    def __call__(self) -> float:
        return self.now


def _gate(**overrides) -> tuple[ForwardGate, _Clock]:
    clock = _Clock()
    cfg = ForwardConfig(enabled=True, target="!0wner000", **overrides)
    return ForwardGate(cfg, clock=clock), clock


class TestForwardConfig(unittest.TestCase):
    def test_off_by_default(self) -> None:
        cfg = forwarding.forward_config({}, {}, "!aabbccdd")
        self.assertFalse(cfg.enabled)
        self.assertEqual(cfg.target, "!aabbccdd")

    def test_target_defaults_to_first_allowlisted_node(self) -> None:
        cfg = forwarding.forward_config(
            {"MESHTASTIC_FORWARD_UNKNOWN": "true"}, {}, " AABBCCDD , !11223344"
        )
        self.assertTrue(cfg.enabled)
        self.assertEqual(cfg.target, "!aabbccdd")

    def test_explicit_target_and_settings(self) -> None:
        env = {
            "MESHTASTIC_FORWARD_UNKNOWN": "1",
            "MESHTASTIC_FORWARD_TO": "!B1AC0074",
            "MESHTASTIC_FORWARD_LANG": "RU",
            "MESHTASTIC_FORWARD_REPLY_COOLDOWN_HOURS": "2",
            "MESHTASTIC_FORWARD_PER_HOUR": "3",
        }
        cfg = forwarding.forward_config(env, {}, "!aabbccdd")
        self.assertEqual(cfg.target, "!b1ac0074")
        self.assertEqual(cfg.language, "ru")
        self.assertEqual(cfg.reply_cooldown_secs, 7200)
        self.assertEqual(cfg.forwards_per_hour, 3)

    def test_plugin_extra_is_read(self) -> None:
        cfg = forwarding.forward_config({}, {"forward_unknown": True, "forward_lang": "ru"}, "!a")
        self.assertTrue(cfg.enabled)
        self.assertEqual(cfg.language, "ru")

    def test_forward_gate_factory_reads_config(self) -> None:
        gate = forwarding.forward_gate({"MESHTASTIC_FORWARD_UNKNOWN": "yes"}, {}, "!aabbccdd")
        self.assertTrue(gate.config.enabled)
        self.assertEqual(gate.config.target, "!aabbccdd")

    def test_no_target_keeps_it_off(self) -> None:
        cfg = forwarding.forward_config({"MESHTASTIC_FORWARD_UNKNOWN": "true"}, {}, "")
        self.assertFalse(cfg.enabled)

    def test_bad_values_fall_back_to_defaults(self) -> None:
        env = {
            "MESHTASTIC_FORWARD_LANG": "de",
            "MESHTASTIC_FORWARD_REPLY_COOLDOWN_HOURS": "soon",
            "MESHTASTIC_FORWARD_PER_HOUR": "-4",
        }
        cfg = forwarding.forward_config(env, {}, "!a")
        self.assertEqual(cfg.language, "en")
        self.assertEqual(cfg.reply_cooldown_secs, forwarding.DEFAULT_REPLY_COOLDOWN_HOURS * 3600)
        self.assertEqual(cfg.forwards_per_hour, 0)


class TestForwardGate(unittest.TestCase):
    def test_disabled_gate_does_nothing(self) -> None:
        gate = ForwardGate(ForwardConfig(enabled=False, target="!0wner000"))
        decision = gate.decide("!stranger", is_broadcast=False)
        self.assertFalse(decision.forward)
        self.assertFalse(decision.auto_reply)

    def test_first_dm_forwards_and_replies(self) -> None:
        gate, _ = _gate()
        decision = gate.decide("!stranger", is_broadcast=False)
        self.assertTrue(decision.forward)
        self.assertTrue(decision.auto_reply)

    def test_broadcast_target_and_self_are_ignored(self) -> None:
        gate, _ = _gate()
        for sender, broadcast in (("!stranger", True), ("!0wner000", False), ("!me", False)):
            decision = gate.decide(sender, is_broadcast=broadcast, my_node_id="!me")
            self.assertFalse(decision.forward, sender)
            self.assertFalse(decision.auto_reply, sender)

    def test_auto_reply_once_per_cooldown(self) -> None:
        gate, clock = _gate(reply_cooldown_secs=3600)
        self.assertTrue(gate.decide("!stranger", is_broadcast=False).auto_reply)
        clock.now += 60
        second = gate.decide("!stranger", is_broadcast=False)
        self.assertTrue(second.forward)
        self.assertFalse(second.auto_reply)
        clock.now += 3600
        self.assertTrue(gate.decide("!stranger", is_broadcast=False).auto_reply)

    def test_hourly_forward_cap_is_sliding(self) -> None:
        gate, clock = _gate(forwards_per_hour=2)
        self.assertTrue(gate.decide("!a", is_broadcast=False).forward)
        clock.now += 10
        self.assertTrue(gate.decide("!a", is_broadcast=False).forward)
        clock.now += 10
        self.assertFalse(gate.decide("!a", is_broadcast=False).forward)
        # Another sender has its own budget.
        self.assertTrue(gate.decide("!b", is_broadcast=False).forward)
        clock.now += forwarding.FORWARD_WINDOW_SECS
        self.assertTrue(gate.decide("!a", is_broadcast=False).forward)

    def test_no_auto_reply_for_a_capped_message(self) -> None:
        # The notice says the message was forwarded; it must not go out for
        # one the cap held back, even once the reply cooldown has expired.
        gate, clock = _gate(forwards_per_hour=1, reply_cooldown_secs=0)
        gate.decide("!a", is_broadcast=False)
        clock.now += 1
        decision = gate.decide("!a", is_broadcast=False)
        self.assertFalse(decision.forward)
        self.assertFalse(decision.auto_reply)

    def test_tracked_senders_are_bounded(self) -> None:
        gate, _ = _gate()
        for i in range(forwarding.SENDER_LIMIT + 50):
            gate.decide(f"!{i:08x}", is_broadcast=False)
        self.assertEqual(len(gate._forwards), forwarding.SENDER_LIMIT)
        self.assertEqual(len(gate._last_reply), forwarding.SENDER_LIMIT)


class TestFormatting(unittest.TestCase):
    def test_forward_names_the_sender_and_keeps_text(self) -> None:
        self.assertEqual(
            forwarding.format_forward("!49b555ec", "AHC", "hi there\n "),
            "✉ AHC (!49b555ec): hi there\n ",
        )

    def test_forward_without_a_known_name(self) -> None:
        self.assertEqual(
            forwarding.format_forward("!49b555ec", "!49b555ec", "hi"), "✉ !49b555ec: hi"
        )

    def test_auto_reply_languages(self) -> None:
        self.assertIn("forwarded to Base", forwarding.format_auto_reply("Base", "en"))
        self.assertIn("владельцу (Base)", forwarding.format_auto_reply("Base", "ru"))
        self.assertEqual(
            forwarding.format_auto_reply("Base", "xx"), forwarding.format_auto_reply("Base", "en")
        )


if __name__ == "__main__":
    unittest.main()
