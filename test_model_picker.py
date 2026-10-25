"""Unit tests for model_picker.py (numbered /model drill-down for LoRa)."""

import unittest

import model_picker
from model_picker import ModelPicker

PROVIDERS = [
    {"slug": "local", "name": "localhost:8080", "models": ["qwen"], "is_current": True},
    {"slug": "anthropic", "name": "Anthropic", "models": ["claude-fable-5", "claude-sonnet-5"]},
    {"slug": "empty", "name": "Empty endpoint", "models": []},
]
CALLBACK = object()


class _Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def _picker() -> tuple[ModelPicker, _Clock]:
    clock = _Clock()
    return ModelPicker(clock=clock), clock


class TestMenus(unittest.TestCase):
    def test_provider_menu_is_plain_numbered_text(self) -> None:
        picker, _ = _picker()
        menu = picker.open("c", PROVIDERS, "qwen", CALLBACK)
        self.assertEqual(
            menu,
            "Model: qwen\n1 localhost:8080 (1) *\n2 Anthropic (2)\nReply with a number, 0 to cancel",
        )

    def test_single_provider_starts_at_its_models(self) -> None:
        picker, _ = _picker()
        menu = picker.open("c", PROVIDERS[1:], "claude-sonnet-5", CALLBACK)
        self.assertEqual(
            menu,
            "Anthropic\n1 claude-fable-5\n2 claude-sonnet-5 *\nReply with a number, 0 to cancel",
        )

    def test_nothing_pickable(self) -> None:
        picker, _ = _picker()
        self.assertIsNone(picker.open("c", PROVIDERS[2:], "qwen", CALLBACK))
        self.assertFalse(picker.answer("c", "1").handled)


class TestAnswers(unittest.TestCase):
    def test_provider_then_model_selects(self) -> None:
        picker, _ = _picker()
        picker.open("c", PROVIDERS, "qwen", CALLBACK)
        step = picker.answer("c", " 2 ")
        self.assertTrue(step.handled)
        self.assertTrue(step.text.startswith("Anthropic\n1 claude-fable-5"))
        pick = picker.answer("c", "1")
        self.assertEqual(
            (pick.handled, pick.model, pick.provider_slug, pick.on_selected),
            (True, "claude-fable-5", "anthropic", CALLBACK),
        )
        # The picker is closed after a choice.
        self.assertFalse(picker.answer("c", "1").handled)

    def test_zero_cancels(self) -> None:
        picker, _ = _picker()
        picker.open("c", PROVIDERS, "qwen", CALLBACK)
        reply = picker.answer("c", "0")
        self.assertEqual((reply.handled, reply.text, reply.model), (True, "Model unchanged", None))
        self.assertFalse(picker.answer("c", "1").handled)

    def test_out_of_range_keeps_the_picker_open(self) -> None:
        picker, _ = _picker()
        picker.open("c", PROVIDERS, "qwen", CALLBACK)
        self.assertEqual(picker.answer("c", "9").text, "Pick 1-2 or 0")
        picker.answer("c", "2")
        self.assertEqual(picker.answer("c", "5").text, "Pick 1-2 or 0")
        self.assertEqual(picker.answer("c", "2").model, "claude-sonnet-5")

    def test_text_passes_through_and_ends_the_picker(self) -> None:
        picker, _ = _picker()
        picker.open("c", PROVIDERS, "qwen", CALLBACK)
        self.assertFalse(picker.answer("c", "what is the weather").handled)
        self.assertFalse(picker.answer("c", "1").handled)

    def test_other_chats_are_untouched(self) -> None:
        picker, _ = _picker()
        picker.open("c", PROVIDERS, "qwen", CALLBACK)
        self.assertFalse(picker.answer("other", "1").handled)
        self.assertTrue(picker.answer("c", "1").handled)

    def test_expired_picker_lets_numbers_through(self) -> None:
        picker, clock = _picker()
        picker.open("c", PROVIDERS, "qwen", CALLBACK)
        clock.now += model_picker.PICKER_TTL_SECS
        self.assertFalse(picker.answer("c", "1").handled)

    def test_choosing_a_provider_extends_the_deadline(self) -> None:
        picker, clock = _picker()
        picker.open("c", PROVIDERS, "qwen", CALLBACK)
        clock.now += model_picker.PICKER_TTL_SECS - 1
        picker.answer("c", "2")
        clock.now += model_picker.PICKER_TTL_SECS - 1
        self.assertEqual(picker.answer("c", "1").model, "claude-fable-5")

    def test_pending_pickers_are_bounded(self) -> None:
        picker, _ = _picker()
        for i in range(model_picker.PICKER_LIMIT + 10):
            picker.open(f"c{i}", PROVIDERS, "qwen", CALLBACK)
        self.assertEqual(len(picker._pending), model_picker.PICKER_LIMIT)
        self.assertFalse(picker.answer("c0", "1").handled)
        self.assertTrue(picker.answer(f"c{model_picker.PICKER_LIMIT + 9}", "1").handled)


if __name__ == "__main__":
    unittest.main()
