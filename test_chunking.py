"""Pure-function tests for the chunking module."""

import os
import time
import unittest
from unittest.mock import patch

import chunking


class TestChunking(unittest.TestCase):
    def setUp(self) -> None:
        self._env_patcher = patch.dict(os.environ, {"MESHTASTIC_CHUNK_BYTES": ""})
        self._env_patcher.start()

    def tearDown(self) -> None:
        self._env_patcher.stop()

    def test_mixed_ascii_emoji_chunk_reconstruction(self):
        """Verify mixed ASCII and emoji chunks reconstruct without dropping spaces."""
        message = ("status update " * 30) + ("💩" * 40) + " final words"
        chunks = chunking.chunk_message(message)

        self.assertGreater(len(chunks), 1)
        for chunk in chunks:
            self.assertLessEqual(len(chunk.encode("utf-8")), chunking.MAX_MESSAGE_LENGTH)

        reconstructed = "".join(chunk.split("] ", 1)[1] for chunk in chunks)
        self.assertEqual(reconstructed, message)

    def test_default_chunk_budget_is_conservative(self):
        """With no override, chunks stay within the conservative default budget.

        The raw protocol ceiling is 233 bytes, but that leaves no room for
        encrypted-DM (PKI) overhead — the radio NAKs oversized DM chunks with
        TOO_LARGE — so the default must be lower.
        """
        self.assertEqual(chunking.DEFAULT_CHUNK_BYTES, 170)
        self.assertEqual(chunking.MAX_MESSAGE_LENGTH, 233)
        # setUp leaves MESHTASTIC_CHUNK_BYTES blank → default budget applies.
        chunks = chunking.chunk_message("A" * 400)
        self.assertGreater(len(chunks), 1)
        for chunk in chunks:
            self.assertLessEqual(len(chunk.encode("utf-8")), chunking.DEFAULT_CHUNK_BYTES)

    def test_chunk_bytes_clamped_to_protocol_ceiling(self):
        """MESHTASTIC_CHUNK_BYTES above the 233-byte ceiling is clamped down."""
        # A single-chunk payload (<= default 170) is unaffected by the override.
        with patch.dict(os.environ, {"MESHTASTIC_CHUNK_BYTES": "500"}):
            chunks = chunking.chunk_message("short message")
        self.assertEqual(chunks, ["short message"])
        # A long payload over 233 bytes must still split — never a single 500-byte chunk.
        long = "y" * 400
        with patch.dict(os.environ, {"MESHTASTIC_CHUNK_BYTES": "500"}):
            chunks = chunking.chunk_message(long)
        self.assertGreater(len(chunks), 1)
        for c in chunks:
            self.assertLessEqual(len(c.encode("utf-8")), chunking.MAX_MESSAGE_LENGTH)

    def test_chunk_bytes_garbage_falls_back_to_default(self):
        """A non-numeric MESHTASTIC_CHUNK_BYTES falls back to the default, not crash."""
        long = "z" * 400  # exceeds the 170 default, so it must still split
        with patch.dict(os.environ, {"MESHTASTIC_CHUNK_BYTES": "not-a-number"}):
            chunks = chunking.chunk_message(long)
        self.assertGreater(len(chunks), 1)
        for c in chunks:
            self.assertLessEqual(len(c.encode("utf-8")), chunking.DEFAULT_CHUNK_BYTES)

    def test_split_utf8_handles_no_whitespace_and_multibyte(self):
        """split_utf8 splits long runs without spaces and respects UTF-8 boundaries."""
        # No whitespace: must still split by byte budget (char_idx<=0 path never trips).
        no_ws = "x" * 500
        parts = chunking.split_utf8(no_ws, 50)
        self.assertTrue(len(parts) > 1)
        self.assertEqual("".join(parts), no_ws)
        # Multi-byte: a split point must never land inside a UTF-8 character.
        multibyte = "日本語" * 50  # 3 bytes/char
        parts = chunking.split_utf8(multibyte, 20)
        self.assertEqual("".join(parts), multibyte)
        for p in parts:
            p.encode("utf-8")  # each part is valid UTF-8 on its own

    def test_empty_and_whitespace_only_content_returns_no_chunks(self):
        """Empty / whitespace-only content yields no chunks (send() fails it)."""
        self.assertEqual(chunking.chunk_message(""), [])
        self.assertEqual(chunking.chunk_message("   "), [])
        self.assertEqual(chunking.chunk_message("\n\t "), [])

    def test_content_whitespace_preserved_through_chunking(self):
        """Leading/trailing whitespace survives chunking — the content is not stripped."""
        message = "  status update " + ("x" * 300) + " final words  "
        chunks = chunking.chunk_message(message)
        self.assertGreater(len(chunks), 1)
        reconstructed = "".join(chunk.split("] ", 1)[1] for chunk in chunks)
        self.assertEqual(reconstructed, message)

    def test_chunk_bytes_zero_negative_clamped(self):
        """0 / negative MESHTASTIC_CHUNK_BYTES clamp to the floor (30)."""
        for value in ("0", "-5"):
            with patch.dict(os.environ, {"MESHTASTIC_CHUNK_BYTES": value}):
                chunks = chunking.chunk_message("A" * 40)
            self.assertGreater(len(chunks), 1)
            for c in chunks:
                self.assertLessEqual(len(c.encode("utf-8")), chunking.MAX_MESSAGE_LENGTH)

    def test_chunk_bytes_float_values_are_honored_not_defaulted(self):
        """Float-looking MESHTASTIC_CHUNK_BYTES change the budget, not a silent default.

        Each probe uses content whose chunk count differs between the honored
        budget and a fallen-back default of 170, so a regression that drops the
        float parse (``int(float(raw))`` in chunking) fails the assertion.
        """
        # Honored 180 vs fallen-back 170: 175 bytes is one chunk at 180, two at 170.
        with patch.dict(os.environ, {"MESHTASTIC_CHUNK_BYTES": "180.0"}):
            chunks = chunking.chunk_message("A" * 175)
        self.assertEqual(chunks, ["A" * 175])
        # Honored 100 ("1e2") vs fallen-back 170: 150 bytes is two chunks at 100, one at 170.
        with patch.dict(os.environ, {"MESHTASTIC_CHUNK_BYTES": "1e2"}):
            chunks = chunking.chunk_message("A" * 150)
        self.assertGreater(len(chunks), 1)
        for c in chunks:
            self.assertLessEqual(len(c.encode("utf-8")), 100)

    def test_chunk_bytes_float_like_garbage_falls_back_to_default(self):
        """A float-shaped value that still fails to parse warns and uses the default."""
        long = "z" * 175  # one chunk at 180, two at 170 — pins the fallback budget
        with (
            patch.dict(os.environ, {"MESHTASTIC_CHUNK_BYTES": "1e"}),
            self.assertLogs("chunking", level="WARNING") as logs,
        ):
            chunks = chunking.chunk_message(long)
        self.assertGreater(len(chunks), 1)
        for c in chunks:
            self.assertLessEqual(len(c.encode("utf-8")), chunking.DEFAULT_CHUNK_BYTES)
        self.assertTrue(any("MESHTASTIC_CHUNK_BYTES" in m for m in logs.output))

    def test_chunk_bytes_garbage_warns_and_falls_back(self):
        """A non-numeric value warns once and falls back to the default."""
        long = "z" * 400
        with (
            patch.dict(os.environ, {"MESHTASTIC_CHUNK_BYTES": "not-a-number"}),
            self.assertLogs("chunking", level="WARNING") as logs,
        ):
            chunks = chunking.chunk_message(long)
        self.assertGreater(len(chunks), 1)
        self.assertTrue(any("MESHTASTIC_CHUNK_BYTES" in m for m in logs.output))

    def test_parse_chunk_prefix(self):
        """The [i/n] fragment prefix is recognized for inbound observability."""
        self.assertEqual(chunking.parse_chunk_prefix("[1/3] hello"), (1, 3))
        self.assertEqual(chunking.parse_chunk_prefix("[10/15] x"), (10, 15))
        self.assertIsNone(chunking.parse_chunk_prefix("plain message"))
        self.assertIsNone(chunking.parse_chunk_prefix("[1/3]"))
        self.assertIsNone(chunking.parse_chunk_prefix("[x/3] y"))
        self.assertIsNone(chunking.parse_chunk_prefix("  [1/3] y"))

    def test_split_utf8_oversized_single_char_emitted_whole(self):
        """A UTF-8 sequence is never split: an over-wide char is emitted whole.

        ``limit >= 4`` (the widest UTF-8 char) keeps every chunk within budget;
        at a pathological smaller limit a chunk may exceed it by one character.
        """
        self.assertEqual(chunking.split_utf8("🙂x", 2), ["🙂", "x"])

    def test_multi_digit_prefix_numbering_consistent(self):
        """[i/n] numbering matches the emitted chunk count for 2-digit totals."""
        chunks = chunking.chunk_message("B" * 2000)
        self.assertGreaterEqual(len(chunks), 10)
        for i, c in enumerate(chunks, start=1):
            self.assertTrue(c.startswith(f"[{i}/{len(chunks)}] "))
        reconstructed = "".join(c.split("] ", 1)[1] for c in chunks)
        self.assertEqual(reconstructed, "B" * 2000)

    def test_chunk_message_caps_total_chunks(self):
        """Absurdly long content raises instead of flooding the shared channel."""
        # 10 KB at the default budget estimates ~64 chunks > the 60-chunk cap.
        with self.assertRaises(ValueError):
            chunking.chunk_message("Z" * 10000)
        # Just under the cap splits normally.
        chunks = chunking.chunk_message("Q" * 3000)
        self.assertGreater(len(chunks), 1)
        self.assertLess(len(chunks), chunking.MAX_CHUNKS_PER_MESSAGE)

    def test_chunk_round_trip_preserves_content_within_budget(self):
        """Split → reconstruct is lossless and every chunk stays in budget.

        A deterministic mix of ASCII runs, whitespace, multi-byte text and
        [i/n]-looking content exercises the split/convergence fallbacks
        (chunking.py non-convergence re-split and the empty-parts guard) the
        same way a fuzz run would, without depending on random seed state.
        """
        now = str(time.time())
        messages = [
            "x" * 500,
            ("word " * 80) + " tail",
            "日本語" * 40,
            "emoji 🚀🚀 " * 30 + "done",
            "prefix-like " + ("b" * 300) + f" [1/3] {now}",
            "  spaced " + ("a" * 300) + " end  ",
            "\t".join(f"c{i}" for i in range(60)),
        ]
        for message in messages:
            with self.subTest(message=message[:30]):
                chunks = chunking.chunk_message(message)
                for c in chunks:
                    self.assertLessEqual(len(c.encode("utf-8")), chunking.MAX_MESSAGE_LENGTH)
                reconstructed = "".join(c.split("] ", 1)[1] for c in chunks)
                self.assertEqual(reconstructed, message)

    def test_chunk_bytes_non_finite_floats_fall_back_to_default(self):
        """inf / -inf / Infinity / 1e400 must not crash — they fall back to default.

        ``int(float("inf"))`` raises ``OverflowError`` (an ``ArithmeticError``,
        not ``ValueError``), which the send path does not catch. Regression
        guard for the widened ``except (ValueError, OverflowError)`` in
        ``_effective_chunk_bytes`` — without it every send (short or long)
        crashes because the budget is parsed before the short-message early-out.
        """
        long = "z" * 175  # one chunk at 180, two at the 170 default
        for value in ("inf", "-inf", "Infinity", "INF", "1e400", "2e308"):
            with (
                self.subTest(value=value),
                patch.dict(os.environ, {"MESHTASTIC_CHUNK_BYTES": value}),
                self.assertLogs("chunking", level="WARNING") as logs,
            ):
                chunks = chunking.chunk_message(long)
            self.assertGreater(len(chunks), 1)
            for c in chunks:
                self.assertLessEqual(len(c.encode("utf-8")), chunking.DEFAULT_CHUNK_BYTES)
            self.assertTrue(any("MESHTASTIC_CHUNK_BYTES" in m for m in logs.output))
        # Short messages also must not crash — the bug fires before the early-out.
        with patch.dict(os.environ, {"MESHTASTIC_CHUNK_BYTES": "inf"}):
            self.assertEqual(chunking.chunk_message("test"), ["test"])

    def test_numbered_split_prefix_consistent_with_emitted_length(self):
        """``_numbered_split`` prefixes always match the actual emitted count.

        Calling it directly with a ``total`` that disagrees with the natural
        count reproduces the inconsistent-numbering hazard the non-convergence
        fallback guards against. With a matching ``total`` the [i/n] prefixes
        line up exactly.
        """
        content = "w" * 400
        chunks = chunking._numbered_split(content, chunking.DEFAULT_CHUNK_BYTES, 3)
        # With the natural count, every prefix denominator equals len(chunks).
        self.assertTrue(all(c.startswith("[") for c in chunks))
        for i, c in enumerate(chunks, start=1):
            self.assertTrue(c.startswith(f"[{i}/{len(chunks)}] "))
        reconstructed = "".join(c.split("] ", 1)[1] for c in chunks)
        self.assertEqual(reconstructed, content)

    def test_chunk_message_numbering_always_self_consistent(self):
        """The non-convergence fallback never emits [i/n] totals that disagree.

        Exercises dense multibyte content (the case most likely to perturb the
        convergence sequence) and verifies the [i/n] denominator equals the
        emitted chunk length for every fragment, even if the fallback re-split
        path were ever taken.
        """
        for content in ("😀" * 1200, "日本語" * 150, ("B" * 2000)):
            with self.subTest(content_len=len(content)):
                chunks = chunking.chunk_message(content)
                self.assertGreater(len(chunks), 1)
                for i, c in enumerate(chunks, start=1):
                    self.assertTrue(c.startswith(f"[{i}/{len(chunks)}] "))
                reconstructed = "".join(c.split("] ", 1)[1] for c in chunks)
                self.assertEqual(reconstructed, content)

    def test_split_utf8_multibyte_performance_under_budget(self):
        """Dense-emoji chunking stays well under 200 ms (O(n), not O(n^2)).

        Regression guard for the single-pass byte-accumulator rewrite of the
        inner fit loop; a quadratic reimplementation would blow this ceiling.
        2000 emoji (8 KB, ~50 chunks) sits just under the 60-chunk cap.
        """
        start = time.perf_counter()
        chunks = chunking.chunk_message("😀" * 2000)
        elapsed = time.perf_counter() - start
        self.assertGreater(len(chunks), 1)
        for c in chunks:
            self.assertLessEqual(len(c.encode("utf-8")), chunking.MAX_MESSAGE_LENGTH)
        self.assertLess(elapsed, 0.2)

    def test_split_utf8_edge_cases(self):
        """Empty input yields no chunks; a tiny limit emits one char per chunk."""
        self.assertEqual(chunking.split_utf8("", 10), [])
        # limit below any character: each char is its own (oversized) chunk.
        single = chunking.split_utf8("abc", 0)
        self.assertEqual("".join(single), "abc")

    def test_chunk_message_none_returns_no_chunks(self):
        """A ``None`` content is coerced to empty and yields no chunks."""
        self.assertEqual(chunking.chunk_message(None), [])  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
