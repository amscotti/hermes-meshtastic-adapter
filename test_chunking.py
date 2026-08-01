"""Pure-function tests for the chunking module."""

import os
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


if __name__ == "__main__":
    unittest.main()
