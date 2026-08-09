"""Split text into LoRa-safe UTF-8-byte chunks with ``[i/n]`` prefixes.

The protocol app-payload ceiling is ``mesh_pb2.Constants.DATA_PAYLOAD_LEN``
(233 bytes) — ``sendData`` raises above it, so every chunk must fit.
"""

import logging
import os
import re

logger = logging.getLogger(__name__)

# Meshtastic's raw Data payload ceiling (bytes) —
# mesh_pb2.Constants.DATA_PAYLOAD_LEN (233), enforced by sendData which
# raises above it. (The 237 figure sometimes quoted is the LoRa frame size;
# the usable app-payload is 233.)
MAX_MESSAGE_LENGTH = 233

# Default per-chunk byte budget. Even the 233 ceiling leaves no headroom for
# PKI/encryption overhead on direct messages — the radio NAKs oversized DM
# chunks with TOO_LARGE — so the out-of-the-box default is conservative and
# also helps multi-hop reliability. Override with MESHTASTIC_CHUNK_BYTES.
DEFAULT_CHUNK_BYTES = 170

# Floor for MESHTASTIC_CHUNK_BYTES. Below this a 0 / negative / tiny value
# would silently degrade every message into tiny ``[1/1]``-prefixed chunks
# (and a 0/-N budget would force the numbered path for every message).
MIN_CHUNK_BYTES = 30

# Cap on chunks per message. Above this a send would flood the shared LoRa
# channel for minutes with no ceiling — chunk_message raises instead.
MAX_CHUNKS_PER_MESSAGE = 60


def chunk_message(content: str) -> list[str]:
    """Split text into LoRa-safe UTF-8 byte chunks with sequence prefixes.

    Content is preserved exactly (including leading/trailing whitespace);
    only the emptiness decision strips. Whitespace-only content yields no
    chunks. Raises ``ValueError`` when the message would need more than
    ``MAX_CHUNKS_PER_MESSAGE`` chunks.
    """
    content = content or ""
    if not content or not content.strip():
        return []
    limit = _effective_chunk_bytes()
    if len(content.encode("utf-8")) <= limit:
        return [content]

    # We will iterate to find the correct number of chunks. The estimate uses
    # a 12-byte prefix budget (the widest a [i/n] prefix needs below ~1000
    # chunks), so it is an upper bound on the numbered count — a safe cap check.
    capacity = max(10, limit - 12)
    total = len(split_utf8(content, capacity))
    if total > MAX_CHUNKS_PER_MESSAGE:
        raise ValueError(
            f"message too long: {total} chunks would exceed the "
            f"{MAX_CHUNKS_PER_MESSAGE}-chunk limit"
        )

    for _ in range(5):
        chunks = _numbered_split(content, limit, total)
        if len(chunks) == total:
            return chunks
        total = len(chunks)

    # Non-convergence is pathological (never observed in fuzz); ``_numbered_split``
    # derives the chunk count from the content/capacity, not from ``total`` (which
    # only drives the prefix string). So if the re-split count disagrees with
    # ``total``, re-split once more with the actual count so the [i/n] numbering
    # is always self-consistent with the emitted length.
    final = _numbered_split(content, limit, total)
    if len(final) == total:
        return final
    return _numbered_split(content, limit, len(final))


def _numbered_split(content: str, limit: int, total: int) -> list[str]:
    """Split ``content`` into ``[i/total]``-prefixed chunks under ``limit`` bytes."""
    chunks: list[str] = []
    remaining = content
    i = 1
    while remaining:
        prefix = f"[{i}/{total}] "
        prefix_len = len(prefix.encode("utf-8"))
        capacity = max(10, limit - prefix_len)

        parts = split_utf8(remaining, capacity)
        if not parts:
            break
        part = parts[0]
        chunks.append(prefix + part)
        remaining = remaining[len(part) :]
        i += 1
    return chunks


def _effective_chunk_bytes() -> int:
    """Parse ``MESHTASTIC_CHUNK_BYTES`` into the per-chunk byte budget.

    Clamped to the protocol ceiling (233) and a sane floor (``MIN_CHUNK_BYTES``)
    so a 0 / negative / tiny value can't silently degrade chunking. Float-
    looking strings ("170.0", "1e2") are accepted; non-numeric values fall
    back to the default with a warning so the misconfiguration is observable.
    """
    raw = os.getenv("MESHTASTIC_CHUNK_BYTES")
    if raw is None or not raw.strip():
        return DEFAULT_CHUNK_BYTES
    try:
        parsed = int(raw)
    except ValueError:
        try:
            parsed = int(float(raw))
        except (ValueError, OverflowError):
            # ValueError: NaN / non-finite-shaped garbage. OverflowError: ±inf
            # / huge values (``1e400``) — ``int(float("inf"))`` raises it (an
            # ArithmeticError, not a ValueError), so without it here the send
            # path crashes (send_path.chunk_send_result catches only ValueError).
            logger.warning(
                "MESHTASTIC_CHUNK_BYTES=%r is not a number; using default %d",
                raw,
                DEFAULT_CHUNK_BYTES,
            )
            return DEFAULT_CHUNK_BYTES
    return min(max(parsed, MIN_CHUNK_BYTES), MAX_MESSAGE_LENGTH)


def parse_chunk_prefix(text: str) -> tuple[int, int] | None:
    """Return ``(index, total)`` when ``text`` carries a leading ``[i/n] `` prefix.

    The receive side emits this as a diagnostic: reassembly of ``[i/n]``
    fragments is deliberately left to the agent (docs/DEVELOPING.md), so at
    least make partial delivery observable rather than silent. Returns
    ``None`` for any message without a well-formed prefix.
    """
    match = re.match(r"^\[(\d+)/(\d+)\] ", text)
    if match is None:
        return None
    return int(match.group(1)), int(match.group(2))


def split_utf8(text: str, limit: int) -> list[str]:
    """Split text by UTF-8 byte length, preferring whitespace boundaries.

    A whole UTF-8 character is never split; when a single character is wider
    than ``limit`` (only reachable with ``limit < 4``), it is emitted whole and
    that chunk exceeds ``limit`` by one character. ``limit >= 4`` keeps every
    chunk within budget.
    """
    remaining = text
    chunks: list[str] = []
    while remaining:
        if len(remaining.encode("utf-8")) <= limit:
            chunks.append(remaining)
            break
        # Fit the largest char count whose UTF-8 byte length stays within
        # budget, accumulating per-character byte sizes in a single pass
        # (O(chars)) rather than re-encoding the shrinking prefix on every
        # decrement (O(chars^2) for dense multibyte text).
        char_idx = 0
        byte_len = 0
        for ch in remaining:
            ch_bytes = len(ch.encode("utf-8"))
            if byte_len + ch_bytes > limit:
                break
            byte_len += ch_bytes
            char_idx += 1
        if char_idx <= 0:
            # Even one character exceeds the limit — take it whole rather than
            # split a multi-byte UTF-8 sequence.
            char_idx = 1
        split_idx = remaining[:char_idx].rfind(" ")
        if split_idx > 0:
            split_at = split_idx + 1
            part = remaining[:split_at]
            remaining = remaining[split_at:]
        else:
            part = remaining[:char_idx]
            remaining = remaining[char_idx:]
        if part:
            chunks.append(part)
    return chunks
