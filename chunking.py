"""Split text into LoRa-safe UTF-8-byte chunks with ``[i/n]`` prefixes.

The protocol app-payload ceiling is ``mesh_pb2.Constants.DATA_PAYLOAD_LEN``
(233 bytes) — ``sendData`` raises above it, so every chunk must fit.
"""

import os

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


def chunk_message(content: str) -> list[str]:
    """Split text into LoRa-safe UTF-8 byte chunks with sequence prefixes."""
    content = (content or "").strip()
    # Clamp to the protocol hard ceiling — sendData raises above
    # DATA_PAYLOAD_LEN (233), so a misconfigured larger value would NAK
    # every full chunk with TOO_LARGE. Non-numeric values fall back to the
    # default (same defensive pattern as _retry_backoff / _send_retries).
    raw = os.getenv("MESHTASTIC_CHUNK_BYTES") or DEFAULT_CHUNK_BYTES
    try:
        limit = min(int(raw), MAX_MESSAGE_LENGTH)
    except (TypeError, ValueError):
        limit = DEFAULT_CHUNK_BYTES

    if len(content.encode("utf-8")) <= limit:
        return [content] if content else []

    # We will iterate to find the correct number of chunks.
    # A prefix is at most 12 bytes. So capacity is limit - 12.
    capacity = max(10, limit - 12)
    raw_chunks = split_utf8(content, capacity)
    total = len(raw_chunks)

    for _ in range(5):
        chunks = []
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

        actual_count = len(chunks)
        if actual_count == total:
            return chunks
        total = actual_count

    return chunks


def split_utf8(text: str, limit: int) -> list[str]:
    """Split text by UTF-8 byte length, preferring whitespace boundaries."""
    remaining = text
    chunks: list[str] = []
    while remaining:
        if len(remaining.encode("utf-8")) <= limit:
            chunks.append(remaining)
            break
        char_idx = min(len(remaining), limit)
        while char_idx > 0 and len(remaining[:char_idx].encode("utf-8")) > limit:
            char_idx -= 1
        if char_idx <= 0:
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
