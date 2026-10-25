"""Forward DMs from non-allowlisted nodes to the owner's node.

The allowlist keeps unknown nodes away from the agent, but the base node is
always on the air and people do write to it. With forwarding enabled
(``MESHTASTIC_FORWARD_UNKNOWN=true``, off by default) a DM from a node that is
not allowed to talk to the agent is relayed verbatim to the owner's node, and
the sender gets a one-line auto-reply naming where it went. The agent never
sees the text — this path runs where the inbound gate drops it.

Both transmissions spend shared airtime, so both are rate limited per sender:
the auto-reply goes out at most once per ``reply_cooldown_secs`` (which also
stops two auto-responders from answering each other forever), and at most
``forwards_per_hour`` messages per sender reach the owner. Channel/broadcast
text is never forwarded or answered.

Pure decisions and formatting only — stdlib, no repo imports. The adapter
resolves names and performs the sends.
"""

from __future__ import annotations

import time
from collections import OrderedDict, deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

DEFAULT_REPLY_COOLDOWN_HOURS = 6.0
DEFAULT_FORWARDS_PER_HOUR = 5
FORWARD_WINDOW_SECS = 3600.0
# Bound on tracked senders so a stream of spoofed ids cannot grow the maps.
SENDER_LIMIT = 512
LANGUAGES = ("en", "ru")
DEFAULT_LANGUAGE = "en"

AUTO_REPLIES = {
    "en": "Auto-reply: your message was forwarded to {name}. A reply may come later.",
    "ru": "Автоответ: сообщение передано владельцу ({name}). Ответ может прийти позже.",
}


@dataclass(frozen=True)
class ForwardConfig:
    """Resolved forwarding settings; ``enabled`` is False unless opted in."""

    enabled: bool = False
    target: str | None = None
    language: str = DEFAULT_LANGUAGE
    reply_cooldown_secs: float = DEFAULT_REPLY_COOLDOWN_HOURS * 3600
    forwards_per_hour: int = DEFAULT_FORWARDS_PER_HOUR


@dataclass(frozen=True)
class ForwardDecision:
    """What to transmit for one unauthorized DM."""

    forward: bool
    auto_reply: bool


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in ("1", "true", "yes", "on")


def _number(value: Any, default: float, *, minimum: float = 0.0) -> float:
    try:
        return max(minimum, float(value))
    except (TypeError, ValueError):
        return default


def first_allowed_node(allowed_nodes_raw: Any) -> str | None:
    """First entry of the comma-separated allowlist, as ``!``-prefixed lowercase."""
    for part in str(allowed_nodes_raw or "").split(","):
        part = part.strip().lower()
        if part:
            return part if part.startswith("!") else f"!{part}"
    return None


def forward_config(
    env: Mapping[str, str], extra: Mapping[str, Any], allowed_nodes_raw: Any
) -> ForwardConfig:
    """Read the forwarding settings from env (preferred) and plugin extra.

    The target defaults to the first allowlisted node — the owner's node in
    the usual one-entry setup. With no target the feature stays off.
    """

    def setting(env_key: str, extra_key: str) -> Any:
        return env.get(env_key) or extra.get(extra_key)

    enabled = _truthy(setting("MESHTASTIC_FORWARD_UNKNOWN", "forward_unknown"))
    target = first_allowed_node(setting("MESHTASTIC_FORWARD_TO", "forward_to"))
    if target is None:
        target = first_allowed_node(allowed_nodes_raw)
    language = str(setting("MESHTASTIC_FORWARD_LANG", "forward_lang") or "").strip().lower()
    if language not in LANGUAGES:
        language = DEFAULT_LANGUAGE
    cooldown_hours = _number(
        setting("MESHTASTIC_FORWARD_REPLY_COOLDOWN_HOURS", "forward_reply_cooldown_hours"),
        DEFAULT_REPLY_COOLDOWN_HOURS,
    )
    per_hour = int(
        _number(
            setting("MESHTASTIC_FORWARD_PER_HOUR", "forward_per_hour"),
            DEFAULT_FORWARDS_PER_HOUR,
        )
    )
    return ForwardConfig(
        enabled=enabled and target is not None,
        target=target,
        language=language,
        reply_cooldown_secs=cooldown_hours * 3600,
        forwards_per_hour=per_hour,
    )


class ForwardGate:
    """Per-sender rate limits for forwarding and auto-replies."""

    def __init__(self, config: ForwardConfig, clock: Callable[[], float] = time.time) -> None:
        self._config = config
        self._clock = clock
        self._last_reply: OrderedDict[str, float] = OrderedDict()
        self._forwards: OrderedDict[str, deque[float]] = OrderedDict()

    @property
    def config(self) -> ForwardConfig:
        return self._config

    def decide(
        self, sender: str, *, is_broadcast: bool, my_node_id: str | None = None
    ) -> ForwardDecision:
        """Decide what to send for one unauthorized text and record it."""
        cfg = self._config
        nothing = ForwardDecision(forward=False, auto_reply=False)
        if not cfg.enabled or is_broadcast or sender in (cfg.target, my_node_id):
            return nothing

        now = self._clock()
        sent = self._forwards.get(sender)
        if sent is None:
            sent = self._forwards[sender] = deque()
        self._forwards.move_to_end(sender)
        while sent and now - sent[0] >= FORWARD_WINDOW_SECS:
            sent.popleft()
        forward = len(sent) < cfg.forwards_per_hour
        if forward:
            sent.append(now)

        # The notice says the message was forwarded, so it never goes out for
        # one the hourly cap held back.
        last = self._last_reply.get(sender)
        auto_reply = forward and (last is None or now - last >= cfg.reply_cooldown_secs)
        if auto_reply:
            self._last_reply[sender] = now
            self._last_reply.move_to_end(sender)

        for store in (self._forwards, self._last_reply):
            while len(store) > SENDER_LIMIT:
                store.popitem(last=False)
        return ForwardDecision(forward=forward, auto_reply=auto_reply)


def forward_gate(
    env: Mapping[str, str], extra: Mapping[str, Any], allowed_nodes_raw: Any
) -> ForwardGate:
    """A gate configured from env / plugin extra (see :func:`forward_config`)."""
    return ForwardGate(forward_config(env, extra, allowed_nodes_raw))


def format_forward(sender: str, sender_name: str, text: str) -> str:
    """The owner's copy: who wrote, then the message exactly as received."""
    who = sender if sender_name in ("", sender) else f"{sender_name} ({sender})"
    return f"✉ {who}: {text}"


def format_auto_reply(target_name: str, language: str) -> str:
    """The sender's one-line notice naming the node the message went to."""
    return AUTO_REPLIES.get(language, AUTO_REPLIES[DEFAULT_LANGUAGE]).format(name=target_name)
