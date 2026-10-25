"""Numbered two-step ``/model`` picker for LoRa.

Without a picker, Hermes answers a bare ``/model`` with its text list: about
900 characters of Markdown that takes six LoRa chunks and still truncates each
provider's models with "(+N more)". Telegram gets an inline-keyboard drill-down
instead (provider → model) through the adapter's ``send_model_picker`` hook.
This module is the same drill-down as plain numbered text: the user answers
with a number, the adapter feeds that reply back here, and the final choice is
handed to Hermes' own ``on_model_selected`` callback, so the switch itself is
Hermes' logic, not ours.

Pure state and formatting only — stdlib, no repo imports. The adapter owns the
sends and the inbound interception.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

PICKER_TTL_SECS = 300.0
# A chat id with a pending picker; bounded so stale chats cannot pile up.
PICKER_LIMIT = 64
CANCEL = 0


@dataclass
class _Pending:
    providers: list[dict[str, Any]]
    current_model: str
    on_selected: Any
    expires_at: float
    provider: dict[str, Any] | None = None


@dataclass(frozen=True)
class PickerReply:
    """What the adapter should do with an inbound reply.

    ``handled`` False means the text was not a picker answer: deliver it to the
    agent as usual. Otherwise send ``text`` back, and when ``model`` is set,
    call ``on_selected(chat_id, model, provider_slug)`` and send its result.
    """

    handled: bool
    text: str | None = None
    model: str | None = None
    provider_slug: str | None = None
    on_selected: Any = None


NOT_HANDLED = PickerReply(handled=False)


def _models(provider: dict[str, Any]) -> list[str]:
    return [str(m) for m in provider.get("models") or [] if m]


def _choice(text: str) -> int | None:
    stripped = text.strip()
    return int(stripped) if stripped.isdigit() else None


def provider_menu(providers: list[dict[str, Any]], current_model: str) -> str:
    lines = [f"Model: {current_model or 'unknown'}"]
    for i, p in enumerate(providers, 1):
        mark = " *" if p.get("is_current") else ""
        lines.append(f"{i} {p.get('name') or p.get('slug')} ({len(_models(p))}){mark}")
    lines.append("Reply with a number, 0 to cancel")
    return "\n".join(lines)


def model_menu(provider: dict[str, Any], current_model: str) -> str:
    lines = [str(provider.get("name") or provider.get("slug"))]
    for i, model in enumerate(_models(provider), 1):
        mark = " *" if model == current_model else ""
        lines.append(f"{i} {model}{mark}")
    lines.append("Reply with a number, 0 to cancel")
    return "\n".join(lines)


@dataclass
class ModelPicker:
    """Pending pickers keyed by chat id."""

    clock: Callable[[], float] = time.monotonic
    _pending: dict[str, _Pending] = field(default_factory=dict)

    def open(
        self,
        chat_id: str,
        providers: list[dict[str, Any]],
        current_model: str,
        on_selected: Any,
    ) -> str | None:
        """Start a picker; returns the first menu, or None if nothing is pickable.

        Providers without listed models are left out — they cannot be chosen
        by number. With a single provider the menu starts at its models.
        """
        usable = [p for p in providers if _models(p)]
        if not usable:
            return None
        pending = _Pending(
            providers=usable,
            current_model=current_model,
            on_selected=on_selected,
            expires_at=self.clock() + PICKER_TTL_SECS,
        )
        self._pending.pop(chat_id, None)
        self._pending[chat_id] = pending
        while len(self._pending) > PICKER_LIMIT:
            self._pending.pop(next(iter(self._pending)))
        if len(usable) == 1:
            pending.provider = usable[0]
            return model_menu(usable[0], current_model)
        return provider_menu(usable, current_model)

    def answer(self, chat_id: str, text: str) -> PickerReply:
        """Route an inbound text from ``chat_id`` through its pending picker."""
        pending = self._pending.get(chat_id)
        if pending is None:
            return NOT_HANDLED
        if self.clock() >= pending.expires_at:
            del self._pending[chat_id]
            return NOT_HANDLED
        choice = _choice(text)
        if choice is None:
            # Anything but a number is a normal message; it also ends the picker.
            del self._pending[chat_id]
            return NOT_HANDLED
        if choice == CANCEL:
            del self._pending[chat_id]
            return PickerReply(handled=True, text="Model unchanged")

        if pending.provider is None:
            if choice > len(pending.providers):
                return PickerReply(handled=True, text=f"Pick 1-{len(pending.providers)} or 0")
            pending.provider = pending.providers[choice - 1]
            pending.expires_at = self.clock() + PICKER_TTL_SECS
            return PickerReply(
                handled=True, text=model_menu(pending.provider, pending.current_model)
            )

        models = _models(pending.provider)
        if choice > len(models):
            return PickerReply(handled=True, text=f"Pick 1-{len(models)} or 0")
        del self._pending[chat_id]
        return PickerReply(
            handled=True,
            model=models[choice - 1],
            provider_slug=str(pending.provider.get("slug") or ""),
            on_selected=pending.on_selected,
        )
