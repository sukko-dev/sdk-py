"""Desired-vs-granted subscription state.

The gateway *silently* drops channels the connection isn't permitted to see at subscribe time — it
returns a ``subscription_ack`` listing only the granted channels. That contradicts the SDK's
no-silent-drop stance (§III) unless surfaced, so the SDK models the difference explicitly:

- ``desired`` — every channel the caller asked for. **Persists across reconnects** (it is what the
  caller wants, independent of any one connection).
- ``granted`` — channels confirmed by a ``subscription_ack`` on the *current* connection. **Cleared
  on disconnect** — a fresh connection has zero subscriptions until re-acked.
- ``not_granted`` = ``desired - granted`` — the retained set that is the referent for **both** the
  escalation delta (re-subscribe these after an api-key→JWT escalation) and reconnect resume
  (re-subscribe ``desired``, the ack re-diffs).

A pure, synchronous state machine — no I/O, trivially unit-testable.
"""

from __future__ import annotations

from collections.abc import Iterable


class SubscriptionState:
    """Tracks what the caller wants vs. what the server granted, surfacing the gap."""

    def __init__(self) -> None:
        self._desired: set[str] = set()
        self._granted: set[str] = set()

    @property
    def desired(self) -> frozenset[str]:
        """Every channel the caller has asked for (persists across reconnects)."""
        return frozenset(self._desired)

    @property
    def granted(self) -> frozenset[str]:
        """Channels confirmed active on the current connection (cleared on disconnect)."""
        return frozenset(self._granted)

    @property
    def not_granted(self) -> frozenset[str]:
        """Desired-but-not-yet-granted — the escalation delta and the resume/retry set."""
        return frozenset(self._desired - self._granted)

    def want(self, channels: Iterable[str]) -> None:
        """Record a caller ``subscribe`` — add to ``desired`` (grant confirmed later by an ack)."""
        self._desired.update(channels)

    def unwant(self, channels: Iterable[str]) -> None:
        """Record a caller ``unsubscribe`` — drop from both ``desired`` and ``granted``."""
        drop = set(channels)
        self._desired -= drop
        self._granted -= drop

    def on_subscription_ack(self, subscribed: Iterable[str]) -> frozenset[str]:
        """Apply a ``subscription_ack``: mark the listed channels granted; return the retained
        not-granted set for the caller to surface (requested channels filtered by permission)."""
        self._granted.update(subscribed)
        return self.not_granted

    def on_unsubscription_ack(self, unsubscribed: Iterable[str], *, forced: bool) -> None:
        """Apply an ``unsubscription_ack``. A **forced** unsubscribe (auth downgrade / permission
        change) removes the channels from ``granted`` but **keeps them in ``desired``** — so they
        flow back into ``not_granted`` and are re-attempted on the next escalation/reconnect
        (the escalation delta). A client-initiated unsubscribe removes them from both."""
        if forced:
            self._granted -= set(unsubscribed)
        else:
            self.unwant(unsubscribed)

    def on_disconnect(self) -> None:
        """Reset per-connection grant state. ``desired`` persists; ``granted`` clears — the next
        connection re-establishes grants from scratch (and the resume diff stays correct)."""
        self._granted.clear()

    def resume_channels(self) -> list[str]:
        """Channels to (re)subscribe on a new connection — the full ``desired`` set. The resulting
        ``subscription_ack`` re-diffs grants, and any offline-deferred escalation delta rides along
        for free."""
        return sorted(self._desired)
