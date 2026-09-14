"""Subscription-state tests — the desired-vs-granted model."""

from __future__ import annotations

from sukko.subscriptions import SubscriptionState


def test_grant_diff_surfaces_and_retains_not_granted() -> None:
    state = SubscriptionState()
    state.want(["a.1", "a.2", "a.3"])
    not_granted = state.on_subscription_ack(["a.1", "a.2"])  # a.3 filtered by permission
    assert not_granted == frozenset({"a.3"})
    assert state.granted == frozenset({"a.1", "a.2"})
    assert state.desired == frozenset({"a.1", "a.2", "a.3"})
    assert state.not_granted == frozenset({"a.3"})  # retained (escalation/resume referent)


def test_forced_unsubscribe_keeps_desired_drops_granted() -> None:
    state = SubscriptionState()
    state.want(["a.1", "a.2"])
    state.on_subscription_ack(["a.1", "a.2"])
    state.on_unsubscription_ack(["a.1"], forced=True)  # auth downgrade / permission change
    assert state.granted == frozenset({"a.2"})
    assert state.desired == frozenset({"a.1", "a.2"})  # still desired → re-attempted later
    assert state.not_granted == frozenset({"a.1"})


def test_client_unsubscribe_removes_from_both() -> None:
    state = SubscriptionState()
    state.want(["a.1", "a.2"])
    state.on_subscription_ack(["a.1", "a.2"])
    state.on_unsubscription_ack(["a.1"], forced=False)  # caller-initiated
    assert state.desired == frozenset({"a.2"})
    assert state.granted == frozenset({"a.2"})
    assert state.not_granted == frozenset()


def test_disconnect_clears_granted_keeps_desired() -> None:
    state = SubscriptionState()
    state.want(["a.1", "a.2"])
    state.on_subscription_ack(["a.1", "a.2"])
    state.on_disconnect()
    assert state.granted == frozenset()
    assert state.desired == frozenset({"a.1", "a.2"})
    assert state.not_granted == frozenset({"a.1", "a.2"})
    assert state.resume_channels() == ["a.1", "a.2"]  # resume re-subscribes the full desired set
