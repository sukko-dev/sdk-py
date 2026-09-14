"""Message models — decode one frame per server type, encode one per client type, and the
optional-field semantics (contract-coverage companion)."""

from __future__ import annotations

from sukko import messages as m

_SERVER_FRAMES: list[tuple[bytes, type]] = [
    (b'{"type":"message","seq":1,"ts":1,"channel":"a.b","data":{}}', m.Message),
    (b'{"type":"auth_ack","data":{"exp":0}}', m.AuthAck),
    (b'{"type":"auth_error","data":{"code":"invalid_token","message":"x"}}', m.AuthError),
    (b'{"type":"subscription_ack","subscribed":["a.b"],"count":1}', m.SubscriptionAck),
    (b'{"type":"unsubscription_ack","unsubscribed":["a.b"]}', m.UnsubscriptionAck),
    (b'{"type":"publish_ack","channel":"a.b","status":"accepted"}', m.PublishAck),
    (b'{"type":"publish_error","code":"rate_limited","message":"x"}', m.PublishError),
    (
        b'{"type":"reconnect_ack","status":"completed","messages_replayed":0,"message":"x"}',
        m.ReconnectAck,
    ),
    (b'{"type":"reconnect_error","code":"not_available","message":"x"}', m.ReconnectError),
    (b'{"type":"pong","ts":1}', m.Pong),
    (b'{"type":"error","code":"invalid_json","message":"x"}', m.Error),
    (b'{"type":"subscribe_error","code":"invalid_request","message":"x"}', m.SubscribeError),
    (b'{"type":"unsubscribe_error","code":"invalid_request","message":"x"}', m.UnsubscribeError),
    (b'{"type":"history_complete","channel":"a.b","count":0,"source":"cache"}', m.HistoryComplete),
    (
        b'{"type":"history_error","code":"history_disabled","channel":"a.b","message":"x"}',
        m.HistoryError,
    ),
    (b'{"type":"gap","channel":"a.b","from_seq":1,"to_seq":2,"last_pos":"2-9","ts":1}', m.Gap),
    (b'{"type":"replay_message","seq":1,"channel":"a.b","ts":1,"data":{}}', m.ReplayMessage),
    (b'{"type":"replay_complete","channel":"a.b","messages_replayed":0}', m.ReplayComplete),
]

_CLIENT_MESSAGES: list[tuple[m.ClientMessage, str]] = [
    (m.Subscribe(data=m.SubscribeData(channels=["a.b"])), "subscribe"),
    (m.Unsubscribe(data=m.UnsubscribeData(channels=["a.b"])), "unsubscribe"),
    (m.Publish(data=m.PublishData(channel="a.b", data={})), "publish"),
    (m.Reconnect(data=m.ReconnectData(client_id="c", last_pos={})), "reconnect"),
    (m.Heartbeat(), "heartbeat"),
    (m.Auth(data=m.AuthData(token="t")), "auth"),
    (m.History(data=m.HistoryData(channel="a.b", limit=1)), "history"),
    (m.Replay(data=m.ReplayData(channel="a.b", from_pos="2-9")), "replay"),
]


def test_every_server_type_decodes_to_its_model() -> None:
    for frame, expected in _SERVER_FRAMES:
        decoded = m.decode_server_message(frame)
        assert isinstance(decoded, expected), f"{frame!r} -> {type(decoded)}, expected {expected}"


def test_every_client_type_encodes_with_its_tag() -> None:
    for message, tag in _CLIENT_MESSAGES:
        encoded = m.encode_client(message)
        assert f'"type":"{tag}"'.encode() in encoded


def test_optional_field_semantics() -> None:
    # history absent -> False; pos absent -> None
    msg = m.decode_server_message(b'{"type":"message","seq":1,"ts":1,"channel":"a.b","data":{}}')
    assert isinstance(msg, m.Message) and msg.history is False and msg.pos is None
    assert msg.mid is None  # pre-field servers omit the stable message identity
    # truncated absent -> False; forced absent -> False; count absent -> None
    rc = m.decode_server_message(
        b'{"type":"replay_complete","channel":"a.b","messages_replayed":0}'
    )
    assert isinstance(rc, m.ReplayComplete) and rc.truncated is False
    ua = m.decode_server_message(b'{"type":"unsubscription_ack","unsubscribed":["a.b"]}')
    assert isinstance(ua, m.UnsubscriptionAck) and ua.forced is False and ua.count is None


def test_mid_semantics() -> None:
    """The stable message identity ``mid`` is exposed on every delivered copy of a message (live,
    history, gap-replay) and on the publish ack; absent (pre-field server / fan-out) -> ``None``."""
    live = m.decode_server_message(
        b'{"type":"message","seq":1,"ts":1,"channel":"a.b","data":{},"mid":"4f8a2e6b0c9d1735-0-99"}'
    )
    assert isinstance(live, m.Message) and live.mid == "4f8a2e6b0c9d1735-0-99"
    hist = m.decode_server_message(
        b'{"type":"message","seq":2,"ts":1,"channel":"a.b","data":{},"history":true,'
        b'"mid":"4f8a2e6b0c9d1735-0-99"}'
    )
    assert isinstance(hist, m.Message) and hist.mid == live.mid  # identical on every copy
    replay = m.decode_server_message(
        b'{"type":"replay_message","seq":3,"channel":"a.b","ts":1,"data":{},'
        b'"mid":"4f8a2e6b0c9d1735-0-99"}'
    )
    assert isinstance(replay, m.ReplayMessage) and replay.mid == live.mid
    ack = m.decode_server_message(
        b'{"type":"publish_ack","channel":"a.b","status":"accepted","mid":"9c5b1f0a-2-1235"}'
    )
    assert isinstance(ack, m.PublishAck) and ack.mid == "9c5b1f0a-2-1235"
    # pre-field servers omit mid entirely -> None (see _SERVER_FRAMES for the message case)
    old_ack = m.decode_server_message(b'{"type":"publish_ack","channel":"a.b","status":"accepted"}')
    assert isinstance(old_ack, m.PublishAck) and old_ack.mid is None
    old_replay = m.decode_server_message(
        b'{"type":"replay_message","seq":1,"channel":"a.b","ts":1,"data":{}}'
    )
    assert isinstance(old_replay, m.ReplayMessage) and old_replay.mid is None


def test_subscribe_with_history_mode_omits_unset_fields() -> None:
    encoded = m.encode_client(
        m.Subscribe(data=m.SubscribeData(channel="a.b", history=m.HistoryMode(limit=5)))
    )
    assert encoded == b'{"type":"subscribe","data":{"channel":"a.b","history":{"limit":5}}}'
