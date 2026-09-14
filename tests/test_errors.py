"""Error-mapping tests — every contract code → the right typed exception."""

from __future__ import annotations

from sukko.constants import CloseDirection
from sukko.errors import (
    AuthError,
    ConnectionClosedError,
    EditionRequiredError,
    HistoryError,
    PayloadTooLargeError,
    PublishError,
    PublishNotRoutableError,
    RateLimitError,
    ReplayError,
    ServiceUnavailableError,
    TenantLimitExceededError,
    error_from_close,
    error_from_http_status,
    error_from_ws_error,
)


def test_http_status_mapping() -> None:
    assert isinstance(error_from_http_status(403, code="EDITION_LIMIT"), EditionRequiredError)
    assert isinstance(error_from_http_status(403, code="FORBIDDEN"), PublishError)
    assert isinstance(error_from_http_status(409), PublishNotRoutableError)
    assert isinstance(error_from_http_status(413), PayloadTooLargeError)
    assert isinstance(error_from_http_status(429, handshake=True), TenantLimitExceededError)
    assert isinstance(error_from_http_status(429), RateLimitError)
    assert isinstance(error_from_http_status(503), ServiceUnavailableError)


def test_ws_error_mapping() -> None:
    assert isinstance(error_from_ws_error("auth_error", "invalid_token", "bad"), AuthError)
    assert isinstance(
        error_from_ws_error("publish_error", "message_too_large", "big"), PayloadTooLargeError
    )
    assert isinstance(
        error_from_ws_error("publish_error", "no_matching_route", "x"), PublishNotRoutableError
    )
    assert isinstance(error_from_ws_error("history_error", "history_disabled", "x"), HistoryError)
    assert isinstance(error_from_ws_error("error", "offset_out_of_range", "x"), ReplayError)


def test_close_4001_is_terminal_others_are_not() -> None:
    terminal = error_from_close(4001, CloseDirection.LOCAL)
    assert isinstance(terminal, ConnectionClosedError)
    assert terminal.terminal is True
    non_terminal = error_from_close(1008, CloseDirection.REMOTE)
    assert non_terminal.terminal is False


def test_error_messages_are_redacted() -> None:
    from sukko._redact import register_secret

    register_secret("leaky-token-xyz-9999")
    err = error_from_ws_error("publish_error", "forbidden", "denied: token=leaky-token-xyz-9999")
    assert "leaky-token-xyz-9999" not in str(err)
