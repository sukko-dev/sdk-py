"""§IX credential-redaction tests — the redactor is asserted, not assumed.

Covers value-based masking (registered secret), pattern masking (token=/api_key= query params +
Authorization/X-API-Key headers + push subscriber keys), the logging filter, and no-false-positives.
"""

from __future__ import annotations

import logging

from sukko._redact import PLACEHOLDER, RedactingFilter, Redactor
from sukko.errors import SukkoError, TransportError


def test_registered_secret_masked_in_error_str_and_repr() -> None:
    redactor = Redactor()
    secret = "eyJhbGciOiJFZDI1NTE5-super-secret-jwt"
    redactor.register(secret)
    text = redactor.redact(f"connect failed for wss://h/ws?token={secret}")
    assert secret not in text
    assert PLACEHOLDER in text


def test_error_message_is_redacted_via_process_redactor() -> None:
    from sukko._redact import register_secret

    token = "tok-live-abcdef123456"
    register_secret(token)
    err = TransportError(f"handshake failed: GET /ws?token={token}")
    assert token not in str(err)
    assert token not in repr(err)
    assert isinstance(err, SukkoError)


def test_pattern_masks_query_params_without_registration() -> None:
    redactor = Redactor()  # nothing registered — pattern masking only
    masked = redactor.redact("url=https://h/ws?api_key=NEVER_REGISTERED_KEY_9999&x=1")
    assert "NEVER_REGISTERED_KEY_9999" not in masked
    assert PLACEHOLDER in masked


def test_pattern_masks_auth_headers() -> None:
    redactor = Redactor()
    text = "headers={'Authorization': 'Bearer HEADERJWT999', 'X-API-Key': 'KEY888'}"
    masked = redactor.redact(text)
    assert "HEADERJWT999" not in masked
    assert "KEY888" not in masked


def test_pattern_masks_push_subscriber_keys() -> None:
    redactor = Redactor()
    masked = redactor.redact('{"p256dh": "P256DHSECRET", "auth_secret": "AUTHSECRET"}')
    assert "P256DHSECRET" not in masked
    assert "AUTHSECRET" not in masked


def test_short_strings_are_not_registered_to_avoid_over_redaction() -> None:
    redactor = Redactor()
    redactor.register("ab")  # below _MIN_SECRET_LEN → ignored
    text = "the word 'ab' is a common substring and must survive"
    assert redactor.redact(text) == text


def test_pattern_masks_actual_push_field_names() -> None:
    # regression (P1): the SDK sends "p256dh_key" and "token", not bare "p256dh" — the pattern must
    # match the real field names, else mobile/web push secrets leak.
    redactor = Redactor()
    body = '{"p256dh_key": "P256DHKEYSECRET", "auth_secret": "AUTHSEC", "token": "FCMDEVICETOKEN"}'
    masked = redactor.redact(body)
    assert "P256DHKEYSECRET" not in masked
    assert "AUTHSEC" not in masked
    assert "FCMDEVICETOKEN" not in masked


def test_sdk_logger_redacts_message_and_extra_fields() -> None:
    # regression (P2): the RedactingFilter is attached to each SDK logger (incl. children) and masks
    # `extra=` fields, not just the message.
    import sukko  # noqa: F401 — importing wires the filter onto sukko.* loggers
    from sukko._redact import register_secret

    register_secret("EXTRAFIELDLEAK99")
    records: list[logging.LogRecord] = []
    handler = logging.Handler()
    handler.emit = records.append  # type: ignore[method-assign]
    child = logging.getLogger("sukko.client")
    child.addHandler(handler)
    child.setLevel(logging.DEBUG)
    try:
        child.warning("connecting", extra={"url": "wss://h/ws?token=EXTRAFIELDLEAK99"})
    finally:
        child.removeHandler(handler)
    assert records
    assert "EXTRAFIELDLEAK99" not in records[0].__dict__["url"]  # extra field masked


def test_redacting_filter_masks_log_records() -> None:
    redactor = Redactor()
    redactor.register("SECRETVALUE12345")
    log_filter = RedactingFilter(redactor)
    record = logging.LogRecord(
        "sukko", logging.WARNING, __file__, 1, "token=%s", ("SECRETVALUE12345",), None
    )
    assert log_filter.filter(record) is True
    rendered = record.getMessage()
    assert "SECRETVALUE12345" not in rendered
    assert PLACEHOLDER in rendered
