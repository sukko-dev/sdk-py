"""Credential redaction for error strings, ``repr``, and log records (§IX).

``httpx`` and ``websockets`` embed the full request URL — including a ``?token=``/``?api_key=``
query param — in their exception strings, so the query-param auth opt-in would leak the
credential into any error the SDK surfaces or logs. Typed wrapping alone is not redaction.

Two complementary strategies:

1. **Value-based masking** — the client registers its *actual* token / api-key / push subscriber
   keys with the redactor; redaction is exact-substring replacement. This is what makes the
   no-leak guarantee hold regardless of where the credential is embedded.
2. **Pattern masking** — structural masking of ``token=``/``api_key=`` query params and
   ``Authorization``/``X-API-Key`` header values, catching credentials that were never registered
   (e.g. a value the SDK never saw because it came from a redirect).

A :class:`RedactingFilter` applies the same redaction to every ``logging`` record.
"""

from __future__ import annotations

import logging
import re
import threading
from typing import Final

PLACEHOLDER: Final = "***REDACTED***"

# Pattern masking. Each entry captures the *prefix* to keep and masks the value that follows.
# Kept deliberately conservative — value-based masking is the primary guarantee; patterns are the
# best-effort backstop for credentials the SDK never registered.
_PATTERNS: Final[tuple[re.Pattern[str], ...]] = (
    # URL query params: ?token=..., &api_key=..., &access_token=... (value ends at & / ws / quote)
    re.compile(r"(?i)([?&](?:token|api_key|api-key|access_token)=)[^&\s\"'<>]+"),
    # Authorization: Bearer <jwt>  (also dict-repr forms: 'Authorization': 'Bearer <jwt>')
    re.compile(r"(?i)(authorization['\"]?\s*[:=]\s*['\"]?bearer\s+)[^\s\"',}<>]+"),
    # X-API-Key: <key>  and dict-repr forms
    re.compile(r"(?i)(x-api-key['\"]?\s*[:=]\s*['\"]?)[^\s\"',}<>]+"),
    # Push subscriber keys + device token in JSON/dict body form (the exact field names the SDK
    # sends: "p256dh_key", "auth_secret", "token", plus the browser "p256dh"/"auth" variants).
    re.compile(r"(?i)(\"(?:p256dh_key|p256dh|auth_secret|auth|token)\"\s*:\s*\")[^\"]+"),
)

_MIN_SECRET_LEN: Final = 4  # never register trivially-short strings that would over-redact output


class Redactor:
    """Holds registered secret values and masks them out of arbitrary text.

    Registration is idempotent and additive; secrets are never un-registered (over-redaction is
    safe, under-redaction is a leak). Shorter-than-:data:`_MIN_SECRET_LEN` values are ignored to
    avoid masking innocuous substrings.
    """

    # Thread-safe: the sync wrapper's ``update_token`` registers from the caller thread while the
    # background loop may be redacting — so register/read of the secret set is guarded by a lock.
    __slots__ = ("_lock", "_secrets")

    def __init__(self) -> None:
        self._secrets: set[str] = set()
        self._lock = threading.Lock()

    def register(self, *values: str | None) -> None:
        """Register one or more secret values to mask exactly. ``None``/short values are ignored."""
        with self._lock:
            for value in values:
                if value and len(value) >= _MIN_SECRET_LEN:
                    self._secrets.add(value)

    def redact(self, text: str) -> str:
        """Return ``text`` with every registered secret and matched credential pattern masked."""
        if not text:
            return text
        # Snapshot under the lock (longest-first so a token containing a shorter registered
        # substring is fully masked), then replace outside it.
        with self._lock:
            secrets = sorted(self._secrets, key=len, reverse=True)
        for secret in secrets:
            if secret in text:
                text = text.replace(secret, PLACEHOLDER)
        for pattern in _PATTERNS:
            text = pattern.sub(rf"\1{PLACEHOLDER}", text)
        return text


#: Process-wide default redactor. Clients register their credentials here so that any error the
#: SDK raises or logs — even from a layer that does not hold a client reference — is masked.
#: Accumulating secrets across clients only ever *over*-redacts, which is safe.
default_redactor: Final = Redactor()


def register_secret(*values: str | None) -> None:
    """Register secret value(s) with the process-wide :data:`default_redactor`."""
    default_redactor.register(*values)


def redact(text: str) -> str:
    """Mask registered secrets and credential patterns out of ``text`` (process-wide redactor)."""
    return default_redactor.redact(text)


#: Standard :class:`logging.LogRecord` attributes — everything else in ``record.__dict__`` is an
#: application-supplied ``extra=`` field that must also be redacted.
_RESERVED_LOGRECORD_ATTRS: Final[frozenset[str]] = frozenset(
    logging.makeLogRecord({}).__dict__.keys() | {"message", "asctime"}
)


class RedactingFilter(logging.Filter):
    """A :class:`logging.Filter` that redacts a record's rendered message **and** its ``extra=``
    fields.

    A logger's filters run only for records that logger creates — not for child records that
    propagate up — so this filter is attached to **every** SDK logger (root and each child), not
    just the parent. The filter redacts ``msg % args`` and every string value in the record's
    ``extra`` dict, so neither the message nor a stray ``extra={"url": raw_url}`` can leak a token
    through any handler the application configures.
    """

    def __init__(self, redactor: Redactor = default_redactor) -> None:
        super().__init__()
        self._redactor = redactor

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            rendered = record.getMessage()
        except Exception:
            # A broken format string must not defeat redaction — fall back to the raw msg.
            rendered = str(record.msg)
        record.msg = self._redactor.redact(rendered)
        record.args = None
        # Redact application-supplied extra fields (skip the standard LogRecord attributes).
        for key, value in record.__dict__.items():
            if key not in _RESERVED_LOGRECORD_ATTRS and isinstance(value, str):
                record.__dict__[key] = self._redactor.redact(value)
        return True
