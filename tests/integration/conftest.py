"""Fixtures for the opt-in live-compose integration suite.

Booted with an **Enterprise license + `MESSAGE_BACKEND=kafka`** (editions are cumulative, so
Enterprise gives full coverage: the Pro-gated SSE and Web Push suites plus Enterprise-gated mobile
FCM/APNs push; REST publish and history need no edition, and the kafka backend is
needed for the replay leg). These tests hit a **real** gateway — configure it via env; they
skip if it is absent, so the default hermetic suite is unaffected:

    SUKKO_IT_WS_URL   wss://host/ws       (required to run)
    SUKKO_IT_TOKEN    <enterprise JWT>    (required to run)
    SUKKO_IT_CHANNEL  sukko.it.smoke      (a tenant-prefixed channel the token may sub + publish to)
"""

from __future__ import annotations

import os

import pytest

_WS_URL = os.environ.get("SUKKO_IT_WS_URL")
_TOKEN = os.environ.get("SUKKO_IT_TOKEN")
_API_KEY = os.environ.get("SUKKO_IT_API_KEY")
_CHANNEL = os.environ.get("SUKKO_IT_CHANNEL", "sukko.it.smoke")
# The stack's license edition, so edition-gated tests (Web Push = Pro) can skip when the
# live stack does not provide them. Defaults to community.
_EDITION = os.environ.get("SUKKO_IT_EDITION", "community")


def _require_live() -> None:
    if not (_WS_URL and _TOKEN):
        pytest.skip(
            "live integration needs SUKKO_IT_WS_URL + SUKKO_IT_TOKEN (Enterprise + kafka stack)"
        )


@pytest.fixture
def ws_url() -> str:
    _require_live()
    assert _WS_URL is not None
    return _WS_URL


@pytest.fixture
def token() -> str:
    _require_live()
    assert _TOKEN is not None
    return _TOKEN


@pytest.fixture
def channel() -> str:
    return _CHANNEL


@pytest.fixture
def edition() -> str:
    """The live stack's license edition (community | pro | enterprise)."""
    return _EDITION


@pytest.fixture
def api_key() -> str:
    """An API key for the tenant — skips if the harness did not provision one."""
    if not _API_KEY:
        pytest.skip("live API-key test needs SUKKO_IT_API_KEY")
    return _API_KEY
