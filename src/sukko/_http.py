"""Shared REST plumbing for the gateway (§X consolidation) — auth injection + typed error mapping.

Both :mod:`sukko.rest` (publish) and :mod:`sukko.push` build on :class:`HttpApi`: one ``httpx``
client, header-default auth (query opt-in, §IX), and every non-2xx response mapped to a typed error
via :func:`~sukko.errors.error_from_http_status`. Credentials are read fresh per request through a
callable, so a token rotated by the WS auth machinery is picked up without rebuilding the client.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from urllib.parse import urlparse, urlunparse

import httpx
import msgspec

from ._redact import register_secret
from .constants import SUKKO_DEFAULTS
from .errors import (
    EditionRequiredError,
    ProtocolError,
    SukkoError,
    TransportError,
    error_from_http_status,
)

#: Returns the current ``(token, api_key)`` — read per request so credential rotation is picked up.
Credentials = Callable[[], "tuple[str | None, str | None]"]


def http_base_from_ws(url: str) -> str:
    """Derive the gateway's HTTP origin from a WebSocket URL (``wss://h/ws`` → ``https://h``)."""
    parsed = urlparse(url)
    scheme = {"ws": "http", "wss": "https"}.get(parsed.scheme, parsed.scheme)
    return urlunparse((scheme, parsed.netloc, "", "", "", ""))


class HttpApi:
    """A thin authed JSON client over one gateway origin."""

    def __init__(
        self,
        base_url: str,
        credentials: Credentials,
        *,
        auth_via: str = "header",
        timeout: float = SUKKO_DEFAULTS.CONNECTION_TIMEOUT,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._credentials = credentials
        self._auth_via = auth_via
        # ``transport`` is a test seam (e.g. httpx.MockTransport) — production leaves it None.
        self._client = httpx.AsyncClient(timeout=timeout, transport=transport)

    def _auth(self) -> tuple[dict[str, str], dict[str, str]]:
        token, api_key = self._credentials()
        register_secret(token, api_key)  # §IX: mask if a URL/header lands in an error
        headers: dict[str, str] = {}
        params: dict[str, str] = {}
        if self._auth_via == "query":
            if token:
                params["token"] = token
            if api_key:
                params["api_key"] = api_key
        else:
            if token:
                headers["Authorization"] = f"Bearer {token}"
            if api_key:
                headers["X-API-Key"] = api_key
        return headers, params

    async def request(
        self, method: str, path: str, *, json: Any = None, edition_gated: bool = False
    ) -> dict[str, Any]:
        """Send a request and return the parsed JSON object. Raises a typed
        :class:`~sukko.errors.SukkoError` on any non-2xx response or transport failure.

        ``edition_gated`` marks an endpoint whose only 4xx cause is the edition gate (the push
        endpoints, per the contract), so a 403 reliably becomes ``EditionRequiredError`` even when
        the gateway omits the ``code``."""
        headers, params = self._auth()
        try:
            response = await self._client.request(
                method, f"{self._base_url}{path}", headers=headers, params=params, json=json
            )
        except httpx.HTTPError as exc:
            raise TransportError(f"http request failed: {exc}") from exc
        if response.status_code >= 400:
            raise self._error_for(response, edition_gated=edition_gated)
        if not response.content:
            return {}
        try:
            return msgspec.json.decode(response.content, type=dict[str, object])
        except (msgspec.DecodeError, ValueError) as exc:
            raise ProtocolError(f"malformed response body: {exc}") from exc

    @staticmethod
    def _error_for(response: httpx.Response, *, edition_gated: bool) -> SukkoError:
        if edition_gated and response.status_code == 403:
            return EditionRequiredError()  # push 403 is only ever the edition gate (contract)
        code: str | None = None
        message: str | None = None
        try:
            body = msgspec.json.decode(response.content, type=dict[str, object])
            raw_code, raw_message = body.get("code"), body.get("message")
            code = raw_code if isinstance(raw_code, str) else None
            message = raw_message if isinstance(raw_message, str) else None
        except (msgspec.DecodeError, ValueError):
            pass
        retry_after = _parse_retry_after(response.headers.get("Retry-After"))
        return error_from_http_status(
            response.status_code, code=code, message=message, retry_after=retry_after
        )

    async def aclose(self) -> None:
        await self._client.aclose()


def _parse_retry_after(value: str | None) -> float | None:
    """Parse a ``Retry-After`` header (delta-seconds). ``None`` if absent/non-numeric (the gateway
    does not currently send it — upstream filing #5 — but honor it when it lands)."""
    if value is None:
        return None
    # HTTP-date form is not honored (the gateway uses delta-seconds when it sends the header).
    try:
        return float(value)
    except ValueError:
        return None
