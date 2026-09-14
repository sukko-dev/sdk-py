"""Automatic auth: single-flight refresh + api-key→JWT escalation.

The SDK owns its credential state and makes the dual-purpose ``auth`` message's mode explicit off
that owned state (§XV), never runtime-detected:

- **Refresh** keeps subscriptions; fired **proactively** from ``auth_ack.exp`` (skip ``exp==0`` =
  no-expiry — never schedule) and **reactively** on an unsolicited ``auth_error``. **Single-flight**
  (one in-flight ``auth`` at a time — concurrent callers coalesce onto the same future), floored at
  ``refresh_min_interval`` with exponential backoff on consecutive failures so an
  ``auth_error``→refresh loop cannot form.
- **Escalation** (api-key-only → JWT) sends the same ``auth`` message but, on success, the *caller*
  re-subscribes the retained not-granted delta. Escalation needs a live socket; offline it
  defers to :meth:`update_token` and the next reconnect.

**Lifecycle discipline (§VI):** the in-flight future is resolved by the read-pump's dispatch of
``auth_ack``/``auth_error``. On epoch teardown :meth:`aclose` **fails** any pending future and
cancels the proactive timer — otherwise ``refresh()`` would hang forever across a disconnect.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from ._clock import SYSTEM_CLOCK, Clock
from ._redact import register_secret
from .constants import SUKKO_DEFAULTS
from .errors import AuthError, NotConnectedError, SukkoError, error_from_ws_error

logger = logging.getLogger("sukko.auth")

#: Cap on the reactive-refresh backoff so a persistently-failing token never stalls forever nor
#: hammers (tokens are ≤15 min, so a 5-minute ceiling is comfortably safe).
_MAX_REFRESH_BACKOFF = 300.0

SendAuth = Callable[[str], Awaitable[None]]
GetToken = Callable[[], Awaitable[str]]


class AuthManager:
    """Owns credential state and the automatic auth machinery. Driven by the client's read-pump
    (:meth:`on_auth_ack`/:meth:`on_auth_error`) and its own proactive timer."""

    def __init__(
        self,
        *,
        token: str | None = None,
        api_key: str | None = None,
        get_token: GetToken | None = None,
        send_auth: SendAuth,
        clock: Clock = SYSTEM_CLOCK,
        refresh_min_interval: float = SUKKO_DEFAULTS.REFRESH_MIN_INTERVAL,
        refresh_lead: float = SUKKO_DEFAULTS.REFRESH_LEAD,
    ) -> None:
        self._token = token
        self._api_key = api_key
        self._get_token = get_token
        self._send_auth = send_auth
        self._clock = clock
        self._min_interval = refresh_min_interval
        self._lead = refresh_lead
        self._pending: asyncio.Future[None] | None = None
        self._timer: asyncio.Task[None] | None = None
        self._last_refresh_at = float("-inf")  # allow the first refresh immediately
        self._failures = 0
        register_secret(token, api_key)

    @property
    def token(self) -> str | None:
        return self._token

    @property
    def api_key(self) -> str | None:
        return self._api_key

    def update_token(self, token: str) -> None:
        """Set the credential for the next connect **without** sending ``auth`` (the offline path,
        and the escalation-while-disconnected deferral)."""
        self._token = token
        register_secret(token)

    # --- single-flight send-and-await ---------------------------------------------------------

    async def _run_auth(self, token_provider: Callable[[], Awaitable[str]]) -> None:
        """Single-flight core: claim the in-flight future **synchronously** (so a concurrent
        proactive + reactive trigger coalesce onto one ``auth``), then resolve the token, send, and
        await the ack. ``token_provider`` runs *after* the claim, so its floor-sleep is shared by
        coalesced callers waiting on the future."""
        if self._pending is not None:
            await self._pending
            return
        pending = asyncio.get_running_loop().create_future()
        self._pending = pending
        try:
            token = await token_provider()
            await self._send_auth(token)
            await pending
        except BaseException as exc:
            # Resolve the shared future so coalesced callers see the same failure instead of
            # hanging forever (the token step or send can raise before any ack arrives).
            if not pending.done():
                pending.set_exception(exc)
            raise
        finally:
            self._pending = None

    def _effective_interval(self) -> float:
        if self._failures == 0:
            return self._min_interval
        return min(_MAX_REFRESH_BACKOFF, self._min_interval * (2.0**self._failures))

    async def refresh(self) -> None:
        """Refresh the token: honor the floor+backoff, fetch a fresh token, send ``auth``, await the
        ack. Raises :class:`~sukko.errors.AuthError` if the refresh is rejected."""

        async def provider() -> str:
            now = self._clock.monotonic()
            earliest = self._last_refresh_at + self._effective_interval()
            if now < earliest:
                await self._clock.sleep(earliest - now)
            if self._get_token is not None:
                token = await self._get_token()
            elif self._token is not None:
                token = self._token
            else:
                raise AuthError("no token available to refresh (set token or get_token)")
            self.update_token(token)
            self._last_refresh_at = self._clock.monotonic()
            return token

        await self._run_auth(provider)

    async def reactive_refresh(self) -> None:
        """Refresh triggered by an unsolicited ``auth_error``; swallows the resulting error (the
        proactive timer / next ``auth_error`` retries) so it cannot crash the read-pump."""
        try:
            await self.refresh()
        except SukkoError as exc:
            logger.warning("reactive auth refresh failed", extra={"error": str(exc)})

    async def escalate(self, jwt: str, *, connected: bool) -> bool:
        """Escalate an api-key connection to JWT. **Waits for any in-flight refresh to FINISH** (not
        succeed — a failed refresh must not abort the escalation), then sends its **own** ``auth``
        frame and awaits its own ack. It never coalesces onto the refresh: escalation is an identity
        change, not a same-identity renewal, so it must not inherit the refresh's token. Returns
        ``True`` if sent + acked (caller re-subscribes the not-granted delta), ``False`` if deferred
        (offline, or the in-flight refresh was dropped by a disconnect)."""
        if not connected:
            self.update_token(jwt)
            return False  # defer: the next reconnect will present the JWT
        # Drain any in-flight refresh to settlement. A rejected refresh is fine — we send our own
        # frame regardless. But a drop (NotConnectedError from aclose()) means we can't send now:
        # store the JWT and defer, like the offline path. The loop re-checks in case another refresh
        # claimed the slot while we awaited.
        while self._pending is not None:
            try:
                await self._pending
            except NotConnectedError:
                self.update_token(jwt)
                return False
            except SukkoError:
                pass  # a rejected refresh must not abort the escalation
        # Set the credential AFTER the wait so a refresh completing during it can't clobber the JWT.
        self.update_token(jwt)

        async def provider() -> str:
            return jwt

        await self._run_auth(provider)
        return True

    # --- read-pump hooks ----------------------------------------------------------------------

    def on_auth_ack(self, exp: int) -> None:
        """Handle ``auth_ack``: resolve any in-flight refresh, reset backoff, (re)arm the proactive
        timer from ``exp`` (``exp==0`` = no-expiry → no timer)."""
        self._failures = 0
        if self._pending is not None and not self._pending.done():
            self._pending.set_result(None)
        self._schedule_proactive(exp)

    def on_auth_error(self, code: str, message: str) -> bool:
        """Handle ``auth_error``. Returns ``True`` when **unsolicited** (no refresh in flight), so
        the caller triggers a reactive refresh; ``False`` when it resolves our own in-flight refresh
        (which must NOT loop)."""
        self._failures += 1
        if self._pending is not None and not self._pending.done():
            self._pending.set_exception(error_from_ws_error("auth_error", code, message))
            return False
        return True

    # --- proactive timer ----------------------------------------------------------------------

    def _schedule_proactive(self, exp: int) -> None:
        self._cancel_timer()
        if exp == 0 or self._get_token is None:
            return  # no-expiry, or no way to fetch a fresh token → nothing to schedule
        self._timer = asyncio.ensure_future(self._proactive_timer(exp))

    async def _proactive_timer(self, exp: int) -> None:
        delay = max(0.0, exp - self._lead - self._clock.now())
        await self._clock.sleep(delay)
        await self.reactive_refresh()

    def _cancel_timer(self) -> None:
        if self._timer is not None and not self._timer.done():
            self._timer.cancel()
        self._timer = None

    async def aclose(self) -> None:
        """Tear down: cancel the proactive timer and fail any pending refresh so no coroutine hangs
        across a disconnect (§VI)."""
        timer = self._timer
        self._cancel_timer()
        if self._pending is not None and not self._pending.done():
            self._pending.set_exception(NotConnectedError("auth refresh interrupted by close"))
        if timer is not None:
            await asyncio.gather(timer, return_exceptions=True)
