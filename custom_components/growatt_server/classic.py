"""Shared Classic API sessions and persistent Growatt login cooldowns."""

import asyncio
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime, timedelta
from functools import partial
from hashlib import sha256
from json import JSONDecodeError
import re
from time import monotonic
from typing import Any
from urllib.parse import urljoin, urlsplit

from growattServer import GrowattApi
from requests import RequestException, Response

from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.storage import Store

from .const import DEFAULT_URL, DEPRECATED_URLS, DOMAIN, SERVER_URLS_NAMES

_COOLDOWN = timedelta(hours=24)
_FAILED_LOGIN_RETRY_SECONDS = 60
_LOGIN_PATHS = {"/login", "/login.do", "/login.jsp", "/newtwologinapi.do"}
_LOGIN_FORM = re.compile(
    r"""<form\b[^>]*\baction\s*=\s*["'][^"']*(?:newTwoLoginAPI\.do|login\.do)["']""",
    re.IGNORECASE,
)
_LOGIN_ERROR_PAGE = re.compile(
    r"""<html\b[^>]*\bdata-name\s*=\s*["']dumpLogin["']""", re.IGNORECASE
)
_LOGIN_ERROR_LINK = re.compile(
    r"""<a\b[^>]*\bhref\s*=\s*["']/login["']""", re.IGNORECASE
)


class ClassicSessionExpired(Exception):
    """A Classic data endpoint explicitly requested authentication."""


class _ClassicRateLimited(Exception):
    """A Classic response explicitly contained the Growatt rate-limit code."""


class ClassicLoginError(Exception):
    """The Classic login endpoint rejected a login."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class ClassicLoginCooldown(ClassicLoginError):
    """Growatt rate-limited the account and login is temporarily blocked."""

    def __init__(self, until: datetime) -> None:
        super().__init__("507")
        self.until = until

    @property
    def retry_after(self) -> float:
        """Return the remaining cooldown for HA's retry scheduling."""
        return max(0.0, (self.until - datetime.now(UTC)).total_seconds())


def _consume_cancelled_waiter_result(task: asyncio.Task[Any]) -> None:
    if not task.cancelled():
        task.exception()


def _check_auth_response(response: Response, *args: Any, **kwargs: Any) -> None:
    """Recognize explicit authentication responses before library decoding."""
    if response.status_code == 401:
        raise ClassicSessionExpired("Growatt requested authentication")

    if (
        response.ok
        and b"507" in response.content
        and response.content.lstrip().startswith(b"{")
    ):
        try:
            payload = response.json()
        except JSONDecodeError:
            pass
        else:
            if isinstance(payload, dict):
                result = payload.get("back", payload)
                if (
                    isinstance(result, dict)
                    and result.get("success") is False
                    and str(result.get("msg")) == "507"
                ):
                    raise _ClassicRateLimited

    url = urlsplit(response.url)
    redirect = response.headers.get("Location")
    if 300 <= response.status_code < 400 and redirect:
        target = urlsplit(urljoin(response.url, redirect))
        if target.netloc == url.netloc and target.path.lower() in _LOGIN_PATHS:
            raise ClassicSessionExpired("Growatt requested authentication")

    if (
        response.ok
        and "text/html" in response.headers.get("Content-Type", "").lower()
        and url.path.lower() != "/newtwologinapi.do"
        and (
            _LOGIN_FORM.search(response.text)
            or (
                url.path.lower() == "/error.do"
                and _LOGIN_ERROR_PAGE.search(response.text)
                and _LOGIN_ERROR_LINK.search(response.text)
            )
        )
    ):
        raise ClassicSessionExpired("Growatt requested authentication")


class _Cooldowns:
    """Persist UTC deadlines independently of configuration entries."""

    def __init__(self, hass: HomeAssistant) -> None:
        self.store = Store[dict[str, str]](
            hass, 1, f"{DOMAIN}.classic_cooldowns", private=True, atomic_writes=True
        )
        self.lock = asyncio.Lock()
        self.deadlines: dict[str, str] | None = None

    async def async_deadline(self, key: str) -> datetime | None:
        async with self.lock:
            if self.deadlines is None:
                self.deadlines = await self.store.async_load() or {}
            value = self.deadlines.get(key)
            return datetime.fromisoformat(value) if value else None

    async def async_block(self, key: str, until: datetime) -> None:
        async with self.lock:
            assert self.deadlines is not None
            self.deadlines[key] = until.isoformat()
            await self.store.async_save(self.deadlines)


class _Account:
    """Own the lock shared by all sessions for a server and account."""

    def __init__(self, cooldowns: _Cooldowns, key: str) -> None:
        self.cooldowns = cooldowns
        self.key = key
        self.lock = asyncio.Lock()
        self.session: ClassicSession | None = None
        self.probe_retry_at = 0.0


class ClassicSession:
    """Serialize requests on one authenticated requests.Session."""

    def __init__(
        self,
        hass: HomeAssistant,
        account: _Account,
        url: str,
        username: str,
        password: str,
    ) -> None:
        self.hass = hass
        self.account = account
        self.username = username
        self.password = password
        self.api = GrowattApi(add_random_user_id=True, agent_identifier=username)
        self.api.server_url = url
        self.api.session.hooks["response"].insert(0, _check_auth_response)
        # The library leaves requests unbounded. Bound the wait while the
        # account lock is held, including when a network connection stalls.
        self.api.session.request = partial(self.api.session.request, timeout=(10, 30))
        self.login_data: dict[str, Any] | None = None
        self.failure: Exception | None = None
        self.retry_at = 0.0

    async def async_login(self) -> dict[str, Any]:
        """Ensure this session is authenticated and return the Classic user ID."""
        return await self._async_finish_request(self._async_run(None))

    async def async_request[T](self, request: Callable[..., T], *args: Any) -> T:
        """Run a read and replay it once after confirmed session expiry."""
        return await self._async_finish_request(self._async_run(request, *args))

    async def _async_finish_request[T](self, operation: Coroutine[Any, Any, T]) -> T:
        task = self.hass.async_create_task(operation, "Growatt Classic request")
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            # Keep the task alive to record a late 507 and consume the orphaned
            # exception once the operation completes.
            task.add_done_callback(_consume_cancelled_waiter_result)
            raise

    async def _async_run(self, request: Callable[..., Any] | None, *args: Any) -> Any:
        # Keep the lock until the executor and any cooldown write finish even
        # when HA cancels the waiting refresh during reload or shutdown.
        async with self.account.lock:
            try:
                await self._async_ensure_login()
                if request is None:
                    return self.login_data
                try:
                    return await self.hass.async_add_executor_job(request, *args)
                except ClassicSessionExpired:
                    self.login_data = None

                await self._async_ensure_login()
                try:
                    return await self.hass.async_add_executor_job(request, *args)
                except ClassicSessionExpired as err:
                    self.login_data = None
                    self._remember_failure(err)
                    raise
            except _ClassicRateLimited as err:
                self.login_data = None
                until = await self.account.cooldowns.async_deadline(self.account.key)
                now = datetime.now(UTC)
                if until is None or until <= now:
                    until = now + _COOLDOWN
                    await self.account.cooldowns.async_block(self.account.key, until)
                raise ClassicLoginCooldown(until) from err

    def _remember_failure(self, error: Exception) -> None:
        self.failure = error
        self.retry_at = monotonic() + _FAILED_LOGIN_RETRY_SECONDS

    async def _async_ensure_login(self) -> None:
        if self.login_data is not None:
            return

        until = await self.account.cooldowns.async_deadline(self.account.key)
        if until is not None and datetime.now(UTC) < until:
            raise ClassicLoginCooldown(until)
        if self.failure is not None and monotonic() < self.retry_at:
            raise self.failure
        if until is not None:
            # A different credentials object may share the account lock.
            # Only one such object may probe immediately after expiry.
            if monotonic() < self.account.probe_retry_at:
                raise ClassicLoginError("507 recovery probe already attempted")
            self.account.probe_retry_at = monotonic() + _FAILED_LOGIN_RETRY_SECONDS

        try:
            result = await self.hass.async_add_executor_job(
                self.api.login, self.username, self.password
            )
            if result.get("success"):
                self.login_data = {"user": {"id": result["user"]["id"]}}
                self.failure = None
                return

            code = str(result.get("msg", "Unknown error"))
            if code == "507":
                raise _ClassicRateLimited
            raise ClassicLoginError(code)
        except (
            ClassicSessionExpired,
            ClassicLoginError,
            RequestException,
            JSONDecodeError,
            AttributeError,
            KeyError,
            TypeError,
        ) as err:
            self._remember_failure(err)
            raise


@callback
def async_get_classic_session(
    hass: HomeAssistant, url: str, username: str, password: str
) -> ClassicSession:
    """Reuse the runtime session and share the account lock across credentials."""
    if url in DEPRECATED_URLS:
        url = DEFAULT_URL
    url = SERVER_URLS_NAMES.get(url, url).rstrip("/") + "/"
    parsed = urlsplit(url)
    port = parsed.port or {"http": 80, "https": 443}.get(parsed.scheme)
    server = f"{parsed.hostname}:{port}"
    key = sha256(f"{server}\0{username.casefold()}".encode()).hexdigest()
    state_key = f"{DOMAIN}.classic_sessions"
    if state_key not in hass.data:
        hass.data[state_key] = ({}, _Cooldowns(hass))
    accounts, cooldowns = hass.data[state_key]
    if key not in accounts:
        accounts[key] = _Account(cooldowns, key)
    account = accounts[key]
    session = account.session
    if (
        session is None
        or session.username != username
        or session.password != password
        or session.api.server_url != url
    ):
        session = account.session = ClassicSession(
            hass, account, url, username, password
        )
    return session
