"""The HTTP client surveys run on: rate-limited, bounded, and retried.

Lifted from the pattern that worked in ``scripts/query_bioproject2sra.py``, with
three changes that matter at corpus scale.

**Bounded concurrency.** The scripts built one coroutine per accession and
handed all 12,756 to ``gather``. The only backpressure was a global lock, so
every request was in flight the moment its turn came and the fixed 30 s timeout
applied to a queue thousands deep. That is the mechanism behind the lost
``PRJEB9859``, ``PRJEB82511`` and ``PRJEB19038`` results, all of them large
projects.

**Timeouts that grow on retry.** Those same three projects are simply big; they
need longer, not another attempt at the same deadline. Each retry widens the
timeout instead of repeating a deadline already known to be too short.

**Whole-chain retry.** ``WebEnv`` and ``query_key`` are session-scoped, so
retrying an ELink or ESummary call on its own reuses a history that may no
longer exist. Retry wraps the entire ``esearch -> elink -> efetch`` chain.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from types import TracebackType
from typing import Any, TypeVar

import httpx

from fetch_series.logging_utils import redact_exception

logger = logging.getLogger(__name__)

T = TypeVar("T")

RETRYABLE_STATUS = frozenset({408, 429, 500, 502, 503, 504})


class MalformedResponseError(Exception):
    """An archive returned a well-formed HTTP response with unusable content.

    E-utilities reports several errors as HTTP 200 with a JSON body that simply
    has no ``result`` key -- "Too many UIDs in request" is the common one. Those
    are retryable in the sense that the request can be reissued differently, and
    must never surface as a ``KeyError`` from deep inside a parser.
    """


def is_retryable(exc: BaseException) -> bool:
    """Whether an exception is worth another attempt.

    ``KeyError`` is here deliberately: NCBI's JSON shape varies between
    responses for the same endpoint, and a missing key is far more often a
    transient backend hiccup than a parser bug. The dominant failure signal in
    this project's logs is NCBI backend flakiness -- ``callMLink: Query failed
    on MegaLink server`` (1,712 occurrences) and ``Couldn't resolve
    #pmquerysrv-mz`` (1,423) -- not client error.
    """
    if isinstance(
        exc,
        httpx.TransportError
        | httpx.TimeoutException
        | json.JSONDecodeError
        | MalformedResponseError
        | KeyError,
    ):
        return True
    return isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code in RETRYABLE_STATUS


@dataclass(frozen=True, slots=True)
class Limits:
    """How hard a survey is allowed to push one provider.

    Args:
        rps: requests per second, across all workers.
        concurrency: requests in flight at once. Kept low deliberately; the
            Nextflow module that does the same work runs at maxForks 7.
        attempts: total attempts per accession, including the first.
        base_timeout: deadline for the first attempt, in seconds.
        timeout_growth: multiplier applied to the deadline on each retry.
        max_timeout: ceiling for the grown deadline.
    """

    rps: float = 7.5
    concurrency: int = 8
    attempts: int = 5
    base_timeout: float = 20.0
    timeout_growth: float = 2.0
    max_timeout: float = 180.0

    def timeout_for(self, attempt: int) -> float:
        """Deadline for a given 1-based attempt."""
        grown = self.base_timeout * (self.timeout_growth ** (attempt - 1))
        return min(grown, self.max_timeout)


class RateLimiter:
    """A global minimum interval between request starts.

    Unlike the version in the scripts, which slept the full interval inside the
    lock on every call, this tracks when the next request is due and sleeps only
    the remainder -- so an idle survey does not pay the interval twice.
    """

    def __init__(self, rps: float) -> None:
        self._interval = 1.0 / rps if rps > 0 else 0.0
        self._lock = asyncio.Lock()
        self._next_allowed = 0.0

    async def acquire(self) -> None:
        if self._interval <= 0:
            return
        async with self._lock:
            now = asyncio.get_running_loop().time()
            wait = self._next_allowed - now
            if wait > 0:
                await asyncio.sleep(wait)
                now = asyncio.get_running_loop().time()
            self._next_allowed = now + self._interval


@dataclass(frozen=True, slots=True)
class Prefix:
    """The start of a remote file.

    Args:
        total: the whole file's size, from ``Content-Range`` or, where the
            server ignored the range, ``Content-Length``; ``None`` if neither.
        ranged: the server answered 206, honouring the range.
        complete: ``data`` is the whole file. A reader must not treat a
            truncated last record as a short read when it is not.
    """

    data: bytes
    status: int
    total: int | None
    ranged: bool
    complete: bool


def _total_size(response: httpx.Response) -> int | None:
    content_range = response.headers.get("content-range", "")
    if "/" in content_range:
        size = content_range.rsplit("/", 1)[1]
        return int(size) if size.isdigit() else None
    if response.status_code == 200 and response.headers.get("content-length", "").isdigit():
        return int(response.headers["content-length"])
    return None


class SurveyClient:
    """Rate-limited, bounded HTTP access for one survey run."""

    def __init__(
        self,
        limits: Limits | None = None,
        api_key: str | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.limits = limits or Limits()
        # An empty key is no key. A CI secret that is not set arrives as "" rather
        # than as nothing, and NCBI answers ``api_key=`` with a 400 that no retry
        # can fix -- the Archive state monitor's false alarm of 2026-10-19.
        self.api_key = api_key or None
        self._limiter = RateLimiter(self.limits.rps)
        self._semaphore = asyncio.Semaphore(self.limits.concurrency)
        self._client = client
        self._owns_client = client is None

    async def __aenter__(self) -> SurveyClient:
        if self._client is None:
            self._client = httpx.AsyncClient(follow_redirects=True)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def get(
        self, url: str, params: dict[str, Any] | None = None, timeout: float | None = None
    ) -> httpx.Response:
        """One rate-limited, concurrency-bounded GET.

        Parameters whose value is None are dropped: httpx encodes them as empty
        values, and ``api_key=`` is rejected by E-utilities with a 400.
        """
        if self._client is None:
            raise RuntimeError("SurveyClient must be used as an async context manager")
        clean = {k: v for k, v in (params or {}).items() if v is not None}
        async with self._semaphore:
            await self._limiter.acquire()
            try:
                response = await self._client.get(
                    url, params=clean, timeout=timeout or self.limits.base_timeout
                )
                response.raise_for_status()
            except Exception as exc:
                raise redact_exception(exc) from None
        return response

    async def head(self, url: str, timeout: float | None = None) -> httpx.Response:
        """One rate-limited HEAD, returned whatever its status.

        Deliberately does not raise for status. A 404 on a published download
        link is the answer being asked for, not an error in asking -- and a
        route that raises here would lose the distinction between "the archive
        says this file is gone" and "the request did not complete".

        Redirects are followed: ENA publishes `ftp.sra.ebi.ac.uk` paths that
        answer over HTTPS after a redirect, and refusing to follow would report
        every one of them as unresolvable.
        """
        if self._client is None:
            raise RuntimeError("SurveyClient must be used as an async context manager")
        async with self._semaphore:
            await self._limiter.acquire()
            try:
                return await self._client.head(
                    url, timeout=timeout or self.limits.base_timeout, follow_redirects=True
                )
            except Exception as exc:
                raise redact_exception(exc) from None

    async def stream_prefix(self, url: str, max_bytes: int, timeout: float | None = None) -> Prefix:
        """The first ``max_bytes`` of a file, and whether that was all of it.

        Asks with a ``Range`` header, and stops reading at ``max_bytes`` whether
        or not the server honoured it: a file server that ignores Range and
        streams 15 GB is cut off at the limit rather than downloaded. The link
        goes through :func:`files.request_url`, so a ``#`` in a published name
        (ERR13855116's ``27865_1#3.cram``) is part of the path and not a
        fragment. Errors are redacted, as every request here is.
        """
        from fetch_series.files import request_url

        if self._client is None:
            raise RuntimeError("SurveyClient must be used as an async context manager")
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        chunks: list[bytes] = []
        held = 0
        async with self._semaphore:
            await self._limiter.acquire()
            try:
                async with self._client.stream(
                    "GET",
                    request_url(url),
                    headers={"Range": f"bytes=0-{max_bytes - 1}"},
                    timeout=timeout or self.limits.base_timeout,
                ) as response:
                    response.raise_for_status()
                    total = _total_size(response)
                    async for chunk in response.aiter_bytes():
                        chunks.append(chunk)
                        held += len(chunk)
                        if held >= max_bytes:
                            break
            except Exception as exc:
                raise redact_exception(exc) from None
        body = b"".join(chunks)[:max_bytes]
        complete = total is not None and len(body) >= total
        return Prefix(
            data=body,
            status=response.status_code,
            total=total,
            ranged=response.status_code == 206,
            complete=complete or (total is None and held < max_bytes),
        )

    async def post(
        self, url: str, data: dict[str, Any], timeout: float | None = None
    ) -> httpx.Response:
        """One rate-limited, concurrency-bounded POST.

        E-utilities documents POST for large ID lists, and it is not optional:
        a few hundred UIDs in a query string overflows the server's URI limit
        and comes back as a 414, which is not retryable and loses the series.
        """
        if self._client is None:
            raise RuntimeError("SurveyClient must be used as an async context manager")
        clean = {k: v for k, v in data.items() if v is not None}
        async with self._semaphore:
            await self._limiter.acquire()
            try:
                response = await self._client.post(
                    url, data=clean, timeout=timeout or self.limits.base_timeout
                )
                response.raise_for_status()
            except Exception as exc:
                raise redact_exception(exc) from None
        return response

    async def with_retry(
        self,
        operation: Callable[[float], Awaitable[T]],
        *,
        label: str = "",
        retryable: Callable[[BaseException], bool] | None = None,
    ) -> T:
        """Run ``operation`` with retries, widening its deadline each attempt.

        ``operation`` receives the timeout for the current attempt. Retry wraps
        the whole operation rather than any single request inside it, because
        an E-utilities history cannot be resumed part-way.

        ``retryable`` replaces :func:`is_retryable` for this call, for a request
        whose failure is known to be permanent although its status says
        otherwise: SDL answers a project with a 504 on every attempt.
        """
        should_retry = retryable or is_retryable
        last: BaseException | None = None
        for attempt in range(1, self.limits.attempts + 1):
            try:
                return await operation(self.limits.timeout_for(attempt))
            except Exception as exc:
                last = exc
                if not should_retry(exc) or attempt == self.limits.attempts:
                    raise
                backoff = min(2.0 ** (attempt - 1), 30.0)
                logger.warning(
                    "Retry %d/%d for %s after %s: %s",
                    attempt,
                    self.limits.attempts,
                    label or "request",
                    type(exc).__name__,
                    exc,
                )
                await asyncio.sleep(backoff)
        assert last is not None
        raise last


MemoKey = tuple[str, str, tuple[tuple[str, str], ...]]


class MemoClient(SurveyClient):
    """A client that answers a repeated request from memory instead of the archive.

    Several mechanisms read one document: three read sra-db-be's runinfo for an
    accession, three an SDRF, two a SOFT family file. Surveyed one after
    another over the same corpus, each would fetch it again -- about 15% of the
    NCBI requests of a full link survey, spent on bytes already in hand.

    Only requests ``memoize`` admits are kept, and only successes. Anything
    carrying session state must never be admitted: an E-utilities WebEnv
    answered from memory an hour later names a history the server has dropped.
    Memory is bounded by bytes, oldest first out; SOFT files run to megabytes.
    """

    def __init__(
        self,
        limits: Limits | None = None,
        api_key: str | None = None,
        client: httpx.AsyncClient | None = None,
        *,
        memoize: Callable[[str, str, dict[str, Any]], bool],
        max_bytes: int = 256 * 1024 * 1024,
    ) -> None:
        super().__init__(limits=limits, api_key=api_key, client=client)
        self._memoize = memoize
        self._max_bytes = max_bytes
        self._memo: dict[MemoKey, httpx.Response] = {}
        self._held = 0
        self.hits = 0

    @staticmethod
    def _key(method: str, url: str, params: dict[str, Any]) -> MemoKey:
        # The key never holds the API key: it is not part of what was asked,
        # and a memo key is the kind of thing that ends up in a debug log.
        clean = tuple(
            sorted((k, str(v)) for k, v in params.items() if v is not None and k != "api_key")
        )
        return (method, url, clean)

    def _remember(self, key: MemoKey, response: httpx.Response) -> None:
        size = len(response.content)
        if size > self._max_bytes:
            return
        while self._memo and self._held + size > self._max_bytes:
            oldest = next(iter(self._memo))
            self._held -= len(self._memo.pop(oldest).content)
        self._memo[key] = response
        self._held += size

    async def get(
        self, url: str, params: dict[str, Any] | None = None, timeout: float | None = None
    ) -> httpx.Response:
        if not self._memoize("GET", url, params or {}):
            return await super().get(url, params=params, timeout=timeout)
        key = self._key("GET", url, params or {})
        if key in self._memo:
            self.hits += 1
            return self._memo[key]
        response = await super().get(url, params=params, timeout=timeout)
        self._remember(key, response)
        return response

    async def post(
        self, url: str, data: dict[str, Any], timeout: float | None = None
    ) -> httpx.Response:
        if not self._memoize("POST", url, data):
            return await super().post(url, data=data, timeout=timeout)
        key = self._key("POST", url, data)
        if key in self._memo:
            self.hits += 1
            return self._memo[key]
        response = await super().post(url, data=data, timeout=timeout)
        self._remember(key, response)
        return response


# NCBI allows 10 requests/second with an API key and 3 without. 7.5 leaves
# headroom for the retries a survey inevitably generates.
NCBI_LIMITS = Limits(rps=7.5, concurrency=8)
# EBI publishes no hard figure; this matches what the surveys have run at
# without being throttled.
EBI_LIMITS = Limits(rps=10.0, concurrency=10)
