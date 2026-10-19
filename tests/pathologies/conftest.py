"""Shared helpers for the inverted pathology suite.

The suite's whole signal is that a failure means *the archive fixed something*.
A rate limit or a transport blip surfacing as a test failure destroys that
signal -- it is indistinguishable from the good news the suite exists to
deliver. So transient failures skip, and only a real answer from the archive can
fail a test here.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

import httpx
import pytest

# Statuses that mean "the archive did not answer", as opposed to "the archive
# answered and said something". 400 and 404 are answers: a malformed request is
# a bug in the test, and must fail fast rather than skip for a year.
_UNANSWERED_STATUS = frozenset({408, 429}) | frozenset(range(500, 600))


def did_not_answer(exc: BaseException) -> bool:
    """Whether an exception means the archive gave no verdict at all.

    Deliberately narrower than ``survey.client.is_retryable``. That predicate
    retries ``KeyError`` and malformed-response errors, because during a survey a
    missing key is usually a backend hiccup. Here it would be the worst possible
    rule: an archive that *fixed* a defect usually changes the shape of its
    answer, so the parser breaks -- and skipping on that would hide exactly the
    good news this suite exists to deliver.
    """
    if isinstance(exc, httpx.TransportError | httpx.TimeoutException):
        return True
    return isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code in _UNANSWERED_STATUS


def _run_or_skip[T](coro: Coroutine[Any, Any, T]) -> T:
    """Run an archive query, skipping the test if the archive would not answer.

    Transport errors, timeouts, 408, 429 and 5xx skip. Everything else --
    a 400, a 404, a ``KeyError`` from a changed response -- is a verdict.
    """
    try:
        return asyncio.run(coro)
    except Exception as exc:
        if did_not_answer(exc):
            pytest.skip(f"archive did not answer ({type(exc).__name__}); not a verdict")
        raise


@pytest.fixture
def run_or_skip() -> Callable[[Coroutine[Any, Any, Any]], Any]:
    """Exposed as a fixture so the suite needs no cross-package import."""
    return _run_or_skip
