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

import pytest

from fetch_series.survey.client import is_retryable


def _run_or_skip[T](coro: Coroutine[Any, Any, T]) -> T:
    """Run an archive query, skipping the test if the archive would not answer.

    ``is_retryable`` is the same predicate the survey harness uses, so "the
    archive was busy" means here exactly what it means everywhere else in this
    project: 429, 5xx, timeouts and transport errors.
    """
    try:
        return asyncio.run(coro)
    except Exception as exc:
        if is_retryable(exc):
            pytest.skip(f"archive did not answer ({type(exc).__name__}); not a verdict")
        raise


@pytest.fixture
def run_or_skip() -> Callable[[Coroutine[Any, Any, Any]], Any]:
    """Exposed as a fixture so the suite needs no cross-package import."""
    return _run_or_skip
