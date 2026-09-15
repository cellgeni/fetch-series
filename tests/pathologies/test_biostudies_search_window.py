"""Regression test for a known EBI BioStudies defect.

**This suite is inverted.** Each test asserts that a documented archive bug
*still reproduces*. A failure here is good news: the archive fixed something.

Knowledge base: docs/pathologies/biostudies-20000-hit-window.md
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from fetch_series.providers.biostudies import SEARCH_WINDOW

pytestmark = pytest.mark.pathology

SEARCH = "https://www.ebi.ac.uk/biostudies/api/v1/arrayexpress/search"
PAGE_SIZE = 100
LAST_GOOD_PAGE = SEARCH_WINDOW // PAGE_SIZE  # 200
FIRST_BAD_PAGE = LAST_GOOD_PAGE + 1


def _get(page: int) -> httpx.Response:
    async def go() -> httpx.Response:
        async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
            return await client.get(
                SEARCH,
                params={"pageSize": PAGE_SIZE, "page": page, "facet.link_type": "ENA"},
            )

    return asyncio.run(go())


def test_the_collection_is_still_larger_than_the_window() -> None:
    """The premise. Without this the rest of the suite proves nothing."""
    response = _get(1)
    response.raise_for_status()
    total = response.json()["totalHits"]
    assert total > SEARCH_WINDOW, (
        f"ArrayExpress now has {total:,} ENA-linked studies, inside the "
        f"{SEARCH_WINDOW:,} window. The defect can no longer be observed on this "
        "collection -- find a larger one or retire the test."
    )


def test_the_last_page_inside_the_window_still_works() -> None:
    assert _get(LAST_GOOD_PAGE).status_code == 200


def test_the_first_page_past_the_window_is_still_a_500() -> None:
    """Not 400, which is what a rejected parameter earns, and not a short page.

    A 500 is the canonical transient failure, so every ordinary retry policy
    retries a permanently impossible request.
    """
    status = _get(FIRST_BAD_PAGE).status_code
    if status == 200:
        pytest.fail(
            f"page {FIRST_BAD_PAGE} now succeeds. The window appears to be gone -- "
            "update docs/pathologies/biostudies-20000-hit-window.md and simplify "
            "biostudies.search()."
        )
    assert status == 500, (
        f"page {FIRST_BAD_PAGE} now returns {status} rather than 500. If that is a 4xx "
        "the cap is being reported honestly and the defect is FIXED; update the page."
    )


def test_the_cursor_that_would_fix_it_is_still_never_issued() -> None:
    """`nextCursor` is present in every response and always null."""
    payload = _get(1).json()
    assert "nextCursor" in payload, "nextCursor is gone from the response shape"
    assert payload["nextCursor"] is None, (
        "BioStudies now issues a nextCursor. That is the documented way past the "
        "window -- use it in biostudies.search() and retire the year partitioning."
    )
