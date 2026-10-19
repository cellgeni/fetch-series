"""Regression test for a known EBI BioStudies defect.

**This suite is inverted.** Each test asserts that a documented archive bug
*still reproduces*. A failure here is good news: the archive fixed something.

Knowledge base: docs/pathologies/biostudies-20000-hit-window.md
"""

from __future__ import annotations

import httpx
import pytest

from fetch_series.providers.biostudies import SEARCH_WINDOW

pytestmark = pytest.mark.pathology

SEARCH = "https://www.ebi.ac.uk/biostudies/api/v1/arrayexpress/search"
PAGE_SIZE = 100
LAST_GOOD_PAGE = SEARCH_WINDOW // PAGE_SIZE  # 200
FIRST_BAD_PAGE = LAST_GOOD_PAGE + 1


async def _get(page: int, *, expect_500: bool = False) -> httpx.Response:
    """Fetch one search page, raising if BioStudies did not answer.

    The status of the page past the window *is* the verdict, so a 500 there is
    returned rather than raised; everywhere else a 429 or 5xx raises and
    ``run_or_skip`` turns it into a skip.
    """
    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        response = await client.get(
            SEARCH,
            params={"pageSize": PAGE_SIZE, "page": page, "facet.link_type": "ENA"},
        )
    status = response.status_code
    unanswered = status in (408, 429) or status >= 500
    if unanswered and not (expect_500 and status == 500):
        response.raise_for_status()
    return response


def test_the_collection_is_still_larger_than_the_window(run_or_skip) -> None:  # type: ignore[no-untyped-def]
    """The premise. Without this the rest of the suite proves nothing."""
    response = run_or_skip(_get(1))
    response.raise_for_status()
    total = response.json()["totalHits"]
    assert total > SEARCH_WINDOW, (
        f"ArrayExpress now has {total:,} ENA-linked studies, inside the "
        f"{SEARCH_WINDOW:,} window. The defect can no longer be observed on this "
        "collection -- find a larger one or retire the test."
    )


def test_the_last_page_inside_the_window_still_works(run_or_skip) -> None:  # type: ignore[no-untyped-def]
    assert run_or_skip(_get(LAST_GOOD_PAGE)).status_code == 200


def test_the_first_page_past_the_window_is_still_a_500(run_or_skip) -> None:  # type: ignore[no-untyped-def]
    """Not 400, which is what a rejected parameter earns, and not a short page.

    A 500 is the canonical transient failure, so every ordinary retry policy
    retries a permanently impossible request. That is also why this one test
    cannot skip on a 500: here a 500 is the defect, not the archive being busy.
    """
    status = run_or_skip(_get(FIRST_BAD_PAGE, expect_500=True)).status_code
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


def test_the_cursor_that_would_fix_it_is_still_never_issued(run_or_skip) -> None:  # type: ignore[no-untyped-def]
    """`nextCursor` is present in every response and always null."""
    payload = run_or_skip(_get(1)).json()
    assert "nextCursor" in payload, "nextCursor is gone from the response shape"
    assert payload["nextCursor"] is None, (
        "BioStudies now issues a nextCursor. That is the documented way past the "
        "window -- use it in biostudies.search() and retire the year partitioning."
    )
