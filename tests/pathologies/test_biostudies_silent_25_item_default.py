"""Regression test for a known EBI BioStudies default.

**This suite is inverted.** Each test asserts that a documented archive bug
*still reproduces*. A failure here is good news: the archive fixed something.
When one fails, update the knowledge-base page and delete the test.

Knowledge base: docs/pathologies/biostudies-silent-25-item-default.md
"""

from __future__ import annotations

from typing import Any

import pytest

from fetch_series.providers.biostudies import API, registered_files
from fetch_series.survey import Limits, SurveyClient

pytestmark = pytest.mark.pathology

STUDY = "E-MTAB-9221"


async def _default() -> dict[str, Any]:
    async with SurveyClient(limits=Limits(rps=2.0, concurrency=1, attempts=3)) as client:
        response = await client.get(f"{API}/studies/{STUDY}/files")
        return response.json()  # type: ignore[no-any-return]


async def _paged() -> list[dict[str, Any]]:
    async with SurveyClient(limits=Limits(rps=2.0, concurrency=1, attempts=3)) as client:
        return await registered_files(client, STUDY, 60.0)


def test_the_listing_still_stops_at_25(run_or_skip):
    answer = run_or_skip(_default())
    total = answer["pagination"]["total"]
    assert len(answer["items"]) == 25 < total, (
        f"BioStudies now lists {len(answer['items'])} of {total} files by default. "
        "Update docs/pathologies/biostudies-silent-25-item-default.md."
    )


def test_paging_still_gets_every_file(run_or_skip):
    total = run_or_skip(_default())["pagination"]["total"]
    assert len(run_or_skip(_paged())) == total
