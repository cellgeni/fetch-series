"""Regression test for BioStudies' file address answering 404 for a registered file.

**This suite is inverted.** Each test asserts that a documented archive bug
*still reproduces*. A failure here is good news: the archive fixed something.
When one fails, update the knowledge-base page and delete the test.

Knowledge base: docs/pathologies/biostudies-files-404s-registered-files.md
"""

from __future__ import annotations

import httpx
import pytest

from fetch_series.providers import biostudies
from fetch_series.survey import Limits, SurveyClient

pytestmark = pytest.mark.pathology

# Six requests in a row for its IDF answered 404 on 2026-10-26; the study lists
# the file and the mirror serves it.
STUDY = "E-MTAB-13459"
NAME = f"{STUDY}.idf.txt"
TRIES = 5


async def _front_door() -> list[int]:
    """The status of each of ``TRIES`` requests for the file at ``FILES``."""
    statuses = []
    async with SurveyClient(limits=Limits(rps=1.0, concurrency=1, attempts=1)) as client:
        for _ in range(TRIES):
            try:
                await client.get(f"{biostudies.FILES}/{STUDY}/{NAME}", timeout=60)
                statuses.append(200)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code != 404:
                    raise
                statuses.append(404)
    return statuses


async def _through_the_fallback() -> str:
    async with SurveyClient(limits=Limits(rps=1.0, concurrency=1, attempts=3)) as client:
        return await biostudies.fetch_idf(client, STUDY, timeout=60)


def test_the_front_door_still_answers_404_for_a_registered_file(run_or_skip):
    statuses = run_or_skip(_front_door())
    assert 404 in statuses, (
        f"{biostudies.FILES}/{STUDY}/{NAME} answered {TRIES} of {TRIES} requests. Update "
        "docs/pathologies/biostudies-files-404s-registered-files.md, and if it holds "
        "across the ArrayExpress corpus, retire the mirror fallback in "
        "providers/biostudies.study_file."
    )


def test_the_mirror_still_serves_it(run_or_skip):
    """The workaround: the study's ``info`` names a mirror that has the file."""
    text = run_or_skip(_through_the_fallback())
    assert "Investigation Title" in text, f"{STUDY}'s IDF did not come back through the mirror"
