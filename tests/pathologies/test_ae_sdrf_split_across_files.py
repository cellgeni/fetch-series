"""Regression test for an ArrayExpress SDRF split across two files.

**This suite is inverted.** Each test asserts that a documented archive
behaviour *still reproduces*. A failure here means the archive changed: update
the knowledge-base page. The workaround can stay, since it is asked only on a
404.

Knowledge base: docs/pathologies/ae-sdrf-split-across-files.md
"""

from __future__ import annotations

import httpx
import pytest

from fetch_series.providers import biostudies
from fetch_series.survey import Limits, SurveyClient

pytestmark = pytest.mark.pathology

# Its IDF names E-GEOD-29264.hyb.sdrf.txt and E-GEOD-29264.seq.sdrf.txt, and it
# registers no SDRF by the usual name (2026-10-27).
STUDY = "E-GEOD-29264"


def _client() -> SurveyClient:
    return SurveyClient(limits=Limits(rps=1.0, concurrency=1, attempts=3))


async def _usual_and_named() -> tuple[int, list[str]]:
    """The usual SDRF's status, through the mirror fallback, and what the IDF names."""
    async with _client() as client:
        try:
            await biostudies.study_file(client, STUDY, f"{STUDY}.sdrf.txt", timeout=60)
            status = 200
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
        named = biostudies.sdrf_files(await biostudies.fetch_idf(client, STUDY, timeout=60))
    return status, named


async def _through_the_fallback() -> list[list[str]]:
    async with _client() as client:
        return await biostudies.sdrf_raw_rows(client, STUDY, timeout=60)


def test_the_study_still_has_no_sdrf_by_the_usual_name(run_or_skip):
    status, named = run_or_skip(_usual_and_named())
    assert status == 404, (
        f"{STUDY}.sdrf.txt answered {status}. Update "
        "docs/pathologies/ae-sdrf-split-across-files.md."
    )
    assert named == [f"{STUDY}.hyb.sdrf.txt", f"{STUDY}.seq.sdrf.txt"], (
        f"{STUDY}'s IDF now names {named}. Update docs/pathologies/ae-sdrf-split-across-files.md."
    )


def test_the_fallback_reads_both_halves(run_or_skip):
    """The workaround: the arrays come from one file, the runs from the other."""
    rows = run_or_skip(_through_the_fallback())
    values = biostudies.sdrf_values(rows, ("Array Data File", "Comment[ENA_RUN]"))
    assert values["Array Data File"], f"{STUDY}'s hyb SDRF was not read"
    assert values["Comment[ENA_RUN]"], f"{STUDY}'s seq SDRF was not read"
