"""Regression test for sra-db-be's history form stopping at 5,000 runs.

**This suite is inverted.** Each test asserts that a documented archive bug
*still reproduces*. A failure here is good news: the archive fixed something.
When one fails, update the knowledge-base page and delete the test.

Knowledge base: docs/pathologies/sra-be-history-caps-at-5000-runs.md
"""

from __future__ import annotations

import pytest

from fetch_series.core import default_api_key
from fetch_series.providers import sra_be
from fetch_series.providers.eutils import esearch
from fetch_series.survey import Limits, SurveyClient

pytestmark = pytest.mark.pathology

TIMEOUT = 600.0


async def _by_history(project: str) -> list[str]:
    async with SurveyClient(
        limits=Limits(rps=2.0, concurrency=1, attempts=3), api_key=default_api_key()
    ) as client:
        found = await esearch(client, db="sra", term=f"{project}[GPRJ]", timeout=TIMEOUT)
        rows = await sra_be.runinfo_by_history(client, found["webenv"], found["querykey"], TIMEOUT)
        return [row["Run"] for row in rows]


async def _by_accession(project: str) -> set[str]:
    async with SurveyClient(
        limits=Limits(rps=2.0, concurrency=1, attempts=3), api_key=default_api_key()
    ) as client:
        rows = await sra_be.runinfo_by_accession(client, project, TIMEOUT)
        return {row["Run"] for row in rows}


def test_the_history_form_still_stops_at_5000_runs(run_or_skip):
    """PRJEB82511 holds 7,263 runs; its history answers 10,000 rows of 5,000."""
    runs = run_or_skip(_by_history("PRJEB82511"))
    assert len(set(runs)) == 5_000 and sra_be.history_was_capped([{"Run": r} for r in runs]), (
        f"sra-db-be's history form now answers {len(set(runs)):,} distinct runs of "
        f"{len(runs):,} rows for PRJEB82511. Update "
        "docs/pathologies/sra-be-history-caps-at-5000-runs.md and the route's fallback."
    )


def test_the_project_by_accession_is_still_answered_whole(run_or_skip):
    """The workaround: the by-accession endpoint answers all 7,263."""
    answer = run_or_skip(_by_accession("PRJEB82511"))
    if not answer:
        pytest.skip("an empty 200: the backend under load, not a verdict")
    assert len(answer) >= 7_263
