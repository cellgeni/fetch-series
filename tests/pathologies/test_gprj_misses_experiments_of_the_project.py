"""Regression test for [GPRJ] missing experiments its project holds.

**This suite is inverted.** Each test asserts that a documented archive bug
*still reproduces*. A failure here is good news: the archive fixed something.
When one fails, update the knowledge-base page and delete the test.

Knowledge base: docs/pathologies/gprj-misses-experiments-of-the-project.md
"""

from __future__ import annotations

import pytest

from fetch_series.core import default_api_key
from fetch_series.providers import sra_be
from fetch_series.providers.eutils import esearch
from fetch_series.survey import Limits, SurveyClient

pytestmark = pytest.mark.pathology

TIMEOUT = 120.0


async def _facts() -> tuple[int, int, str]:
    async with SurveyClient(
        limits=Limits(rps=2.0, concurrency=1, attempts=3), api_key=default_api_key()
    ) as client:
        alone = await esearch(client, db="sra", term="SRX135150[ACCN]", timeout=TIMEOUT)
        within = await esearch(
            client, db="sra", term="SRX135150[ACCN] AND PRJNA66167[GPRJ]", timeout=TIMEOUT
        )
        rows = await sra_be.runinfo_by_accession(client, "SRR453077", TIMEOUT)
        return int(alone["count"]), int(within["count"]), rows[0]["BioProject"] if rows else ""


def test_an_experiment_of_the_project_is_still_outside_its_gprj(run_or_skip):
    alone, within, project = run_or_skip(_facts())
    if not project:
        pytest.skip("runinfo answered empty: the backend under load, not a verdict")
    assert (alone, project) == (1, "PRJNA66167")
    assert within == 0, (
        "SRX135150 is now found under PRJNA66167[GPRJ]. Update "
        "docs/pathologies/gprj-misses-experiments-of-the-project.md."
    )
