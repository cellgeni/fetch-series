"""Regression test for ELink giving a BioProject its siblings' runs.

**This suite is inverted.** Each test asserts that a documented archive bug
*still reproduces*. A failure here is good news: the archive fixed something.
When one fails, update the knowledge-base page and delete the test.

Knowledge base: docs/pathologies/elink-returns-runs-of-sibling-projects.md
"""

from __future__ import annotations

import pytest

from fetch_series.core import default_api_key
from fetch_series.providers import sra_be
from fetch_series.routes import IMPLEMENTATIONS
from fetch_series.survey import Limits, SurveyClient

pytestmark = pytest.mark.pathology

TIMEOUT = 120.0


async def _facts() -> tuple[set[str], set[str], str]:
    async with SurveyClient(
        limits=Limits(rps=2.0, concurrency=1, attempts=3), api_key=default_api_key()
    ) as client:
        elink = IMPLEMENTATIONS["bioproject->run:efetch_elink"]
        direct = IMPLEMENTATIONS["bioproject->run:efetch_direct"]
        via_elink = set(await elink("PRJNA1033389", client, TIMEOUT))
        by_gprj = set(await direct("PRJNA1033389", client, TIMEOUT))
        rows = await sra_be.runinfo_by_accession(client, "SRR29287414", TIMEOUT)
        return via_elink, by_gprj, rows[0]["BioProject"] if rows else ""


def test_a_siblings_run_still_comes_through_elink(run_or_skip):
    via_elink, by_gprj, owner = run_or_skip(_facts())
    if not owner:
        pytest.skip("runinfo answered empty: the backend under load, not a verdict")
    assert owner == "PRJNA1121326" and "SRR29287414" not in by_gprj
    assert "SRR29287414" in via_elink, (
        "ELink from PRJNA1033389 no longer returns its sibling's SRR29287414. Update "
        "docs/pathologies/elink-returns-runs-of-sibling-projects.md."
    )
