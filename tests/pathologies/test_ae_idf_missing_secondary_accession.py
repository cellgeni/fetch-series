"""Regression test for a known EBI ArrayExpress gap.

**This suite is inverted.** Each test asserts that a documented archive bug
*still reproduces*. A failure here is good news: the archive fixed something.
When one fails, update the knowledge-base page and delete the test.

Knowledge base: docs/pathologies/ae-idf-missing-secondary-accession.md
"""

from __future__ import annotations

import pytest

from fetch_series.providers.biostudies import idf_secondary_accessions
from fetch_series.survey import Limits, SurveyClient

pytestmark = pytest.mark.pathology


async def _secondary(accession: str) -> list[str]:
    async with SurveyClient(limits=Limits(rps=2.0, concurrency=1, attempts=3)) as client:
        return await idf_secondary_accessions(client, accession, 60.0)


def test_e_mtab_6505s_idf_still_declares_no_ena_study(run_or_skip):
    """Its ENA study, ERP111731, is reachable only through the SDRF's BioSamples."""
    assert run_or_skip(_secondary("E-MTAB-6505")) == [], (
        "E-MTAB-6505's IDF now declares a secondary accession. "
        "Update docs/pathologies/ae-idf-missing-secondary-accession.md."
    )
