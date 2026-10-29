"""Regression test for a known EBI ArrayExpress gap.

**This suite is inverted.** Each test asserts that a documented archive bug
*still reproduces*. A failure here is good news: the archive fixed something.
When one fails, update the knowledge-base page and delete the test.

Until 2026-10-28 this gap was counted as 551 studies, and 540 of them were this
project's reader: it read only the first cell of the row, and nothing from a
quoted IDF. These tests read through the fixed reader, so what they assert is
the archive's.

Knowledge base: docs/pathologies/ae-idf-missing-secondary-accession.md
"""

from __future__ import annotations

import pytest

from fetch_series.providers.biostudies import fetch_idf, idf_comments, idf_secondary
from fetch_series.survey import Limits, SurveyClient

pytestmark = pytest.mark.pathology


async def _idf(accession: str) -> str:
    async with SurveyClient(limits=Limits(rps=2.0, concurrency=1, attempts=3)) as client:
        return await fetch_idf(client, accession, 60.0)


def test_e_mtab_6505s_idf_still_declares_no_ena_study(run_or_skip):
    """Its ENA study, ERP111731, is reachable only through the SDRF's BioSamples."""
    text = run_or_skip(_idf("E-MTAB-6505"))
    assert idf_secondary(text) == [], (
        "E-MTAB-6505's IDF now declares a secondary accession. "
        "Update docs/pathologies/ae-idf-missing-secondary-accession.md."
    )


def test_e_mtab_13231_names_its_ena_study_only_as_a_related_experiment(run_or_skip):
    """Every one of its 345 SDRF runs is in ERP133516, which it names as a related experiment."""
    text = run_or_skip(_idf("E-MTAB-13231"))
    assert idf_secondary(text) == [] and "ERP133516" in idf_comments(text).get(
        "RelatedExperiment", []
    ), (
        "E-MTAB-13231's IDF has changed: it now declares a secondary accession, or no "
        "longer names ERP133516. Update docs/pathologies/ae-idf-missing-secondary-accession.md."
    )
