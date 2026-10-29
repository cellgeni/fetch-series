"""Regression test for a known EBI ArrayExpress behaviour.

**This suite is inverted.** Each test asserts that a documented archive bug
*still reproduces*. A failure here is good news: the archive fixed something.
When one fails, update the knowledge-base page and delete the test.

Knowledge base: docs/pathologies/ae-idf-run-range-is-not-a-run-list.md
"""

from __future__ import annotations

import pytest

from fetch_series.providers.biostudies import (
    fetch_idf,
    idf_comments,
    sdrf_raw_rows,
    sdrf_values,
    sequence_data,
)
from fetch_series.survey import Limits, SurveyClient

pytestmark = pytest.mark.pathology

PAGE = "Update docs/pathologies/ae-idf-run-range-is-not-a-run-list.md."


async def _ranges(accession: str) -> list[str]:
    async with SurveyClient(limits=Limits(rps=2.0, concurrency=1, attempts=3)) as client:
        text = await fetch_idf(client, accession, 60.0)
    return [value.rpartition("/")[2] for value in idf_comments(text).get("SequenceDataURI", [])]


async def _range_and_sdrf_runs(accession: str) -> tuple[list[str], set[str]]:
    async with SurveyClient(limits=Limits(rps=2.0, concurrency=1, attempts=3)) as client:
        text = await fetch_idf(client, accession, 60.0)
        rows = await sdrf_raw_rows(client, accession, 60.0)
    named = sequence_data(idf_comments(text).get("SequenceDataURI", [])).accessions
    return named, set(sdrf_values(rows, ("Comment[ENA_RUN]",))["Comment[ENA_RUN]"])


def test_e_mtab_4556s_range_still_spans_118_813_accessions_for_two_runs(run_or_skip):
    """Its SDRF lists ERR1313817 and ERR1432629, and nothing between them."""
    assert "ERR1313817-ERR1432629" in run_or_skip(_ranges("E-MTAB-4556")), (
        f"E-MTAB-4556's IDF no longer writes ERR1313817-ERR1432629. {PAGE}"
    )


def test_e_geod_47691s_range_still_descends(run_or_skip):
    """Its runs' first and last in string order: SRR1204762 sorts before SRR886300."""
    assert "SRR1204762-SRR886300" in run_or_skip(_ranges("E-GEOD-47691")), (
        f"E-GEOD-47691's IDF no longer writes the descending SRR1204762-SRR886300. {PAGE}"
    )


def test_e_mtab_5101s_range_still_covers_runs_its_sdrf_does_not_list(run_or_skip):
    """ERR1638087-ERR1638118 is 32 runs of ERP017164; the SDRF lists 16 of them."""
    named, listed = run_or_skip(_range_and_sdrf_runs("E-MTAB-5101"))
    assert listed and set(named) - listed, (
        f"E-MTAB-5101's IDF range now names only the runs its SDRF lists. {PAGE}"
    )
