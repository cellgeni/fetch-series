"""Regression test for a known EBI ENA defect.

**This suite is inverted.** Each test asserts that a documented archive bug
*still reproduces*. A failure here is good news: the archive fixed something.
When one fails, update the knowledge-base page, re-rank the affected route, and
delete the test.

Knowledge base: docs/pathologies/ena-paired-library-single-fastq.md
"""

from __future__ import annotations

import csv
import io

import pytest

from fetch_series.files import FileSet, from_ena_row, recommend
from fetch_series.providers import ena_portal, sra_be
from fetch_series.survey import Limits, SurveyClient

pytestmark = pytest.mark.pathology

FIELDS = (
    "run_accession",
    "library_layout",
    "fastq_ftp",
    "fastq_md5",
    "fastq_bytes",
    "read_count",
    "base_count",
    "submitted_ftp",
)

# Runs where ENA declares the library PAIRED and publishes one fastq, and SRA
# holds more bases than ENA serves. Drawn from the 1,177 the 2026-09-15 census
# found; three is enough for a regression signal and keeps the suite fast.
TRUNCATED = (
    "SRR14855026",
    "SRR21502011",
    "SRR29534765",
    "SRR33854124",
)


def _client() -> SurveyClient:
    return SurveyClient(limits=Limits(rps=3.0, concurrency=2, attempts=3))


async def _both(run: str) -> tuple[dict[str, str], dict[str, str]]:
    async with _client() as client:
        rows = await ena_portal.read_run_report(client, run, timeout=30, fields=FIELDS)
        response = await client.get(
            sra_be.CGI, params={"rettype": "runinfo", "term": run}, timeout=30
        )
    ena = next((r for r in rows if r.get("run_accession") == run), {})
    sra = next((r for r in csv.DictReader(io.StringIO(response.text)) if r.get("Run") == run), {})
    return ena, sra


@pytest.mark.parametrize("run", TRUNCATED)
def test_ena_still_publishes_one_fastq_for_a_paired_library(run: str, run_or_skip) -> None:
    ena, sra = run_or_skip(_both(run))

    # The premise. If these fail the run has changed, not the archive's policy.
    assert ena, f"{run} is no longer in ENA's filereport"
    assert sra, f"{run} is no longer in SRA's runinfo"

    files = [f for f in (ena.get("fastq_ftp") or "").split(";") if f]
    assert ena["library_layout"] == "PAIRED", (
        f"ENA now reports {run} as {ena['library_layout']}. If it publishes one fastq "
        "and says SINGLE, the metadata is consistent and the defect is FIXED -- update "
        "docs/pathologies/ena-paired-library-single-fastq.md."
    )
    assert len(files) == 1, (
        f"ENA now publishes {len(files)} fastq files for {run}. The defect appears to be "
        "FIXED -- update the knowledge-base page and re-rank run->file:ena_fastq."
    )

    ena_bases, sra_bases = int(ena["base_count"]), int(sra["bases"])
    assert sra_bases > ena_bases * 1.05, (
        f"SRA now holds {sra_bases:,} bases against ENA's {ena_bases:,} for {run}. "
        "ENA appears to publish everything the archive has -- the defect is FIXED."
    )


def test_the_file_layer_still_refuses_the_truncated_set(run_or_skip) -> None:
    """The workaround, checked end to end against the live archive.

    Not a test of the archive: a test that our own detection still fires on a
    live response, so a change to ENA's field names cannot silently disable it.
    """
    ena, _ = run_or_skip(_both("SRR29534765"))
    records = from_ena_row(ena, "fastq", "run->file:ena_fastq")
    assert records, "no fastq records parsed; ENA's column names may have changed"
    recommendation = recommend(FileSet("SRR29534765", tuple(records)))
    assert recommendation.chosen is not None
    assert recommendation.chosen.demonstrably_incomplete
    assert "incomplete" in recommendation.reason
