"""Regression test for a known EBI ENA defect.

**This suite is inverted.** Each test asserts that a documented archive bug
*still reproduces*. A failure here is good news: the archive fixed something.
When one fails, update the knowledge-base page and delete the test.

Knowledge base: docs/pathologies/ena-fastq-is-lite-quality.md
"""

from __future__ import annotations

import pytest

from fetch_series.inspect.checks import quality_verdict
from fetch_series.inspect.readers.fastq import read_fastq
from fetch_series.survey import Limits, SurveyClient

pytestmark = pytest.mark.pathology

# SRR11442250's ENA fastq: every quality '?', where NCBI's Normalized object of
# the run has real ones (the 2026-10-22 content survey).
LITE = "https://ftp.sra.ebi.ac.uk/vol1/fastq/SRR114/050/SRR11442250/SRR11442250_1.fastq.gz"


async def _prefix(url: str, size: int):
    async with SurveyClient(limits=Limits(rps=3.0, concurrency=2, attempts=3)) as client:
        return await client.stream_prefix(url, size)


def test_enas_fastq_still_carries_substituted_qualities(run_or_skip):
    prefix = run_or_skip(_prefix(LITE, 200_000))
    sample = read_fastq(prefix.data, LITE, max_reads=500, complete=prefix.complete)
    assert sample.records, "ENA served no reads: check the link before reading this as a fix"
    assert quality_verdict(sample) == "substituted", (
        "ENA's fastq of SRR11442250 now carries real qualities: ENA re-derived it "
        "from the Normalized object. Update docs/pathologies/ena-fastq-is-lite-quality.md."
    )
