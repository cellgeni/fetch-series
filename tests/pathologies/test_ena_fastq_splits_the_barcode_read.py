"""Regression test for a known EBI ENA defect.

**This suite is inverted.** Each test asserts that a documented archive bug
*still reproduces*. A failure here is good news: the archive fixed something.
When one fails, update the knowledge-base page and delete the test.

Knowledge base: docs/pathologies/ena-fastq-splits-the-barcode-read.md
"""

from __future__ import annotations

import pytest

from fetch_series.inspect.readers.fastq import read_fastq
from fetch_series.survey import Limits, SurveyClient

pytestmark = pytest.mark.pathology

BASE = "https://ftp.sra.ebi.ac.uk/vol1/fastq/SRR145/047/SRR14522447/SRR14522447_{}.fastq.gz"
# The Normalized object's first spot: index read, then barcode + UMI.
INDEX = "TAATGACC"
BARCODE = "CATGCACTCCGTAGGCNAATAAGCGTCC"


async def _first_reads() -> list[str]:
    async with SurveyClient(limits=Limits(rps=3.0, concurrency=2, attempts=3)) as client:
        reads = []
        for mate in (1, 2):
            url = BASE.format(mate)
            prefix = await client.stream_prefix(url, 20_000)
            sample = read_fastq(prefix.data, url, max_reads=5, complete=prefix.complete)
            reads.append(sample.records[0].seq)
        return reads


def test_enas_split_still_cuts_through_the_barcode_read(run_or_skip):
    first, second = run_or_skip(_first_reads())
    assert (len(first), len(second)) == (28, 99) and first == INDEX + BARCODE[:20], (
        "ENA's fastqs of SRR14522447 no longer split the spot 28 + 99 through the barcode "
        "read. Check whether ENA now publishes 8 + 28 + 91, and update "
        "docs/pathologies/ena-fastq-splits-the-barcode-read.md."
    )
