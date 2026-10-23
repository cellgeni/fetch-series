"""Regression test for a known EBI ENA defect.

**This suite is inverted.** Each test asserts that a documented archive bug
*still reproduces*. A failure here is good news: the archive fixed something.
When one fails, update the knowledge-base page and delete the test.

Knowledge base: docs/pathologies/ena-serves-a-listed-fastq-wrongly.md
"""

from __future__ import annotations

import pytest

from fetch_series.files import FileKind, FileRecord
from fetch_series.filesets import Verdict, verify
from fetch_series.survey import Limits, SurveyClient

pytestmark = pytest.mark.pathology

DIRECTORY = FileRecord(
    "SRR11040464",
    "https://ftp.sra.ebi.ac.uk/vol1/fastq/SRR110/064/SRR11040464/SRR11040464_1.fastq.gz",
    "SRR11040464_1.fastq.gz",
    FileKind.FASTQ,
    "ena",
    size=583_713_371,
)
TRUNCATED = FileRecord(
    "ERR15320482",
    "https://ftp.sra.ebi.ac.uk/vol1/fastq/ERR153/082/ERR15320482/ERR15320482_2.fastq.gz",
    "ERR15320482_2.fastq.gz",
    FileKind.FASTQ,
    "ena",
    size=7_197_985_953,
)


async def _verdict(record: FileRecord) -> Verdict:
    async with SurveyClient(limits=Limits(rps=2.0, concurrency=1, attempts=3)) as client:
        return await verify(record, client)


def test_a_listed_fastq_is_still_served_as_a_directory(run_or_skip):
    verdict = run_or_skip(_verdict(DIRECTORY))
    assert verdict.served_directory, (
        f"SRR11040464_1.fastq.gz now answers {verdict.status} at {verdict.final_url}: "
        "ENA may have restored the file. Update docs/pathologies/ena-serves-a-listed-fastq-wrongly.md."
    )
    assert not verdict.resolves, "verify must never call a directory a file"


def test_a_listed_fastq_still_serves_four_kilobytes(run_or_skip):
    verdict = run_or_skip(_verdict(TRUNCATED))
    assert verdict.served_bytes == 4_096, (
        f"ERR15320482_2.fastq.gz now serves {verdict.served_bytes} bytes: ENA may have "
        "re-uploaded it. Update docs/pathologies/ena-serves-a-listed-fastq-wrongly.md."
    )
    assert verdict.size_matches is False, "verify must flag a size the archive did not publish"
