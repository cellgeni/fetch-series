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

# Listed with a size and an md5; each path is a 301 to an empty directory.
DIRECTORIES = (
    FileRecord(
        "SRR11040464",
        "https://ftp.sra.ebi.ac.uk/vol1/fastq/SRR110/064/SRR11040464/SRR11040464_1.fastq.gz",
        "SRR11040464_1.fastq.gz",
        FileKind.FASTQ,
        "ena",
        size=583_713_371,
    ),
    FileRecord(
        "SRR27782870",
        "https://ftp.sra.ebi.ac.uk/vol1/fastq/SRR277/070/SRR27782870/SRR27782870_1.fastq.gz",
        "SRR27782870_1.fastq.gz",
        FileKind.FASTQ,
        "ena",
        size=606_656_529,
    ),
)


async def _verdict(record: FileRecord) -> Verdict:
    async with SurveyClient(limits=Limits(rps=2.0, concurrency=1, attempts=3)) as client:
        return await verify(record, client)


@pytest.mark.parametrize("record", DIRECTORIES, ids=lambda r: r.run)
def test_a_listed_fastq_is_still_served_as_a_directory(run_or_skip, record):
    verdict = run_or_skip(_verdict(record))
    assert verdict.served_directory, (
        f"{record.name} now answers {verdict.status} at {verdict.final_url}: ENA may have "
        "restored the file. Update docs/pathologies/ena-serves-a-listed-fastq-wrongly.md."
    )
    assert not verdict.resolves, "verify must never call a directory a file"
