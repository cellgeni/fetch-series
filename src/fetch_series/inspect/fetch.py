"""Fetch the start of each file a run offers and read it with the right reader."""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import replace
from typing import Any

from fetch_series.files import FileKind, FileRecord, classify
from fetch_series.inspect.checks import check_set
from fetch_series.inspect.model import FileSample
from fetch_series.inspect.readers.bam import read_bam
from fetch_series.inspect.readers.fastq import read_fastq
from fetch_series.logging_utils import redact, redact_exception
from fetch_series.survey.client import SurveyClient

# A run's classification is made on its first reads. 100,000 is the plan's
# default; whether 200,000 changes a call is measured, not assumed.
DEFAULT_READS = 100_000
MiB = 1024 * 1024
# How much of a file to ask for. A gzipped 28-base read costs ~30 bytes and a
# 91-base one ~80, so 16 MiB holds 100,000 of either with room; a BAM record
# carries its tags as well.
PREFIX_BYTES = {FileKind.FASTQ: 16 * MiB, FileKind.BAM: 32 * MiB, FileKind.CRAM: 32 * MiB}


def unavailable(record: FileRecord, why: str) -> FileSample:
    """A file that could not be read: undetermined, which is never "unfit"."""
    return FileSample(
        source=record.url or record.name,
        format=str(record.kind),
        facts={"reader": "unavailable", "why": why},
    )


async def sample_file(
    record: FileRecord, client: SurveyClient, *, reads: int = DEFAULT_READS
) -> list[FileSample]:
    """The first ``reads`` reads of one file, as one sample per original read.

    A FASTQ is one sample; a BAM is one per read it holds (R1 rebuilt from its
    tags, R2 from its records). A failure to fetch or to parse is a sample that
    says so -- a 404 on DDBJ's mirrored-run path is ``readable=False``, a check
    and not an outcome.
    """
    source = record.url or record.name
    if record.kind is FileKind.OTHER:
        # NCBI versions the objects it stores -- SRR10377488's submitted BAM is
        # `possorted_genome_bam1.bam.1` -- and the version hides the format.
        bare = re.sub(r"\.\d+$", "", (record.name or record.url).rsplit("/", 1)[-1])
        record = replace(record, kind=classify(bare))
    if record.kind not in PREFIX_BYTES:
        return [unavailable(record, f"no reader for {record.kind} yet")]
    if not record.url:
        return [unavailable(record, "no download link")]
    if record.kind is FileKind.CRAM:
        return [unavailable(record, "CRAM needs the inspect extra (pysam)")]
    try:
        # Retried like any request: ERR4265292's BAM failed once and read cleanly
        # on the next attempt, and an unretried blip would record it unreadable.
        url, size = record.url, PREFIX_BYTES[record.kind]
        prefix = await client.with_retry(
            lambda timeout: client.stream_prefix(url, size, timeout=timeout),
            label=f"prefix {source}",
        )
        if record.kind is FileKind.FASTQ:
            return [read_fastq(prefix.data, source, max_reads=reads, complete=prefix.complete)]
        return read_bam(prefix.data, source, max_reads=reads, complete=prefix.complete)
    except Exception as exc:
        error = redact_exception(exc)
        return [unavailable(record, redact(f"{type(error).__name__}: {error}")[:300])]


async def inspect_set(
    records: Iterable[FileRecord], client: SurveyClient, *, reads: int = DEFAULT_READS
) -> dict[str, Any]:
    """Read every data file of one run's set and make the built-in checks."""
    data = [r for r in records if r.is_data]
    samples: list[FileSample] = []
    for record in data:
        samples.extend(await sample_file(record, client, reads=reads))
    checks = check_set(samples, data)
    checks["unreadable"] = [
        {"source": s.source, "why": s.facts.get("why")}
        for s in samples
        if s.facts.get("reader") == "unavailable"
    ]
    checks["reads_per_file"] = reads
    return checks


__all__ = ["DEFAULT_READS", "PREFIX_BYTES", "inspect_set", "sample_file", "unavailable"]
