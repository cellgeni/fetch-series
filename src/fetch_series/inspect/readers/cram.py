"""CRAM, through pysam, found at runtime: the ``[inspect]`` extra.

A CRAM's reads are reference-compressed, so unlike a BAM it cannot be read with
``struct`` alone. pysam reads a range-fetched prefix in a fresh temporary
directory, with ``REF_PATH`` pointed at EBI's CRAM reference registry for a
CRAM that was aligned. Checked on ERR13855116's ``27865_1#3.cram`` on
2026-10-19: the first 32 MB, unaligned and so needing no reference, read 100,000
records in 0.1 s, paired by 0x40/0x80, R1 26 bases and R2 96.

Without pysam a CRAM is *undetermined (reader unavailable)*, never unfit.
"""

from __future__ import annotations

import os
import tempfile
from importlib.util import find_spec
from pathlib import Path
from typing import Any

from fetch_series.inspect.model import FileSample
from fetch_series.inspect.readers.bam import (
    _BAMTOFASTQ,
    NOT_PRIMARY,
    BamPrefix,
    BamRecord,
    samples_from,
)

# EBI's CRAM reference registry serves a reference by the MD5 a CRAM records.
REF_PATH = "https://www.ebi.ac.uk/ena/cram/md5/%s"


def available() -> bool:
    return find_spec("pysam") is not None


def _record(read: Any) -> BamRecord:
    quals = read.query_qualities
    qual = None if quals is None else "".join(chr(q + 33) for q in quals)
    return BamRecord(
        name=read.query_name or "",
        flag=read.flag,
        seq=read.query_sequence or "",
        qual=qual,
        tags=dict(read.get_tags()),
    )


def read_cram(
    data: bytes, source: str, *, max_reads: int, complete: bool = False
) -> list[FileSample]:
    """The first records of a CRAM prefix, grouped into reads as a BAM's are."""
    if not available():
        raise FileNotFoundError("pysam is not installed: install fetch-series[inspect]")
    import pysam

    with tempfile.TemporaryDirectory(prefix="fetch-cram-") as scratch:
        path = Path(scratch) / "prefix.cram"
        path.write_bytes(data)
        # Set for this read only, and a cache in the scratch directory: a
        # reference fetched for one CRAM is not kept as a side effect.
        saved = {k: os.environ.get(k) for k in ("REF_PATH", "REF_CACHE")}
        os.environ["REF_PATH"] = REF_PATH
        os.environ["REF_CACHE"] = str(Path(scratch) / "%2s/%2s/%s")
        records: list[BamRecord] = []
        truncated = False
        try:
            with pysam.AlignmentFile(str(path), "rc", check_sq=False) as handle:
                header = handle.header.to_dict()
                try:
                    for read in handle.fetch(until_eof=True):
                        if read.flag & NOT_PRIMARY:
                            continue
                        records.append(_record(read))
                        if len(records) >= max_reads * 2:
                            break
                except (OSError, ValueError):
                    truncated = True  # the prefix ended inside a container
        finally:
            for key, value in saved.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
    bamtofastq: dict[str, list[tuple[str, str]]] = {}
    for line in header.get("CO", []):
        for read_name, spec in _BAMTOFASTQ.findall(str(line)):
            parts = [p.split(":") for p in spec.split(",") if p]
            bamtofastq[read_name] = [(p[0], p[1] if len(p) > 1 else "") for p in parts]
    prefix = BamPrefix(
        sort_order=(header.get("HD") or {}).get("SO"),
        references=len(header.get("SQ", [])),
        programs=[str(pg.get("PN") or pg.get("ID")) for pg in header.get("PG", [])],
        bamtofastq=bamtofastq,
        records=records,
        reached_end=complete and not truncated,
    )
    return samples_from(prefix, source, "cram", max_reads=max_reads)


__all__ = ["REF_PATH", "available", "read_cram"]
