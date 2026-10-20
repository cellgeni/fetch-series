"""CRAM, through pysam, found at runtime: the ``[inspect]`` extra.

A CRAM's reads are reference-compressed, so unlike a BAM it cannot be read with
``struct`` alone. pysam reads a range-fetched prefix in a fresh temporary
directory, with ``REF_PATH`` pointed at EBI's CRAM reference registry for a
CRAM that was aligned. Checked on ERR13855116's ``27865_1#3.cram`` on
2026-10-19: the first 32 MB, unaligned and so needing no reference, read 100,000
records in 0.1 s, paired by 0x40/0x80, R1 26 bases and R2 96.

Without pysam a CRAM is *undetermined (reader unavailable)*, never unfit.

htslib fetches an aligned CRAM's reference with libcurl and no deadline. On
2026-10-20 one fetch from EBI's registry stalled for over ten minutes inside a
worker, and since a run node's fan-out finishes only when its last run does,
it held the whole content survey. A call in C cannot be interrupted from
Python, so :func:`read_cram_bounded` reads in a child process it can kill.
"""

from __future__ import annotations

import asyncio
import os
import pickle
import sys
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
# Long enough to fetch a chromosome's reference, short of holding a survey.
TIMEOUT_S = 600.0


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


async def read_cram_bounded(
    data: bytes,
    source: str,
    *,
    max_reads: int,
    complete: bool = False,
    timeout: float = TIMEOUT_S,
) -> list[FileSample]:
    """:func:`read_cram` in a child process, killed if it outlives ``timeout``."""
    if not available():
        raise FileNotFoundError("pysam is not installed: install fetch-series[inspect]")
    with tempfile.TemporaryDirectory(prefix="fetch-cram-") as scratch:
        path = Path(scratch) / "prefix.cram"
        path.write_bytes(data)
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            __name__,
            str(path),
            source,
            str(max_reads),
            "1" if complete else "0",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            out, err = await asyncio.wait_for(process.communicate(), timeout)
        except TimeoutError:
            process.kill()
            await process.wait()
            raise TimeoutError(
                f"the CRAM read did not finish in {timeout:.0f} s: a reference fetch stalled"
            ) from None
    if process.returncode != 0:
        tail = err.decode(errors="replace").strip().splitlines()[-1:] or ["no message"]
        raise RuntimeError(f"the CRAM reader exited {process.returncode}: {tail[0][:200]}")
    samples: list[FileSample] = pickle.loads(out)
    return samples


def _main(argv: list[str]) -> None:
    path, source, max_reads, complete = argv
    samples = read_cram(
        Path(path).read_bytes(), source, max_reads=int(max_reads), complete=complete == "1"
    )
    sys.stdout.buffer.write(pickle.dumps(samples))


__all__ = ["REF_PATH", "TIMEOUT_S", "available", "read_cram", "read_cram_bounded"]


if __name__ == "__main__":
    _main(sys.argv[1:])
