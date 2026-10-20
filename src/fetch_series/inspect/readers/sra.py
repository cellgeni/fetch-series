"""SRA objects, through sra-tools' ``fastq-dump``, found at runtime.

``fastq-dump -X N --split-files <url>`` reads the first N spots of one specific
object -- the Normalized one or the Lite one, by its URL -- and writes one file
per read of the spot, technical reads included (``fasterq-dump`` would need
``--include-technical`` and has no ``-X``). Checked on SRR25056225 on
2026-10-19: the Normalized object's first spot reads 28 + 91 bases with real
qualities, the Lite object's the same bases with ``?`` throughout.

Without sra-tools a file is *undetermined (reader unavailable)*, never unfit:
an absent tool says nothing about a library.
"""

from __future__ import annotations

import asyncio
import errno
import shutil
import tempfile
from pathlib import Path

from fetch_series.inspect.model import FileSample
from fetch_series.inspect.readers.fastq import read_fastq
from fetch_series.logging_utils import redact

TIMEOUT_S = 900.0


def available() -> str | None:
    return shutil.which("fastq-dump")


async def read_sra(target: str, *, max_reads: int, timeout: float = TIMEOUT_S) -> list[FileSample]:
    """The first ``max_reads`` spots of an SRA object, one sample per read of the spot.

    ``target`` is an object's URL (preferred: it names Normalized or Lite) or a
    run accession, which leaves the choice of object to sra-tools.
    """
    tool = available()
    if tool is None:
        raise FileNotFoundError(
            "sra-tools (fastq-dump) is not installed: install the inspect image"
        )
    with tempfile.TemporaryDirectory(prefix="fetch-sra-") as scratch:
        process = await asyncio.create_subprocess_exec(
            tool,
            "-X",
            str(max_reads),
            "--split-files",
            "-O",
            scratch,
            target,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _, stderr = await asyncio.wait_for(process.communicate(), timeout)
        except TimeoutError:
            process.kill()
            raise TimeoutError(f"fastq-dump did not finish in {timeout:.0f} s") from None
        if process.returncode != 0:
            # The target can be a URL carrying credentials in a query string;
            # sra-tools echoes it, so its stderr is redacted like any log line.
            message = redact(stderr.decode(errors="replace"))[-300:]
            if "storage exhausted" in message:
                # The local disk, not the object: sra-tools' word for ENOSPC.
                raise OSError(errno.ENOSPC, f"fastq-dump: {message.strip()[-120:]}")
            raise RuntimeError(f"fastq-dump exited {process.returncode}: {message}")
        files = sorted(Path(scratch).glob("*.fastq"), key=lambda p: p.name)
        samples = []
        for path in files:
            mate = path.stem.rsplit("_", 1)[-1]
            sample = read_fastq(path.read_bytes(), target, max_reads=max_reads, complete=True)
            sample.format = "sra"
            sample.role_hint = f"R{mate}" if mate.isdigit() else None
            samples.append(sample)
        return samples


__all__ = ["available", "read_sra"]
