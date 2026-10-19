"""FASTQ: plain, gzip (multi-member) or bzip2, from the start of a file."""

from __future__ import annotations

import re

from fetch_series.inspect.decompress import inflate
from fetch_series.inspect.model import FileSample, Record

_SUFFIX = re.compile(r"/([123])$")
# The sequencer's own names: TSF2_S35_L002_R1_001.fastq.gz, and the archives'
# _1/_2. A name is a hint, never a verdict: reads decide.
_ROLE = re.compile(r"_([RI][123])(?:_\d{3})?\.f(?:ast)?q", re.IGNORECASE)
_MATE = re.compile(r"_([123])\.f(?:ast)?q", re.IGNORECASE)


def role_hint(name: str) -> str | None:
    """What a file's name says it is: ``R1``, ``R2``, ``I1``, ``I2``, or None."""
    bare = name.rsplit("/", 1)[-1]
    if match := _ROLE.search(bare):
        return match.group(1).upper()
    if match := _MATE.search(bare):
        return f"R{match.group(1)}"
    return None


def read_fastq(
    data: bytes,
    source: str,
    *,
    max_reads: int,
    complete: bool = False,
) -> FileSample:
    """Records from the start of a FASTQ file.

    Args:
        data: the file's first bytes, compressed or not.
        complete: ``data`` is the whole file. Only then is a short last record
            a short read rather than one the prefix cut off.
        max_reads: stop after this many records.
    """
    text_bytes, clean = inflate(data, limit=None)
    reached_end = complete and clean
    lines = text_bytes.decode("ascii", errors="replace").split("\n")
    # The text after the last newline is either nothing (the file ended on a
    # newline) or a line the prefix cut off part-way. Neither is a line.
    lines = lines[:-1]
    whole = len(lines) - len(lines) % 4
    sample = FileSample(source=source, format="fastq", role_hint=role_hint(source))
    instruments: list[str] = []
    sample.facts["instrument_ids"] = instruments
    for i in range(0, whole, 4):
        header, seq, plus, qual = lines[i : i + 4]
        if not header.startswith("@") or not plus.startswith("+") or len(seq) != len(qual):
            if i + 4 == whole and not reached_end:
                break  # the prefix cut the last record inside its quality line
            raise ValueError(f"{source}: not a FASTQ record at line {i + 1}")
        words = header[1:].split()
        name = words[0] if words else ""
        if len(instruments) < 20:
            # The sequencer's own name, where the archive kept it: SRA writes
            # `@SRR25056225.1 A01044:65:HVVJ5DMXX:2:1101:1217:1000 length=28`.
            original = next((w for w in words if w.count(":") >= 6), None)
            if original:
                instruments.append(original)
        # The mate suffix ends the *header*, which may be after a description:
        # `@SRR29534765.1 K00337:202:HWNYLBBXX:6:1101:2747:1297/3`.
        if match := _SUFFIX.search(header.rstrip()):
            sample.suffixes[match.group(1)] = sample.suffixes.get(match.group(1), 0) + 1
        sample.records.append(Record(name=name, seq=seq, qual=qual))
        if len(sample.records) >= max_reads:
            break
    sample.reached_end = reached_end and len(sample.records) < max_reads
    return sample


def spot_name(name: str) -> str:
    """A read name with its mate suffix removed, for pairing mates by name."""
    return _SUFFIX.sub("", name)
