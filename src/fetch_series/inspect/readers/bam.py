"""BAM, in pure Python: BGZF, then the header and records by ``struct``.

The SAM/BAM specification is short and stable, and the first few hundred
thousand records are all a sample needs, so this reads them without pysam.
That keeps the core of ``fetch inspect`` free of a compiled dependency: CRAM
needs one (it needs references), BAM does not.

A 10x BAM matters here because it is sometimes the only copy of the barcode
read. ERR4265292 was submitted as a BAM, and its SRA object holds the 91-base
cDNA read alone. Cell Ranger keeps the raw barcode and UMI in tags (``CR``/``CY``,
``UR``/``UY``) and records in ``@CO 10x_bam_to_fastq:`` header lines how
``bamtofastq`` should rebuild each original read from them. This reader follows
those lines and rebuilds the reads, marked as derived.
"""

from __future__ import annotations

import re
import struct
from dataclasses import dataclass, field
from typing import Any

from fetch_series.inspect.decompress import gunzip_prefix
from fetch_series.inspect.model import FileSample, Record

MAGIC = b"BAM\x01"
_SEQ = "=ACMGRSVTWYHKDBN"
_COMPLEMENT = str.maketrans("ACGTNacgtn", "TGCANtgcan")
# 0x100 secondary, 0x800 supplementary: neither is a read of its own.
NOT_PRIMARY = 0x900
# Cell Ranger's barcode and UMI tags, and the SAM specification's own: a BAM
# not written by Cell Ranger keeps the UMI in RX/QX (or OX/BZ). ERR4265292's,
# written by `fastq_pre_barcodes`, has CR/CY and RX/QX and no 10x_bam_to_fastq
# header -- the barcode read is there, 16 + 12 bases, just not by 10x's name.
TEN_X_TAGS = ("CR", "CY", "CB", "UR", "UY", "UB", "RX", "QX", "OX", "BZ")
_UMI_TAGS = (("UR", "UY"), ("RX", "QX"), ("OX", "BZ"))


def _barcode_spec(records: list[BamRecord]) -> list[tuple[str, str]] | None:
    """How to rebuild the barcode read from tags, where no header says how.

    The raw cell barcode, then the raw UMI under whichever name the writer used.
    Raw, never corrected: ``CB``/``UB`` are what a pipeline *decided* the
    barcode was, and a whitelist check on those would pass by construction.
    """
    if not records or not all("CR" in r.tags for r in records[:100]):
        return None
    spec = [("CR", "CY")]
    for umi, qual in _UMI_TAGS:
        if all(umi in r.tags for r in records[:100]):
            spec.append((umi, qual))
            break
    return spec


# `@CO	10x_bam_to_fastq:R1(CR:CY,UR:UY)` -- read R1 is the concatenation of the
# CR sequence and the UR sequence, with CY and UY as their qualities.
# `10x_bam_to_fastq:R2(SEQ:QUAL)` -- R2 is the record's own sequence.
_BAMTOFASTQ = re.compile(r"10x_bam_to_fastq:([RI][123])\(([^)]*)\)")

_SCALAR = {"c": "<b", "C": "<B", "s": "<h", "S": "<H", "i": "<i", "I": "<I", "f": "<f"}


@dataclass(slots=True)
class BamRecord:
    name: str
    flag: int
    seq: str
    qual: str | None
    tags: dict[str, Any] = field(default_factory=dict)

    @property
    def reverse(self) -> bool:
        return bool(self.flag & 0x10)


@dataclass(slots=True)
class BamPrefix:
    """What the start of a BAM said: its header facts and its first primary records."""

    sort_order: str | None
    references: int
    programs: list[str]
    bamtofastq: dict[str, list[tuple[str, str]]]
    records: list[BamRecord]
    reached_end: bool


def _tags(blob: bytes) -> dict[str, Any]:
    tags: dict[str, Any] = {}
    i = 0
    while i + 3 <= len(blob):
        tag = blob[i : i + 2].decode("ascii", errors="replace")
        kind = chr(blob[i + 2])
        i += 3
        if kind == "A":
            tags[tag] = chr(blob[i])
            i += 1
        elif kind in _SCALAR:
            fmt = _SCALAR[kind]
            size = struct.calcsize(fmt)
            (tags[tag],) = struct.unpack_from(fmt, blob, i)
            i += size
        elif kind in ("Z", "H"):
            end = blob.index(b"\x00", i)
            tags[tag] = blob[i:end].decode("ascii", errors="replace")
            i = end + 1
        elif kind == "B":
            sub = chr(blob[i])
            (count,) = struct.unpack_from("<i", blob, i + 1)
            size = struct.calcsize(_SCALAR[sub])
            i += 5 + count * size
            tags[tag] = f"<array {sub}x{count}>"
        else:
            raise ValueError(f"unknown BAM tag type {kind!r} for {tag}")
    return tags


def parse_bam(data: bytes, *, max_records: int, complete: bool = False) -> BamPrefix:
    """The header and first primary records from the start of a BAM.

    Secondary and supplementary records (``0x900``) are skipped: they repeat a
    read already counted. A record the prefix cut off is dropped.
    """
    raw, clean = gunzip_prefix(data)
    if not raw.startswith(MAGIC):
        raise ValueError("not a BAM: no BAM\\1 magic after BGZF")
    (l_text,) = struct.unpack_from("<i", raw, 4)
    text = raw[8 : 8 + l_text].decode("utf-8", errors="replace")
    pos = 8 + l_text
    (n_ref,) = struct.unpack_from("<i", raw, pos)
    pos += 4
    for _ in range(n_ref):
        (l_name,) = struct.unpack_from("<i", raw, pos)
        pos += 4 + l_name + 4

    sort_order = None
    programs: list[str] = []
    bamtofastq: dict[str, list[tuple[str, str]]] = {}
    for line in text.splitlines():
        if line.startswith("@HD"):
            for field_ in line.split("\t")[1:]:
                if field_.startswith("SO:"):
                    sort_order = field_[3:]
        elif line.startswith("@PG"):
            # PN is the program's name; some writers put it after ID with a
            # space rather than a tab, so read it out of the whole line.
            if match := re.search(r"\bPN:(\S+)", line):
                programs.append(match.group(1))
        elif line.startswith("@CO"):
            for read, spec in _BAMTOFASTQ.findall(line):
                parts = [p.split(":") for p in spec.split(",") if p]
                bamtofastq[read] = [(p[0], p[1] if len(p) > 1 else "") for p in parts]

    records: list[BamRecord] = []
    while pos + 4 <= len(raw) and len(records) < max_records:
        (block_size,) = struct.unpack_from("<i", raw, pos)
        if pos + 4 + block_size > len(raw):
            break  # cut off by the prefix
        body = raw[pos + 4 : pos + 4 + block_size]
        pos += 4 + block_size
        (_ref, _at, l_name, _mapq, _bin, n_cigar, flag, l_seq) = struct.unpack_from(
            "<iiBBHHHi", body, 0
        )
        if flag & NOT_PRIMARY:
            continue
        cursor = 32
        name = body[cursor : cursor + l_name - 1].decode("ascii", errors="replace")
        cursor += l_name + 4 * n_cigar
        packed = body[cursor : cursor + (l_seq + 1) // 2]
        cursor += (l_seq + 1) // 2
        seq = "".join(_SEQ[b >> 4] + _SEQ[b & 0xF] for b in packed)[:l_seq]
        quals = body[cursor : cursor + l_seq]
        cursor += l_seq
        qual = None if l_seq == 0 or quals[0] == 0xFF else "".join(chr(q + 33) for q in quals)
        records.append(BamRecord(name, flag, seq, qual, _tags(body[cursor:])))
    return BamPrefix(
        sort_order=sort_order,
        references=n_ref,
        programs=programs,
        bamtofastq=bamtofastq,
        records=records,
        reached_end=complete and clean and pos >= len(raw),
    )


def _original(record: BamRecord) -> tuple[str, str | None]:
    """The read as sequenced: an aligned reverse-strand read is stored reverse-complemented."""
    if not record.reverse:
        return record.seq, record.qual
    return record.seq.translate(_COMPLEMENT)[::-1], record.qual[::-1] if record.qual else None


def _rebuild(record: BamRecord, spec: list[tuple[str, str]]) -> Record | None:
    """One original read from a record, as ``bamtofastq`` would assemble it."""
    seqs: list[str] = []
    quals: list[str] = []
    for seq_tag, qual_tag in spec:
        if seq_tag == "SEQ":
            seq, qual = _original(record)
            seqs.append(seq)
            quals.append(qual or "")
            continue
        value = record.tags.get(seq_tag)
        if not isinstance(value, str):
            return None
        seqs.append(value)
        quality = record.tags.get(qual_tag)
        quals.append(quality if isinstance(quality, str) else "")
    seq = "".join(seqs)
    qual = "".join(quals)
    return Record(record.name, seq, qual if len(qual) == len(seq) else None)


def read_bam(
    data: bytes, source: str, *, max_reads: int, complete: bool = False
) -> list[FileSample]:
    """A BAM's start as the reads it holds: one sample per original read, where it says.

    With ``10x_bam_to_fastq`` lines, each read they describe is rebuilt and
    returned as a derived sample (R1 from ``CR``+``UR``, R2 from the record).
    Without them, the records' own sequences are one sample, split by
    ``0x40``/``0x80`` where the BAM is paired.
    """
    bam = parse_bam(data, max_records=max_reads * 2, complete=complete)
    facts: dict[str, object] = {
        "sort_order": bam.sort_order,
        "references": bam.references,
        "programs": bam.programs,
        "bamtofastq": sorted(bam.bamtofastq),
        "tags": {tag: sum(tag in r.tags for r in bam.records) for tag in TEN_X_TAGS},
        "paired": {
            "first": sum(bool(r.flag & 0x40) for r in bam.records),
            "second": sum(bool(r.flag & 0x80) for r in bam.records),
            "unpaired": sum(not r.flag & 0xC0 for r in bam.records),
        },
        "records": len(bam.records),
    }
    if bam.sort_order == "coordinate":
        # The first records of a coordinate-sorted BAM are the start of the
        # first reference -- chr1 -- not a sample of the library. Fine for
        # barcodes and read structure; noted for anything that is not.
        facts["first_reads_are_chr1"] = True
    samples: list[FileSample] = []
    specs = dict(bam.bamtofastq)
    if not specs and (barcode := _barcode_spec(bam.records)) is not None:
        specs = {"R1": barcode, "R2": [("SEQ", "QUAL")]}
        facts["rebuilt_from_tags"] = True
    if specs:
        for read, spec in sorted(specs.items()):
            rebuilt = [r for r in (_rebuild(rec, spec) for rec in bam.records) if r is not None]
            samples.append(
                FileSample(
                    source=source,
                    format="bam",
                    role_hint=read,
                    records=rebuilt[:max_reads],
                    reached_end=bam.reached_end,
                    derived=any(tag != "SEQ" for tag, _ in spec),
                    facts=facts,
                )
            )
        return samples
    by_mate: dict[str | None, list[Record]] = {}
    for rec in bam.records:
        mate = "R1" if rec.flag & 0x40 else "R2" if rec.flag & 0x80 else None
        seq, qual = _original(rec)
        by_mate.setdefault(mate, []).append(Record(rec.name, seq, qual))
    for mate, records in sorted(by_mate.items(), key=lambda kv: kv[0] or ""):
        samples.append(
            FileSample(
                source=source,
                format="bam",
                role_hint=mate,
                records=records[:max_reads],
                reached_end=bam.reached_end,
                facts=facts,
            )
        )
    return samples
