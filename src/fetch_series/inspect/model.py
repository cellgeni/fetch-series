"""What a reader returns: records, and what is known about how they were read."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class Record:
    """One read. ``qual`` is ``None`` where the file holds no quality at all."""

    name: str
    seq: str
    qual: str | None


@dataclass(slots=True)
class FileSample:
    """The reads a reader got from the start of one file.

    Args:
        source: the link or path the bytes came from, as published.
        format: ``fastq``, ``bam``, ``cram`` or ``sra``.
        role_hint: what the file's name says it is -- ``R1``, ``I1`` -- before
            any read is looked at; ``None`` if the name says nothing.
        records: complete records only. A last record cut off by the prefix is
            dropped, never kept short: a truncated read would look like a short
            one, and read length is most of what is being measured.
        reached_end: the whole file was read, so the count is the file's own.
        suffixes: how many read names ended ``/1``, ``/2``, ``/3``. ENA names
            the one read it kept of a paired 10x library ``/3`` when it dropped
            the barcode read, which is how SRR29534765 says which read survived.
        derived: these reads were rebuilt, not read -- a barcode read
            reassembled from a BAM's tags as ``bamtofastq`` would.
        facts: format-specific facts (a BAM's sort order, its tags) that are
            not reads.
    """

    source: str
    format: str
    role_hint: str | None = None
    records: list[Record] = field(default_factory=list)
    reached_end: bool = False
    suffixes: dict[str, int] = field(default_factory=dict)
    derived: bool = False
    facts: dict[str, object] = field(default_factory=dict)

    @property
    def lengths(self) -> list[int]:
        return [len(r.seq) for r in self.records]
