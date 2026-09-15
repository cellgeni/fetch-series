"""The file layer: what is actually downloadable for a run, and whether to trust it.

Resolving a run accession is not the deliverable. The deliverable is a set of
files that exist, are complete, and are what the submitter actually deposited.
Those three properties fail independently, and the archives disagree about all
of them.

The motivating case is E-MTAB-8060 against E-MTAB-9221. Both ArrayExpress
studies carry SDRF URIs pointing at the decommissioned pre-BioStudies mirror.
E-MTAB-9221 registers all 40 of its fastq files anyway; E-MTAB-8060 registers
none of its 36, and its real submission is a BAM held elsewhere. Taking the SDRF
at face value silently discarded every file for all 15 of 8060's runs. No
special case catches that -- only asking each candidate source what it holds and
comparing the answers does.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

# Paired-end mates that a 10x pipeline requires by name. The incumbent
# fetch10xmeta matches `_1`/`_2` by regex and drops anything else, so a file set
# that is complete but differently named reads as incomplete.
MATE_SUFFIXES = {"_1": 1, "_2": 2, "_3": 3, "_4": 4, "_I1": 0, "_I2": -1}


class FileKind(StrEnum):
    """What kind of thing a file is, which decides whether it is usable data."""

    FASTQ = "fastq"
    BAM = "bam"
    CRAM = "cram"
    SRA = "sra"
    INDEX = "index"
    OTHER = "other"


# Index files are the trap: a .bai sits in the same listing as its .bam, is
# named almost identically, and is 16 bytes. Counting it as a data file makes a
# single-file submission look like a pair.
INDEX_SUFFIXES = (".bai", ".crai", ".tbi", ".csi", ".idx", ".fai")


def classify(name: str) -> FileKind:
    """Classify a file by name.

    Index suffixes are tested first and unconditionally: `x.bam.bai` contains
    `.bam`, so any test that looks for the data suffix first misclassifies every
    index in the archive.
    """
    lowered = name.lower()
    if lowered.endswith(INDEX_SUFFIXES):
        return FileKind.INDEX
    if ".fastq" in lowered or ".fq" in lowered:
        return FileKind.FASTQ
    if lowered.endswith((".bam", ".ubam")):
        return FileKind.BAM
    if lowered.endswith(".cram"):
        return FileKind.CRAM
    if lowered.endswith(".sra") or "/sra/" in lowered:
        return FileKind.SRA
    return FileKind.OTHER


def mate_of(name: str) -> int | None:
    """Which mate of a pair a file is, from its name, or None if unmarked.

    Returns 0 for an I1 index read and -1 for I2 -- they are real reads carrying
    barcodes, not alignment indexes, and a 10x run needs them, so they must be
    distinguishable from both data mates and from `FileKind.INDEX`.
    """
    stem = name.split("/")[-1]
    for suffix in (".gz", ".bz2", ".zst"):
        stem = stem.removesuffix(suffix)
    for extension in (".fastq", ".fq", ".bam", ".cram", ".sra"):
        stem = stem.removesuffix(extension)
    for suffix, mate in MATE_SUFFIXES.items():
        if stem.endswith(suffix):
            return mate
    return None


@dataclass(frozen=True, slots=True)
class FileRecord:
    """One downloadable file, with everything known about whether to trust it.

    ``md5`` and ``size`` are the archive's own published values, not measured.
    They are what a download can be verified *against*; carrying them is the
    open issue in the reprocessing pipeline, where ENA publishes ``fastq_md5``
    and nothing plumbs it through.
    """

    run: str
    url: str
    name: str
    kind: FileKind
    source: str
    size: int | None = None
    md5: str | None = None
    mate: int | None = None
    pay_required: bool = False
    rehydration_required: bool = False

    @property
    def is_data(self) -> bool:
        """Whether this file carries reads, as opposed to indexing them."""
        return self.kind is not FileKind.INDEX

    @property
    def is_free(self) -> bool:
        return not self.pay_required and not self.rehydration_required

    @property
    def verifiable(self) -> bool:
        """Whether a download of this file can be checked against the archive."""
        return self.md5 is not None


@dataclass(frozen=True, slots=True)
class FileSet:
    """Every file any route offered for one run, with the disagreements intact.

    Deliberately not a single chosen answer. Two routes offering different files
    for the same run is the signal the file layer exists to surface -- collapsing
    it to one pick before the caller sees it is the failure mode that lost all of
    E-MTAB-8060.
    """

    run: str
    records: tuple[FileRecord, ...] = field(default=())

    def __len__(self) -> int:
        return len(self.records)

    def of_kind(self, kind: FileKind) -> tuple[FileRecord, ...]:
        return tuple(r for r in self.records if r.kind is kind)

    def from_source(self, source: str) -> tuple[FileRecord, ...]:
        return tuple(r for r in self.records if r.source == source)

    @property
    def sources(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(r.source for r in self.records))

    @property
    def data_files(self) -> tuple[FileRecord, ...]:
        return tuple(r for r in self.records if r.is_data)

    @property
    def mates(self) -> set[int]:
        """Which mates are present across all sources."""
        return {r.mate for r in self.data_files if r.mate is not None}

    @property
    def is_paired(self) -> bool:
        """Whether both members of a read pair are present.

        Mates 1 and 2 specifically: a run offering `_1` and `_I1` has two files
        and half a pair.
        """
        return {1, 2} <= self.mates

    @property
    def fully_verifiable(self) -> bool:
        data = self.data_files
        return bool(data) and all(r.verifiable for r in data)


def from_ena_row(row: dict[str, Any], column: str, source: str) -> list[FileRecord]:
    """Build records from one of ENA's semicolon-separated file columns.

    ENA packs a run's files into parallel `;`-joined lists -- `fastq_ftp`,
    `fastq_md5`, `fastq_bytes` -- which are only meaningful positionally. A row
    whose lists are different lengths is malformed, and pairing them by index
    anyway would attach one file's checksum to another file. That is worse than
    no checksum, so the mismatched fields are dropped and the files kept.
    """
    urls = [u for u in (row.get(f"{column}_ftp") or "").split(";") if u]
    if not urls:
        return []
    md5s = [m for m in (row.get(f"{column}_md5") or "").split(";") if m]
    sizes = [s for s in (row.get(f"{column}_bytes") or "").split(";") if s]
    aligned = len(md5s) == len(urls)
    sized = len(sizes) == len(urls)

    run = (row.get("run_accession") or "").strip()
    records = []
    for i, url in enumerate(urls):
        name = url.split("/")[-1]
        records.append(
            FileRecord(
                run=run,
                # ENA publishes these as bare host/path, with no scheme.
                url=url if "://" in url else f"https://{url}",
                name=name,
                kind=classify(name),
                source=source,
                md5=md5s[i] if aligned else None,
                size=int(sizes[i]) if sized and sizes[i].isdigit() else None,
                mate=mate_of(name),
            )
        )
    return records


def from_sdl_files(run: str, files: list[dict[str, Any]], source: str) -> list[FileRecord]:
    """Build records from one SDL bundle's file list.

    SDL offers the same bytes from several services, so one file becomes several
    records differing only in URL. They are kept separate: a caller choosing
    between S3 and EBI is choosing between real alternatives, and merging them
    would hide that one of them is the one that costs money.
    """
    records = []
    for entry in files:
        name = entry.get("name") or ""
        kind = classify(name)
        # SDL's own `type` is authoritative where the name carries no suffix --
        # an `.sra` object is usually named for its accession alone.
        if kind is FileKind.OTHER and entry.get("type") == "sra":
            kind = FileKind.SRA
        for location in entry.get("locations") or []:
            if not (link := location.get("link")):
                continue
            records.append(
                FileRecord(
                    run=run,
                    url=link,
                    name=name,
                    kind=kind,
                    source=source,
                    size=entry.get("size"),
                    md5=entry.get("md5"),
                    mate=mate_of(name),
                    pay_required=bool(location.get("payRequired")),
                    rehydration_required=bool(location.get("rehydrationRequired")),
                )
            )
    return records
