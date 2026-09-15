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

    Only fastq files are considered. A `_1` suffix means "mate 1" by convention
    in fastq naming and nothing at all anywhere else: E-MTAB-8060's runs deposit
    `Sample_1.bam`, where the `_1` is part of the submitter's sample name. A BAM
    carries both mates interleaved by construction, so reading a mate number off
    one is not merely unreliable, it is meaningless.
    """
    if classify(name) is not FileKind.FASTQ:
        return None
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
    # What the archive says the library is, which is not always what the archive
    # publishes. ENA declares ERP129702 PAIRED and offers one fastq per run.
    declared_paired: bool | None = None

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
    declared = (row.get("library_layout") or "").strip().upper()
    layout = {"PAIRED": True, "SINGLE": False}.get(declared)
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
                declared_paired=layout,
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


# What a reprocessing pipeline can consume without a conversion step. This is a
# statement about the downstream tool, not a claim about which archive is
# better: STARsolo reads fastq, so a fastq set is preferred over the same data
# as BAM, and an .sra object is last because it must be dumped before it is
# anything at all.
FORMAT_PREFERENCE: tuple[FileKind, ...] = (
    FileKind.FASTQ,
    FileKind.BAM,
    FileKind.CRAM,
    FileKind.SRA,
    FileKind.OTHER,
)


@dataclass(frozen=True, slots=True)
class Candidate:
    """One route's complete offer for a run, scored against what is needed."""

    source: str
    files: tuple[FileRecord, ...]
    kind: FileKind

    @property
    def demonstrably_incomplete(self) -> bool:
        """The archive declares a paired library and publishes an unpaired set.

        This is a fact, not a preference: ENA reports ``library_layout=PAIRED``
        for all 15 runs of ERP129702 (E-MTAB-8060) and publishes a single fastq
        for each, 25.7 GB against a 50.2 GB submitted BAM. A pipeline that takes
        the fastq gets one mate and no warning.

        Only fastq sets can be caught this way. A BAM holds both mates
        interleaved, so a single BAM file for a paired library is exactly right.
        """
        if self.kind is not FileKind.FASTQ:
            return False
        declared = {f.declared_paired for f in self.files}
        return declared == {True} and not self.is_paired

    @property
    def mates(self) -> set[int]:
        return {f.mate for f in self.files if f.mate is not None}

    @property
    def is_paired(self) -> bool:
        return {1, 2} <= self.mates

    @property
    def verifiable(self) -> bool:
        return bool(self.files) and all(f.md5 is not None for f in self.files)

    @property
    def free(self) -> bool:
        return all(f.is_free for f in self.files)

    @property
    def total_bytes(self) -> int | None:
        sizes = [f.size for f in self.files]
        return sum(s for s in sizes if s is not None) if all(s is not None for s in sizes) else None

    def why(self) -> str:
        """The one-line reason a caller can read back."""
        parts = [f"{len(self.files)} {self.kind} file{'s' if len(self.files) != 1 else ''}"]
        parts.append("paired" if self.is_paired else f"mates {sorted(self.mates) or 'unmarked'}")
        parts.append("md5 published" if self.verifiable else "no checksum")
        # The size decides whether a choice is affordable, and the alternatives
        # here routinely differ by 2x -- ERR6039559's BAM is 50.2 GB against a
        # 25.7 GB fastq, and the 25.7 GB one is the incomplete offer.
        if (total := self.total_bytes) is not None:
            parts.append(f"{total / 1e9:.1f} GB" if total >= 1e9 else f"{total / 1e6:.0f} MB")
        if self.demonstrably_incomplete:
            parts.append("INCOMPLETE: archive declares the library paired")
        if not self.free:
            parts.append("retrieval is not free")
        return ", ".join(parts)


@dataclass(frozen=True, slots=True)
class Recommendation:
    """A chosen candidate, the alternatives, and why the choice was made.

    The alternatives are carried, not discarded. A recommendation the caller
    cannot argue with is a recommendation they cannot check, and the whole point
    of the file layer is that the archives disagree in ways worth seeing.
    """

    run: str
    chosen: Candidate | None
    alternatives: tuple[Candidate, ...]
    reason: str

    def explain(self) -> str:
        lines = [f"{self.run}: {self.reason}"]
        if self.chosen:
            lines.append(f"  -> {self.chosen.source}: {self.chosen.why()}")
        for other in self.alternatives:
            lines.append(f"     {other.source}: {other.why()}")
        return "\n".join(lines)


def candidates(fileset: FileSet) -> list[Candidate]:
    """Split a file set into one candidate per (source, format).

    Split by format as well as source because a single source routinely offers
    two incompatible things at once -- SDL hands back a submitted BAM and an
    .sra object for the same run, and they are not one offer.
    """
    grouped: dict[tuple[str, FileKind], list[FileRecord]] = {}
    for record in fileset.data_files:
        grouped.setdefault((record.source, record.kind), []).append(record)
    return [
        Candidate(source=source, kind=kind, files=tuple(files))
        for (source, kind), files in grouped.items()
    ]


def recommend(fileset: FileSet) -> Recommendation:
    """Rank the candidate file sets and say why the winner won.

    Two different kinds of judgement meet here, and they are kept apart on
    purpose. Which *route* to try first is an evidence question, answered by
    ``RouteRegistry.ranked()`` from measured survey results. Which *files* to
    use, once several routes have answered, is a requirements question: the
    downstream pipeline needs a complete, checksummed, free, fastq-shaped set,
    and every term below is one of those requirements. Neither is a preference
    between archives.
    """
    ranked = sorted(
        candidates(fileset),
        key=lambda c: (
            not c.is_paired,  # a half pair is not usable
            # A fastq set the archive's own metadata proves incomplete ranks
            # below every format that is merely inconvenient.
            c.demonstrably_incomplete,
            FORMAT_PREFERENCE.index(c.kind),
            not c.free,
            not c.verifiable,
            -len(c.files),
            c.source,
        ),
    )
    if not ranked:
        return Recommendation(fileset.run, None, (), "no route offered any data file")

    best, rest = ranked[0], tuple(ranked[1:])
    if best.demonstrably_incomplete:
        return Recommendation(
            fileset.run,
            best,
            rest,
            "every offer is incomplete: the archive declares a paired library "
            "and publishes an unpaired file set",
        )
    if not best.is_paired and not rest:
        reason = "only one offer, and it is not a complete read pair"
    elif not best.is_paired:
        reason = "no route offered a complete read pair"
    elif best.kind is FileKind.FASTQ and best.verifiable and best.free:
        reason = "complete fastq pair with published checksums, free to retrieve"
    else:
        missing = []
        if best.kind is not FileKind.FASTQ:
            missing.append(f"{best.kind} needs conversion")
        if not best.verifiable:
            missing.append("no published checksum")
        if not best.free:
            missing.append("retrieval is not free")
        reason = "best available: " + "; ".join(missing)
    return Recommendation(fileset.run, best, rest, reason)
