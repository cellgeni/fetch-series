"""The ``links.tsv`` the reprocessing pipeline consumes today.

This is the M3 parity contract. ``fetch10xmeta`` emits

    run <tab> species <tab> url[;url...] <tab> type <tab> sample

and everything downstream -- ``WGET10X`` first -- reads that shape. Emitting it
byte-for-byte is what lets the bash module be replaced without touching the rest
of the pipeline, so the incumbent's *ordering* is reproduced here deliberately,
including the places where this project's own file layer would choose
differently.

Where they differ is recorded rather than quietly resolved:

``ENAFQ`` requires ENA to offer both ``_1.fastq.gz`` and ``_2.fastq.gz``. That is
a name test, and it rejects a genuinely single-end library along with a truncated
paired one. ``fetch_series.files`` reaches the same verdict for ERP129702 from
``library_layout``, which is the fact rather than a proxy for it -- but the two
agree on every case the incumbent's snapshots cover, and parity comes first.

``ORIFQ`` from the submitted columns applies only when the submission carries no
BAM. A 10x BAM keeps the original reads including the barcode read, so it beats
a submitter fastq that may not; the incumbent encodes that as an ordering and
this module keeps it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from fetch_series.entities import MISSING
from fetch_series.files import FileKind, FileRecord, FileSet

LINKS_COLUMNS = ("run", "species", "url", "type", "sample")

# The four types WGET10X dispatches on. ORIFQ and ENAFQ are both fastq and are
# fetched the same way; the distinction records provenance, which matters when a
# download turns out to be wrong.
ENA_FASTQ = "run->file:ena_fastq"
ENA_SUBMITTED = "run->file:ena_submitted"
ENA_SRA = "run->file:ena_sra"
SDL = "run->file:sdl"
AE_SDRF = "ae_experiment->file:sdrf"

# ENA names the reads it serves itself `_1`/`_2`. The incumbent requires both.
MATE_1 = re.compile(r"_1\.fastq\.gz$")
MATE_2 = re.compile(r"_2\.fastq\.gz$")


@dataclass(frozen=True, slots=True)
class LinkRow:
    run: str
    species: str
    urls: tuple[str, ...]
    type: str
    sample: str

    def as_row(self) -> tuple[str, ...]:
        return (
            self.run,
            self.species or MISSING,
            ";".join(self.urls) or MISSING,
            self.type or MISSING,
            self.sample or MISSING,
        )


def _by_source(fileset: FileSet, source: str, kind: FileKind | None = None) -> list[FileRecord]:
    return [
        record
        for record in fileset.records
        if record.source == source and (kind is None or record.kind is kind)
    ]


def offer_for(fileset: FileSet) -> tuple[str, tuple[str, ...]]:
    """The incumbent's type and URL list for one run's file set.

    The nine-step chain from ``parse_metadata.sh``, in order. Steps 8 and 9 --
    the SRA table's archive URL and a ``srapath`` shell-out -- have no
    equivalent here: both ask the SRA toolkit for a signed, rotating link, which
    is a download-time concern rather than a resolution result, and the
    incumbent's own test suite excludes those URLs from its snapshots for that
    reason.
    """
    # 1: the submitter's own fastq, as named in an ArrayExpress SDRF.
    if sdrf := _by_source(fileset, AE_SDRF, FileKind.FASTQ):
        return "ORIFQ", tuple(r.url for r in sdrf)

    # 2: ENA's derived fastqs, but only as a complete _1/_2 pair.
    ena_fastq = _by_source(fileset, ENA_FASTQ, FileKind.FASTQ)
    names = [r.url for r in ena_fastq]
    if any(MATE_1.search(n) for n in names) and any(MATE_2.search(n) for n in names):
        return "ENAFQ", tuple(names)

    # 3 and 4: the submitter's deposit. A BAM in it outranks a fastq beside it.
    submitted = _by_source(fileset, ENA_SUBMITTED)
    bams = [r for r in submitted if r.kind is FileKind.BAM]
    fastqs = [r for r in submitted if r.kind is FileKind.FASTQ]
    if fastqs and not bams:
        return "ORIFQ", tuple(r.url for r in fastqs)
    if bams:
        return "BAM", tuple(r.url for r in bams)

    # 5: a BAM from SDL, free and not needing a cold-storage restore.
    sdl_bams = [r for r in _by_source(fileset, SDL, FileKind.BAM) if r.is_free]
    if sdl_bams:
        return "BAM", tuple(r.url for r in sdl_bams)

    # 6: ENA's mirror of the SRA archive object.
    if ena_sra := _by_source(fileset, ENA_SRA):
        return "SRA", tuple(r.url for r in ena_sra)

    # 7: the SRA object from SDL, free only.
    sdl_sra = [r for r in _by_source(fileset, SDL, FileKind.SRA) if not r.pay_required]
    if sdl_sra:
        return "SRA", (sdl_sra[0].url,)

    return "", ()


def links_for(
    filesets: dict[str, FileSet],
    species: dict[str, str],
    samples: dict[str, str],
) -> list[LinkRow]:
    """One row per run, in the incumbent's column order.

    A run with no offer still gets a row. The incumbent's step 9 falls back to
    ``srapath`` and therefore always emits something; dropping the run instead
    would make a series look smaller than it is, which is the failure mode that
    hides missing data rather than reporting it.
    """
    rows = []
    for run, fileset in filesets.items():
        kind, urls = offer_for(fileset)
        rows.append(
            LinkRow(
                run=run,
                species=species.get(run, "UNKNOWN"),
                urls=urls,
                type=kind,
                sample=samples.get(run, "NA"),
            )
        )
    return sorted(rows, key=lambda row: row.run)
