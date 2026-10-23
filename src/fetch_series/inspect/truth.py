"""What the reprocessing pipeline's own STARsolo runs used and achieved, per sample.

The read classifier's mapping block says what STARsolo *should* be given. The
pipeline (cellgeni/nf-reprocessing-public-10x, ``starsolo_10x_auto.sh``) has
already mapped thousands of public samples, choosing the whitelist by matching
200,000 barcodes against each list and the strand by mapping a test sample both
ways. Its choices, and how the runs came out, are the record a mapping block is
checked against.

The bundle is the pipeline's iRODS collections (``/archive/cellgeni/datasets``)
with only the small files kept, one directory per dataset:

* ``<dataset>/*solo_qc*.tsv`` -- one row per sample: ``WL`` (``v1``, ``v2``,
  ``v3``, ``arc``, ...), ``Paired``, ``Strand``, ``Good_BC`` and the mapping
  rates;
* ``<dataset>/<sample>/output/GeneFull/Summary.csv`` -- STARsolo's own summary:
  valid barcodes, sequencing saturation, Q30 in CB+UMI.

A STARsolo output is per **sample** -- every run of it mapped together -- so
the comparison is per sample too. Controlled-access datasets (EGA, HRA) are
counted and set aside: their reads cannot be read, and their sample names are
not written to the committed record.

The split into design and held-out halves is by dataset and seeded, as the
2026-10-19 classifier benchmark's was: a threshold is set on the design half,
and the held-out half is compared once, after it is frozen.
"""

from __future__ import annotations

import csv
import hashlib
from collections.abc import Iterator
from dataclasses import asdict, dataclass, fields
from pathlib import Path

SEED = "20261022"
# The pipeline's WL column names its list files by version; these are the
# register's ids for them (``lists.tsv``).
WL_LIST = {
    "v1": "tenx_v1",
    "v2": "tenx_v2",
    "v3": "tenx_v3",
    "v4": "tenx_v4_3p",
    "v4-3p": "tenx_v4_3p",
    "v4-5p": "tenx_v3_5p",
    "arc": "tenx_arc_gex",
}
CONTROLLED = ("EGAD", "HRA")


@dataclass(frozen=True, slots=True)
class Sample:
    """One sample's STARsolo run, as the pipeline recorded it."""

    dataset: str
    sample: str
    wl: str
    barcode_list: str
    species: str
    paired: str
    strand: str
    good_bc: float | None
    reads: int | None
    cells: int | None
    mapped: float | None
    genefull_unique: float | None
    valid_barcodes: float | None
    saturation: float | None
    q30_cb_umi: float | None
    split: str
    public: bool


def _float(value: str | None) -> float | None:
    try:
        return float(value) if value not in (None, "", "NA") else None
    except ValueError:
        return None


def _int(value: str | None) -> int | None:
    number = _float(value)
    return int(number) if number is not None else None


def split_of(dataset: str) -> str:
    """``design`` or ``heldout``, by a seeded hash of the dataset: stable, and never by sample."""
    digest = hashlib.sha256(f"{SEED}:{dataset}".encode()).digest()
    return "design" if digest[0] % 2 == 0 else "heldout"


def summary(path: Path) -> dict[str, str]:
    """STARsolo's ``Summary.csv``: one ``name,value`` a line."""
    if not path.exists():
        return {}
    found = {}
    for line in path.read_text().splitlines():
        name, _, value = line.rpartition(",")
        if name:
            found[name] = value
    return found


def read_bundle(bundle: Path) -> Iterator[Sample]:
    """Every sample with a ``solo_qc`` row, once; the first file's row where two repeat it."""
    root = bundle / "irods" if (bundle / "irods").is_dir() else bundle
    for directory in sorted(p for p in root.iterdir() if p.is_dir()):
        dataset = directory.name
        seen: set[str] = set()
        for table in sorted(directory.glob("*solo_qc*.tsv")):
            with table.open(newline="") as handle:
                for row in csv.DictReader(handle, delimiter="\t"):
                    sample = (row.get("Sample") or "").strip()
                    if not sample or sample in seen:
                        continue
                    seen.add(sample)
                    solo = summary(directory / sample / "output" / "GeneFull" / "Summary.csv")
                    wl = (row.get("WL") or "").strip()
                    yield Sample(
                        dataset=dataset,
                        sample=sample,
                        wl=wl,
                        barcode_list=WL_LIST.get(wl, ""),
                        species=row.get("Species", ""),
                        paired=row.get("Paired", ""),
                        strand=row.get("Strand", ""),
                        good_bc=_float(row.get("Good_BC")),
                        reads=_int(row.get("Rd_all")),
                        cells=_int(row.get("Cells")),
                        mapped=_float(row.get("all_u+m")),
                        genefull_unique=_float(row.get("full_u")),
                        valid_barcodes=_float(solo.get("Reads With Valid Barcodes")),
                        saturation=_float(solo.get("Sequencing Saturation")),
                        q30_cb_umi=_float(solo.get("Q30 Bases in CB+UMI")),
                        split=split_of(dataset),
                        public=not dataset.startswith(CONTROLLED),
                    )


COLUMNS = tuple(f.name for f in fields(Sample))


def write(samples: list[Sample], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, COLUMNS)
        writer.writeheader()
        for sample in samples:
            writer.writerow(asdict(sample))


def load(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


__all__ = ["COLUMNS", "WL_LIST", "Sample", "load", "read_bundle", "split_of", "summary", "write"]
