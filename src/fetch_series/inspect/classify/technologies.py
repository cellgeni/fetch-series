"""Other single-cell technologies, as table rows: the constant sequences their barcode reads carry.

This round only **detects and names** them. A copy read as another technology
is never given a 10x layout, a mapping block or ``fit=True``: its barcodes are
not 10x's, and what STARsolo needs for it is not in this table.

The rows (``technologies.tsv``) come from Teichlab/scg_lib_structs at the
commit the barcode lists are pinned to, each citing its page. A row is a set of
constants, each with the window of starts it may sit at; a read carries the
technology if every constant is at one of its starts, exactly. The constants
are 12 to 30 bases where a technology has them; the shorter ones (BD's enhanced
beads, sci-RNA-seq3) are named only because a share of reads far above chance
is required, never a single read.

What cannot be named this way is said, not guessed: Drop-seq, Seq-Well,
CEL-seq2, MARS-seq and Quartz-seq2 read random bases and then poly-T, with no
constant to find, and are ``unknown`` here. Parse Evercode carries microSPLiT's
linkers (measured on SRR24749105), so the two share a row, and that row asks for
the second linker alone: SRR32391187's first linker varies from read to read,
where its 22-base second linker sits at 56 in 64% of reads exactly.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fetch_series.inspect.model import FileSample

TABLE_PATH = Path(__file__).with_name("technologies.tsv")
# Provisional: the share of reads that must carry a row's constants. The
# measured runs carry them in 72-91% of reads; chance puts a 12-base constant
# at a fixed start in one read of 16 million.
TECH_SHARE = 0.5
TECH_SAMPLE = 5_000
MIN_READS = 100


@dataclass(frozen=True, slots=True)
class Constant:
    """A fixed sequence, the starts it may sit at, and the substitutions it tolerates.

    One substitution in ten bases: an exact 30-base linker matched only 54-62%
    of the reads of Parse runs whose linker is plainly there (SRR31555279,
    SRR32654238), because a read of 30 bases carries an error that often. A
    constant under ten bases must match exactly.
    """

    seq: str
    starts: tuple[int, ...]

    @property
    def tolerance(self) -> int:
        return len(self.seq) // 10

    def at(self, read: str) -> bool:
        size = len(self.seq)
        for start in self.starts:
            window = read[start : start + size]
            if window == self.seq:
                return True
            if self.tolerance and len(window) == size:
                misses = 0
                for a, b in zip(window, self.seq, strict=True):
                    if a != b:
                        misses += 1
                        if misses > self.tolerance:
                            break
                else:
                    return True
        return False

    @property
    def end(self) -> int:
        return max(self.starts) + len(self.seq)


@dataclass(frozen=True, slots=True)
class Technology:
    id: str
    name: str
    constants: tuple[Constant, ...]
    source: str
    note: str

    def share(self, seqs: list[str]) -> tuple[float, int] | None:
        end = max(c.end for c in self.constants)
        long_enough = [s for s in seqs if len(s) >= end]
        if len(long_enough) < MIN_READS:
            return None
        hits = sum(all(c.at(s) for c in self.constants) for s in long_enough)
        return hits / len(long_enough), len(long_enough)


def _constant(text: str) -> Constant:
    seq, _, window = text.partition("@")
    low, _, high = window.partition("-")
    return Constant(seq, tuple(range(int(low), int(high or low) + 1)))


def load(path: Path = TABLE_PATH) -> tuple[Technology, ...]:
    with path.open(newline="") as handle:
        return tuple(
            Technology(
                id=row["id"],
                name=row["name"],
                constants=tuple(_constant(c) for c in row["constants"].split(";")),
                source=row["source"],
                note=row["note"],
            )
            for row in csv.DictReader(handle, delimiter="\t")
        )


TECHNOLOGIES = load()
BY_ID = {t.id: t for t in TECHNOLOGIES}


def detect(samples: list[FileSample]) -> dict[str, Any] | None:
    """The first table row whose constants most reads of one file carry, or ``None``."""
    for technology in TECHNOLOGIES:
        for index, sample in enumerate(samples):
            seqs = [r.seq for r in sample.records[:TECH_SAMPLE]]
            measured = technology.share(seqs)
            if measured is not None and measured[0] >= TECH_SHARE:
                return {
                    "id": technology.id,
                    "name": technology.name,
                    "read": index,
                    "share": round(measured[0], 4),
                    "reads": measured[1],
                }
    return None


__all__ = ["BY_ID", "TECHNOLOGIES", "TECH_SHARE", "Technology", "detect", "load"]
