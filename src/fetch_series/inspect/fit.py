"""Whether a source's files are fit for a 10x gene-expression run: a requirement, versioned.

This is not evidence about a route and never ranks one. It is the file layer's
requirement -- can STARsolo use these reads -- applied to what the reads showed,
and it is versioned so a call cited later can be reproduced from its rule.

**Proven unusable** needs positive evidence, one of:

* no read of 50 bases or more: there is no cDNA read to align;
* no read of 26 bases or more whose first 16 bases are a listed barcode above
  background: there is no barcode read.

Geometry alone never proves absence, and a library that is not gene expression
is ``None``: whether a VDJ or feature-barcode library is usable depends on a
pipeline this rule does not describe. Undetermined -- a reader unavailable, a
fetch failed, a call too weak -- is ``None`` too, and has no effect on a
recommendation.

``fit-2`` adds three conditions, each found by the 2026-10-20 content survey
reading files that ``fit-1`` judged wrongly:

* **An SRA object's first spots must account for the run.** DRR878211's object
  starts with 814 million spots' worth of 26 + 8-base barcode spots and holds
  its 98-base cDNA reads in spots of their own further in: a mean of 66 bases a
  spot against the sample's 34. A prefix that is shorter than the run's mean
  spot is not the run, so it refuses nothing. An object holds every spot of a
  run, which is why the mean (``run_spot``, bases over spots from the run's
  metadata) can be held against it. A derived fastq cannot be held to it:
  ENA's lone fastq of SRR29534765 is short of the mean *because* it omits a
  read, which is the defect being refused.
* **A read of 11 to 25 bases may be a barcode no list holds.** SRR9105023 is
  10x 3' v1: 98-base cDNA, a 14-base barcode read and a 10-base UMI read. No
  v1 list is registered, so "no listed barcode" says nothing about it.
* **The barcode read and the cDNA read must be mates.** DDBJ's fastqs of
  DRR878211 pair the 26-base barcode read with an 8-base index read, and put
  the 98-base reads in a third file of unpaired spots. Their spot names agree
  for none of the first reads, so the set was not fit, whatever its reads are.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

from fetch_series.inspect.checks import BARCODE_READ_MIN, CDNA_READ_MIN, concordance
from fetch_series.inspect.classify import LOW_NEAR, NO_LISTS, Call
from fetch_series.inspect.model import FileSample

RULE = "fit-2"

# A sample index is 8 or 10 bases; anything between that and a barcode read
# could be a barcode of a layout no list is registered for (3' v1's is 14).
INDEX_READ_MAX = 10
# A spot's bases are the sum of its reads' modes; the run's mean may round.
SPOT_SLACK = 1.0
# Mates share a spot name; a few renamed or filtered reads do not unpair a set.
MATES_MIN = 0.9


def _mode(sample: FileSample) -> int:
    return Counter(sample.lengths).most_common(1)[0][0] if sample.records else 0


def _short_of_run(read: list[FileSample], run_spot: float | None) -> str | None:
    """Why an SRA object's first spots may not be its run, or ``None`` if they are."""
    if not read or any(s.format != "sra" for s in read):
        return None
    if run_spot is None:
        return "an SRA object's first spots, and the run's mean spot length unknown"
    spot = sum(_mode(s) for s in read)
    if spot < run_spot - SPOT_SLACK:
        return (
            f"the first spots are {spot} bases and the run's mean spot is {run_spot:g}: "
            "the rest of the object holds reads the sample never reached"
        )
    return None


def _mates(read: list[FileSample], barcode: list[int]) -> float | None:
    """How far the barcode read and the best cDNA read share spot names."""
    best: float | None = None
    for b in barcode:
        for i, sample in enumerate(read):
            if i == b or _mode(sample) < CDNA_READ_MIN:
                continue
            # Reads rebuilt from one BAM record, or split from one SRA spot,
            # are mates by construction.
            if read[b].source == sample.source:
                return 1.0
            agree = concordance(read[b], sample)
            if agree is not None and (best is None or agree > best):
                best = agree
    return best


def fit(samples: list[FileSample], call: Call, *, run_spot: float | None = None) -> dict[str, Any]:
    """``fit`` True, False or None, the reason, and what it was judged on, under :data:`RULE`.

    ``run_spot`` is the run's mean bases per spot, from its metadata; it is what
    an SRA object's first spots are held against.
    """
    read = [s for s in samples if s.records]
    inputs: dict[str, Any] = {"run_spot": run_spot}

    def verdict(value: bool | None, reason: str) -> dict[str, Any]:
        return {"fit": value, "rule": RULE, "reason": reason, **inputs}

    if not read:
        return verdict(None, "nothing could be read")
    short = _short_of_run(read, run_spot)
    if not any(_mode(s) >= CDNA_READ_MIN for s in read):
        if short:
            return verdict(None, short)
        return verdict(False, f"no read of {CDNA_READ_MIN} bases or more")
    hits = call.evidence.get("hits") or []
    listed = [
        h
        for h in hits
        if h["near"] >= LOW_NEAR
        and h["exact"] >= 10 * h["background"]
        and _mode(read[h["read"]]) >= BARCODE_READ_MIN
    ]
    if not listed:
        if (
            not any(_mode(s) >= BARCODE_READ_MIN for s in read)
            or call.hint == NO_LISTS
            or call.hint == "nothing could be read"
        ):
            return verdict(None, call.hint or "no barcode list to check against")
        unlisted = sorted({_mode(s) for s in read if INDEX_READ_MAX < _mode(s) < BARCODE_READ_MIN})
        if unlisted:
            return verdict(
                None,
                f"a {unlisted[0]}-base read: a barcode read of a layout no list is "
                "registered for, as 10x 3' v1's is",
            )
        if short:
            return verdict(None, short)
        return verdict(False, "no read whose first 16 bases are a listed barcode")
    if call.library_type not in (None, "GEX"):
        return verdict(None, f"a {call.library_type} library: not gene expression")
    if not (call.level in ("HIGH", "MEDIUM") and call.layout):
        return verdict(None, f"listed barcodes, call {call.level}")
    mates = _mates(read, sorted({h["read"] for h in listed}))
    inputs["mates"] = mates
    if mates is not None and mates < MATES_MIN:
        return verdict(
            None,
            f"the cDNA reads are not the barcode reads' mates: spot names agree for "
            f"{mates:.0%} of the first reads",
        )
    return verdict(True, f"{call.layout} at {call.level}")


__all__ = ["RULE", "fit"]
