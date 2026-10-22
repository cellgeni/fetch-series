"""Whether a source's files are fit for a 10x gene-expression run: a requirement, versioned.

This is not evidence about a route and never ranks one. It is the file layer's
requirement -- can STARsolo use these reads -- applied to what the reads showed,
and it is versioned so a call cited later can be reproduced from its rule.

**Proven unusable** needs positive evidence, one of:

* no read of 30 bases or more besides the barcode read: there is no cDNA read
  to align;
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

``fit-3`` changes one thing, found the same way:

* **A cDNA read is any read of 30 bases or more that is not the barcode read.**
  ``fit-2`` asked for 50, and refused every copy of SRR34778689 -- 28 + 47, a
  3' v3 library whose 47-base reads map. A read that long is not an index read
  (``INDEX_READ_MAX``), and the barcode read is excluded by position, not by
  length, because some are 30 or 31 bases (SRR22434015, SRR15425104).

  Excluding it also corrects ``fit-2``, which let the barcode read count as the
  cDNA read and passed sets that have no other: DRR259182's lone 150-base
  fastq, every read of which starts with a listed barcode, was fit. Such a set
  is unfit only when the barcode read is too short to carry a cDNA read after
  its barcode and UMI; a longer one may be the two reads joined
  (SRR26701885's 107 bases), so it is undetermined, never fit.

A verdict records what it was judged on -- each file's format, source and modal
length, the call, ``run_spot``, and the spot-name agreement of every pair of
files -- so :func:`fit_from_detail` can judge it again under a later rule
without reading a file. Moving to ``fit-2`` meant reading every run of the content
survey again, because ``fit-1`` recorded none of that.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from fetch_series.inspect.checks import BARCODE_READ_MIN, CDNA_READ_MIN, concordance
from fetch_series.inspect.classify import LOW_NEAR, NO_LISTS, Call
from fetch_series.inspect.model import FileSample

RULE = "fit-3"

# The longest 10x gene-expression barcode and UMI: 16 + 12 on v3 and v4.
BARCODE_UMI_MAX = 28
# A sample index is 8 or 10 bases; anything between that and a barcode read
# could be a barcode of a layout no list is registered for (3' v1's is 14).
INDEX_READ_MAX = 10
# A spot's bases are the sum of its reads' modes; the run's mean may round.
SPOT_SLACK = 1.0
# Mates share a spot name; a few renamed or filtered reads do not unpair a set.
MATES_MIN = 0.9


class MissingInput(Exception):
    """A recorded verdict lacks something the current rule judges on: read it again."""


@dataclass(frozen=True, slots=True)
class _Read:
    """What the rule needs of one file that was read: never its reads."""

    source: str
    format: str
    mode: int


# The spot-name agreement of files i and j of a set, i < j.
Agree = Callable[[int, int], float | None]


def _mode(sample: FileSample) -> int:
    return Counter(sample.lengths).most_common(1)[0][0] if sample.records else 0


def _short_of_run(read: list[_Read], run_spot: float | None) -> str | None:
    """Why an SRA object's first spots may not be its run, or ``None`` if they are."""
    if not read or any(r.format != "sra" for r in read):
        return None
    if run_spot is None:
        return "an SRA object's first spots, and the run's mean spot length unknown"
    spot = sum(r.mode for r in read)
    if spot < run_spot - SPOT_SLACK:
        return (
            f"the first spots are {spot} bases and the run's mean spot is {run_spot:g}: "
            "the rest of the object holds reads the sample never reached"
        )
    return None


def _mates(read: list[_Read], barcode: list[int], agree: Agree) -> float | None:
    """How far the barcode read and the best cDNA read share spot names."""
    best: float | None = None
    for b in barcode:
        for i, other in enumerate(read):
            if i == b or other.mode < CDNA_READ_MIN:
                continue
            # Reads rebuilt from one BAM record, or split from one SRA spot,
            # are mates by construction.
            if read[b].source == other.source:
                return 1.0
            share = agree(min(b, i), max(b, i))
            if share is not None and (best is None or share > best):
                best = share
    return best


def _judge(
    read: list[_Read], call: dict[str, Any], run_spot: float | None, agree: Agree
) -> dict[str, Any]:
    inputs: dict[str, Any] = {"run_spot": run_spot, "mates": None}

    def verdict(value: bool | None, reason: str) -> dict[str, Any]:
        return {"fit": value, "rule": RULE, "reason": reason, **inputs}

    if not read:
        return verdict(None, "nothing could be read")
    short = _short_of_run(read, run_spot)
    if not any(r.mode >= CDNA_READ_MIN for r in read):
        if short:
            return verdict(None, short)
        return verdict(False, f"no read of {CDNA_READ_MIN} bases or more")
    hits = (call.get("evidence") or {}).get("hits") or []
    listed = [
        h
        for h in hits
        if h["near"] >= LOW_NEAR
        and h["exact"] >= 10 * h["background"]
        and read[h["read"]].mode >= BARCODE_READ_MIN
    ]
    hint = call.get("hint")
    if not listed:
        if (
            not any(r.mode >= BARCODE_READ_MIN for r in read)
            or hint == NO_LISTS
            or hint == "nothing could be read"
        ):
            return verdict(None, hint or "no barcode list to check against")
        unlisted = sorted({r.mode for r in read if INDEX_READ_MAX < r.mode < BARCODE_READ_MIN})
        if unlisted:
            return verdict(
                None,
                f"a {unlisted[0]}-base read: a barcode read of a layout no list is "
                "registered for, as 10x 3' v1's is",
            )
        if short:
            return verdict(None, short)
        return verdict(False, "no read whose first 16 bases are a listed barcode")
    barcode = sorted({h["read"] for h in listed})
    if not any(r.mode >= CDNA_READ_MIN for i, r in enumerate(read) if i not in barcode):
        longest = max(read[b].mode for b in barcode)
        if longest >= BARCODE_UMI_MAX + CDNA_READ_MIN:
            return verdict(
                None,
                f"no cDNA read besides the {longest}-base barcode read, which has room for "
                "one after its barcode and UMI",
            )
        if short:
            return verdict(None, short)
        return verdict(False, f"no read of {CDNA_READ_MIN} bases or more besides the barcode read")
    library = call.get("library_type")
    if library not in (None, "GEX"):
        return verdict(None, f"a {library} library: not gene expression")
    level, layout = call.get("level"), call.get("layout")
    if not (level in ("HIGH", "MEDIUM") and layout):
        return verdict(None, f"listed barcodes, call {level}")
    mates = _mates(read, barcode, agree)
    inputs["mates"] = mates
    if mates is not None and mates < MATES_MIN:
        return verdict(
            None,
            f"the cDNA reads are not the barcode reads' mates: spot names agree for "
            f"{mates:.0%} of the first reads",
        )
    return verdict(True, f"{layout} at {level}")


def fit(samples: list[FileSample], call: Call, *, run_spot: float | None = None) -> dict[str, Any]:
    """``fit`` True, False or None, the reason, and what it was judged on, under :data:`RULE`.

    ``run_spot`` is the run's mean bases per spot, from its metadata; it is what
    an SRA object's first spots are held against.
    """
    samples = [s for s in samples if s.records]
    read = [_Read(s.source, s.format, _mode(s)) for s in samples]
    return _judge(
        read,
        call.as_detail(),
        run_spot,
        lambda i, j: concordance(samples[i], samples[j]),
    )


def fit_from_detail(detail: dict[str, Any]) -> dict[str, Any]:
    """The verdict the current rule gives a recorded one, from its ``detail`` alone.

    The same judgement as :func:`fit`, on what ``check_set`` and the call
    recorded instead of on the reads. Raises :class:`MissingInput` only when
    the judgement needs something the record lacks: a verdict recorded before
    every pair's agreement was (``fit-2`` and earlier) is judged again unless
    its barcode and cDNA reads came from different files.
    """
    checks = detail.get("checks") or {}
    try:
        read = [
            _Read(f["source"], f["format"], f["length"]["mode"]) for f in checks.get("files", [])
        ]
    except (KeyError, TypeError) as exc:
        raise MissingInput(f"a file's facts lack {exc}") from None
    if "call" not in detail:
        raise MissingInput("no call recorded")
    recorded = checks.get("concordance")

    def agree(i: int, j: int) -> float | None:
        if recorded is None:
            raise MissingInput("spot-name agreement between files not recorded")
        for a, b, share in recorded:
            if (a, b) == (i, j):
                return share  # type: ignore[no-any-return]
        raise MissingInput(f"no spot-name agreement recorded for files {i} and {j}")

    return _judge(read, detail["call"], detail.get("run_spot"), agree)


__all__ = ["RULE", "MissingInput", "fit", "fit_from_detail"]
