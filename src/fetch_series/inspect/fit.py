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
"""

from __future__ import annotations

from collections import Counter
from typing import Any

from fetch_series.inspect.checks import BARCODE_READ_MIN, CDNA_READ_MIN
from fetch_series.inspect.classify import LOW_NEAR, NO_LISTS, Call
from fetch_series.inspect.model import FileSample

RULE = "fit-1"


def _mode(sample: FileSample) -> int:
    return Counter(sample.lengths).most_common(1)[0][0] if sample.records else 0


def fit(samples: list[FileSample], call: Call) -> dict[str, Any]:
    """``fit`` True, False or None, and the reason, under :data:`RULE`."""
    read = [s for s in samples if s.records]
    if not read:
        return {"fit": None, "rule": RULE, "reason": "nothing could be read"}
    if not any(_mode(s) >= CDNA_READ_MIN for s in read):
        return {"fit": False, "rule": RULE, "reason": f"no read of {CDNA_READ_MIN} bases or more"}
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
            any(_mode(s) >= BARCODE_READ_MIN for s in read)
            and call.hint != NO_LISTS
            and call.hint != "nothing could be read"
        ):
            return {
                "fit": False,
                "rule": RULE,
                "reason": "no read whose first 16 bases are a listed barcode",
            }
        return {
            "fit": None,
            "rule": RULE,
            "reason": call.hint or "no barcode list to check against",
        }
    if call.library_type not in (None, "GEX"):
        return {
            "fit": None,
            "rule": RULE,
            "reason": f"a {call.library_type} library: not gene expression",
        }
    if call.level in ("HIGH", "MEDIUM") and call.layout:
        return {"fit": True, "rule": RULE, "reason": f"{call.layout} at {call.level}"}
    return {"fit": None, "rule": RULE, "reason": f"listed barcodes, call {call.level}"}


__all__ = ["RULE", "fit"]
