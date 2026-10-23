"""Set ``fetch inspect``'s calls beside what the pipeline's STARsolo runs used, per sample.

Usage::

    uv run python scripts/inspect_vs_starsolo_report.py data/benchmarks/inspect/starsolo-<date> [--split heldout]

Per sample, the copy compared is the best-called source of the run read: fit
before undetermined before unfit, then HIGH before MEDIUM before anything else.
Prints markdown and writes ``summary-<split>.json``. A sample whose run could
not be resolved or read is counted apart, never as a disagreement.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import Counter
from pathlib import Path
from typing import Any

LEVEL = {"HIGH": 0, "MEDIUM": 1, "LOW": 2, "ambiguous": 3, "none": 4}
FIT = {"True": 0, "": 1, "None": 1, "False": 2}
SOLO = {"Forward": "Forward", "Reverse": "Reverse"}


def best(rows: list[dict[str, str]]) -> dict[str, str]:
    return min(rows, key=lambda r: (FIT.get(r["fit"], 1), LEVEL.get(r["level"], 5), r["source"]))


def _float(value: str | None) -> float | None:
    try:
        return float(value) if value else None
    except ValueError:
        return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("out", type=Path)
    parser.add_argument("--split", default="heldout")
    args = parser.parse_args()
    with (args.out / "truth.csv").open() as handle:
        truth = {(r["dataset"], r["sample"]): r for r in csv.DictReader(handle)}
    calls: dict[tuple[str, str], list[dict[str, str]]] = {}
    for path in sorted(args.out.glob(f"calls-{args.split}-[0-9]*of*.csv")):
        with path.open() as handle:
            for row in csv.DictReader(handle):
                calls.setdefault((row["dataset"], row["sample"]), []).append(row)
    with (args.out / f"draw-{args.split}.csv").open() as handle:
        drawn = [(r["dataset"], r["sample"]) for r in csv.DictReader(handle)]

    unread: Counter[str] = Counter()
    lists: Counter[str] = Counter()
    strands: Counter[str] = Counter()
    fits: Counter[str] = Counter()
    kinds: Counter[str] = Counter()
    rate_pairs: list[tuple[float, float]] = []
    lists_off: list[str] = []
    strands_off: list[str] = []
    unfit: list[str] = []
    for key in drawn:
        rows = [r for r in calls.get(key, []) if not r["error"]]
        if not rows:
            errors = [r["error"] for r in calls.get(key, [])]
            unread[errors[0][:40] if errors else "not read"] += 1
            continue
        record = truth[key]
        chosen = best(rows)
        kinds[chosen["kind"]] += 1
        # The list.
        if not record["barcode_list"]:
            lists[f"pipeline named no list ({record['wl']})"] += 1
        elif not chosen["barcode_list"] or chosen["level"] not in (
            "HIGH",
            "MEDIUM",
            "ambiguous",
            "LOW",
        ):
            lists["no call from the reads"] += 1
        elif chosen["barcode_list"] == record["barcode_list"]:
            lists["same list"] += 1
        else:
            lists["different list"] += 1
            lists_off.append(
                f"{key[1]} {chosen['run']}: pipeline {record['wl']}, reads {chosen['barcode_list']} ({chosen['level']})"
            )
        # The strand: the pipeline's test mapping, against the measured strand.
        expected = SOLO.get(record["strand"])
        ours = chosen["solo_strand"]
        if expected is None:
            strands[f"pipeline strand {record['strand'] or 'missing'}"] += 1
        elif not ours:
            strands["no soloStrand from the reads"] += 1
        elif ours == expected:
            strands[f"same ({expected})"] += 1
        else:
            strands["different"] += 1
            strands_off.append(
                f"{key[1]} {chosen['run']}: pipeline {expected}, reads {ours} ({chosen['layout']} {chosen['level']})"
            )
        # Fit: every one of these samples mapped.
        verdicts = {r["fit"] for r in rows}
        if "True" in verdicts:
            fits["a fit copy"] += 1
        elif verdicts <= {"False"}:
            fits["every copy unfit"] += 1
            unfit.append(f"{key[1]} {chosen['run']}: {chosen['reason']}")
        else:
            fits[
                "undetermined, none unfit" if "False" not in verdicts else "undetermined or unfit"
            ] += 1
        # The listed-barcode rate against STARsolo's valid barcodes.
        ours_rate = _float(chosen["within_one"])
        theirs = _float(record["valid_barcodes"]) or _float(record["good_bc"])
        if (
            ours_rate is not None
            and theirs is not None
            and chosen["barcode_list"] == record["barcode_list"]
        ):
            rate_pairs.append((ours_rate, theirs))

    diffs = [o - t for o, t in rate_pairs]
    summary: dict[str, Any] = {
        "split": args.split,
        "drawn": len(drawn),
        "unread": dict(unread),
        "compared_kind": dict(kinds),
        "lists": dict(lists),
        "strands": dict(strands),
        "fit": dict(fits),
        "listed_rate_vs_valid_barcodes": {
            "pairs": len(rate_pairs),
            "median_difference": round(statistics.median(diffs), 4) if diffs else None,
            "within_0.05": sum(abs(d) <= 0.05 for d in diffs),
            "correlation": round(statistics.correlation(*zip(*rate_pairs, strict=True)), 3)
            if len(rate_pairs) > 2
            else None,
        },
        "lists_off": lists_off,
        "strands_off": strands_off,
        "every_copy_unfit": unfit,
    }
    (args.out / f"summary-{args.split}.json").write_text(json.dumps(summary, indent=1) + "\n")
    print(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
