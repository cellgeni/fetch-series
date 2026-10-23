"""Choose the classifier's strand and tail thresholds on the design half, against the pipeline's labels.

Usage::

    uv run python scripts/tune_classifier.py data/benchmarks/inspect/starsolo-2026-10-22 [--draw design]

Every copy the design draw read was recorded with its evidence
(``.cache/starsolo-compare/<run>.json``), so each threshold setting is scored by
deciding every recorded call again with ``classify.decide`` -- no read is made.
The labels are the reprocessing pipeline's, made without this method: the
whitelist it chose (``WL``) and the strand its test mapping found (``Strand``;
Forward is 3', Reverse is 5').

Per setting, over samples, scored on the copy each sample's mapping block would
come from (the best-called gene-expression copy):

* ambiguous: a v2-list call left between 3' v2 and 5' v1/v2;
* wrong layout: a call whose strand class contradicts the pipeline's strand;
* wrong list: a call whose list is not the pipeline's;
* soloStrand given, right, wrong;
* feature-barcode calls on samples the pipeline mapped to genes at 30% or more.

A setting is admissible only with no wrong layout, list or strand. Of those, the
one with least ambiguity is chosen, ties going to the setting nearest the
current one. Prints the table and writes ``tune-<draw>.csv``.
"""

from __future__ import annotations

import argparse
import copy
import csv
import itertools
import json
from collections import Counter
from pathlib import Path
from typing import Any

from fetch_series.inspect import classify as C

REPORTS = Path(".cache/starsolo-compare")
SOLO = {"sense": "Forward", "antisense": "Reverse"}
LEVEL = {"HIGH": 0, "MEDIUM": 1, "LOW": 2, "ambiguous": 3, "none": 4}


def settings() -> list[dict[str, Any]]:
    grid = []
    for tolerated, mouse, tail, tso, votes in itertools.product(
        (
            frozenset({"housekeeping_human"}),
            frozenset({"housekeeping_human", "poly_a"}),
            frozenset({"housekeeping_human", "poly_a", "chrM"}),
        ),
        (False, True),
        (0.5, 0.4, 0.3, 0.25, 0.2),
        (1.01, 0.3, 0.2, 0.1),
        (2, 1),
    ):
        grid.append(
            {
                "UNDECIDED_TOLERATED": tolerated,
                "HOUSEKEEPING_HUMAN_ONLY": mouse,
                "TAIL_SHARE": tail,
                "TSO_ARTEFACT_SHARE": tso,
                "MIN_STRAND_VOTES": votes,
            }
        )
    return grid


def apply(setting: dict[str, Any]) -> None:
    for name, value in setting.items():
        setattr(C, name, value)


def copies(out: Path, draw: str) -> list[tuple[dict[str, str], list[dict[str, Any]]]]:
    """Each read sample's truth row and its copies' recorded calls (with the TSO share added)."""
    truth = {
        (r["dataset"], r["sample"]): r for r in csv.DictReader((out / "truth.csv").open())
    }
    runs: dict[tuple[str, str], str] = {}
    for path in sorted(out.glob(f"calls-{draw}-*of*.csv")):
        for row in csv.DictReader(path.open()):
            if row["run"] and not row["error"]:
                runs[(row["dataset"], row["sample"])] = row["run"]
    found = []
    for key, run in sorted(runs.items()):
        report_path = REPORTS / f"{run}.json"
        if not report_path.exists():
            continue
        report = json.loads(report_path.read_text())
        calls = []
        for source in report["sources"]:
            evidence = copy.deepcopy((source["call"] or {}).get("evidence") or {})
            if not evidence.get("files") or "technology" in evidence:
                continue
            library = source.get("library") or {}
            if "tso_share" in library and evidence.get("library") is not None:
                evidence["library"].setdefault("tso_share", library["tso_share"])
            calls.append({"source": source["source"], "evidence": evidence, "fit": source["fit"]})
        found.append((truth[key], calls))
    return found


def score(samples: list[tuple[dict[str, str], list[dict[str, Any]]]]) -> Counter[str]:
    tally: Counter[str] = Counter()
    for label, calls in samples:
        decided = [(C.decide(copy.deepcopy(c["evidence"])), c) for c in calls]
        if not decided:
            continue
        call, _ = min(
            decided, key=lambda d: (LEVEL.get(d[0].level, 5), d[0].layout is None, d[1]["source"])
        )
        tally["samples"] += 1
        if call.library_type == "feature barcode" and float(label["genefull_unique"] or 0) >= 0.3:
            tally["feature barcode on a mapped GEX sample"] += 1
        if call.barcode_list and label["barcode_list"]:
            tally["list right" if call.barcode_list == label["barcode_list"] else "list WRONG"] += 1
        if call.level == "ambiguous" and set(call.candidates) >= {"10x_3p_v2", "10x_5p_v1_v2"}:
            tally["ambiguous 3' v2 / 5'"] += 1
        layout = C.BY_ID.get(call.layout or "")
        if layout is not None and label["strand"] in ("Forward", "Reverse"):
            expected = SOLO.get(str(layout.strand))
            if expected:
                tally["layout right" if expected == label["strand"] else "layout WRONG"] += 1
        strand = ((call.evidence or {}).get("strand") or {}).get("strand")
        if call.layout and strand in SOLO and label["strand"] in ("Forward", "Reverse"):
            tally["soloStrand right" if SOLO[strand] == label["strand"] else "soloStrand WRONG"] += 1
        elif call.layout:
            tally["soloStrand omitted"] += 1
    return tally


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("out", type=Path)
    parser.add_argument("--draw", default="design")
    args = parser.parse_args()
    samples = copies(args.out, args.draw)
    current = {name: getattr(C, name) for name in settings()[0]}
    rows = []
    for setting in settings():
        apply(setting)
        tally = score(samples)
        rows.append({**{k: (sorted(v) if isinstance(v, frozenset) else v) for k, v in setting.items()}, **tally})
    apply(current)
    columns = sorted({k for r in rows for k in r}, key=lambda k: (k not in current, k))
    with (args.out / f"tune-{args.draw}.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, columns, restval=0)
        writer.writeheader()
        writer.writerows(rows)
    admissible = [
        r for r in rows
        if not r.get("list WRONG") and not r.get("layout WRONG") and not r.get("soloStrand WRONG")
    ]
    print(f"{len(samples)} samples, {len(rows)} settings, {len(admissible)} admissible")
    for r in sorted(admissible, key=lambda r: (r.get("ambiguous 3' v2 / 5'", 0), r.get("feature barcode on a mapped GEX sample", 0)))[:8]:
        print(r)
    baseline = next(
        r
        for r in rows
        if r["TAIL_SHARE"] == 0.5
        and r["TSO_ARTEFACT_SHARE"] == 1.01
        and not r["HOUSEKEEPING_HUMAN_ONLY"]
        and r["UNDECIDED_TOLERATED"] == ["housekeeping_human"]
        and r["MIN_STRAND_VOTES"] == 2
    )
    print("current:", baseline)


if __name__ == "__main__":
    main()
