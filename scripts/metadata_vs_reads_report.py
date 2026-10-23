"""Three instruments for one question: what kit made a sample. Metadata, reads, and the pipeline.

Usage::

    uv run python scripts/metadata_vs_reads_report.py \\
        --metadata data/benchmarks/metadata/chemistry-<date>/calls.csv \\
        --starsolo data/benchmarks/inspect/starsolo-2026-10-22

* **Metadata**: the assay layer's ``chemistry_of`` on ENA protocol fields and GEO
  SOFT text, per sample (``scripts/metadata_vs_reads.py``).
* **Reads**: ``fetch inspect``'s call on the best-called copy of the run read for
  each sample, re-decided under the current classifier from its recorded
  evidence (``classify.decide``; the reports are in ``.cache/starsolo-compare``).
* **Pipeline**: the whitelist the reprocessing pipeline chose and the strand its
  test mapping found (``truth.csv``). Neither was made from metadata, nor by
  this project's reading.

Prints a JSON summary per half (design, held-out) and writes
``summary.json`` beside the metadata calls. Every number on
``docs/benchmarks/metadata-vs-reads-2026-10.md`` comes from it.
"""

from __future__ import annotations

import argparse
import copy
import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any

from fetch_series.assays.chemistry import ChemistryCall
from fetch_series.inspect.agree import with_metadata
from fetch_series.inspect.classify import BY_ID, Call, decide
from fetch_series.inspect.classify.layouts import list_class

REPORTS = Path(".cache/starsolo-compare")
SOLO = {"sense": "Forward", "antisense": "Reverse"}
LEVEL = {"HIGH": 0, "MEDIUM": 1, "LOW": 2, "ambiguous": 3, "none": 4}
PAIR = {"10x_3p_v2", "10x_5p_v1_v2"}


def metadata(path: Path) -> dict[tuple[str, str], dict[str, str]]:
    """The last row per sample: a dataset that failed and was retried appears twice."""
    rows: dict[tuple[str, str], dict[str, str]] = {}
    with path.open() as handle:
        for row in csv.DictReader(handle):
            rows[(row["dataset"], row["sample"])] = row
    return rows


def reads(starsolo: Path) -> dict[tuple[str, str], Call]:
    """The best-called copy of each read sample, decided under the current classifier."""
    runs: dict[tuple[str, str], str] = {}
    for path in sorted(starsolo.glob("calls-*-*of*.csv")):
        with path.open() as handle:
            for row in csv.DictReader(handle):
                if row["run"] and not row["error"]:
                    runs[(row["dataset"], row["sample"])] = row["run"]
    calls: dict[tuple[str, str], Call] = {}
    for key, run in runs.items():
        report = REPORTS / f"{run}.json"
        if not report.exists():
            continue
        made = []
        for source in json.loads(report.read_text())["sources"]:
            evidence = (source["call"] or {}).get("evidence") or {}
            if evidence.get("files") and "technology" not in evidence:
                made.append(decide(copy.deepcopy(evidence)))
        if made:
            calls[key] = min(made, key=lambda c: (LEVEL.get(c.level, 5), c.layout is None))
    return calls


def summarise(
    keys: list[tuple[str, str]],
    meta: dict[tuple[str, str], dict[str, str]],
    truth: dict[tuple[str, str], dict[str, str]],
    calls: dict[tuple[str, str], Call],
) -> dict[str, Any]:
    named: Counter[str] = Counter()
    vs_pipeline: Counter[str] = Counter()
    vs_reads: Counter[str] = Counter()
    tie: Counter[str] = Counter()
    disagreements: list[str] = []
    for key in keys:
        row, label = meta.get(key), truth[key]
        if row is None or row["error"]:
            named["no metadata harvested"] += 1
            continue
        candidates = tuple(c for c in row["candidates"].split("|") if c)
        named[
            "no kit named"
            if not candidates
            else "one kit"
            if len(candidates) == 1
            else "several kits"
        ] += 1
        chem = ChemistryCall(candidates=candidates)
        if len(candidates) == 1 and label["barcode_list"] and candidates[0] in BY_ID:
            same = list_class(candidates[0]) == label["barcode_list"]
            vs_pipeline["list same" if same else "list different"] += 1
            expected = SOLO.get(str(BY_ID[candidates[0]].strand))
            if expected and label["strand"] in ("Forward", "Reverse"):
                vs_pipeline[
                    "strand same" if expected == label["strand"] else "strand different"
                ] += 1
        call = calls.get(key)
        if call is None:
            continue
        vs_reads["samples read"] += 1
        if len(candidates) != 1:
            vs_reads["metadata names no kit, or several"] += 1
        elif call.layout is None:
            vs_reads[
                "reads make no call; the metadata's kit is a candidate"
                if candidates[0] in call.candidates
                else "reads make no call"
            ] += 1
        else:
            agreement = with_metadata(call, chem)
            if agreement.exact:
                vs_reads["the same kit"] += 1
            else:
                kind = "same list, another kit" if agreement.same_class else "another list"
                vs_reads[kind] += 1
                # Who the pipeline sides with: its whitelist, or its strand where
                # the list is shared.
                if agreement.same_class:
                    read_strand = SOLO.get(str(BY_ID[call.layout].strand))
                    meta_strand = SOLO.get(str(BY_ID[candidates[0]].strand))
                    side = (
                        "reads"
                        if label["strand"] == read_strand
                        else "metadata"
                        if label["strand"] == meta_strand
                        else "neither"
                    )
                else:
                    side = (
                        "reads"
                        if label["barcode_list"] == list_class(call.layout)
                        else "metadata"
                        if label["barcode_list"] == list_class(candidates[0])
                        else "neither"
                    )
                vs_reads[f"{kind}: the pipeline sides with the {side}"] += 1
                disagreements.append(
                    f"{key[0]} {key[1]}: metadata {candidates[0]}, reads {call.layout} "
                    f"({call.level}); pipeline {label['wl']} {label['strand']}"
                )
        # The case reads cannot settle: a v2 list, 3' or 5'.
        if call.level == "ambiguous" and set(call.candidates) >= PAIR:
            tie["reads ambiguous 3' v2 / 5'"] += 1
            pick = [c for c in candidates if c in PAIR]
            if len(pick) != 1:
                tie["metadata names neither, or both"] += 1
                continue
            expected = SOLO[str(BY_ID[pick[0]].strand)]
            if label["strand"] not in ("Forward", "Reverse"):
                tie["metadata names one; the pipeline's strand unknown"] += 1
            elif expected == label["strand"]:
                tie["metadata settles it, as the pipeline did"] += 1
            else:
                tie["metadata settles it, against the pipeline"] += 1
    return {
        "samples": len(keys),
        "metadata": dict(named),
        "metadata_vs_pipeline": dict(vs_pipeline),
        "metadata_vs_reads": dict(vs_reads),
        "where_reads_abstain": dict(tie),
        "metadata_vs_reads_disagreements": disagreements,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--starsolo", type=Path, required=True)
    args = parser.parse_args()
    meta = metadata(args.metadata)
    with (args.starsolo / "truth.csv").open() as handle:
        truth = {(r["dataset"], r["sample"]): r for r in csv.DictReader(handle)}
    calls = reads(args.starsolo)
    out = {
        half: summarise(
            sorted(k for k, r in truth.items() if r["split"] == half), meta, truth, calls
        )
        for half in ("design", "heldout")
    }
    out["all"] = summarise(sorted(truth), meta, truth, calls)
    (args.metadata.parent / "summary.json").write_text(json.dumps(out, indent=1) + "\n")
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
