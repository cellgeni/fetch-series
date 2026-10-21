#!/usr/bin/env python3
"""Tables for the content-survey results page, from the committed exports.

    uv run python scripts/inspect_survey_report.py data/surveys/<corpus slug> \
        [--anchor data/surveys/sample_3000@reprocessed-srr/run-to-file_ena_fastq.csv.gz]

Reads only `screen_*.csv.gz` exports, so every number on the page regenerates
from the repository. Prints, in order:

* per source (link): runs listed, fit, unfit, undetermined, and the kits called;
* per run: whether any source was fit, and what the undetermined ones were;
* ENA's fastqs against NCBI's Normalized object, run by run;
* the Lite object against the Normalized one;
* why sources were undetermined, and why unfit;
* with ``--anchor``, ENA's listing in the census against the reads of ENA's fastq.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

csv.field_size_limit(sys.maxsize)

ENA_FASTQ = "fastq_ena_filereport_fastq"
NORMALIZED = "sra_normalized_sdl"
LITE = "sra_lite_sdl_lite"


def rows(path: Path) -> list[dict[str, str]]:
    with gzip.open(path, "rt", newline="") as handle:
        return list(csv.DictReader(handle))


def load(directory: Path) -> dict[str, dict[str, dict[str, Any]]]:
    """Resolved verdicts as ``{link (run node dropped): {run: detail}}``."""
    out: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for path in sorted(directory.glob("screen_*.csv.gz")):
        link = path.name.split("_", 2)[2].removesuffix(".csv.gz").split("-to-", 1)[1]
        for row in rows(path):
            if row["outcome"] == "resolved":
                out[link][row["accession"]] = json.loads(row["detail"])
    return out


def word(fit: object) -> str:
    return {True: "fit", False: "unfit", None: "undetermined"}[fit]  # type: ignore[index]


def matrix(title: str, a: dict[str, dict], b: dict[str, dict]) -> None:
    cells: Counter[tuple[object, object]] = Counter(
        (a[run].get("fit"), b[run].get("fit")) for run in set(a) & set(b)
    )
    print(f"\n| {title} | fit | unfit | undetermined |\n|---|---:|---:|---:|")
    for left in (True, False, None):
        counts = " | ".join(str(cells[(left, right)]) for right in (True, False, None))
        print(f"| {word(left)} | {counts} |")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("--anchor", type=Path)
    args = parser.parse_args()
    links = load(args.directory)
    rules = {d.get("rule") for runs in links.values() for d in runs.values()}
    print(f"rules: {sorted(map(str, rules))}")

    print("\n| Source | Runs listed | Fit | Unfit | Undetermined | Kits called |")
    print("|---|---:|---:|---:|---:|---|")
    for link, runs in sorted(links.items()):
        fits = Counter(d.get("fit") for d in runs.values())
        kits = Counter((d.get("call") or {}).get("layout") or "-" for d in runs.values())
        shown = ", ".join(f"{k} {n}" for k, n in kits.most_common(4))
        print(f"| `{link}` | {len(runs)} | {fits[True]} | {fits[False]} | {fits[None]} | {shown} |")

    per_run: dict[str, dict[str, dict]] = defaultdict(dict)
    for link, runs in links.items():
        for run, detail in runs.items():
            per_run[run][link] = detail
    best: Counter[str] = Counter()
    for sources in per_run.values():
        fits = {d.get("fit") for d in sources.values()}
        best[
            "some source fit"
            if True in fits
            else "nothing fit, a source unfit"
            if False in fits
            else "every source undetermined"
        ] += 1
    print(f"\nruns: {len(per_run)}  {dict(best)}")

    if ENA_FASTQ in links and NORMALIZED in links:
        matrix("ENA fastq ↓ / Normalized object →", links[ENA_FASTQ], links[NORMALIZED])
    if LITE in links and NORMALIZED in links:
        matrix("Lite object ↓ / Normalized object →", links[LITE], links[NORMALIZED])

    for value in (None, False):
        reasons: Counter[tuple[str, str]] = Counter()
        for link, runs in links.items():
            for detail in runs.values():
                if detail.get("fit") is value:
                    reason = detail.get("reason", "")
                    reason = (
                        reason.split(":")[0] if reason.startswith("the first spots") else reason
                    )
                    reasons[(link, reason[:80])] += 1
        print(f"\n{word(value)} reasons (link, reason, sources):")
        for (link, reason), n in sorted(reasons.items()):
            print(f"  {link:42s} {n:5d}  {reason}")

    if args.anchor is not None and ENA_FASTQ in links:
        single = {
            row["accession"]: len(
                [v for v in row["results"].split(";") if v.endswith((".fastq.gz", ".fq.gz"))]
            )
            == 1
            for row in rows(args.anchor)
            if row["outcome"] == "resolved"
        }
        cells: Counter[tuple[bool, object]] = Counter(
            (single[run], detail.get("fit"))
            for run, detail in links[ENA_FASTQ].items()
            if run in single
        )
        print(
            "\n| ENA's listing in the census ↓ / reads of ENA's fastq → | fit | unfit | undetermined |"
        )
        print("|---|---:|---:|---:|")
        for one in (True, False):
            counts = " | ".join(str(cells[(one, fit)]) for fit in (True, False, None))
            print(f"| {'one fastq' if one else 'two or more'} | {counts} |")


if __name__ == "__main__":
    main()
