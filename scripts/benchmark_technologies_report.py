"""Summarise a technology benchmark's calls.csv: per label, how often the reads name the label.

Usage::

    uv run python scripts/benchmark_technologies_report.py data/benchmarks/inspect/technologies-<date> calls-<v>.csv

Prints a markdown table per half and writes ``summary-<v>.json`` beside the
calls. A run whose files could not be read is counted apart, never as a miss.

The halves are by study, seeded: a change to the table is made on what the
design half shows, and the held-out half is what is reported. Studies whose
runs were read by eye while changing the table (``LOOKED_AT``) are left out
of the held-out half, whichever half they hash to.
"""

from __future__ import annotations

import csv
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path

SEED = "20261022"
# Read by eye on 2026-10-22, to find why runs went unnamed; and the runs the
# table's rows were first written from.
LOOKED_AT = frozenset(
    {
        "PRJNA1225175",
        "PRJNA1223908",
        "PRJNA1192294",
        "PRJNA1400634",
        "PRJNA1234640",
        "PRJNA1090902",
        "PRJNA611616",
        "PRJNA1001845",
        "PRJNA1478627",
        "PRJNA1197496",
        "PRJEB102200",
        "PRJNA811439",
        "PRJNA734034",
        "PRJNA976679",
        "PRJDB12314",
        "PRJDB13013",
    }
)


def half(study: str) -> str:
    if study in LOOKED_AT:
        return "design"
    digest = hashlib.sha256(f"{SEED}:{study}".encode()).digest()
    return "design" if digest[0] % 2 == 0 else "heldout"


def main() -> None:
    out = Path(sys.argv[1])
    name = sys.argv[2] if len(sys.argv) > 2 else "calls.csv"
    with (out / name).open() as handle:
        rows = list(csv.DictReader(handle))
    summary = {}
    for which in ("design", "heldout"):
        summary[which] = table([r for r in rows if half(r["study"]) == which])
        print(f"\n**{which}**\n")
        render(summary[which])
    stem = name.removesuffix(".csv").replace("calls", "summary")
    (out / f"{stem}.json").write_text(json.dumps(summary, indent=1) + "\n")


def table(rows: list[dict[str, str]]) -> dict[str, dict[str, object]]:
    summary: dict[str, dict[str, object]] = {}
    for label in dict.fromkeys(r["label"] for r in rows):
        mine = [r for r in rows if r["label"] == label]
        read = [r for r in mine if not r["error"]]
        expected = set(mine[0]["expected"].split("|"))
        named = Counter(r["technology"] for r in read)
        right = sum(
            r["technology"] in expected
            or (expected == {"unknown"} and r["technology"] == "unknown")
            for r in read
        )
        summary[label] = {
            "drawn": len(mine),
            "read": len(read),
            "named_as_label": right,
            "called_10x": named.get("10x", 0),
            "named": dict(named.most_common()),
            "unreadable": len(mine) - len(read),
        }
    return summary


def render(summary: dict[str, dict[str, object]]) -> None:
    print("| Label | Drawn | Read | Named as labelled | Called 10x | What the reads named |")
    print("|---|---:|---:|---:|---:|---|")
    for label, s in summary.items():
        named = ", ".join(f"{k} {v}" for k, v in s["named"].items())  # type: ignore[attr-defined]
        print(
            f"| {label} | {s['drawn']} | {s['read']} | {s['named_as_label']} | "
            f"{s['called_10x']} | {named} |"
        )


if __name__ == "__main__":
    main()
