#!/usr/bin/env python3
"""Read the inspect benchmark: set thresholds on the design half, report the held-out half.

    uv run python scripts/benchmark_inspect_report.py data/benchmarks/inspect/<date>

A label is a kit named by metadata, so it is wrong sometimes: a submitter who
writes "3' v2" for a v3 kit. Where the read call, ``tchem`` and ``rename10xrun``
all agree with each other and against the label, the label is *disputed* and
counted apart, not as the classifier's error -- the same treatment ``tchem``'s
own validation gives its disputed calls. Where the instruments do not agree,
a disagreement with the label stays an error.

Prints Markdown and writes ``summary.json`` beside the CSVs.
"""

from __future__ import annotations

import csv
import json
import sys
from collections import Counter
from pathlib import Path

LIST_CLASS = {
    "10x_3p_v1": "tenx_v1",
    "10x_3p_v2": "tenx_v2",
    "10x_5p_v1_v2": "tenx_v2",
    "10x_3p_v3_family": "tenx_v3",
    "10x_3p_v4_gemx": "tenx_v4_3p",
    "10x_5p_v3_gemx": "tenx_v3_5p",
    "10x_multiome_arc_gex": "tenx_arc_gex",
    "10x_multiome_arc_atac": "tenx_arc_atac",
    "10x_atac_v1_v1.1_v2": "tenx_atac",
}
TCHEM = {
    "10x 3' v2": "10x_3p_v2",
    "10x 5' v1/v2": "10x_5p_v1_v2",
    "10x 3' v3/v3.1": "10x_3p_v3_family",
    "10x 3' v4 (GEM-X)": "10x_3p_v4_gemx",
    "10x 5' v3 (GEM-X)": "10x_5p_v3_gemx",
    "10x Multiome GEX": "10x_multiome_arc_gex",
}


def tchem_id(value: str) -> str | None:
    verdict, _, name = value.partition(":")
    return TCHEM.get(name) if verdict == "match" else None


def r10x_class(value: str) -> str | None:
    if value == "10x_3p_v2_or_5p_v1_v2":
        return "tenx_v2"
    return LIST_CLASS.get(value)


V2_PAIR = {"10x_3p_v2", "10x_5p_v1_v2"}


def judge(row: dict[str, str], *, high: float, medium: float) -> dict[str, object]:
    """One row under candidate thresholds and the classifier's rules, from stored fields.

    Reproduces the classifier's level logic exactly from what calls.csv keeps:
    the near share, the margin to the next list, contradictions, and the strand
    votes. A 3' v2 / 5' v1/v2 call settled by fewer than two strand votes is
    ambiguous, as the classifier now rules.
    """
    call = row["call_100k"] or None
    near = float(row["near"]) if row["near"] else 0.0
    level = row["level_100k"]
    votes = json.loads(row["strand_votes"] or "{}")
    directional = [v for v in votes.values() if v in ("sense", "antisense")]
    if call in V2_PAIR and len(directional) < 2:
        call, level = None, "ambiguous"
    if call and level in ("HIGH", "MEDIUM", "LOW"):
        contradicted = bool(row["contradictions"])
        margin = float(row["margin"]) if row["margin"] else None
        clear = margin is None or margin >= 0.2
        if near >= high and not contradicted and clear:
            level = "HIGH"
        elif near >= medium and clear:
            level = "MEDIUM"
        else:
            level = "LOW"
    label = row["label"]
    tchem = tchem_id(row["tchem"])
    r10x = r10x_class(row["rename10xrun"])
    # A refusal is an abstention, not a disagreement: rename10xrun fails closed
    # on variable read lengths (SRR23687132) and says nothing about the kit.
    r10x_ok = r10x is None or r10x == LIST_CLASS.get(call or "")
    instruments_agree = call is not None and tchem == call and r10x_ok
    if call is None:
        verdict = "no call"
    elif call == label:
        verdict = "exact"
    elif instruments_agree:
        verdict = "label disputed"
    elif LIST_CLASS.get(call) == LIST_CLASS.get(label):
        verdict = "same list"
    else:
        verdict = "wrong"
    return {"level": level, "verdict": verdict, "call": call}


def table(rows: list[dict[str, str]], high: float, medium: float) -> Counter[tuple[str, str]]:
    return Counter(
        (str(j["level"]), str(j["verdict"]))
        for j in (judge(r, high=high, medium=medium) for r in rows)
    )


def error_rate(counts: Counter[tuple[str, str]], level: str) -> tuple[int, int]:
    """Errors among calls at ``level``: wrong or merely same-list, disputed labels set aside."""
    judged = sum(n for (lv, v), n in counts.items() if lv == level and v != "label disputed")
    errors = sum(n for (lv, v), n in counts.items() if lv == level and v in ("wrong", "same list"))
    return errors, judged


def main() -> None:
    directory = Path(sys.argv[1])
    rows = [r for r in csv.DictReader((directory / "calls.csv").open()) if not r["error"]]
    design = [r for r in rows if r["split"] == "design"]
    held = [r for r in rows if r["split"] == "held-out"]

    # Thresholds on design: the lowest HIGH bar whose HIGH calls carry no
    # error, and the lowest MEDIUM bar whose MEDIUM calls err at most once in 20.
    grid = [round(0.30 + 0.05 * i, 2) for i in range(14)]
    high = next((t for t in grid if error_rate(table(design, t, 0.3), "HIGH")[0] == 0), grid[-1])
    medium = next(
        (
            t
            for t in grid
            if t <= high
            and (lambda e: e[1] == 0 or e[0] / e[1] <= 0.05)(
                error_rate(table(design, high, t), "MEDIUM")
            )
        ),
        high,
    )
    summary: dict[str, object] = {"thresholds": {"HIGH_NEAR": high, "MEDIUM_NEAR": medium}}
    for name, part in (("design", design), ("held-out", held)):
        counts = table(part, high, medium)
        summary[name] = {
            "runs": len(part),
            "by_level": {f"{lv}|{v}": n for (lv, v), n in sorted(counts.items())},
            "HIGH_errors": error_rate(counts, "HIGH"),
            "MEDIUM_errors": error_rate(counts, "MEDIUM"),
        }
    summary["stable_100k_200k"] = sum(r["call_100k"] == r["call_200k"] for r in rows), len(rows)
    summary["tchem_agrees"] = (
        sum(tchem_id(r["tchem"]) == (r["call_100k"] or None) for r in rows if r["call_100k"]),
        sum(bool(r["call_100k"]) for r in rows),
    )
    summary["rename10xrun_agrees_on_list"] = (
        sum(
            r10x_class(r["rename10xrun"]) == LIST_CLASS.get(r["call_100k"])
            for r in rows
            if r["call_100k"]
        ),
        sum(bool(r["call_100k"]) for r in rows),
    )
    summary["labels"] = dict(Counter(r["label"] for r in rows).most_common())
    (directory / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")
    print(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
