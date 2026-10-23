"""Read the samples the reprocessing pipeline mapped, and set ``fetch inspect`` beside what STARsolo used.

Usage::

    uv run fetch inspect truth import <bundle> --out data/benchmarks/inspect/starsolo-<date>
    uv run python scripts/inspect_vs_starsolo.py --out data/benchmarks/inspect/starsolo-<date> \\
        [--split heldout] [--samples 300] [--shard 0/4]

1. **Draw.** Public samples of one half (``truth.csv``'s ``split``) named by an
   accession -- GSM, ERS, SRS -- seeded, one per dataset before a second from
   any. Studies the classifier's thresholds were set on are left out. Written once to ``draw-<split>.csv``; a later call reads it back.
2. **Read.** Each sample's runs resolved as ``fetch`` resolves them, one run
   chosen by seed, and every source of it inspected as ``fetch inspect run``
   does, with the first 100,000 reads of each file.
3. **Record.** One row per (sample, source) in ``calls-<split>.csv``: the call,
   the measured strand and the STARsolo strand it implies, the listed-barcode
   rates, the fit verdict. Counts and calls, never a sequence. The full reports
   go to ``.cache/starsolo-compare/``, uncommitted.

Shards (``--shard i/n``) split the draw between processes, each reading one
run at a time: classification is CPU work, and one process holding several
runs' downloads stalled them.

The held-out half is compared once, after any threshold is set on the design
half. The report is ``scripts/inspect_vs_starsolo_report.py``.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import random
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from fetch_series.accession import try_parse
from fetch_series.core import default_api_key
from fetch_series.filesets import files_for_run, runs_of
from fetch_series.inspect import lists, references, truth
from fetch_series.inspect.report import inspect_fileset
from fetch_series.logging_utils import configure_logging, redact, run_logfile
from fetch_series.survey.client import Limits, SurveyClient

SEED = 20261022
REPORTS = Path(".cache/starsolo-compare")
COLUMNS = (
    "dataset",
    "sample",
    "run",
    "runs",
    "source",
    "kind",
    "classifier",
    "fit_rule",
    "technology",
    "layout",
    "level",
    "barcode_list",
    "strand",
    "solo_strand",
    "exact",
    "within_one",
    "umi_entropy",
    "modes",
    "fit",
    "reason",
    "mapping",
    "error",
)


# The read classifier's thresholds were set on this benchmark's design half
# (2026-10-19): its studies cannot be held out against it.
CLASSIFIER_LABELS = Path("data/benchmarks/inspect/2026-10-19/labels.csv")


def tuned_on() -> set[str]:
    if not CLASSIFIER_LABELS.exists():
        return set()
    with CLASSIFIER_LABELS.open() as handle:
        return {r["study"] for r in csv.DictReader(handle) if r["split"] == "design"}


def spent(out: Path) -> tuple[set[str], set[str]]:
    """Samples and datasets an earlier draw read: looked at, so never held out again."""
    samples: set[str] = set()
    datasets: set[str] = set()
    for path in out.glob("draw-*.csv"):
        with path.open() as handle:
            for row in csv.DictReader(handle):
                samples.add(row["sample"])
                datasets.add(row["dataset"])
    return samples, datasets


def draw(
    rows: list[dict[str, str]],
    split: str,
    n: int,
    *,
    strata: list[tuple[str, str, int | None]] | None = None,
    avoid: tuple[set[str], set[str]] = (set(), set()),
) -> list[dict[str, str]]:
    """Seeded, one sample per dataset before a second; by stratum where strata are given.

    A stratum is (whitelist, strand, size), ``*`` matching anything and a size of
    ``None`` taking every sample: the pipeline mapped 5' libraries ten times less
    often than 3' ones, so a draw that must tell them apart asks for both.
    ``avoid`` holds samples never to draw, and datasets to draw from only once
    the others are spent.
    """
    excluded = tuned_on()
    avoid_samples, avoid_datasets = avoid
    pool = [
        r
        for r in rows
        if r["split"] == split
        and r["dataset"] not in excluded
        and r["sample"] not in avoid_samples
        and r["public"] == "True"
        and (parsed := try_parse(r["sample"])) is not None
        and parsed.value == r["sample"]
    ]
    rng = random.Random(SEED)
    drawn: list[dict[str, str]] = []
    for wl, strand, size in strata or [("*", "*", n)]:
        members = [
            r
            for r in pool
            if wl in ("*", r["wl"]) and strand in ("*", r["strand"]) and r not in drawn
        ]
        by_dataset: dict[str, list[dict[str, str]]] = {}
        for row in members:
            by_dataset.setdefault(row["dataset"], []).append(row)
        fresh = sorted(d for d in by_dataset if d not in avoid_datasets)
        used = sorted(d for d in by_dataset if d in avoid_datasets)
        rng.shuffle(fresh)
        rng.shuffle(used)
        datasets = fresh + used
        for samples in by_dataset.values():
            samples.sort(key=lambda r: r["sample"])
            rng.shuffle(samples)
        want = len(members) if size is None else size
        taken: list[dict[str, str]] = []
        while len(taken) < want and any(by_dataset[d] for d in datasets):
            for dataset in datasets:
                if by_dataset[dataset] and len(taken) < want:
                    taken.append(by_dataset[dataset].pop())
        drawn.extend(taken)
    return drawn


def parse_strata(text: str) -> list[tuple[str, str, int | None]]:
    """``v2:Forward:150,v2:Reverse:150,v3:*:60,arc:*:all`` as (whitelist, strand, size)."""
    strata = []
    for part in text.split(","):
        wl, strand, size = part.split(":")
        strata.append((wl, strand, None if size == "all" else int(size)))
    return strata


def rows_of(
    sample: dict[str, str], run: str, runs: int, report: dict[str, Any]
) -> list[dict[str, Any]]:
    out = []
    for source in report["sources"]:
        call = source["call"]
        strand = ((call.get("evidence") or {}).get("strand") or {}).get("strand")
        mapping = source.get("mapping") or {}
        barcodes = (source.get("qc") or {}).get("barcodes") or {}
        out.append(
            {
                "dataset": sample["dataset"],
                "sample": sample["sample"],
                "run": run,
                "runs": runs,
                "source": source["source"],
                "kind": source["kind"],
                "classifier": report["classifier"],
                "fit_rule": report["fit_rule"],
                "technology": (source.get("technology") or {}).get("id"),
                "layout": call.get("layout") or "|".join(call.get("candidates") or []),
                "level": call.get("level"),
                "barcode_list": call.get("barcode_list"),
                "strand": strand,
                "solo_strand": (mapping.get("starsolo") or {}).get("soloStrand"),
                "exact": barcodes.get("exact"),
                "within_one": barcodes.get("within_one"),
                "umi_entropy": (source.get("qc") or {}).get("umi", {}).get("entropy"),
                "modes": "+".join(
                    str(f["length"]["mode"]) for f in source["checks"].get("files", [])
                ),
                "fit": source["fit"],
                "reason": source["reason"],
                "mapping": bool(mapping),
                "error": "",
            }
        )
    return out


async def one(sample: dict[str, str], client: SurveyClient, held, refs) -> list[dict[str, Any]]:
    parsed = try_parse(sample["sample"])
    assert parsed is not None
    runs = sorted(await runs_of(parsed, client))
    if not runs:
        return [{"dataset": sample["dataset"], "sample": sample["sample"], "error": "no runs"}]
    run = random.Random(f"{SEED}:{sample['sample']}").choice(runs)
    fileset = await files_for_run(run, client)
    report = await inspect_fileset(fileset, client, held, refs)
    REPORTS.mkdir(parents=True, exist_ok=True)
    (REPORTS / f"{run}.json").write_text(json.dumps(report) + "\n")
    rows = rows_of(sample, run, len(runs), report)
    return rows or [
        {"dataset": sample["dataset"], "sample": sample["sample"], "run": run, "error": "no source"}
    ]


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--split", default="heldout", choices=("design", "heldout"))
    parser.add_argument("--samples", type=int, default=300)
    parser.add_argument("--shard", default="0/1")
    parser.add_argument("--draw-only", action="store_true", help="Write the draw, read nothing.")
    parser.add_argument(
        "--strata", help="Draw by stratum, e.g. v2:Forward:150,v2:Reverse:150,v3:*:60,arc:*:all"
    )
    parser.add_argument(
        "--name", help="The draw's name, for a second draw of one half (draw-<name>.csv)."
    )
    args = parser.parse_args()
    load_dotenv()
    configure_logging(logfile=run_logfile("inspect-vs-starsolo"))
    held = lists.available()
    if not held:
        sys.exit("fetch the lists first: uv run fetch inspect lists --fetch")
    refs = {k: v for k in references.REGISTER if (v := references.index(k)) is not None}
    name = args.name or args.split
    drawn_path = args.out / f"draw-{name}.csv"
    if drawn_path.exists():
        drawn = truth.load(drawn_path)
    else:
        drawn = draw(
            truth.load(args.out / "truth.csv"),
            args.split,
            args.samples,
            strata=parse_strata(args.strata) if args.strata else None,
            avoid=spent(args.out),
        )
        with drawn_path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, list(drawn[0]))
            writer.writeheader()
            writer.writerows(drawn)
    if args.draw_only:
        print(f"{len(drawn)} samples drawn -> {drawn_path}", file=sys.stderr)
        return
    index, count = (int(x) for x in args.shard.split("/"))
    mine = [s for i, s in enumerate(drawn) if i % count == index]
    calls = args.out / f"calls-{name}-{index}of{count}.csv"
    # Samples any shard of this draw has read, under any shard count: a draw
    # re-sharded to read faster must not read a sample twice.
    done: set[str] = set()
    for earlier in args.out.glob(f"calls-{name}-*of*.csv"):
        with earlier.open() as handle:
            done |= {r["sample"] for r in csv.DictReader(handle) if not r["error"]}
    if not calls.exists():
        with calls.open("w", newline="") as handle:
            csv.DictWriter(handle, COLUMNS).writeheader()
    async with SurveyClient(
        Limits(rps=3, concurrency=8, base_timeout=120), api_key=default_api_key()
    ) as client:
        for sample in mine:
            if sample["sample"] in done:
                continue
            try:
                rows = await one(sample, client, held, refs)
            except Exception as exc:
                rows = [
                    {
                        "dataset": sample["dataset"],
                        "sample": sample["sample"],
                        "error": redact(f"{type(exc).__name__}: {exc}")[:200],
                    }
                ]
            with calls.open("a", newline="") as handle:
                csv.DictWriter(handle, COLUMNS, restval="").writerows(rows)
            print(
                f"{sample['sample']} {rows[0].get('run', '')} {rows[0].get('error', '')}",
                file=sys.stderr,
                flush=True,
            )


if __name__ == "__main__":
    asyncio.run(main())
