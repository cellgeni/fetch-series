#!/usr/bin/env python3
"""Benchmark the read classifier against labels that did not come from the reads.

    uv run python scripts/benchmark_inspect.py --out data/benchmarks/inspect/<date> \
        [--studies 240] [--tchem DIR --tchem-lists DIR] [--rename10xrun FILE --raw-lists DIR]

1. **Labels.** Studies are drawn, seeded, from the reprocessed 10x table, one run
   each. A run is labelled where its metadata -- ENA's protocol fields and GEO's
   SOFT text, read by ``assays.chemistry_of`` -- names exactly one kit. A study
   naming two (GSE111360) is not a label for either. The gate runs' studies are
   excluded: they were chosen because they break things, and the provisional
   thresholds were looked at on them.
2. **Split.** Each study goes to the *design* half or the *held-out* half by a
   seeded draw on its accession. Thresholds are set on design; held-out is read
   once, after, and reported as it falls.
3. **Reads.** The first 200,000 spots of each run's SRA object, by
   ``fastq-dump -X``. The classifier calls the first 100,000 and all 200,000, which
   answers whether 100,000 is enough (the plan's S0 question 3).
4. **Reference instruments.** ``tchem`` and ``rename10xrun`` read the same first
   100,000 spots, written to temporary files. They are instruments here, never
   engines: their calls are reported beside ours.

Writes ``labels.csv`` and ``calls.csv``, resumable, with counts and calls and
never a sequence.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import random
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from fetch_series.assays.chemistry import chemistry_of
from fetch_series.assays.registry import is_10x
from fetch_series.core import default_api_key
from fetch_series.filesets import ASSAY_FIELDS
from fetch_series.inspect import lists, references
from fetch_series.inspect.checks import quality_verdict
from fetch_series.inspect.classify import classify
from fetch_series.inspect.model import FileSample
from fetch_series.inspect.readers.sra import read_sra
from fetch_series.logging_utils import configure_logging, redact, run_logfile
from fetch_series.providers import ena_portal, geo
from fetch_series.survey.client import Limits, SurveyClient
from fetch_series.survey.corpora import SAMPLE_TABLE_COLUMNS, _sample_table_path

SEED = 20261019
# Studies of the S4 gate runs, and of anything the classifier was tuned on.
EXCLUDED = {"GSE236084", "GSE145926", "E-MTAB-9221", "GSE131685", "GSE139324", "GSE155673"}
LABEL_COLUMNS = ("study", "run", "split", "chemistry", "candidates", "assay", "library_type")
CALL_COLUMNS = (
    "run",
    "study",
    "split",
    "label",
    "reads_files",
    "quality",
    "call_100k",
    "level_100k",
    "candidates_100k",
    "call_200k",
    "level_200k",
    "list",
    "near",
    "exact",
    "margin",
    "umi_entropy",
    "strand",
    "strand_votes",
    "library_type",
    "contradictions",
    "hint",
    "tchem",
    "tchem_level",
    "rename10xrun",
    "error",
)


def studies() -> dict[str, tuple[str, list[str]]]:
    """Every study of the reprocessed table: its BioProject and runs. GSE where there is one."""
    by_study: dict[str, tuple[str, list[str]]] = {}
    gse, prj, srr = (SAMPLE_TABLE_COLUMNS.index(c) for c in ("gse", "prj", "srr"))
    with _sample_table_path().open() as handle:
        for line in handle:
            fields = line.rstrip("\n").split("\t")
            if len(fields) <= srr or not fields[prj].startswith("PRJ"):
                continue
            study = fields[gse] if fields[gse].startswith("GSE") else fields[prj]
            runs = [
                r.strip() for r in fields[srr].split(",") if r.strip()[:3] in ("SRR", "ERR", "DRR")
            ]
            entry = by_study.setdefault(study, (fields[prj], []))
            entry[1].extend(runs)
    return by_study


async def label_rows(study: str, project: str, client: SurveyClient) -> list[tuple[str, Any, Any]]:
    """Every run's assay and chemistry call from its metadata, in two requests.

    ``filesets.screen`` joins every archive per run, which is right for a user
    and far too slow for 240 studies. This asks ENA once for the project's runs
    and GEO once for the series' SOFT text -- the same two sources ``screen``
    reads the protocol from, keyed by the GSM ENA records as the sample alias.
    """
    rows = await ena_portal.read_run_report(
        client, project, 120.0, fields=(*ASSAY_FIELDS, "sample_alias")
    )
    soft: dict[str, dict[str, str]] = {}
    if study.startswith("GSE"):
        try:
            family = geo.parse_soft_family(study, await geo.fetch_soft_family(client, study, 120.0))
            soft = family.sample_text
        except Exception:
            soft = {}
    out = []
    for row in rows:
        merged = dict(row)
        if extra := soft.get(str(row.get("sample_alias") or "")):
            merged.update(extra)
        out.append((str(row.get("run_accession")), is_10x(merged), chemistry_of(merged)))
    return out


def split_of(study: str) -> str:
    return "design" if random.Random(f"split:{SEED}:{study}").random() < 0.5 else "held-out"


def _read(path: Path, columns: tuple[str, ...]) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def _append(path: Path, columns: tuple[str, ...], row: dict[str, Any]) -> None:
    new = not path.exists()
    with path.open("a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        if new:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in columns})


async def label(out: Path, wanted: int, client: SurveyClient) -> list[dict[str, str]]:
    path = out / "labels.csv"
    done = _read(path, LABEL_COLUMNS)
    tried = {row["study"] for row in done}
    labelled = [row for row in done if row["run"]]
    pool = studies()
    order = sorted(pool)
    random.Random(SEED).shuffle(order)
    for study in order:
        if len(labelled) >= wanted:
            break
        if study in tried or study in EXCLUDED:
            continue
        try:
            project, _ = pool[study]
            calls = await client.with_retry(
                lambda timeout, s=study, p=project: label_rows(s, p, client), label=f"label {study}"
            )
        except Exception as exc:
            print(f"{study}: screen failed: {redact(str(exc))[:120]}", file=sys.stderr)
            _append(path, LABEL_COLUMNS, {"study": study})
            continue
        table = set(pool[study][1])
        named = sorted(
            (run, assay, chemistry)
            for run, assay, chemistry in calls
            if run in table and chemistry.chemistry and assay.assay == "10x"
        )
        row: dict[str, Any] = {"study": study, "split": split_of(study)}
        if named:
            run, assay, chemistry = random.Random(f"{SEED}:{study}").choice(named)
            row |= {
                "run": run,
                "chemistry": chemistry.chemistry,
                "candidates": "|".join(chemistry.candidates),
                "assay": assay.assay,
                "library_type": assay.library_type or "",
            }
            labelled.append(row)
        _append(path, LABEL_COLUMNS, row)
        print(
            f"{study}: {row.get('run', '-')} {row.get('chemistry', '')}",
            file=sys.stderr,
            flush=True,
        )
    return labelled


def _write_fastq(sample: FileSample, path: Path, n: int) -> None:
    with path.open("w") as handle:
        for i, record in enumerate(sample.records[:n]):
            handle.write(
                f"@{record.name or i}\n{record.seq}\n+\n{record.qual or 'I' * len(record.seq)}\n"
            )


def tchem_call(binary: str, list_dir: str, samples: list[FileSample]) -> tuple[str, str]:
    with tempfile.TemporaryDirectory() as scratch:
        paths = []
        for i, sample in enumerate(samples):
            path = Path(scratch) / f"x_S1_L001_{sample.role_hint or f'R{i + 1}'}_001.fastq"
            _write_fastq(sample, path, 100_000)
            paths.append(str(path))
        result = subprocess.run(
            [binary, "--whitelists", list_dir, "--format", "json", "--reads", "100000", *paths],
            capture_output=True,
            text=True,
            timeout=900,
        )
        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError:
            return "", f"exit {result.returncode}: {redact(result.stderr)[-120:]}"
        layout = payload.get("layout") or {}
        name = layout.get("name", "") if isinstance(layout, dict) else str(layout)
        return f"{payload.get('verdict', '')}:{name}", str(payload.get("confidence") or "")


def rename10xrun_call(script: str, raw_lists: str, samples: list[FileSample]) -> str:
    with tempfile.TemporaryDirectory() as scratch:
        paths = []
        for i, sample in enumerate(samples):
            path = Path(scratch) / f"x_{i + 1}.fastq"
            _write_fastq(sample, path, 100_000)
            paths.append(str(path))
        report = Path(scratch) / "report.json"
        result = subprocess.run(
            [
                sys.executable,
                "-I",
                script,
                *paths,
                "--sample",
                "x",
                "--whitelists",
                raw_lists,
                "--outdir",
                str(Path(scratch) / "out"),
                "--json",
                str(report),
                "--manifest",
                str(Path(scratch) / "manifest.tsv"),
                "--dry-run",
                "--sample-records",
                "100000",
            ],
            capture_output=True,
            text=True,
            timeout=900,
        )
        if report.exists():
            payload = json.loads(report.read_text())
            return str(payload.get("chemistry_id") or payload.get("error") or "")
        tail = (result.stderr or result.stdout).strip().splitlines()[-1:] or [""]
        return f"refused: {redact(tail[0])[:120]}"


def _call_fields(call: Any, suffix: str) -> dict[str, Any]:
    return {
        f"call_{suffix}": call.layout or "",
        f"level_{suffix}": call.level,
        f"candidates_{suffix}": "|".join(call.candidates),
    }


async def inspect(out: Path, labelled: list[dict[str, str]], args: argparse.Namespace) -> None:
    path = out / "calls.csv"
    done = {row["run"] for row in _read(path, CALL_COLUMNS)}
    barcode_lists = lists.available()
    refs = {k: v for k in references.REGISTER if (v := references.index(k)) is not None}
    for row in labelled:
        if row["run"] in done:
            continue
        result: dict[str, Any] = {
            "run": row["run"],
            "study": row["study"],
            "split": row["split"],
            "label": row["chemistry"],
        }
        try:
            samples = await read_sra(row["run"], max_reads=200_000)
            first = [
                FileSample(s.source, s.format, s.role_hint, s.records[:100_000], facts=s.facts)
                for s in samples
            ]
            small, full = (
                classify(first, barcode_lists, refs),
                classify(samples, barcode_lists, refs),
            )
            best = (small.evidence.get("hits") or [{}])[0]
            strand = small.evidence.get("strand") or {}
            result |= _call_fields(small, "100k") | _call_fields(full, "200k")
            result |= {
                "reads_files": "+".join(
                    str(max(set(s.lengths), key=s.lengths.count)) for s in samples if s.records
                ),
                "quality": "|".join(sorted({quality_verdict(s) for s in samples})),
                "list": small.barcode_list or "",
                "near": best.get("near", ""),
                "exact": best.get("exact", ""),
                "margin": small.evidence.get("margin", ""),
                "umi_entropy": small.evidence.get("umi_entropy", ""),
                "strand": strand.get("strand", "") or strand.get("skipped", ""),
                "strand_votes": json.dumps(strand.get("votes", {}), sort_keys=True),
                "library_type": small.library_type or "",
                "contradictions": " | ".join(small.contradictions),
                "hint": small.hint or "",
            }
            if args.tchem:
                result["tchem"], result["tchem_level"] = tchem_call(
                    args.tchem, args.tchem_lists, first
                )
            if args.rename10xrun:
                result["rename10xrun"] = rename10xrun_call(args.rename10xrun, args.raw_lists, first)
        except Exception as exc:
            result["error"] = redact(f"{type(exc).__name__}: {exc}")[:200]
        _append(path, CALL_COLUMNS, result)
        print(
            f"{row['run']} {row['chemistry']} -> {result.get('call_100k', '')} {result.get('level_100k', '')} "
            f"tchem={result.get('tchem', '')} r10x={result.get('rename10xrun', '')} {result.get('error', '')}",
            file=sys.stderr,
            flush=True,
        )


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--studies", type=int, default=240)
    parser.add_argument("--tchem")
    parser.add_argument("--tchem-lists")
    parser.add_argument("--rename10xrun")
    parser.add_argument("--raw-lists", default=".cache/lists/raw")
    # Run beside a link survey, EBI's ceiling is already spent by its buckets;
    # labelling asks ENA once per run, so it gets what is left.
    parser.add_argument("--rps", type=float, default=2.0)
    args = parser.parse_args()
    load_dotenv()
    configure_logging(logfile=run_logfile("benchmark-inspect"))
    args.out.mkdir(parents=True, exist_ok=True)
    api_key = default_api_key()
    async with SurveyClient(
        Limits(rps=args.rps, concurrency=2, base_timeout=60), api_key=api_key
    ) as client:
        labelled = await label(args.out, args.studies, client)
    await inspect(args.out, labelled, args)


if __name__ == "__main__":
    asyncio.run(main())
