"""Harvest the metadata chemistry call for every sample the reprocessing pipeline mapped.

Usage::

    uv run python scripts/metadata_vs_reads.py \\
        --truth data/benchmarks/inspect/starsolo-2026-10-22/truth.csv \\
        --out data/benchmarks/metadata/chemistry-<date>

For every dataset in ``truth.csv`` (the pipeline's own record, one row per
sample), the assay layer's two cheapest sources, as ``fetch screen`` reads
them but in two requests a study rather than several a run:

* ENA's ``read_run`` report for the dataset's BioProject, with the protocol
  fields ``assays`` reads (``filesets.ASSAY_FIELDS``) and the sample keys;
* GEO's SOFT family for a GSE, joined to ENA's rows by the GSM ENA records as
  the sample alias.

Per sample -- a GSM by its alias, an ERS or SRS by its accession -- the runs'
``chemistry_of`` and ``is_10x`` calls, merged: the union of candidates, and
whether the runs disagreed. Counts, ids and the fields that matched, never the
protocol text. Resumable by dataset. The comparison is
``scripts/metadata_vs_reads_report.py``.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from fetch_series.accession import EntityType, try_parse
from fetch_series.assays.chemistry import chemistry_of
from fetch_series.assays.registry import is_10x
from fetch_series.core import default_api_key
from fetch_series.filesets import ASSAY_FIELDS
from fetch_series.logging_utils import configure_logging, redact, run_logfile
from fetch_series.providers import ena_portal, geo
from fetch_series.resolver import resolve_path
from fetch_series.survey.client import Limits, SurveyClient
from fetch_series.survey.corpora import SAMPLE_TABLE_COLUMNS, _sample_table_path

COLUMNS = (
    "dataset",
    "sample",
    "runs",
    "is_10x",
    "candidates",
    "chemistry",
    "runs_disagree",
    "fields",
    "error",
)


def projects() -> dict[str, str]:
    """Each dataset of the reprocessed table -- GSE, E-MTAB or PRJ -- to its BioProject."""
    gse, prj = (SAMPLE_TABLE_COLUMNS.index(c) for c in ("gse", "prj"))
    found: dict[str, str] = {}
    with _sample_table_path().open() as handle:
        for line in handle:
            fields = line.rstrip("\n").split("\t")
            if len(fields) <= prj or not fields[prj].startswith("PRJ"):
                continue
            found.setdefault(fields[gse], fields[prj])
            found.setdefault(fields[prj], fields[prj])
    return found


async def projects_of(dataset: str, table: dict[str, str], client: SurveyClient) -> list[str]:
    """The dataset's BioProjects: the reprocessed table's, else as ``fetch resolve`` finds them.

    The table predates most of the record, and ENA's report will not take a GSE
    or an E-MTAB, so a dataset it lacks is resolved over the measured routes.
    """
    if dataset in table:
        return [table[dataset]]
    parsed = try_parse(dataset)
    if parsed is None:
        return []
    if parsed.entity is EntityType.BIOPROJECT:
        return [parsed.value]
    resolution = await resolve_path(parsed, EntityType.BIOPROJECT, client)
    return sorted(resolution.values)[:3]


async def harvest(
    dataset: str, projects: list[str], samples: set[str], client: SurveyClient
) -> list[dict[str, Any]]:
    rows = [
        row
        for project in projects
        for row in await ena_portal.read_run_report(
            client,
            project,
            120.0,
            fields=(*ASSAY_FIELDS, "sample_alias", "secondary_sample_accession"),
        )
    ]
    soft: dict[str, dict[str, str]] = {}
    if dataset.startswith("GSE"):
        try:
            family = geo.parse_soft_family(
                dataset, await geo.fetch_soft_family(client, dataset, 120.0)
            )
            soft = family.sample_text
        except Exception:
            soft = {}
    per_sample: dict[str, list[tuple[Any, Any]]] = {}
    for row in rows:
        keys = {
            str(row.get("sample_alias") or ""),
            str(row.get("secondary_sample_accession") or ""),
        }
        sample = next((k for k in keys if k in samples), None)
        if sample is None:
            continue
        merged = dict(row)
        if extra := soft.get(str(row.get("sample_alias") or "")):
            merged.update(extra)
        per_sample.setdefault(sample, []).append((is_10x(merged), chemistry_of(merged)))
    out = []
    for sample in sorted(samples):
        calls = per_sample.get(sample, [])
        if not calls:
            out.append({"dataset": dataset, "sample": sample, "runs": 0, "error": "no ENA run"})
            continue
        candidates = sorted({c for _, chem in calls for c in chem.candidates})
        distinct = {tuple(chem.candidates) for _, chem in calls}
        out.append(
            {
                "dataset": dataset,
                "sample": sample,
                "runs": len(calls),
                "is_10x": any(assay.recognised for assay, _ in calls),
                "candidates": "|".join(candidates),
                "chemistry": candidates[0] if len(candidates) == 1 else "",
                "runs_disagree": len(distinct) > 1,
                "fields": "|".join(sorted({m.field for _, chem in calls for m in chem.matches})),
                "error": "",
            }
        )
    return out


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--truth", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--rps", type=float, default=2.0)
    args = parser.parse_args()
    load_dotenv()
    configure_logging(logfile=run_logfile("metadata-vs-reads"))
    args.out.mkdir(parents=True, exist_ok=True)
    with args.truth.open() as handle:
        truth = list(csv.DictReader(handle))
    by_dataset: dict[str, set[str]] = {}
    for row in truth:
        by_dataset.setdefault(row["dataset"], set()).add(row["sample"])
    path = args.out / "calls.csv"
    done: set[str] = set()
    if path.exists():
        with path.open() as handle:
            done = {r["dataset"] for r in csv.DictReader(handle) if not r["error"].startswith("!")}
    else:
        with path.open("w", newline="") as handle:
            csv.DictWriter(handle, COLUMNS).writeheader()
    project_of = projects()
    async with SurveyClient(
        Limits(rps=args.rps, concurrency=2, base_timeout=120), api_key=default_api_key()
    ) as client:
        for dataset in sorted(by_dataset):
            if dataset in done:
                continue
            found: list[str] = []
            try:
                found = await projects_of(dataset, project_of, client)
                if not found:
                    raise LookupError("no BioProject found")
                rows = await harvest(dataset, found, by_dataset[dataset], client)
            except Exception as exc:
                # A failed dataset is retried on the next call: "!" marks it.
                rows = [
                    {
                        "dataset": dataset,
                        "sample": s,
                        "error": "!" + redact(f"{type(exc).__name__}: {exc}")[:200],
                    }
                    for s in sorted(by_dataset[dataset])
                ]
            with path.open("a", newline="") as handle:
                csv.DictWriter(handle, COLUMNS, restval="").writerows(rows)
            named = sum(bool(r.get("candidates")) for r in rows)
            print(
                f"{dataset:14s} {','.join(found) or '-':24s} {len(rows):4d} samples, {named} named",
                file=sys.stderr,
                flush=True,
            )


if __name__ == "__main__":
    asyncio.run(main())
