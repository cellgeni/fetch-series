"""Detect-and-name benchmark: does the read classifier name the technology a run's metadata names?

Usage::

    uv run python scripts/benchmark_technologies.py --out data/benchmarks/inspect/technologies-<date>

1. **Labels.** For each technology, ENA runs whose ``library_construction_protocol``
   names it, searched by the terms in ``LABELS``. A protocol that also names
   10x/Chromium or another technology's terms is not a label for either. Runs
   are drawn seeded, one per study before a second from any, up to ``--per`` a
   technology. A 10x control is drawn from the reprocessed corpus, which is 10x
   by construction.
2. **Reads.** The first 4 MiB of each of a run's ENA fastqs -- tens of thousands
   of reads, more than the 5,000 detection looks at -- read with the same reader
   ``fetch inspect`` uses. Only ENA's fastqs: what is measured is the
   classifier, not the archives.
3. **Calls.** ``classify`` with every fetched list and reference: the
   technology it names, and any 10x layout.

Writes ``labels.csv`` and ``calls-<classifier>.csv``, resumable, with counts and calls and
never a sequence. The report is ``scripts/benchmark_technologies_report.py``.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import random
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from fetch_series.inspect import lists, references
from fetch_series.inspect.classify import VERSION, classify
from fetch_series.inspect.readers.fastq import read_fastq
from fetch_series.logging_utils import configure_logging, redact, run_logfile
from fetch_series.survey import corpora
from fetch_series.survey.client import Limits, SurveyClient

SEED = 20261022
PREFIX = 4 * 1024 * 1024
ENA_SEARCH = "https://www.ebi.ac.uk/ena/portal/api/search"
ENA_FILEREPORT = "https://www.ebi.ac.uk/ena/portal/api/filereport"

# label -> (ENA protocol queries, the table rows that name it). An empty set
# means the technology has no constant to read: the right answer is unknown.
LABELS: dict[str, tuple[tuple[str, ...], frozenset[str]]] = {
    "parse_evercode": (("*Evercode*",), frozenset({"microsplit_parse", "splitseq_family"})),
    "bd_rhapsody": (("*Rhapsody*",), frozenset({"bd_rhapsody", "bd_rhapsody_enhanced"})),
    "indrop": (("*inDrop*",), frozenset({"indrop"})),
    "hydrop": (("*HyDrop*",), frozenset({"hydrop_rna"})),
    "splitseq": (
        ("*splitseq*", "*microSPLiT*"),
        frozenset({"splitseq", "splitseq_preprint", "microsplit_parse", "splitseq_family"}),
    ),
    "microwell_seq": (("*Microwell*",), frozenset({"microwell_seq"})),
    "seq_well": (("*Seq*Well*",), frozenset()),
    "drop_seq": (("*Dropseq*",), frozenset()),
}
# Words that make a protocol name more than one technology.
OTHERS = (
    "10x",
    "chromium",
    "evercode",
    "rhapsody",
    "indrop",
    "hydrop",
    "split",
    "microwell",
    "seq-well",
    "seqwell",
    "drop-seq",
    "dropseq",
)
LABEL_COLUMNS = ("label", "run", "study", "fastqs")
CALL_COLUMNS = (
    "label",
    "run",
    "study",
    "expected",
    "files",
    "modes",
    "technology",
    "share",
    "layout",
    "level",
    "classifier",
    "error",
)


def _own_terms(label: str) -> tuple[str, ...]:
    queries = LABELS[label][0]
    return tuple(q.strip("*").lower().replace("*", "") for q in queries)


async def _search(client: SurveyClient, query: str) -> list[dict[str, str]]:
    params = {
        "result": "read_run",
        "query": f'library_construction_protocol="{query}"',
        "fields": "run_accession,study_accession,library_construction_protocol,fastq_ftp",
        "format": "tsv",
        "limit": "0",
    }
    response = await client.with_retry(
        lambda t: client.get(ENA_SEARCH, params=params, timeout=t), label=f"search {query}"
    )
    lines = response.text.splitlines()
    if not lines:
        return []
    header = lines[0].split("\t")
    return [dict(zip(header, line.split("\t"), strict=False)) for line in lines[1:]]


def _draw(rows: list[dict[str, str]], n: int, rng: random.Random) -> list[dict[str, str]]:
    """One run per study, seeded, before a second from any."""
    by_study: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        by_study.setdefault(row["study_accession"], []).append(row)
    studies = sorted(by_study)
    rng.shuffle(studies)
    for runs in by_study.values():
        runs.sort(key=lambda r: r["run_accession"])
        rng.shuffle(runs)
    drawn: list[dict[str, str]] = []
    while len(drawn) < n and any(by_study[s] for s in studies):
        for study in studies:
            if by_study[study] and len(drawn) < n:
                drawn.append(by_study[study].pop())
    return drawn


async def label(out: Path, per: int, controls: int, client: SurveyClient) -> list[dict[str, str]]:
    path = out / "labels.csv"
    if path.exists():
        with path.open() as handle:
            return list(csv.DictReader(handle))
    rng = random.Random(SEED)
    labelled: list[dict[str, str]] = []
    for name in LABELS:
        own = _own_terms(name)
        rows: dict[str, dict[str, str]] = {}
        for query in LABELS[name][0]:
            for row in await _search(client, query):
                protocol = row.get("library_construction_protocol", "").lower()
                foreign = [
                    w for w in OTHERS if w in protocol and not any(w in o or o in w for o in own)
                ]
                if row.get("fastq_ftp") and not foreign:
                    rows[row["run_accession"]] = row
        for row in _draw(list(rows.values()), per, rng):
            labelled.append(
                {
                    "label": name,
                    "run": row["run_accession"],
                    "study": row["study_accession"],
                    "fastqs": row["fastq_ftp"],
                }
            )
        print(
            f"{name}: {len(rows)} runs labelled, {sum(r['label'] == name for r in labelled)} drawn",
            file=sys.stderr,
        )
    control = corpora.load(f"sample:{controls}@reprocessed-srr")
    for accession in control.values:
        response = await client.with_retry(
            lambda t, a=accession: client.get(
                ENA_FILEREPORT,
                params={
                    "accession": a,
                    "result": "read_run",
                    "fields": "fastq_ftp,study_accession",
                    "format": "tsv",
                },
                timeout=t,
            ),
            label=f"filereport {accession}",
        )
        lines = response.text.splitlines()
        if len(lines) < 2:
            continue
        row = dict(zip(lines[0].split("\t"), lines[1].split("\t"), strict=False))
        if row.get("fastq_ftp"):
            labelled.append(
                {
                    "label": "10x",
                    "run": accession,
                    "study": row.get("study_accession", ""),
                    "fastqs": row["fastq_ftp"],
                }
            )
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, LABEL_COLUMNS)
        writer.writeheader()
        writer.writerows(labelled)
    return labelled


async def call_one(
    row: dict[str, str], client: SurveyClient, held: dict[str, Any], refs: dict[str, Any]
) -> dict[str, Any]:
    expected = (
        "10x" if row["label"] == "10x" else "|".join(sorted(LABELS[row["label"]][1])) or "unknown"
    )
    result: dict[str, Any] = {
        "label": row["label"],
        "run": row["run"],
        "study": row["study"],
        "expected": expected,
        "classifier": VERSION,
    }
    urls = [u for u in row["fastqs"].split(";") if u][:4]
    samples = []
    try:
        for url in urls:
            link = url if "://" in url else f"https://{url}"
            prefix = await client.with_retry(
                lambda t, link=link: client.stream_prefix(link, PREFIX, timeout=t),
                label=f"prefix {link}",
            )
            samples.append(
                read_fastq(prefix.data, link, max_reads=50_000, complete=prefix.complete)
            )
        call = classify(samples, held, refs)
        technology = call.evidence.get("technology") or {}
        result.update(
            files=len(urls),
            modes="+".join(
                str(max(set(s.lengths), key=s.lengths.count)) if s.records else "-" for s in samples
            ),
            technology=call.technology or "unknown",
            share=technology.get("share", ""),
            layout=call.layout or "|".join(call.candidates),
            level=call.level,
        )
    except Exception as exc:
        result["error"] = redact(f"{type(exc).__name__}: {exc}")[:200]
    return result


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--per", type=int, default=30)
    parser.add_argument("--controls", type=int, default=100)
    parser.add_argument("--rps", type=float, default=2.0)
    # A new classifier version is called over the same labels into its own
    # file: an old version's calls are a record, not something to overwrite.
    parser.add_argument("--calls", default=f"calls-{VERSION}.csv")
    args = parser.parse_args()
    load_dotenv()
    configure_logging(logfile=run_logfile("benchmark-technologies"))
    args.out.mkdir(parents=True, exist_ok=True)
    held = lists.available()
    refs = {k: v for k in references.REGISTER if (v := references.index(k)) is not None}
    if not held:
        sys.exit("fetch the lists first: uv run fetch inspect lists --fetch")
    async with SurveyClient(Limits(rps=args.rps, concurrency=4, base_timeout=120)) as client:
        labelled = await label(args.out, args.per, args.controls, client)
        path = args.out / args.calls
        done = set()
        if path.exists():
            with path.open() as handle:
                done = {r["run"] for r in csv.DictReader(handle) if not r["error"]}
        else:
            with path.open("w", newline="") as handle:
                csv.DictWriter(handle, CALL_COLUMNS).writeheader()
        for row in labelled:
            if row["run"] in done:
                continue
            result = await call_one(row, client, held, refs)
            with path.open("a", newline="") as handle:
                csv.DictWriter(handle, CALL_COLUMNS).writerow(result)
            print(
                f"{row['label']:15s} {row['run']:12s} {result.get('modes', ''):14s} "
                f"{result.get('technology', '')} {result.get('layout', '')} {result.get('error', '')}",
                file=sys.stderr,
                flush=True,
            )


if __name__ == "__main__":
    asyncio.run(main())
