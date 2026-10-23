"""HEAD every ENA fastq link a census listed, and count the links that do not serve their file.

Usage::

    uv run python scripts/ena_listing_census.py \\
        --census data/surveys/sample_3000@reprocessed-srr/run-to-file_ena_fastq.csv.gz \\
        --out data/benchmarks/ena-listing/<date>

The census recorded what ENA's ``filereport`` *lists* for 3,000 runs. This asks
the file server about every listed link, as ``fetch files --verify`` does: a
HEAD, redirects followed, through ``files.request_url``. Per link it records the
status, whether the server answered with a directory (SRR11040464's
``_1.fastq.gz``), and the served size against the size ENA published. Counts,
links and sizes, nothing read. Resumable.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import gzip
import sys
from collections import Counter
from pathlib import Path

from fetch_series.files import FileKind, FileRecord
from fetch_series.filesets import verify
from fetch_series.logging_utils import configure_logging, run_logfile
from fetch_series.providers import ena_portal
from fetch_series.survey.client import Limits, SurveyClient

csv.field_size_limit(sys.maxsize)
COLUMNS = (
    "run",
    "url",
    "published_bytes",
    "status",
    "served_bytes",
    "directory",
    "final_url",
    "error",
)


def listed(census: Path) -> list[str]:
    with gzip.open(census, "rt") as handle:
        return [r["accession"] for r in csv.DictReader(handle) if r["outcome"] == "resolved"]


async def records_of(run: str, client: SurveyClient) -> list[FileRecord]:
    """ENA's fastq links and published sizes for one run, as listed today."""
    rows = await ena_portal.read_run_report(
        client, run, 60.0, fields=("run_accession", "fastq_ftp", "fastq_bytes")
    )
    out = []
    for row in rows:
        links = [u for u in str(row.get("fastq_ftp") or "").split(";") if u]
        sizes = [s for s in str(row.get("fastq_bytes") or "").split(";")]
        for i, link in enumerate(links):
            size = sizes[i] if i < len(sizes) and sizes[i].isdigit() else None
            url = link if "://" in link else f"https://{link}"
            out.append(
                FileRecord(
                    run,
                    url,
                    url.rsplit("/", 1)[-1],
                    FileKind.FASTQ,
                    "ena",
                    size=int(size) if size else None,
                )
            )
    return out


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--census", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--rps", type=float, default=4.0)
    args = parser.parse_args()
    configure_logging(logfile=run_logfile("ena-listing-census"))
    args.out.mkdir(parents=True, exist_ok=True)
    path = args.out / "links.csv"
    done: set[str] = set()
    if path.exists():
        with path.open() as handle:
            done = {r["run"] for r in csv.DictReader(handle)}
    else:
        with path.open("w", newline="") as handle:
            csv.DictWriter(handle, COLUMNS).writeheader()
    runs = [r for r in listed(args.census) if r not in done]
    tally: Counter[str] = Counter()
    async with SurveyClient(Limits(rps=args.rps, concurrency=6, base_timeout=60)) as client:

        async def one(run: str) -> list[dict[str, object]]:
            try:
                records = await records_of(run, client)
            except Exception as exc:
                return [{"run": run, "error": f"filereport: {type(exc).__name__}"}]
            rows = []
            for record in records:
                verdict = await verify(record, client)
                rows.append(
                    {
                        "run": run,
                        "url": record.url,
                        "published_bytes": record.size,
                        "status": verdict.status,
                        "served_bytes": verdict.served_bytes,
                        "directory": verdict.served_directory,
                        "final_url": verdict.final_url or "",
                        "error": verdict.error or "",
                    }
                )
            return rows or [{"run": run, "error": "no fastq listed today"}]

        for start in range(0, len(runs), 24):
            batch = await asyncio.gather(*(one(r) for r in runs[start : start + 24]))
            with path.open("a", newline="") as handle:
                writer = csv.DictWriter(handle, COLUMNS, restval="")
                for rows in batch:
                    writer.writerows(rows)
                    for row in rows:
                        tally[
                            "directory"
                            if row.get("directory")
                            else str(row.get("status") or row.get("error"))
                        ] += 1
            print(f"{start + len(batch)}/{len(runs)} {dict(tally)}", file=sys.stderr, flush=True)


if __name__ == "__main__":
    asyncio.run(main())
