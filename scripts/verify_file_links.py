#!/usr/bin/env python3
"""Do the links this tool hands you actually work?

The milestone-2 exit question, asked at scale. `fetch files --verify` answers it
for one accession; this answers it for a sample, and records the verdicts so the
rate can be compared against itself later.

Verification is a HEAD, not a download: the published md5 cannot be checked
without fetching the bytes, and at this scale that is a mirror rather than a
survey. What a HEAD does catch is the two failures that matter most -- a link
into a decommissioned mirror, and a file replaced without its metadata being
updated.

Only the *recommended* set is verified. Checking every alternative triples the
requests to test links the caller was never going to use, and the recommendation
is the thing this project is claiming is good.

Usage::

    uv run python scripts/verify_file_links.py [n] [corpus]
"""

from __future__ import annotations

import asyncio
import csv
import gzip
import sys
from collections import Counter
from pathlib import Path

from dotenv import load_dotenv

from fetch_series.core import default_api_key
from fetch_series.files import recommend
from fetch_series.filesets import files_for_run, verify
from fetch_series.logging_utils import configure_logging, run_logfile
from fetch_series.survey.client import Limits, SurveyClient
from fetch_series.survey.corpora import load

OUT = Path("data/surveys/file-links")
COLUMNS = (
    "run",
    "source",
    "kind",
    "mate",
    "url",
    "status",
    "published_bytes",
    "served_bytes",
    "size_matches",
    "error",
)


async def main(n: int, corpus_name: str) -> None:
    configure_logging(level="WARNING", logfile=run_logfile("file-link-verification"))
    corpus = load(f"sample:{n}@{corpus_name}")
    runs = list(corpus.values)
    print(f"verifying the recommended file set for {len(runs):,} runs", flush=True)

    OUT.mkdir(parents=True, exist_ok=True)
    verdicts: Counter[str] = Counter()
    # Bounded separately from the client's own limiter: one run means up to four
    # metadata requests and then a HEAD per file, and EBI should not see all of
    # a 3,000-run sample's worth of that at once.
    gate = asyncio.Semaphore(6)
    rows: list[list[object]] = []

    async def one(run: str) -> None:
        async with gate:
            try:
                fileset = await files_for_run(run, client)
            except Exception as exc:
                verdicts[f"metadata failed: {type(exc).__name__}"] += 1
                return
            recommendation = recommend(fileset)
            if recommendation.chosen is None:
                verdicts["no route offered a data file"] += 1
                return
            for record in recommendation.chosen.files:
                outcome = await verify(record, client)
                if outcome.error:
                    verdicts[f"request failed: {outcome.error}"] += 1
                elif not outcome.resolves:
                    verdicts[f"HTTP {outcome.status}"] += 1
                elif outcome.size_matches is False:
                    verdicts["resolves, size disagrees"] += 1
                elif outcome.size_matches is None:
                    verdicts["resolves, size not checkable"] += 1
                else:
                    verdicts["resolves, size matches"] += 1
                rows.append(
                    [
                        run,
                        record.source,
                        str(record.kind),
                        record.mate if record.mate is not None else "",
                        record.url,
                        outcome.status if outcome.status is not None else "",
                        record.size if record.size is not None else "",
                        outcome.served_bytes if outcome.served_bytes is not None else "",
                        "" if outcome.size_matches is None else outcome.size_matches,
                        outcome.error or "",
                    ]
                )

    async with SurveyClient(Limits(rps=8, concurrency=8), api_key=default_api_key()) as client:
        await asyncio.gather(*(one(run) for run in runs))

    path = OUT / f"sample_{n}@{corpus_name.replace(':', '_')}.csv.gz"
    with gzip.open(path, "wt", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(COLUMNS)
        writer.writerows(rows)

    total = sum(verdicts.values())
    print(f"\n{len(rows):,} links verified across {len(runs):,} runs -> {path}")
    for verdict, count in verdicts.most_common():
        print(f"  {count:>7,}  {count / total:>6.2%}  {verdict}")


if __name__ == "__main__":
    load_dotenv()
    size = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    asyncio.run(main(size, sys.argv[2] if len(sys.argv) > 2 else "reprocessed-srr"))
