#!/usr/bin/env python3
"""Compare ``fetch links`` against fetch10xmeta's own nf-test snapshots.

Usage::

    uv run python scripts/parity_fetch10xmeta.py path/to/main.nf.test.snap

The snapshot records ``(run, species, type, sample)`` per run and deliberately
omits the URLs, because SDL hands out signed links that rotate. That omission is
what makes the comparison the right one: it asks which files were *chosen*, not
which mirror served them.

Reads the incumbent's ``.snap`` directly rather than carrying a copy, so the
comparison stays true as the incumbent's own tests change. Cases are matched on
the snapshot's test name; a name that is not in the map below is skipped and
reported, which is how a new incumbent test announces itself.
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

from fetch_series.core import default_api_key
from fetch_series.filesets import links_table
from fetch_series.logging_utils import configure_logging
from fetch_series.relations import resolve_input
from fetch_series.survey.client import Limits, SurveyClient

# Every case except "GEO - stub", which asserts file names rather than content.
# A value is the accession, or (accession, sample filter) for the cases that
# exercise the module's sample_ids argument.
CASES: dict[str, str | tuple[str, str]] = {
    "GEO - ENA paired-end fastq": "GSE111360",
    "GEO - ENA paired-end fastq - mouse": "GSE160513",
    "GEO - one run per sample": "GSE250130",
    "GEO - mixed ENA fastq and SRA archive runs": "GSE264508",
    "GEO - SRA archive only": "GSE117988",
    "GEO - 10x BAM": "GSE274955",
    "ArrayExpress - submitter fastq from the SDRF": "E-MTAB-9221",
    "ArrayExpress - SDRF fastq URIs the study does not register": "E-MTAB-8060",
    "BioProject - samples taken straight from ENA": "PRJNA511433",
    "GEO - family file without SRA relations - all samples": "GSE135325",
    "GEO - family file without SRA relations - twelve samples": "GSE137444",
    "GEO - subset of samples": ("GSE117988", "GSM3330564,GSM3330560"),
    "ArrayExpress - subset of samples": ("E-MTAB-9221", "ERS4689152,ERS4689153"),
    "GEO - family file without SRA relations - subset of samples": (
        "GSE135325",
        "GSM4005490,GSM4005491",
    ),
}


async def main(snapshot_path: Path) -> int:
    configure_logging(level="WARNING")
    snapshots = json.loads(snapshot_path.read_text())
    report: dict[str, dict[str, object]] = {}

    async with SurveyClient(Limits(rps=5, concurrency=6), api_key=default_api_key()) as client:
        for name, case in CASES.items():
            accession, wanted = case if isinstance(case, tuple) else (case, None)
            entry = snapshots.get(name)
            if entry is None:
                # Not a failure: a test can assert without snapshotting, and
                # E-MTAB-8060 does exactly that.
                report[name] = {"accession": accession, "status": "no snapshot to diff"}
                continue
            expected = {tuple(row) for row in entry["content"][0]}
            rows = await links_table(resolve_input(accession), client)
            if wanted:
                keep = {token.strip() for token in wanted.split(",")}
                rows = [row for row in rows if row.sample in keep]
            got = {(r.run, r.species, r.type, r.sample) for r in rows}
            report[name] = {
                "accession": accession,
                "expected": len(expected),
                "got": len(got),
                "identical": expected == got,
                "missing": sorted(expected - got)[:8],
                "extra": sorted(got - expected)[:8],
            }
            print(f"{'OK  ' if expected == got else 'DIFF'}  {name}  ({len(got)} runs)", flush=True)

    diffs = [name for name, r in report.items() if r.get("identical") is False]
    matched = sum(1 for r in report.values() if r.get("identical"))
    print(f"\n{matched} of {len(CASES)} snapshots reproduced exactly")
    for name in diffs:
        print(json.dumps({name: report[name]}, indent=2))
    return 1 if diffs else 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    raise SystemExit(asyncio.run(main(Path(sys.argv[1]))))
