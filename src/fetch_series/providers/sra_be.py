"""The SRA backend CGI at trace.ncbi.nlm.nih.gov.

Not a documented API. Over 12,755 BioProjects (2026-10-26) it returned 784,935
unique runs by ``[GPRJ]``, within 0.2% of ``efetch`` and of ENA, once its
history form's 5,000-run cap is worked around and a cut-off or empty answer is
failed rather than recorded (``docs/pathologies/sra-be-history-caps-at-5000-runs.md``).

It is driven from an E-utilities history rather than an accession list, so it
always follows an ESearch and inherits that history's session scoping -- retry
the whole chain, never this call alone.
"""

from __future__ import annotations

import csv
import io
from typing import Any

from fetch_series.survey.client import MalformedResponseError, SurveyClient

CGI = "https://trace.ncbi.nlm.nih.gov/Traces/sra-db-be/sra-db-be.cgi"
RUNINFO = "https://trace.ncbi.nlm.nih.gov/Traces/sra-db-be/runinfo"


# sra-db-be's history form answers at most 10,000 rows, and past 5,000 runs it
# repeats them: PRJEB82511's 7,263 runs come back as 10,000 rows of 5,000
# distinct runs, whatever retstart or retmax ask (2026-10-23). The by-accession
# endpoint answers all 7,263.
HISTORY_ROW_CAP = 10_000


def history_was_capped(rows: list[dict[str, Any]]) -> bool:
    """Whether a history answer hit the backend's cap: full, or repeating itself."""
    runs = [r.get("Run") for r in rows]
    return len(rows) >= HISTORY_ROW_CAP or len(set(runs)) < len(runs)


async def runinfo_by_history(
    client: SurveyClient, webenv: str, query_key: str, timeout: float
) -> list[dict[str, Any]]:
    """Fetch runinfo rows for an ESearch history. Check :func:`history_was_capped`."""
    response = await client.get(
        CGI,
        params={
            "rettype": "runinfo",
            "WebEnv": webenv,
            "query_key": query_key,
        },
        timeout=timeout,
    )
    return parse_runinfo(response.text)


async def runinfo_by_accession(
    client: SurveyClient, accession: str, timeout: float
) -> list[dict[str, Any]]:
    """Runinfo rows for one accession, with no ESearch in front.

    The backend resolves the accession itself, and accepts any of a project,
    study, BioSample, sample, experiment, run, GEO series or GEO sample --
    every one of GSE60450's family returns the same twelve rows or its own one.
    A submission accession is answered with an empty body, which
    :func:`parse_runinfo` reads as no runs.
    """
    response = await client.get(RUNINFO, params={"acc": accession}, timeout=timeout)
    return parse_runinfo(response.text)


def parse_runinfo(text: str) -> list[dict[str, Any]]:
    """Parse a runinfo CSV.

    An empty body is a legitimate answer -- the project has no runs -- but a
    non-empty body without a ``Run`` column is not: it means the backend
    returned something other than runinfo, and treating that as "no runs" would
    record a fabricated true negative.
    """
    body = text.strip()
    if not body:
        return []
    reader = csv.DictReader(io.StringIO(body))
    # Validate the header, not the first row. A one-line error page or a
    # header-only response yields zero rows, so a rows-based check waves it
    # through as a genuine "this project has no runs" -- precisely the
    # fabricated true negative this function exists to prevent.
    if reader.fieldnames is None or "Run" not in reader.fieldnames:
        raise MalformedResponseError(
            f"response has no Run column; not a runinfo table: {body[:120]!r}"
        )
    rows = list(reader)
    # A body cut off mid-stream ends in a short row. Under load the backend
    # stops after some minutes and closes cleanly: PRJEB14362's 46,468 runs came
    # back as 23,748 rows ending in a 6-field row, then 15,265 ending in a
    # 27-field one, each with HTTP 200 (2026-10-23). DictReader fills the missing
    # fields with None, which a complete row -- however many fields are empty --
    # never has.
    if rows and None in rows[-1].values():
        raise MalformedResponseError(
            f"runinfo cut off after {len(rows) - 1} complete rows; the last row is short"
        )
    return rows


def column(rows: list[dict[str, Any]], field: str) -> list[str]:
    """Distinct non-empty values of one runinfo column, sorted.

    Deduplication is not cosmetic here. The 2026-05 survey's CGI results file
    holds 824,216 rows but only 754,277 distinct runs -- 69,923 rows repeat
    within a single BioProject -- so counting rows overstates this route's
    completeness by about 9%.
    """
    return sorted({row[field].strip() for row in rows if row.get(field, "").strip()})
