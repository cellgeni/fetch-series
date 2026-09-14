"""The SRA backend CGI at trace.ncbi.nlm.nih.gov.

Not a documented API, but measurably more complete than ``efetch`` for the same
question: over 12,756 BioProjects it returned 754,277 unique runs against
efetch's 673,157, a gap of about 12%.

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


async def runinfo_by_history(
    client: SurveyClient, webenv: str, query_key: str, timeout: float
) -> list[dict[str, Any]]:
    """Fetch runinfo rows for an ESearch history."""
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
    rows = list(csv.DictReader(io.StringIO(body)))
    if rows and "Run" not in rows[0]:
        raise MalformedResponseError("response has no Run column; not a runinfo table")
    return rows


def column(rows: list[dict[str, Any]], field: str) -> list[str]:
    """Distinct non-empty values of one runinfo column, sorted.

    Deduplication is not cosmetic here. The 2026-05 survey's CGI results file
    holds 824,216 rows but only 754,277 distinct runs -- 69,923 rows repeat
    within a single BioProject -- so counting rows overstates this route's
    completeness by about 9%.
    """
    return sorted({row[field].strip() for row in rows if row.get(field, "").strip()})
