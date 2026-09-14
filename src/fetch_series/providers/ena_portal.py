"""ENA portal API.

The single most complete route measured so far: over 12,756 BioProjects it
returned 784,026 unique runs, including 111,665 that NCBI's ``efetch`` never
does, while only about 1,800 runs were missing from it.

It also accepts almost any accession as the ``accession`` parameter -- project,
study, sample, experiment, run -- which makes one endpoint serve a large part of
the graph.
"""

from __future__ import annotations

import csv
import io
from typing import Any

from fetch_series.survey.client import SurveyClient

FILEREPORT = "https://www.ebi.ac.uk/ena/portal/api/filereport"
LINKS = "https://www.ebi.ac.uk/ena/portal/api/links"

# The identity columns. File columns (fastq_ftp, submitted_ftp, sra_ftp,
# fastq_md5) belong to the file layer and are deliberately not requested here.
IDENTITY_FIELDS = (
    "study_accession",
    "secondary_study_accession",
    "sample_accession",
    "secondary_sample_accession",
    "experiment_accession",
    "run_accession",
    "tax_id",
    "scientific_name",
    "library_strategy",
    "library_source",
)


async def read_run_report(
    client: SurveyClient,
    accession: str,
    timeout: float,
    *,
    fields: tuple[str, ...] = IDENTITY_FIELDS,
    result: str = "read_run",
) -> list[dict[str, Any]]:
    """Fetch a filereport as a list of row dicts.

    ``limit=0`` means no limit. Omitting it returns a truncated report without
    saying so.
    """
    response = await client.get(
        FILEREPORT,
        params={
            "accession": accession,
            "result": result,
            "fields": ",".join(fields),
            "format": "tsv",
            "limit": 0,
        },
        timeout=timeout,
    )
    text = response.text.strip()
    if not text:
        return []
    return list(csv.DictReader(io.StringIO(text), delimiter="\t"))


def column(rows: list[dict[str, Any]], field: str) -> list[str]:
    """Distinct non-empty values of one column, sorted."""
    return sorted({row[field].strip() for row in rows if row.get(field, "").strip()})
