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
import re
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


# ENA carries the submitter's own names for a run, experiment and sample, and
# for a GEO-brokered submission those names are usually the GEO accession
# itself. That makes them a route from a run to its GSM that needs neither the
# SOFT family file nor a BioSample join -- and it works on GSE135325 and
# GSE137444, the two series where GEO records no SRA relation at all.
#
# `sample_alias` first: it is the GSM exactly in most cases, where the other two
# carry a `_r1` suffix. Anchored, and required to end at a non-alphanumeric, so
# a submitter name that merely starts with those letters cannot be mistaken for
# an accession. An alias is free text -- SRR27190799's is `E12` and
# DRR188691's is `SAMD00142600` -- so this route is expected to return nothing
# often, and must never guess.
ALIAS_FIELDS = ("sample_alias", "experiment_alias", "run_alias")
GSM_IN_ALIAS = re.compile(r"^(GSM\d+)(?![A-Za-z0-9])")
AE_IN_ALIAS = re.compile(r"^(E-[A-Z]{4}-\d+)(?![A-Za-z0-9])")


def alias_accessions(rows: list[dict[str, Any]], pattern: re.Pattern[str]) -> list[str]:
    """Accessions the submitter's own aliases carry, where they carry one."""
    found: set[str] = set()
    for row in rows:
        for field in ALIAS_FIELDS:
            if match := pattern.match((row.get(field) or "").strip()):
                found.add(match.group(1))
                break
    return sorted(found)


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
