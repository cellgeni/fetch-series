"""EBI BioStudies and ArrayExpress.

ArrayExpress studies describe themselves in two files: an IDF of study-level
metadata and an SDRF table of samples. Neither is guaranteed to carry the link
you want, which is why the fallback chain matters -- E-MTAB-6505 declares no
secondary accession at all and is only reachable through its BioSamples.
"""

from __future__ import annotations

import csv
import io
import re
from typing import Any

from fetch_series.survey.client import SurveyClient

FILES = "https://www.ebi.ac.uk/biostudies/files"
API = "https://www.ebi.ac.uk/biostudies/api/v1"

# Accepts ERP/SRP/DRP and EGA study accessions.
SECONDARY = re.compile(r"Comment\s*\[SecondaryAccession\]\s*((?:[SED]RP|EGA[SD])\w*\d+)")

# BioStudies pages silently: with no explicit limit it returns the first 25 of
# however many there are, which would have declared 15 of E-MTAB-9221's 40
# registered fastq files unregistered.
PAGE_SIZE = 500


async def idf_secondary_accessions(
    client: SurveyClient, accession: str, timeout: float
) -> list[str]:
    """Secondary study accessions declared in a study's IDF file."""
    response = await client.get(f"{FILES}/{accession}/{accession}.idf.txt", timeout=timeout)
    return sorted(set(SECONDARY.findall(response.text)))


async def sdrf_rows(client: SurveyClient, accession: str, timeout: float) -> list[dict[str, Any]]:
    """The SDRF sample table as row dicts."""
    response = await client.get(f"{FILES}/{accession}/{accession}.sdrf.txt", timeout=timeout)
    text = response.text.strip()
    if not text:
        return []
    return list(csv.DictReader(io.StringIO(text), delimiter="\t"))


def sdrf_column(rows: list[dict[str, Any]], name: str) -> list[str]:
    """Distinct values of an SDRF column.

    ``csv.DictReader`` fills absent trailing columns with None, so the key can be
    present with a None value; short rows are common in hand-edited SDRFs.
    """
    values = {
        value.strip() for row in rows if (value := row.get(name)) is not None and value.strip()
    }
    return sorted(values)


async def registered_files(
    client: SurveyClient, accession: str, timeout: float
) -> list[dict[str, Any]]:
    """Every file a study actually registers with BioStudies, paged in full."""
    collected: list[dict[str, Any]] = []
    offset = 0
    while True:
        response = await client.get(
            f"{API}/studies/{accession}/files",
            params={"limit": PAGE_SIZE, "offset": offset},
            timeout=timeout,
        )
        payload = response.json()
        files = payload.get("files", payload if isinstance(payload, list) else [])
        collected.extend(files)
        total = (
            (payload.get("pagination") or {}).get("total") if isinstance(payload, dict) else None
        )
        offset += PAGE_SIZE
        if not files or total is None or offset >= total:
            break
    return collected
