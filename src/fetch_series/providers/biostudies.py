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
from datetime import date
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


SEARCH = f"{API}/{{collection}}/search"

# BioStudies caps pageSize; 100 is accepted everywhere and keeps a full
# enumeration of the ArrayExpress collection to a few hundred requests.
SEARCH_PAGE_SIZE = 100

# BioStudies serves at most this many hits per query, however they are paged,
# and answers anything past it with HTTP 500 -- not 400, and not a truncated
# page. A client that treats 5xx as transient retries a permanent condition
# forever. Enumerating a larger collection means partitioning the query.
SEARCH_WINDOW = 20_000

# Released-year is the one facet that partitions the whole ArrayExpress
# collection into windows small enough to page. 2001 is the first year with any
# content; the upper bound is open so a new year needs no code change.
FIRST_RELEASE_YEAR = 2001


async def search(
    client: SurveyClient,
    timeout: float,
    query: str | None = None,
    collection: str = "arrayexpress",
    facets: dict[str, str] | None = None,
    max_pages: int | None = None,
) -> list[str]:
    """Accessions matching a BioStudies search, paged to exhaustion.

    ``totalHits`` is authoritative and is used to stop, rather than trusting an
    empty page: BioStudies returns the first 25 hits and no warning when no
    explicit size is given, which is the pathology this project already
    documents for the files endpoint.
    """
    params: dict[str, str | int] = {"pageSize": SEARCH_PAGE_SIZE}
    if query:
        params["query"] = query
    for key, value in (facets or {}).items():
        params[key] = value

    accessions: list[str] = []
    page = 1
    while True:
        response = await client.get(
            SEARCH.format(collection=collection), params={**params, "page": page}, timeout=timeout
        )
        payload = response.json()
        hits = payload.get("hits") or []
        total = payload.get("totalHits", 0)
        if total > SEARCH_WINDOW and max_pages is None:
            raise ValueError(
                f"BioStudies reports {total:,} hits but serves only the first "
                f"{SEARCH_WINDOW:,}, then answers HTTP 500. Partition the query "
                f"(see search_by_year) rather than paging into the error."
            )
        accessions.extend(hit["accession"] for hit in hits if hit.get("accession"))
        if not hits or len(accessions) >= total:
            break
        page += 1
        if max_pages is not None and page > max_pages:
            break
    return accessions


async def search_by_year(
    client: SurveyClient,
    timeout: float,
    collection: str = "arrayexpress",
    facets: dict[str, str] | None = None,
    through_year: int | None = None,
) -> list[str]:
    """Enumerate a whole collection by partitioning on release year.

    Each year is well under the 20,000-hit window, so every partition pages
    cleanly. The partitions are disjoint and exhaustive by construction -- every
    study has exactly one release year -- so the union is the full collection
    and needs no deduplication beyond ordinary defensiveness.
    """
    last = through_year or date.today().year
    accessions: set[str] = set()
    for year in range(FIRST_RELEASE_YEAR, last + 1):
        accessions.update(
            await search(
                client,
                timeout,
                collection=collection,
                facets={**(facets or {}), "facet.released_year": str(year)},
            )
        )
    return sorted(accessions)
