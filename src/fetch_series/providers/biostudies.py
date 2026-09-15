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


async def sdrf_raw_rows(client: SurveyClient, accession: str, timeout: float) -> list[list[str]]:
    """The SDRF as raw field lists, header included.

    SDRF repeats column names by design -- E-MTAB-9221 has two
    ``Comment[FASTQ_URI]`` columns, one per mate -- and ``csv.DictReader`` keeps
    only the last value for a repeated key. Reading that study through a dict
    silently returned one URI per run instead of two, which for paired reads is
    exactly half the data. Anything that reads a column which may repeat must
    use this, not ``sdrf_rows``.
    """
    response = await client.get(f"{FILES}/{accession}/{accession}.sdrf.txt", timeout=timeout)
    text = response.text.strip()
    if not text:
        return []
    return [line.split("\t") for line in text.splitlines()]


async def sdrf_rows(client: SurveyClient, accession: str, timeout: float) -> list[dict[str, Any]]:
    """The SDRF sample table as row dicts.

    Convenient, and lossy for any column name that repeats -- see
    :func:`sdrf_raw_rows`.
    """
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
        # The list is under `items`, and `files` is not an alias for it -- a
        # .get("files", []) returns an empty list and no error, which read as
        # "this study registers nothing". For the guard that uses this list,
        # registering nothing is the finding, so the bug was invisible: it
        # turned every study into E-MTAB-8060.
        if isinstance(payload, list):
            page = payload
            total = len(page)
        else:
            page = payload.get("items") or []
            total = (payload.get("pagination") or {}).get("total", len(page))
        collected.extend(page)
        offset += PAGE_SIZE
        if not page or offset >= total:
            break
    return collected


def registered_names(files: list[dict[str, Any]]) -> set[str]:
    """The bare filenames a study registers, for comparing against SDRF URIs."""
    return {
        str(entry.get("path") or entry.get("Name") or "").rsplit("/", 1)[-1]
        for entry in files
        if entry.get("path") or entry.get("Name")
    }


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


# The pre-BioStudies ArrayExpress mirror. A SDRF URI under this path is a stale
# spelling that means two different things depending on whether the study still
# registers the file, which is why it can never be taken at face value.
AE_MIRROR = re.compile(r"/pub/databases/(microarray|arrayexpress)/data/experiment/")

# Anything fastq-shaped, however the submitter spelled it.
FASTQ_URI = re.compile(r"(?:ftp|https?)://\S*\.f(?:ast)?q(?:\.gz)?\b", re.IGNORECASE)


def sdrf_fastq_uris(
    rows: list[list[str]], accession: str, registered: set[str] | None
) -> dict[str, list[str]]:
    """Submitter fastq URIs from an SDRF, keyed by the run they belong to.

    Takes raw field lists, not row dicts: SDRF repeats ``Comment[FASTQ_URI]``
    once per mate, and a dict keeps only the last. Columns are not standardised
    anyway, so URIs are found by shape rather than by name and a row is
    attributed to whatever run accession it mentions.

    A URI under the decommissioned pre-BioStudies mirror is believed only if
    the study still registers a file of that name, and rewritten to the
    BioStudies path when it does. E-MTAB-8060 and E-MTAB-9221 are
    indistinguishable at this point -- both point every fastq URI at the mirror
    and both also declare a BAM -- and yet 9221 registers all 40 of its fastqs
    while 8060 registers none of its 36. Taking 8060's URIs at face value
    discarded the real BAM for all 15 of its runs, silently.

    ``registered`` of None means the file list could not be fetched. Every URI
    is then kept as written: an unreachable API must not be able to reroute a
    whole study to its BAMs.
    """
    by_run: dict[str, list[str]] = {}
    for row in rows:
        line = "\t".join(field for field in row if field)
        uris = FASTQ_URI.findall(line)
        if not uris:
            continue
        runs = sorted(set(re.findall(r"\b[SED]RR\d+\b", line)))
        if not runs:
            continue
        kept: list[str] = []
        for uri in uris:
            if registered is None or not AE_MIRROR.search(uri):
                kept.append(uri)
                continue
            name = uri.rsplit("/", 1)[-1]
            if name in registered:
                kept.append(f"{FILES}/{accession}/{name}")
        for run in runs:
            by_run.setdefault(run, []).extend(kept)
    return {run: sorted(dict.fromkeys(uris)) for run, uris in by_run.items() if uris}
