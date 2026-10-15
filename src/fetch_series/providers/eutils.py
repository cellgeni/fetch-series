"""Async NCBI E-utilities access.

The sync wrappers in ``core.py`` remain for interactive use; these are the ones
surveys and the resolver run on, because they take a :class:`SurveyClient` and
so inherit its rate limiting, bounded concurrency and retry policy.

Two behaviours are encoded here rather than at each call site:

* **Errors arrive as HTTP 200.** E-utilities reports "Too many UIDs in request.
  Maximum number of UIDs is 500 for JSON format output" as a perfectly ordinary
  200 with a JSON body that has an ``error`` key and no ``result``. Reading
  ``summary["result"]`` then raises ``KeyError`` from deep inside a parser.
  :func:`require_result` turns that into a typed error instead.
* **Histories are session-scoped.** ``WebEnv``/``query_key`` cannot be resumed,
  so callers must retry the whole chain. Nothing here retries individually.
"""

from __future__ import annotations

import logging
import re

# Parsed with the standard library. NCBI is a trusted source and these payloads
# declare no entities, but ElementTree expands internal entities, so this is not
# the parser to point at an arbitrary URL.
import xml.etree.ElementTree as ElementTree
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from fetch_series.survey.client import MalformedResponseError, SurveyClient

logger = logging.getLogger(__name__)

BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

# esummary refuses more than this many UIDs per JSON request, and says so with
# a 200. Anything paging UIDs must respect it.
MAX_UIDS_PER_SUMMARY = 500

# Above this many UIDs the request goes by POST. A UID plus its separator is
# about nine characters, so a few hundred already approach the ~4 KB that
# servers commonly accept in a URI.
POST_THRESHOLD_UIDS = 200


def require_result(payload: dict[str, Any], context: str) -> dict[str, Any]:
    """Return ``payload["result"]``, or raise a typed error explaining why not."""
    if "result" not in payload:
        raise MalformedResponseError(
            f"esummary returned no 'result' for {context}: "
            f"{payload.get('error') or payload.get('esummaryresult') or payload}"
        )
    result = payload["result"]
    if not isinstance(result, dict):
        raise MalformedResponseError(f"esummary 'result' is not an object for {context}")
    return result


async def esearch(
    client: SurveyClient,
    db: str,
    term: str,
    timeout: float,
    *,
    usehistory: bool = True,
    retmax: int | None = None,
) -> dict[str, Any]:
    """Run an ESearch and return its ``esearchresult`` block."""
    response = await client.get(
        f"{BASE}/esearch.fcgi",
        params={
            "db": db,
            "term": term,
            "retmode": "json",
            "usehistory": "y" if usehistory else None,
            "retmax": retmax,
            "api_key": client.api_key,
        },
        timeout=timeout,
    )
    payload = response.json()
    if "esearchresult" not in payload:
        raise MalformedResponseError(f"esearch returned no 'esearchresult' for {term!r}")
    result: dict[str, Any] = payload["esearchresult"]
    if "ERROR" in result:
        raise MalformedResponseError(f"esearch error for {term!r}: {result['ERROR']}")
    return result


async def esummary_by_ids(
    client: SurveyClient, db: str, uids: list[str], timeout: float
) -> dict[str, Any]:
    """ESummary for an explicit UID list, batched under the 500-UID ceiling."""
    merged: dict[str, Any] = {}
    for start in range(0, len(uids), MAX_UIDS_PER_SUMMARY):
        batch = uids[start : start + MAX_UIDS_PER_SUMMARY]
        params = {
            "db": db,
            "id": ",".join(batch),
            "retmode": "json",
            "api_key": client.api_key,
        }
        # A GET carries the UID list in the query string, and a few hundred of
        # them overflow the server's URI limit: GSE115931 and GSE111860 both
        # came back 414, which is not retryable, so the series was simply lost.
        if len(batch) > POST_THRESHOLD_UIDS:
            response = await client.post(f"{BASE}/esummary.fcgi", data=params, timeout=timeout)
        else:
            response = await client.get(f"{BASE}/esummary.fcgi", params=params, timeout=timeout)
        merged.update(require_result(response.json(), f"{db} uids {batch[0]}..."))
    merged.pop("uids", None)
    return merged


async def esummary_by_history(
    client: SurveyClient,
    db: str,
    webenv: str,
    query_key: str,
    count: int,
    timeout: float,
) -> dict[str, Any]:
    """ESummary over a history, paged.

    The page size is the same 500-UID ceiling. Requesting a history of 600
    records in one go returns the "Too many UIDs" error, not the first 500 --
    so this is a correctness requirement, not an optimisation.
    """
    merged: dict[str, Any] = {}
    for retstart in range(0, max(count, 1), MAX_UIDS_PER_SUMMARY):
        response = await client.get(
            f"{BASE}/esummary.fcgi",
            params={
                "db": db,
                "WebEnv": webenv,
                "query_key": query_key,
                "retstart": retstart,
                "retmax": MAX_UIDS_PER_SUMMARY,
                "retmode": "json",
                "api_key": client.api_key,
            },
            timeout=timeout,
        )
        merged.update(require_result(response.json(), f"{db} history {webenv}"))
    merged.pop("uids", None)
    return merged


async def elink_uids(
    client: SurveyClient,
    dbfrom: str,
    db: str,
    uids: list[str],
    timeout: float,
    *,
    linkname: str | None = None,
) -> list[str]:
    """ELink with ``cmd=neighbor``, returning target UIDs directly.

    Deliberately not ``cmd=neighbor_history``. ELink can return a valid history
    response with no usable ``querykey``; all 27 BioProjects that hit that in the
    2026-05 survey resolved through this direct form. "No usable history link"
    is not the same as "no link".
    """
    response = await client.get(
        f"{BASE}/elink.fcgi",
        params={
            "dbfrom": dbfrom,
            "db": db,
            "id": ",".join(uids),
            "retmode": "json",
            "cmd": "neighbor",
            "linkname": linkname,
            "api_key": client.api_key,
        },
        timeout=timeout,
    )
    payload = response.json()
    if "linksets" not in payload:
        raise MalformedResponseError(f"elink returned no 'linksets' for {dbfrom}->{db}")

    # A single ELink response lists the same neighbours under several link
    # names -- bioproject->biosample comes back as bioproject_biosample,
    # _biosample_all and _biosample_sp, each holding the identical UIDs -- so
    # concatenating them triples the list and triples the ESummary payload.
    found: set[str] = set()
    for linkset in payload["linksets"]:
        for setdb in linkset.get("linksetdbs") or []:
            if linkname and setdb.get("linkname") != linkname:
                continue
            if setdb.get("dbto") != db:
                continue
            found.update(str(uid) for uid in setdb.get("links") or [])
    return sorted(found)


async def efetch_text(
    client: SurveyClient,
    db: str,
    timeout: float,
    *,
    uids: list[str] | None = None,
    webenv: str | None = None,
    query_key: str | None = None,
    rettype: str = "runinfo",
    retmode: str = "text",
) -> str:
    """EFetch returning text (runinfo CSV, typically).

    Large UID lists go by POST, for the same reason ESummary's do. GSE241770
    links to enough SRA records that the joined list exceeded httpx's own URI
    limit -- the request was refused as ``InvalidURL`` before it was sent, so
    the series was lost without the archive ever being asked. That is the fifth
    time a UID list in a query string has cost this project data.
    """
    params: dict[str, Any] = {
        "db": db,
        "id": ",".join(uids) if uids else None,
        "WebEnv": webenv,
        "query_key": query_key,
        "rettype": rettype,
        "retmode": retmode,
        "api_key": client.api_key,
    }
    if uids and len(uids) > POST_THRESHOLD_UIDS:
        clean = {k: v for k, v in params.items() if v is not None}
        response = await client.post(f"{BASE}/efetch.fcgi", data=clean, timeout=timeout)
    else:
        response = await client.get(f"{BASE}/efetch.fcgi", params=params, timeout=timeout)
    return response.text


# ESearch returns 20 UIDs when asked for no particular number. That default is
# the same trap as ESummary's 500-UID ceiling and BioStudies' 25-item page: a
# silent truncation that looks like a complete, successful answer.
ESEARCH_PAGE = 500


async def esearch_all_uids(client: SurveyClient, db: str, term: str, timeout: float) -> list[str]:
    """Every UID matching ``term``, paged.

    Never use ``esearch(...)["idlist"]`` directly when the caller wants all of
    them: without ``retmax`` it silently returns the first 20.
    """
    first = await esearch(client, db=db, term=term, timeout=timeout, retmax=ESEARCH_PAGE)
    uids = [str(uid) for uid in first.get("idlist") or []]
    try:
        total = int(first.get("count", len(uids)))
    except (TypeError, ValueError):
        total = len(uids)

    for retstart in range(ESEARCH_PAGE, total, ESEARCH_PAGE):
        page = await client.get(
            f"{BASE}/esearch.fcgi",
            params={
                "db": db,
                "term": term,
                "retmode": "json",
                "retstart": retstart,
                "retmax": ESEARCH_PAGE,
                "api_key": client.api_key,
            },
            timeout=timeout,
        )
        result = page.json().get("esearchresult", {})
        uids.extend(str(uid) for uid in result.get("idlist") or [])
    return uids


async def epost_history(
    client: SurveyClient, db: str, uids: list[str], timeout: float
) -> tuple[str, str]:
    """Upload a UID list and return its ``(WebEnv, query_key)``.

    EPost exists precisely so a large UID set need not travel in a query string.
    Rebuilding a history by joining UIDs into an ESearch term produces a
    many-kilobyte GET and a non-retryable 414 on exactly the large projects that
    matter -- the same failure that cost 12,849 experiments through ESummary.
    """
    response = await client.post(
        f"{BASE}/epost.fcgi",
        data={"db": db, "id": ",".join(uids), "api_key": client.api_key},
        timeout=timeout,
    )
    text = response.text
    webenv = re.search(r"<WebEnv>(\S+)</WebEnv>", text)
    query_key = re.search(r"<QueryKey>(\d+)</QueryKey>", text)
    if not webenv or not query_key:
        raise MalformedResponseError(
            f"epost returned no usable history for {len(uids)} {db} uids: {text[:160]!r}"
        )
    return webenv.group(1), query_key.group(1)


# ---------------------------------------------------------------------------
# BioSample attributes
#
# The richest structured metadata any of these archives serves, and until now
# this project threw it away: `bioproject_to_biosample_elink` fetches ESummary
# records whose `sampledata` field holds the whole attribute block, and keeps
# only `accession`.
#
# ## `efetch db=biosample` does not accept accessions, and pretends it does
#
# This cost the most time of anything here and is the reason for the extra
# request. `id=SAMN12011228` appears to work. It does not: **efetch strips the
# alphabetic prefix and treats the remainder as a UID.** For a `SAMN`
# accession the number happens to *be* the UID -- SAMN12011228 is UID
# 12011228 -- so the bug is invisible on NCBI-registered samples, which is
# most of them.
#
# For anything else the number is a different, valid UID, and efetch returns a
# **different sample** with HTTP 200 and a full attribute block:
#
#     id=SAMEA11294529  ->  SAMN11294529, "Tissierellaceae bacterium", 14 attributes
#
# SAMEA11294529 is a human liver sample in E-MTAB-10553. SAMN11294529 is a
# rumen bacterium. Nothing in the response says a substitution happened; a
# count check passes, because one sample was asked for and one was returned.
# The real SAMEA11294529 is UID 23423599, which `esearch` finds correctly.
#
# So every accession is resolved through `esearch ... [accession]` first, and
# every returned record is checked against what was asked for. That affects
# every EBI (`SAMEA`) and DDBJ (`SAMD`) BioSample -- and this project's corpus
# is full of them: ENA finds 125,376 BioSamples NCBI's own link table does not.
#
# * **Attributes carry NCBI's own `harmonized_name`.** For `sampling enviroment`
#   -- the submitter's typo, preserved -- it is absent, so the raw name is the
#   fallback; but where present it is a curated key normalisation from the
#   archive itself, which is strictly better than guessing at one here.
# ---------------------------------------------------------------------------

BIOSAMPLE_BATCH = 200


@dataclass(frozen=True, slots=True)
class BioSampleAttribute:
    """One attribute pair, with both spellings kept."""

    #: NCBI's ``harmonized_name`` where it has one, else the submitter's own.
    name: str
    #: The submitter's spelling, always. Kept because the harmonised name hides
    #: what was actually written, and the evidence for a claim has to be the
    #: text the submitter wrote.
    raw_name: str
    value: str
    #: Whether NCBI supplied a ``harmonized_name``. Absent for keys it does not
    #: recognise, which includes every submitter typo -- ``sampling enviroment``
    #: is real, in SAMN12039425 -- so this distinguishes a curated key from one
    #: this project has to normalise itself.
    harmonised: bool = False


@dataclass(frozen=True, slots=True)
class BioSampleRecord:
    """What BioSample holds for one sample."""

    accession: str
    title: str | None = None
    organism: str | None = None
    taxon: str | None = None
    #: Free-text description paragraph, where the submitter wrote one.
    paragraph: str | None = None
    #: The BioSample package, e.g. ``Human.1.0`` or ``Generic.1.0``. NCBI's own
    #: declaration of what kind of thing the sample is, and the only
    #: non-guessing way to read a context-dependent attribute: the Human
    #: package defines ``isolate`` as the individual the sample came from,
    #: where the Microbe package means a strain isolate by it.
    package: str | None = None
    #: The submitter's own name for the sample, from ``Id[db_label="Sample name"]``.
    submitter_name: str | None = None
    attributes: tuple[BioSampleAttribute, ...] = ()
    #: Other archives' accessions for this same sample, as BioSample states
    #: them: ``{"SRA": "SRS4937050", "GEO": "GSM3880020"}``. Harvested as
    #: aliases, deliberately *not* declared as a route -- a route needs a
    #: survey, and nobody has measured this one.
    cross_links: tuple[tuple[str, str], ...] = ()


def _text(element: ElementTree.Element | None) -> str | None:
    if element is None or element.text is None:
        return None
    return " ".join(element.text.split()) or None


def _parse_biosample(node: ElementTree.Element) -> BioSampleRecord | None:
    accession = (node.get("accession") or "").strip()
    if not accession:
        return None
    organism = taxon = None
    if (organism_node := node.find("Description/Organism")) is not None:
        organism = (organism_node.get("taxonomy_name") or "").strip() or None
        taxon = (organism_node.get("taxonomy_id") or "").strip() or None

    submitter_name = None
    links: list[tuple[str, str]] = []
    for identifier in node.iter("Id"):
        value = _text(identifier)
        if not value:
            continue
        if (identifier.get("db_label") or "").strip().lower() == "sample name":
            submitter_name = value
        elif (db := (identifier.get("db") or "").strip()) and db != "BioSample":
            links.append((db, value))

    attributes: list[BioSampleAttribute] = []
    for attribute in node.iter("Attribute"):
        raw_name = (attribute.get("attribute_name") or "").strip()
        value = _text(attribute)
        if not raw_name or not value:
            continue
        harmonised = (attribute.get("harmonized_name") or "").strip()
        attributes.append(
            BioSampleAttribute(
                name=harmonised or raw_name,
                raw_name=raw_name,
                value=value,
                harmonised=bool(harmonised),
            )
        )

    return BioSampleRecord(
        accession=accession,
        package=_text(node.find("Package")),
        title=_text(node.find("Description/Title")),
        organism=organism,
        taxon=taxon,
        paragraph=_text(node.find("Description/Comment/Paragraph")),
        submitter_name=submitter_name,
        attributes=tuple(attributes),
        cross_links=tuple(links),
    )


def parse_biosample_set(xml: str) -> list[BioSampleRecord]:
    """Parse a ``BioSampleSet`` document into records.

    A malformed document raises :class:`MalformedResponseError` rather than
    ``ParseError``: E-utilities answers several errors with a 200 and a body
    that is not the document asked for, and a parser exception escaping from
    here would be indistinguishable from a bug in this module.
    """
    try:
        root = ElementTree.fromstring(xml)
    except ElementTree.ParseError as exc:
        raise MalformedResponseError(f"biosample efetch returned unparseable XML: {exc}") from None
    return [record for node in root.iter("BioSample") if (record := _parse_biosample(node))]


async def biosample_uids(
    client: SurveyClient, accessions: Sequence[str], timeout: float
) -> list[str]:
    """Resolve BioSample accessions to UIDs, batched.

    Searched on the ``[accession]`` field rather than as bare terms, so a
    number cannot match a UID or an unrelated field. The term goes by POST once
    it is long: 200 accessions is over 5 KB, and a UID list in a query string
    has cost this project data five times already.

    Returns UIDs without saying which accession each belongs to. ESearch
    appears to answer in query order and that is not relied on -- the records
    carry their own accession, and :func:`fetch_biosamples` checks them.
    """
    terms = " OR ".join(f"{a}[accession]" for a in accessions)
    params: dict[str, Any] = {
        "db": "biosample",
        "term": terms,
        "retmode": "json",
        "retmax": MAX_UIDS_PER_SUMMARY,
        "api_key": client.api_key,
    }
    if len(accessions) > POST_THRESHOLD_UIDS:
        clean = {k: v for k, v in params.items() if v is not None}
        response = await client.post(f"{BASE}/esearch.fcgi", data=clean, timeout=timeout)
    else:
        response = await client.get(f"{BASE}/esearch.fcgi", params=params, timeout=timeout)
    payload = response.json()
    if not isinstance(payload, dict) or "esearchresult" not in payload:
        raise MalformedResponseError(f"esearch returned no result for {len(accessions)} biosamples")
    return [str(uid) for uid in (payload["esearchresult"].get("idlist") or [])]


async def fetch_biosamples(
    client: SurveyClient, accessions: Sequence[str], timeout: float
) -> list[BioSampleRecord]:
    """BioSample records for ``accessions``, batched, verified.

    Two checks, both of which have caught something:

    * **Every returned accession must have been asked for.** This is what
      catches the prefix-stripping substitution described above; without it a
      rumen bacterium's attributes get attributed to a human liver sample.
    * **A short answer is logged.** A request returning 180 of 200 samples with
      no error is the same class of failure as BioStudies' silent 25-item page,
      and the only defence is counting.
    """
    wanted = list(dict.fromkeys(a for a in accessions if a))
    records: list[BioSampleRecord] = []
    for start in range(0, len(wanted), BIOSAMPLE_BATCH):
        batch = wanted[start : start + BIOSAMPLE_BATCH]
        uids = await biosample_uids(client, batch, timeout)
        if not uids:
            logger.warning("biosample esearch resolved none of %d accessions", len(batch))
            continue
        xml = await efetch_text(
            client, db="biosample", timeout=timeout, uids=uids, rettype="full", retmode="xml"
        )
        expected = set(batch)
        kept: list[BioSampleRecord] = []
        for record in parse_biosample_set(xml):
            if record.accession in expected:
                kept.append(record)
            else:
                # Not a paranoid check. See the module note: efetch answers
                # `id=SAMEA11294529` with SAMN11294529, a different organism.
                logger.warning(
                    "biosample efetch returned %s, which was not requested; discarded",
                    record.accession,
                )
        if missing := sorted(expected - {r.accession for r in kept}):
            logger.warning(
                "biosample returned %d of %d requested; missing %s",
                len(kept),
                len(batch),
                ", ".join(missing[:5]),
            )
        records.extend(kept)
    return records
