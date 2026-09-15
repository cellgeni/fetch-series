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

import re
from typing import Any

from fetch_series.survey.client import MalformedResponseError, SurveyClient

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
) -> str:
    """EFetch returning text (runinfo CSV, typically)."""
    response = await client.get(
        f"{BASE}/efetch.fcgi",
        params={
            "db": db,
            "id": ",".join(uids) if uids else None,
            "WebEnv": webenv,
            "query_key": query_key,
            "rettype": rettype,
            "retmode": "text",
            "api_key": client.api_key,
        },
        timeout=timeout,
    )
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
