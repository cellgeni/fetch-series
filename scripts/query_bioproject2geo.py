import asyncio
import logging
import os
from collections.abc import Callable, Coroutine
from typing import Any, Dict

import httpx
import pandas as pd
from dotenv import load_dotenv
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential
from tqdm.asyncio import tqdm_asyncio

load_dotenv()

NCBI_API_KEY = os.getenv("NCBI_API_KEY") or os.getenv("NCBI_KEY")
OUTPUT_DIR = "data/query_results"
LOG_PATH = os.path.join(OUTPUT_DIR, "bioproject2geo.log")

ThrottledGet = Callable[..., Coroutine[Any, Any, httpx.Response]]


def configure_logging() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(LOG_PATH, mode="w"),
            logging.StreamHandler(),
        ],
        force=True,
    )


def _is_retryable(e: Exception) -> bool:
    return isinstance(e, (httpx.TransportError, httpx.TimeoutException)) or (
        isinstance(e, httpx.HTTPStatusError)
        and e.response.status_code in (429, 500, 502, 503, 504)
    )


def _make_throttled_get(
    client: httpx.AsyncClient, min_interval: float = 0.13
) -> ThrottledGet:
    lock = asyncio.Lock()

    async def throttled_get(url: str, **kwargs) -> httpx.Response:
        async with lock:
            await asyncio.sleep(min_interval)
        return await client.get(url, **kwargs)

    return throttled_get


async def eutils_search(
    query: str,
    db: str,
    get: ThrottledGet,
    api_key: str | None = None,
) -> Dict[str, Any]:
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    params = {
        "db": db,
        "term": query,
        "retmode": "json",
        "usehistory": "y",
        "api_key": api_key,
    }

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception(_is_retryable),
        before_sleep=lambda rs: logging.warning(
            "Retry %d/5 for %r: %s", rs.attempt_number, query, rs.outcome.exception()
        ),
    )
    async def _fetch():
        response = await get(base, params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    return await _fetch()


async def eutils_link(
    dbfrom: str,
    db: str,
    get: ThrottledGet,
    webenv: str,
    query_key: str,
    cmd: str = "neighbor_history",
    api_key: str | None = None,
) -> Dict[str, Any]:
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi"
    params = {
        "dbfrom": dbfrom,
        "db": db,
        "WebEnv": webenv,
        "query_key": query_key,
        "cmd": cmd,
        "retmode": "json",
        "api_key": api_key,
    }
    # No retry here: webenv is session-scoped and would be stale after a long backoff.
    # The caller (bioproject2geo) retries the entire chain from scratch instead.
    response = await get(base, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


async def eutils_summary(
    db: str,
    get: ThrottledGet,
    webenv: str,
    query_key: str,
    api_key: str | None = None,
    retmax: int = 500,
) -> Dict[str, Any]:
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"

    # No retry here: webenv is session-scoped and would be stale after a long backoff.
    # The caller (bioproject2geo) retries the entire chain from scratch instead.
    async def _fetch_page(retstart: int) -> Dict[str, Any]:
        params = {
            "db": db,
            "WebEnv": webenv,
            "query_key": query_key,
            "retmode": "json",
            "retstart": retstart,
            "retmax": retmax,
            "api_key": api_key,
        }
        response = await get(base, params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    combined: Dict[str, Any] | None = None
    retstart = 0

    while True:
        page = await _fetch_page(retstart)
        result = page.get("result")

        if not isinstance(result, dict):
            return page if combined is None else combined

        page_uids = result.get("uids", [])
        if combined is None:
            combined = {**page, "result": {"uids": []}}

        combined_result = combined["result"]
        combined_result["uids"].extend(page_uids)
        for uid in page_uids:
            if uid in result:
                combined_result[uid] = result[uid]

        if len(page_uids) < retmax:
            return combined

        retstart += retmax


async def eutils_summary_by_ids(
    db: str,
    get: ThrottledGet,
    ids: list[str],
    api_key: str | None = None,
    batch_size: int = 500,
) -> Dict[str, Any] | None:
    if not ids:
        return None

    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception(_is_retryable),
        before_sleep=lambda rs: logging.warning(
            "Retry %d/5 for summary IDs: %s",
            rs.attempt_number,
            rs.outcome.exception(),
        ),
    )
    async def _fetch_batch(batch: list[str]) -> Dict[str, Any]:
        params = {
            "db": db,
            "id": ",".join(batch),
            "retmode": "json",
            "api_key": api_key,
        }
        response = await get(base, params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    combined: Dict[str, Any] | None = None

    for start in range(0, len(ids), batch_size):
        page = await _fetch_batch(ids[start : start + batch_size])
        result = page.get("result")

        if not isinstance(result, dict):
            return page if combined is None else combined

        page_uids = result.get("uids", [])
        if combined is None:
            combined = {**page, "result": {"uids": []}}

        combined_result = combined["result"]
        combined_result["uids"].extend(page_uids)
        for uid in page_uids:
            if uid in result:
                combined_result[uid] = result[uid]

    return combined


def _geo_ids_from_links(links: Dict[str, Any]) -> list[str]:
    geo_ids = []
    for linkset in links.get("linksets", []):
        for linksetdb in linkset.get("linksetdbs", []):
            if linksetdb.get("dbto") == "gds":
                geo_ids.extend(linksetdb.get("links", []))
    return geo_ids


async def _geo_summary_from_neighbor_links(
    accession: str,
    get: ThrottledGet,
    webenv: str,
    query_key: str,
    api_key: str | None = None,
) -> Dict[str, Any] | None:
    fallback_links = await eutils_link(
        dbfrom="bioproject",
        db="gds",
        get=get,
        webenv=webenv,
        query_key=query_key,
        cmd="neighbor",
        api_key=api_key,
    )
    geo_ids = _geo_ids_from_links(fallback_links)
    if not geo_ids:
        return None

    logging.warning(
        "Falling back to direct GEO UID summaries for %r: %d linked UIDs",
        accession,
        len(geo_ids),
    )
    return await eutils_summary_by_ids(
        db="gds",
        get=get,
        ids=geo_ids,
        api_key=api_key,
    )


async def bioproject2geo(
    accession: str,
    get: ThrottledGet,
    api_key: str | None = None,
) -> Dict[str, Any] | None:
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception(_is_retryable),
        before_sleep=lambda rs: logging.warning(
            "Retry %d/5 for %r (full chain): %s",
            rs.attempt_number,
            accession,
            rs.outcome.exception(),
        ),
    )
    async def _run():
        return await _bioproject2geo_once(accession=accession, get=get, api_key=api_key)

    return await _run()


async def _bioproject2geo_once(
    accession: str,
    get: ThrottledGet,
    api_key: str | None = None,
) -> Dict[str, Any] | None:
    search = await eutils_search(
        query=f"{accession}[PRJNA]",
        db="bioproject",
        get=get,
        api_key=api_key,
    )
    if not search["esearchresult"]["idlist"]:
        logging.warning("No BioProject found for %r", accession)
        return None

    links = await eutils_link(
        dbfrom="bioproject",
        db="gds",
        get=get,
        webenv=search["esearchresult"]["webenv"],
        query_key=search["esearchresult"]["querykey"],
        api_key=api_key,
    )

    search_webenv = search["esearchresult"]["webenv"]
    search_query_key = search["esearchresult"]["querykey"]

    linksets = links.get("linksets", [])
    if not linksets or not linksets[0].get("linksetdbhistories"):
        fallback_summary = await _geo_summary_from_neighbor_links(
            accession=accession,
            get=get,
            webenv=search_webenv,
            query_key=search_query_key,
            api_key=api_key,
        )
        if fallback_summary is not None:
            return fallback_summary

        logging.warning("No GEO links found for %r", accession)
        return None

    link_history = next(
        (
            history
            for history in linksets[0]["linksetdbhistories"]
            if history.get("dbto") == "gds" and history.get("querykey")
        ),
        None,
    )
    if link_history is None:
        fallback_summary = await _geo_summary_from_neighbor_links(
            accession=accession,
            get=get,
            webenv=search_webenv,
            query_key=search_query_key,
            api_key=api_key,
        )
        if fallback_summary is not None:
            return fallback_summary

        logging.warning("No usable GEO link history found for %r", accession)
        return None

    return await eutils_summary(
        db="gds",
        get=get,
        webenv=linksets[0]["webenv"],
        query_key=link_history["querykey"],
        api_key=api_key,
    )


def parse_geo_summary(accession: str, summary: Dict[str, Any] | None) -> list[dict]:
    failure_row = {
        "bioproject_accession": accession,
        "geo_uid": None,
        "accession": None,
        "taxon": None,
        "n_samples": None,
        "ftplink": None,
        "sample_accessions": None,
        "success": False,
    }
    if summary is None:
        return [failure_row]

    result = summary.get("result")
    if not isinstance(result, dict):
        logging.warning(
            "No GEO summary result for %r. Response keys: %s",
            accession,
            sorted(summary.keys()),
        )
        return [failure_row]

    rows = []
    for uid in result.get("uids", []):
        entry = result.get(uid, {})
        rows.append(
            {
                "bioproject_accession": accession,
                "geo_uid": uid,
                "accession": entry.get("accession"),
                "taxon": entry.get("taxon"),
                "n_samples": entry.get("n_samples"),
                "ftplink": entry.get("ftplink"),
                "sample_accessions": ";".join(
                    s["accession"] for s in entry.get("samples", [])
                ),
                "success": True,
            }
        )
    return rows or [failure_row]


async def main():
    configure_logging()
    columns = ["sample", "gse", "prj", "srs", "srx", "srr", "species"]
    samples10x = pd.read_csv("data/All_10x.sample_table.tsv", sep="\t", names=columns)
    bioproject_list = samples10x.prj.unique().tolist()
    bioproject_list = [
        acc
        for acc in bioproject_list
        if isinstance(acc, str) and acc.startswith("PRJNA")
    ]
    # bioproject_list = bioproject_list[:10]  # Limit to first 10 for testing
    print(f"Fetching GEO links for {len(bioproject_list)} BioProjects...")
    logging.info("Fetching GEO links for %d BioProjects", len(bioproject_list))

    async def safe_fetch(acc: str) -> Dict[str, Any] | None:
        try:
            return await bioproject2geo(accession=acc, get=get, api_key=NCBI_API_KEY)
        except (
            httpx.HTTPStatusError,
            httpx.TransportError,
            httpx.TimeoutException,
        ) as e:
            logging.error("Failed to fetch %r: %s", acc, e)
            return None

    async with httpx.AsyncClient() as client:
        get = _make_throttled_get(client, min_interval=0.13)
        tasks = [safe_fetch(acc) for acc in bioproject_list]
        results = await tqdm_asyncio.gather(*tasks, desc="Fetching BioProject→GEO")

    rows = []
    for accession, summary in zip(bioproject_list, results):
        rows.extend(parse_geo_summary(accession, summary))

    df = pd.DataFrame(rows)
    output_path = os.path.join(OUTPUT_DIR, "bioproject2geo_summary.csv")
    df.to_csv(output_path, index=False)
    logging.info("Wrote %d rows to %s", len(df), output_path)


if __name__ == "__main__":
    asyncio.run(main())
