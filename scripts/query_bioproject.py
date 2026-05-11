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

NCBI_API_KEY = os.getenv("NCBI_API_KEY")

ThrottledGet = Callable[..., Coroutine[Any, Any, httpx.Response]]


def _is_retryable(e: Exception) -> bool:
    return isinstance(e, (httpx.TransportError, httpx.TimeoutException)) or (
        isinstance(e, httpx.HTTPStatusError)
        and e.response.status_code in (429, 500, 502, 503, 504)
    )


def _make_throttled_get(
    client: httpx.AsyncClient, min_interval: float = 0.13
) -> ThrottledGet:
    """Returns a drop-in for client.get that enforces a minimum gap between request starts."""
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


async def eutils_summary(
    db: str,
    get: ThrottledGet,
    ids: str | None = None,
    webenv: str | None = None,
    query_key: str | None = None,
    api_key: str | None = None,
) -> Dict[str, Any]:
    if (ids is None) == (webenv is None or query_key is None):
        raise ValueError("Must specify either ids OR webenv and query_key")

    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
    params = {
        "db": db,
        "id": ids,
        "WebEnv": webenv,
        "query_key": query_key,
        "retmode": "json",
        "api_key": api_key,
    }

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception(_is_retryable),
        before_sleep=lambda rs: logging.warning(
            "Retry %d/5 for summary %r: %s",
            rs.attempt_number,
            ids or webenv,
            rs.outcome.exception(),
        ),
    )
    async def _fetch():
        response = await get(base, params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    return await _fetch()


async def view_bioproject(
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
        logging.warning("No results found for %r", accession)
        return None
    return await eutils_summary(
        db="bioproject",
        get=get,
        webenv=search["esearchresult"]["webenv"],
        query_key=search["esearchresult"]["querykey"],
        api_key=api_key,
    )


def parse_summary(accession: str, summary: Dict[str, Any] | None) -> list[dict]:
    if summary is None:
        return [
            {
                "bioproject_accession": accession,
                "uid": None,
                "project_type": None,
                "project_data_type": None,
                "project_target_material": None,
                "success": False,
            }
        ]
    rows = []
    for uid in summary["result"]["uids"]:
        entry = summary["result"][uid]
        rows.append(
            {
                "bioproject_accession": accession,
                "uid": uid,
                "project_type": entry.get("project_type"),
                "project_data_type": entry.get("project_data_type"),
                "project_target_material": entry.get("project_target_material"),
                "success": True,
            }
        )
    return rows


async def main():
    columns = ["sample", "gse", "prj", "srs", "srx", "srr", "species"]
    samples10x = pd.read_csv("data/All_10x.sample_table.tsv", sep="\t", names=columns)
    bioproject_list = samples10x.prj.unique().tolist()
    print(f"Fetching summaries for {len(bioproject_list)} BioProjects...")

    async def safe_view(acc: str) -> Dict[str, Any] | None:
        try:
            return await view_bioproject(accession=acc, get=get, api_key=NCBI_API_KEY)
        except (
            httpx.HTTPStatusError,
            httpx.TransportError,
            httpx.TimeoutException,
        ) as e:
            logging.error("Failed to fetch %r: %s", acc, e)
            return None

    async with httpx.AsyncClient() as client:
        # 0.13s between requests = ~7.5 rps, comfortably under the 10 rps API key limit
        get = _make_throttled_get(client, min_interval=0.13)
        tasks = [safe_view(acc) for acc in bioproject_list]
        results = await tqdm_asyncio.gather(*tasks, desc="Fetching BioProject records")

    rows = []
    for accession, summary in zip(bioproject_list, results):
        rows.extend(parse_summary(accession, summary))

    df = pd.DataFrame(rows)
    df.to_csv("data/query_results/bioproject_summary.csv", index=False)


if __name__ == "__main__":
    asyncio.run(main())
