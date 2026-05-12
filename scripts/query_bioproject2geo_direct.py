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
OUTPUT_DIR = "data/query/results"
LOG_PATH = "data/query/logs/bioproject2geo_direct.log"

ThrottledGet = Callable[..., Coroutine[Any, Any, httpx.Response]]


def configure_logging() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
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


async def eutils_summary(
    db: str,
    get: ThrottledGet,
    webenv: str,
    query_key: str,
    api_key: str | None = None,
    retmax: int = 500,
) -> Dict[str, Any]:
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"

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


async def bioproject2geo_direct(
    accession: str,
    get: ThrottledGet,
    api_key: str | None = None,
) -> Dict[str, Any] | None:
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception(_is_retryable),
        before_sleep=lambda rs: logging.warning(
            "Retry %d/5 for %r: %s",
            rs.attempt_number,
            accession,
            rs.outcome.exception(),
        ),
    )
    async def _run():
        search = await eutils_search(
            query=f"{accession}[ALL]",
            db="gds",
            get=get,
            api_key=api_key,
        )
        esearch = search.get("esearchresult", {})
        if not esearch.get("idlist"):
            logging.warning("No GEO records found for %r", accession)
            return None

        return await eutils_summary(
            db="gds",
            get=get,
            webenv=esearch["webenv"],
            query_key=esearch["querykey"],
            api_key=api_key,
        )

    return await _run()


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
    print(f"Fetching GEO records (direct search) for {len(bioproject_list)} BioProjects...")
    logging.info("Fetching GEO records (direct) for %d BioProjects", len(bioproject_list))

    async def safe_fetch(acc: str) -> Dict[str, Any] | None:
        try:
            return await bioproject2geo_direct(accession=acc, get=get, api_key=NCBI_API_KEY)
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
        results = await tqdm_asyncio.gather(*tasks, desc="Fetching BioProject→GEO (direct)")

    rows = []
    for accession, summary in zip(bioproject_list, results):
        rows.extend(parse_geo_summary(accession, summary))

    df = pd.DataFrame(rows)
    output_path = os.path.join(OUTPUT_DIR, "bioproject2geo_direct_summary.csv")
    df.to_csv(output_path, index=False)
    logging.info("Wrote %d rows to %s", len(df), output_path)
    print(f"Wrote {len(df)} rows to {output_path}")


if __name__ == "__main__":
    asyncio.run(main())
