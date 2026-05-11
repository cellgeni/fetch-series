import asyncio
import csv
import io
import json
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
LOG_PATH = os.path.join(OUTPUT_DIR, "bioproject2sra_be.log")

# Columns to keep from SRA runinfo CSV (absent fields become None)
SRA_COLUMNS = [
    "Run",
    "Experiment",
    "Sample",
    "BioSample",
    "Submission",
    "LibraryStrategy",
    "LibrarySource",
    "LibraryLayout",
    "SampleName",
]

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


class MalformedResponseError(Exception):
    """Raised when an NCBI response is missing required fields or has wrong types."""


def _is_retryable(e: Exception) -> bool:
    return isinstance(
        e,
        (
            httpx.TransportError,
            httpx.TimeoutException,
            json.JSONDecodeError,
            MalformedResponseError,
            KeyError,
        ),
    ) or (
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
    # No retry: webenv is session-scoped and stale after a long backoff.
    # The caller retries the full chain from scratch instead.
    response = await get(base, params=params, timeout=10)
    response.raise_for_status()
    try:
        data = response.json()
    except json.JSONDecodeError:
        logging.warning(
            "Bad JSON from elink (content-type: %s): %.200r",
            response.headers.get("content-type"),
            response.text,
        )
        raise

    # NCBI sometimes returns valid JSON but with an ERROR field inside
    # linksetdbhistories, indicating a transient backend failure.
    for linkset in data.get("linksets", []):
        for h in linkset.get("linksetdbhistories", []):
            if "ERROR" in h:
                raise MalformedResponseError(
                    f"elink backend error in linksetdbhistories: {h['ERROR']!r:.200}"
                )
    return data


async def sra_be_fetch_runinfo(
    get: ThrottledGet,
    webenv: str,
    query_key: str,
) -> str:
    base = "https://trace.ncbi.nlm.nih.gov/Traces/sra-db-be/sra-"
    params = {
        "WebEnv": webenv,
        "rettype": "runinfo",
        "query_key": query_key,
    }
    # No retry: webenv is session-scoped and stale after a long backoff.
    response = await get(base, params=params, timeout=30)
    response.raise_for_status()
    return response.text


async def bioproject2sra(
    accession: str,
    get: ThrottledGet,
    api_key: str | None = None,
) -> str | None:
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception(_is_retryable),
        before_sleep=lambda rs: logging.warning(
            "Retry %d/3 for %r (full chain): %s",
            rs.attempt_number,
            accession,
            rs.outcome.exception(),
        ),
    )
    async def _run():
        return await _bioproject2sra_once(accession=accession, get=get, api_key=api_key)

    return await _run()


async def _bioproject2sra_once(
    accession: str,
    get: ThrottledGet,
    api_key: str | None = None,
) -> str | None:
    search = await eutils_search(
        query=f"{accession}[PRJNA]",
        db="bioproject",
        get=get,
        api_key=api_key,
    )

    esearch = search.get("esearchresult")
    if not isinstance(esearch, dict):
        raise MalformedResponseError(
            f"esearch response missing 'esearchresult' for {accession!r}: {search!r:.200}"
        )
    for field in ("webenv", "querykey", "idlist"):
        if field not in esearch:
            raise MalformedResponseError(
                f"esearchresult missing {field!r} for {accession!r}"
            )
    try:
        int(esearch["querykey"])
    except (TypeError, ValueError):
        raise MalformedResponseError(
            f"esearchresult querykey not an integer for {accession!r}: {esearch['querykey']!r}"
        )

    if not esearch["idlist"]:
        logging.warning("No BioProject found for %r", accession)
        return None

    links = await eutils_link(
        dbfrom="bioproject",
        db="sra",
        get=get,
        webenv=esearch["webenv"],
        query_key=esearch["querykey"],
        api_key=api_key,
    )

    linksets = links.get("linksets")
    if not isinstance(linksets, list):
        raise MalformedResponseError(
            f"elink response missing 'linksets' list for {accession!r}"
        )

    if not linksets or not linksets[0].get("linksetdbhistories"):
        logging.warning("No SRA links found for %r", accession)
        return None

    linkset = linksets[0]
    if "webenv" not in linkset:
        raise MalformedResponseError(f"linkset missing 'webenv' for {accession!r}")

    # Prefer bioproject_sra_all; fall back to any sra history entry with a querykey
    histories = linkset["linksetdbhistories"]
    link_history = next(
        (
            h
            for h in histories
            if h.get("linkname") == "bioproject_sra_all" and h.get("querykey")
        ),
        None,
    ) or next(
        (h for h in histories if h.get("dbto") == "sra" and h.get("querykey")),
        None,
    )

    if link_history is None:
        logging.warning("No usable SRA link history found for %r", accession)
        return None

    try:
        int(link_history["querykey"])
    except (TypeError, ValueError):
        raise MalformedResponseError(
            f"link_history querykey not an integer for {accession!r}: {link_history['querykey']!r}"
        )

    return await sra_be_fetch_runinfo(
        get=get,
        webenv=linkset["webenv"],
        query_key=link_history["querykey"],
    )


def parse_sra_runinfo(accession: str, runinfo_text: str | None) -> list[dict]:
    failure_row = {col: None for col in SRA_COLUMNS}
    failure_row["bioproject_accession"] = accession
    failure_row["success"] = False

    if runinfo_text is None:
        return [failure_row]

    text = runinfo_text.strip()
    if not text:
        return [failure_row]

    reader = csv.DictReader(io.StringIO(text))
    rows = []
    for raw in reader:
        if not raw.get("Run"):
            continue
        row = {"bioproject_accession": accession, "success": True}
        for col in SRA_COLUMNS:
            value = raw.get(col) or None
            row[col] = value
        rows.append(row)

    return rows or [failure_row]


async def main():
    configure_logging()
    columns = ["sample", "gse", "prj", "srs", "srx", "srr", "species"]
    samples10x = pd.read_csv("data/All_10x.sample_table.tsv", sep="\t", names=columns)
    bioproject_list = samples10x.prj.unique().tolist()
    print(f"Fetching SRA runinfo for {len(bioproject_list)} BioProjects...")

    async def safe_fetch(acc: str) -> str | None:
        try:
            return await bioproject2sra(accession=acc, get=get, api_key=NCBI_API_KEY)
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
        results = await tqdm_asyncio.gather(*tasks, desc="Fetching BioProject→SRA")

    rows = []
    for accession, runinfo_text in zip(bioproject_list, results):
        rows.extend(parse_sra_runinfo(accession, runinfo_text))

    df = pd.DataFrame(rows)
    output_path = os.path.join(OUTPUT_DIR, "bioproject2sra_be_summary.csv")
    df.to_csv(output_path, index=False)
    logging.info("Wrote %d rows to %s", len(df), output_path)
    print(f"Wrote {len(df)} rows to {output_path}")


if __name__ == "__main__":
    asyncio.run(main())
