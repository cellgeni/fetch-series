import asyncio
import json
import logging
import os
from typing import Any, Dict

import httpx
import pandas as pd
from dotenv import load_dotenv
from tenacity import (
    RetryError,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)
from tqdm.asyncio import tqdm_asyncio

from query_bioproject2sra import (
    NCBI_API_KEY,
    MalformedResponseError,
    ThrottledGet,
    _is_retryable,
    _make_throttled_get,
    eutils_link,
    eutils_search,
)

load_dotenv()

OUTPUT_DIR = "data/query"
LOG_PATH = os.path.join(OUTPUT_DIR, "logs", "bioproject2biosample.log")

OUTPUT_COLUMNS = ["uid", "accession", "organism"]


def configure_logging() -> None:
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


async def eutils_summary(
    get: ThrottledGet,
    webenv: str,
    query_key: str,
    db: str,
    api_key: str | None = None,
) -> Dict[str, Any]:
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
    params = {
        "db": db,
        "WebEnv": webenv,
        "query_key": query_key,
        "retmode": "json",
        "api_key": api_key,
    }
    # No retry: webenv is session-scoped and stale after a long backoff.
    response = await get(base, params=params, timeout=30)
    response.raise_for_status()
    try:
        return response.json()
    except json.JSONDecodeError:
        logging.warning(
            "Bad JSON from esummary (content-type: %s): %.200r",
            response.headers.get("content-type"),
            response.text,
        )
        raise


def parse_biosample_summary(
    accession: str, summary: Dict[str, Any] | None
) -> list[dict]:
    failure_row = {col: None for col in OUTPUT_COLUMNS}
    failure_row["bioproject_accession"] = accession
    failure_row["success"] = False

    if summary is None:
        return [failure_row]

    result = summary.get("result", {})
    uids = result.get("uids", [])
    if not uids:
        return [failure_row]

    rows = []
    for uid in uids:
        record = result.get(uid, {})
        rows.append(
            {
                "bioproject_accession": accession,
                "success": True,
                "uid": uid,
                "accession": record.get("accession") or None,
                "organism": record.get("organism") or None,
            }
        )
    return rows


async def bioproject2biosample(
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
            db="biosample",
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
            logging.warning("No BioSample links found for %r", accession)
            return None

        linkset = linksets[0]
        if "webenv" not in linkset:
            raise MalformedResponseError(f"linkset missing 'webenv' for {accession!r}")

        histories = linkset["linksetdbhistories"]
        link_history = next(
            (
                h
                for h in histories
                if h.get("dbto") == "biosample" and h.get("querykey")
            ),
            None,
        )

        if link_history is None:
            logging.warning("No usable BioSample link history found for %r", accession)
            return None

        try:
            int(link_history["querykey"])
        except (TypeError, ValueError):
            raise MalformedResponseError(
                f"link_history querykey not an integer for {accession!r}: {link_history['querykey']!r}"
            )

        return await eutils_summary(
            get=get,
            webenv=linkset["webenv"],
            query_key=link_history["querykey"],
            db="biosample",
            api_key=api_key,
        )

    return await _run()


async def main():
    configure_logging()
    columns = ["sample", "gse", "prj", "srs", "srx", "srr", "species"]
    samples10x = pd.read_csv("data/All_10x.sample_table.tsv", sep="\t", names=columns)
    bioproject_list = samples10x.prj.unique().tolist()
    print(f"Fetching BioSamples for {len(bioproject_list)} BioProjects...")

    async def safe_fetch(acc: str) -> Dict[str, Any] | None:
        try:
            return await bioproject2biosample(
                accession=acc, get=get, api_key=NCBI_API_KEY
            )
        except (
            httpx.HTTPStatusError,
            httpx.TransportError,
            httpx.TimeoutException,
            MalformedResponseError,
            RetryError,
        ) as e:
            logging.error("Failed to fetch %r: %s", acc, e)
            return None

    async with httpx.AsyncClient() as client:
        get = _make_throttled_get(client, min_interval=0.13)
        tasks = [safe_fetch(acc) for acc in bioproject_list]
        results = await tqdm_asyncio.gather(
            *tasks, desc="Fetching BioProject→BioSample"
        )

    rows = []
    for accession, summary in zip(bioproject_list, results):
        rows.extend(parse_biosample_summary(accession, summary))

    df = pd.DataFrame(rows)
    output_path = os.path.join(
        OUTPUT_DIR, "results", "bioproject2biosample_summary.csv"
    )
    df.to_csv(output_path, index=False)
    logging.info("Wrote %d rows to %s", len(df), output_path)
    print(f"Wrote {len(df)} rows to {output_path}")


if __name__ == "__main__":
    asyncio.run(main())
