import asyncio
import csv
import io
import logging
import os

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
    SRA_COLUMNS,
    MalformedResponseError,
    ThrottledGet,
    _is_retryable,
    _make_throttled_get,
    eutils_search,
)

load_dotenv()

SRA_COLUMNS = SRA_COLUMNS + ["ScientificName"]

OUTPUT_DIR = "data/query"
LOG_PATH = os.path.join(OUTPUT_DIR, "logs", "bioproject2sra_direct.log")


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
            row[col] = raw.get(col) or None
        rows.append(row)

    return rows or [failure_row]


async def efetch_runinfo(
    get: ThrottledGet,
    webenv: str,
    query_key: str,
    api_key: str | None = None,
) -> str:
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    params = {
        "db": "sra",
        "WebEnv": webenv,
        "query_key": query_key,
        "rettype": "runinfo",
        "retmode": "text",
        "api_key": api_key,
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
            query=f"{accession}[GPRJ]",
            db="sra",
            get=get,
            api_key=api_key,
        )

        esearch = search.get("esearchresult")
        if not isinstance(esearch, dict):
            raise MalformedResponseError(
                f"esearch response missing 'esearchresult' for {accession!r}: {search!r:.200}"
            )
        for field in ("webenv", "querykey", "count"):
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

        if esearch["count"] == "0":
            logging.warning("No SRA records found for %r", accession)
            return None

        return await efetch_runinfo(
            get=get,
            webenv=esearch["webenv"],
            query_key=esearch["querykey"],
            api_key=api_key,
        )

    return await _run()


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
            MalformedResponseError,
            RetryError,
        ) as e:
            logging.error("Failed to fetch %r: %s", acc, e)
            return None

    async with httpx.AsyncClient() as client:
        get = _make_throttled_get(client, min_interval=0.13)
        tasks = [safe_fetch(acc) for acc in bioproject_list]
        results = await tqdm_asyncio.gather(
            *tasks, desc="Fetching BioProject→SRA (direct)"
        )

    rows = []
    for accession, runinfo_text in zip(bioproject_list, results):
        rows.extend(parse_sra_runinfo(accession, runinfo_text))

    df = pd.DataFrame(rows)
    output_path = os.path.join(
        OUTPUT_DIR, "results", "bioproject2sra_direct_summary.csv"
    )
    df.to_csv(output_path, index=False)
    logging.info("Wrote %d rows to %s", len(df), output_path)
    print(f"Wrote {len(df)} rows to {output_path}")


if __name__ == "__main__":
    asyncio.run(main())
