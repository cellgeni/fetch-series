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
    ThrottledGet,
    _is_retryable,
    _make_throttled_get,
)

load_dotenv()

OUTPUT_DIR = "data/query"
LOG_PATH = os.path.join(OUTPUT_DIR, "logs", "bioproject2ena.log")

ENA_FIELDS = [
    "run_accession",
    "experiment_accession",
    "sample_accession",
    "secondary_sample_accession",
    "study_accession",
    "library_strategy",
    "library_source",
    "library_layout",
    "sample_alias",
    "scientific_name",
]

# Map ENA TSV field names to output column names matching the SRA runinfo style
ENA_COLUMN_MAP = {
    "run_accession": "Run",
    "experiment_accession": "Experiment",
    "sample_accession": "BioSample",
    "secondary_sample_accession": "Sample",
    "study_accession": "Submission",
    "library_strategy": "LibraryStrategy",
    "library_source": "LibrarySource",
    "library_layout": "LibraryLayout",
    "sample_alias": "SampleName",
    "scientific_name": "ScientificName",
}

OUTPUT_COLUMNS = list(ENA_COLUMN_MAP.values())


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


def parse_ena_response(accession: str, text: str | None) -> list[dict]:
    failure_row = {col: None for col in OUTPUT_COLUMNS}
    failure_row["bioproject_accession"] = accession
    failure_row["success"] = False

    if text is None:
        return [failure_row]

    text = text.strip()
    if not text:
        return [failure_row]

    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    rows = []
    for raw in reader:
        if not raw.get("run_accession"):
            continue
        row = {"bioproject_accession": accession, "success": True}
        for ena_field, col in ENA_COLUMN_MAP.items():
            row[col] = raw.get(ena_field) or None
        rows.append(row)

    return rows or [failure_row]


async def ena_fetch_runs(
    get: ThrottledGet,
    accession: str,
) -> str:
    base = "https://www.ebi.ac.uk/ena/portal/api/filereport"
    params = {
        "accession": accession,
        "result": "read_run",
        "format": "tsv",
        "fields": ",".join(ENA_FIELDS),
    }
    response = await get(base, params=params, timeout=30)
    response.raise_for_status()
    return response.text


async def bioproject2ena(
    accession: str,
    get: ThrottledGet,
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
        text = await ena_fetch_runs(get=get, accession=accession)
        lines = text.strip().splitlines()
        if len(lines) <= 1:
            logging.warning("No ENA runs found for %r", accession)
            return None
        return text

    return await _run()


async def main():
    configure_logging()
    columns = ["sample", "gse", "prj", "srs", "srx", "srr", "species"]
    samples10x = pd.read_csv("data/All_10x.sample_table.tsv", sep="\t", names=columns)
    bioproject_list = samples10x.prj.unique().tolist()
    print(f"Fetching ENA runs for {len(bioproject_list)} BioProjects...")

    async def safe_fetch(acc: str) -> str | None:
        try:
            return await bioproject2ena(accession=acc, get=get)
        except (
            httpx.HTTPStatusError,
            httpx.TransportError,
            httpx.TimeoutException,
            RetryError,
        ) as e:
            logging.error("Failed to fetch %r: %s", acc, e)
            return None

    async with httpx.AsyncClient() as client:
        get = _make_throttled_get(client, min_interval=0.13)
        tasks = [safe_fetch(acc) for acc in bioproject_list]
        results = await tqdm_asyncio.gather(*tasks, desc="Fetching BioProject→ENA")

    rows = []
    for accession, text in zip(bioproject_list, results):
        rows.extend(parse_ena_response(accession, text))

    df = pd.DataFrame(rows)
    output_path = os.path.join(OUTPUT_DIR, "results", "bioproject2ena_summary.csv")
    df.to_csv(output_path, index=False)
    logging.info("Wrote %d rows to %s", len(df), output_path)
    print(f"Wrote {len(df)} rows to {output_path}")


if __name__ == "__main__":
    asyncio.run(main())
