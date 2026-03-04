import re
import sys
import json
import logging
import time
from typing import Optional, Dict, Any, Literal, List
import typer
import httpx


logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
)
logger = logging.getLogger(__name__)


def eutils_search(
    query: str,
    db: str,
) -> Dict[str, Any]:
    """
    Searches the NCBI Entrez database using the E-utilities API
    Args:
        query (str): query string to search for
        db (str): database to search in

    Returns:
        Dict[str, Any]: JSON response from the E-utilities API containing search results
    """
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    params = {
        "db": db,
        "term": query,
        "retmode": "json",
        "usehistory": "y",
    }

    response = httpx.get(base, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def eutils_summary(
    webenv: str,
    query_key: str,
    db: str,
) -> Dict[str, Any]:
    """
    Retrieves summaries of records from the NCBI Entrez database using the E-utilities API
    Args:
        webenv (str): WebEnv string obtained from eutils_search
        query_key (str): Query key obtained from eutils_search
        db (str): database to retrieve summaries from

    Returns:
        Dict[str, Any]: JSON response from the E-utilities API containing summaries of records
    """
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
    params = {
        "db": db,
        "webenv": webenv,
        "query_key": query_key,
        "retmode": "json",
    }

    response = httpx.get(base, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def geo_dataset_search(
    series: str,
    stype: Literal["gse", "bioproject"] = "gse",
) -> Dict[str, Any]:
    """
    Searches the GEO database for datasets matching the given series identifier
    Args:
        series (str): series identifier to search for (e.g., GSE12345)
        stype (Literal['gse', 'bioproject']): type of identifier to search for (default: 'gse')
    Returns:
        Dict[str, Any]: JSON response containing search results from the GEO database
    """
    if stype == "gse":
        query = f"{series}[ACCN]+GSE[ETYP]"
    elif stype == "bioproject":
        query = f"{series}[ALL]+GSE[ETYP]"
    else:
        raise ValueError(f"Invalid stype: {stype}")

    attempts = 1
    while attempts <= 5:
        try:
            search_results = eutils_search(query=query, db="gds")
            return search_results
        except httpx.HTTPError as e:
            logger.warning(
                "Attempt %s to search for %s failed with error: %s",
                attempts,
                series,
                e,
            )
            attempts += 1
            time.sleep(0.3)


def check_series(series: str) -> str:
    """
    Checks the format of the series identifier and determines the type of identifier (GSE, Bioproject, E-MTAB, ERP, SRP, etc.)
    Args:
        series (str): series identifier to check (e.g., GSE12345, PRJNA12345, E-MTAB12345, ERP12345, SRP12345)

    Raises:
        typer.BadParameter: if the series identifier is not in a valid format

    Returns:
        str: search type corresponding to the series identifier (e.g., 'gse', 'bioproject', etc.)
    """
    if re.match(r"GSE\d+$", series):
        stype = "gse"
    elif re.match(r"PRJNA\d+$", series):
        stype = "sra-bioproject"
    elif re.match(r"PRJEB\d+$", series):
        stype = "ebi-bioproject"
    elif re.match(r"PRJDB\d+$", series):
        stype = "ddbj-bioproject"
    elif re.match(r"PRJEA\d+$", series):
        stype = "ebi-ncbi-bioproject"
    elif re.match(r"PRJDA\d+$", series):
        stype = "ddbj-ncbi-bioproject"
    elif re.match(r"E-MTAB\d+$", series):
        stype = "arrayexpress"
    elif re.match(r"ERP\d+$", series):
        stype = "ena"
    elif re.match(r"SRP\d+$", series):
        stype = "sra"
    else:
        logger.error("Invalid series identifier: %s", series)
        raise typer.BadParameter(f"Invalid series identifier: {series}")
    return stype


def bs_from_bioproject(bioproject: str, stype: str) -> List[str]:
    # Get GSE accessions from BioProject (if it was submitted to GEO)
    if stype in ["sra-bioproject", "ebi-ncbi-bioproject", "ddbj-ncbi-bioproject"]:
        search_results = geo_dataset_search(series=bioproject, stype="bioproject")
        # if int(search_results["esearchresult"]["count"]) > 0:
        #     return search_results["esearchresult"]["idlist"]
    # Get biosamples from SRA
    pass
    


def cli(
    series: str = typer.Argument(
        ..., help="Series identifier to search for (e.g., GSE12345)"
    )
):
    # Check if the series identifier is valid (GSE, Bioproject, E-MTAB, ERP, SRP, etc.)
    stype = check_series(series)

    if stype in [
        "sra-bioproject",
        "ebi-bioproject",
        "ddbj-bioproject",
        "ebi-ncbi-bioproject",
        "ddbj-ncbi-bioproject",
    ]:
        bs_from_bioproject(series, stype)
    else:
        raise NotImplementedError(f"Search type {stype} is not implemented yet.")


def main():
    """Entry point for the CLI."""
    typer.run(cli)


if __name__ == "__main__":
    main()
