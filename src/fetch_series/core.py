import csv
import io
import logging
import os
import re
from typing import Any, Literal

import httpx
from httpx_retries import Retry, RetryTransport

# Enable retry logic for HTTP requests to handle transient errors and rate limiting when accessing the NCBI E-utilities API
retry = Retry(
    total=5,
    backoff_factor=0.3,  # Wait 0.3s, 0.6s, 1.2s, 2.4s, 4.8s between retries
    status_forcelist=[
        408,  # Request Timeout - server took too long to respond
        429,  # Too Many Requests - rate limiting (common with NCBI)
        500,  # Internal Server Error - temporary server issue
        502,  # Bad Gateway - gateway/proxy error
        503,  # Service Unavailable - server overloaded/maintenance
        504,  # Gateway Timeout - gateway didn't get response in time
    ],
)

transport = RetryTransport(retry=retry)


def default_api_key() -> str | None:
    """Return the NCBI API key from the environment, if one is configured.

    Both spellings are accepted because the two have been used interchangeably
    in this project; ``NCBI_API_KEY`` is the preferred one.
    """
    return os.getenv("NCBI_API_KEY") or os.getenv("NCBI_KEY")


def _clean_params(params: dict[str, Any]) -> dict[str, Any]:
    """Drop parameters whose value is None.

    httpx encodes ``None`` as an empty value rather than omitting the parameter,
    so ``api_key=None`` is sent as ``api_key=`` -- which E-utilities rejects with
    a 400. Every call that did not pass an explicit key used to fail this way.
    """
    return {key: value for key, value in params.items() if value is not None}


def eutils_search(
    query: str,
    db: str,
    api_key: str | None = None,
) -> dict[str, Any]:
    """
    Searches the NCBI Entrez database using the E-utilities API
    Args:
        query (str): query string to search for
        db (str): database to search in
        api_key (str | None): API key for the NCBI E-utilities API

    Returns:
        Dict[str, Any]: JSON response from the E-utilities API containing search results
    """
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    params = {
        "db": db,
        "term": query,
        "retmode": "json",
        "usehistory": "y",
        "api_key": api_key or default_api_key(),
    }
    with httpx.Client(transport=transport) as client:
        response = client.get(base, params=_clean_params(params), timeout=10)
    response.raise_for_status()
    payload: dict[str, Any] = response.json()
    return payload


def eutils_summary(
    db: str,
    ids: str | None = None,
    webenv: str | None = None,
    query_key: str | None = None,
    api_key: str | None = None,
) -> dict[str, Any]:
    """
    Retrieves summaries of records from the NCBI Entrez database using the E-utilities API
    Args:
        db (str): database to retrieve summaries from
        ids (str | None): comma-separated list of record IDs to retrieve summaries for (optional if webenv and query_key are provided)
        webenv (str | None): WebEnv string from a previous search to retrieve summaries for (optional if ids are provided)
        query_key (str | None): query key from a previous search to retrieve summaries for (optional if ids are provided)
        api_key (str | None): API key for the NCBI E-utilities API

    Returns:
        Dict[str, Any]: JSON response from the E-utilities API containing summaries of records
    """
    # Check that either ids or webenv and query_key are specified
    if (ids is None) == (webenv is None or query_key is None):
        raise ValueError("Must specify either ids OR webenv and query_key")

    # Retrieve summaries
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
    params = {
        "db": db,
        "id": ids,
        "WebEnv": webenv,
        "query_key": query_key,
        "retmode": "json",
        "api_key": api_key or default_api_key(),
    }

    with httpx.Client(transport=transport) as client:
        response = client.get(base, params=_clean_params(params), timeout=10)
    response.raise_for_status()
    payload: dict[str, Any] = response.json()
    return payload


def eutils_fetch(
    db: str,
    ids: str | None = None,
    webenv: str | None = None,
    query_key: str | None = None,
    api_key: str | None = None,
    rettype: str = "full",
    retmode: str = "json",
) -> Any:
    """
    Retrieves records from the NCBI Entrez database using the E-utilities API
    Args:
        db (str): database to retrieve records from
        ids (str | None): comma-separated list of record IDs to retrieve (optional if webenv and query_key are provided)
        webenv (str | None): WebEnv string from a previous search to retrieve records for (optional if ids are provided)
        query_key (str | None): query key from a previous search to retrieve records for (optional if ids are provided)
        api_key (str | None): API key for the NCBI E-utilities API
        rettype (str): type of return format (default: "full")
        retmode (str): mode of return format (default: "json")

    Returns:
        Dict[str, Any]: JSON response from the E-utilities API containing records
    """
    # Check that either ids or webenv and query_key are specified
    if (ids is None) == (webenv is None or query_key is None):
        raise ValueError("Must specify either ids OR webenv and query_key")

    # Retrieve records
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    params = {
        "db": db,
        "retmode": retmode,
        "rettype": rettype,
        "api_key": api_key or default_api_key(),
    }

    if ids is not None:
        params["id"] = ids
    if webenv is not None:
        params["WebEnv"] = webenv
    if query_key is not None:
        params["query_key"] = query_key

    with httpx.Client(transport=transport) as client:
        response = client.get(base, params=_clean_params(params), timeout=10)
    response.raise_for_status()
    if retmode == "json":
        payload: dict[str, Any] = response.json()
        return payload
    return response.text


def eutils_link(
    dbfrom: str,
    db: str,
    ids: str | None = None,
    webenv: str | None = None,
    query_key: str | None = None,
    retmode: str = "json",
    cmd: str = "neighbor_history",
    api_key: str | None = None,
) -> dict[str, Any]:
    """
    Search for linked items in a target database using the E-utilities API.
    Args:
        dbfrom (str): database to search from
        db (str): database to search in
        id (str): ID of the item to link
        retmode (str): return mode for the response
        api_key (str | None): API key for the NCBI E-utilities API

    Returns:
        Dict[str, Any]: JSON response from the E-utilities API containing search results
    """
    # Check that either ids or webenv and query_key are specified
    if (ids is None) == (webenv is None or query_key is None):
        raise ValueError("Must specify either ids OR webenv and query_key")

    # Retrieve linked items using the E-utilities API
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi"
    params = {
        "dbfrom": dbfrom,
        "db": db,
        "id": ids,
        "retmode": retmode,
        "WebEnv": webenv,
        "query_key": query_key,
        "cmd": cmd,
        "api_key": api_key or default_api_key(),
    }
    with httpx.Client(transport=transport) as client:
        response = client.get(base, params=_clean_params(params), timeout=10)
    response.raise_for_status()
    payload: dict[str, Any] = response.json()
    return payload


def geo_dataset_id(
    series: str,
    stype: Literal["gse", "bioproject"] = "gse",
    timeout: float = 0.3,
) -> str | None:
    """
    Searches the GEO database for datasets matching the given series identifier
    Args:
        series (str): series identifier to search for (e.g., GSE12345)
        stype (Literal['gse', 'bioproject']): type of identifier to search for (default: 'gse')
    Returns:
        str | None: The ID of the matching dataset, or None if no match is found
    """
    # Construct the query based on the specified stype
    if stype == "gse":
        query = f"{series}[ACCN]+GSE[ETYP]"
    elif stype == "bioproject":
        query = f"{series}[ALL]+GSE[ETYP]"
    else:
        raise ValueError(f"Invalid stype: {stype}")

    # Attempt the search up to 5 times with a delay between attempts in case of HTTP errors
    try:
        search_results = eutils_search(query=query, db="gds")
    except httpx.HTTPError as e:
        logging.error("Failed to search for %s after retries: %s", series, e)
        return None
    except Exception as e:
        logging.error("Unexpected error while searching for %s: %s", series, e)
        return None

    # Check if the dataset was found and return the ID, or log a warning if not found or multiple matches
    if search_results["esearchresult"]["count"] == "0":
        logging.warning("No results found for %s", series)
        return None
    elif int(search_results["esearchresult"]["count"]) > 1:
        logging.warning(
            "Multiple results found for %s: %s",
            series,
            search_results["esearchresult"]["idlist"],
        )
        return None
    else:
        uid: str = search_results["esearchresult"]["idlist"][0]
        return uid


def ae2secondary(series: str) -> list[str]:
    """
    Retrieves SecondaryAccession values associated with a given series from the EBI BioStudies database
    Args:
        series (str): series identifier to retrieve SecondaryAccession values for (e.g., E-MTAB-10018)

    Returns:
        List[str]: A list of SecondaryAccession values associated with the series
    """
    # Get SecondaryAccession values from idf file
    base = f"https://www.ebi.ac.uk/biostudies/files/{series}/{series}.idf.txt"
    with httpx.Client(transport=transport) as client:
        response = client.get(base, follow_redirects=True, timeout=30)
    response.raise_for_status()

    # Check if there are SecondaryAccession values in the idf file
    pattern = re.compile(r"Comment\s*\[SecondaryAccession\]\s*((?:ERP|EGA\w)\d+)")
    secondary = re.findall(pattern, response.text)

    # Try to get secondary accesssions from ENA if not found in idf file
    if not secondary:
        biosamples = ae2biosamples(series=series)
        if not biosamples:
            logging.warning(
                "No biosamples found for %s, cannot retrieve secondary accessions",
                series,
            )
            return []

        # Attempt to retrieve secondary accessions from ENA for each biosample
        for biosample in biosamples:
            secondary_accessions = ena2bioproject(series=biosample)
            if secondary_accessions:
                secondary.extend(secondary_accessions)

        secondary = list(set(secondary))  # Remove duplicates

    if not secondary:
        logging.warning("No secondary accessions found for %s", series)
    return secondary


def ae2biosamples(series: str) -> list[str]:
    """
    Retrieves biosample IDs associated with a given series from the EBI BioStudies database
    Args:
        series (str): series identifier to retrieve biosample IDs for (e.g., E-MTAB-10018)

    Returns:
        List[str] | None: A list of biosample IDs associated with the series, or None if no biosamples are found
    """
    base = f"https://www.ebi.ac.uk/biostudies/files/{series}/{series}.sdrf.txt"
    with httpx.Client(transport=transport) as client:
        response = client.get(base, follow_redirects=True, timeout=30)
    response.raise_for_status()

    biosamples = []
    reader = csv.DictReader(io.StringIO(response.text), delimiter="\t")
    biosamples = [
        row.get("Comment[BioSD_SAMPLE]").strip()
        for row in reader
        if "Comment[BioSD_SAMPLE]" in row
    ]
    biosamples = list(filter(None, biosamples))  # Remove empty strings
    return biosamples


def read_enaruns(
    series: str,
    format: Literal["json", "tsv"] = "json",
    fields: str = "study_accession,experiment_accession,run_accession",
    limit: int | None = None,
) -> list[dict[str, Any]] | None:
    """
    Retrieves ENA run accessions associated with a given series from the EBI ENA database
    Args:
        series (str): series identifier to retrieve ENA run accessions for (e.g., E-MTAB-10018)
        format (Literal['json', 'tsv']): format to return the results in (default: 'json')
        fields (str): comma-separated list of fields to include in the results (default: 'study_accession,experiment_accession,run_accession')

    Returns:
        List[Dict[str, Any]] | None: A list of dictionaries containing ENA run information, or None if no runs are found
    """
    base = "https://www.ebi.ac.uk/ena/portal/api/filereport"
    params = {
        "accession": series,
        "result": "read_run",
        "format": format,
        "fields": fields,
        "limit": limit,
    }
    with httpx.Client(transport=transport) as client:
        response = client.get(
            base, params=_clean_params(params), timeout=10.0, follow_redirects=True
        )
    response.raise_for_status()

    if format == "json":
        payload: list[dict[str, Any]] = response.json()
        return payload
    elif format == "tsv":
        reader = csv.DictReader(io.StringIO(response.text), delimiter="\t")
        return list(reader)


def ena2bioproject(series: str) -> list[str]:
    """
    Retrieves the BioProject ID associated with a given ENA series from the EBI ENA database
    Args:
        series (str): series identifier to retrieve the BioProject ID for (e.g., E-MTAB-10018)

    Returns:
        List[str]: A list of BioProject IDs associated with the series, or an empty list if no BioProjects are found
    """
    runs = read_enaruns(series=series, format="json", fields="study_accession")
    if not runs or "study_accession" not in runs[0]:
        logging.warning("No BioProject found for %s", series)
        return []
    accessions = set(run["study_accession"] for run in runs if "study_accession" in run)
    return list(accessions)


def bioproject2ena(series: str) -> list[str] | None:
    """
    Retrieves the ENA series accessions associated with a given BioProject ID from the EBI ENA database
    Args:
        series (str): BioProject ID to retrieve ENA series accessions for (e.g., PRJEB12345)

    Returns:
        List[str] | None: A list of ENA series accessions associated with the BioProject, or None if no accessions are found
    """
    runs = read_enaruns(series=series, format="json", fields="secondary_study_accession")
    if not runs or "secondary_study_accession" not in runs[0]:
        logging.warning("No ENA series found for %s", series)
        return None
    return runs[0]["secondary_study_accession"]


def query_ae(query: str, pagesize: int = 100) -> dict[str, Any]:
    """
    Queries the EBI BioStudies database for datasets matching the given query string
    Args:
        query (str): query string to search for (e.g., "E-MTAB-10018")
        pagesize (int): number of results to return per page (default: 100)
    Returns:
        Dict[str, Any]: A dictionary containing the search results
    """
    base = "https://www.ebi.ac.uk/biostudies/api/v1/search"
    params = {
        "query": query,
        "collection": "arrayexpress",
        "pageSize": pagesize,
    }

    with httpx.Client(transport=transport) as client:
        response = client.get(
            base, params=_clean_params(params), timeout=10.0, follow_redirects=True
        )
    response.raise_for_status()
    payload: dict[str, Any] = response.json()
    return payload


def ena2ae(series: str) -> list[str] | None:
    """
    Retrieves the ArrayExpress accessions associated with a given ENA series from the EBI BioStudies database
    Args:
        series (str): series identifier to retrieve ArrayExpress accessions for (e.g., E-MTAB-10018)
    Returns:
        List[str] | None: A list of ArrayExpress accessions associated with the series, or None if no accessions are found
    """
    results = query_ae(query=f"{series}", pagesize=100)
    if "totalHits" not in results or results["totalHits"] == 0:
        logging.warning("No ArrayExpress accessions found for %s", series)
        return None
    accessions = [hit["accession"] for hit in results["hits"]]
    return accessions
