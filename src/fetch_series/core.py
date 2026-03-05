import io
import csv
import httpx
from httpx_retries import Retry, RetryTransport
import logging
from typing import Any, Dict, List, Literal

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
    with httpx.Client(transport=transport) as client:
        response = client.get(base, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def eutils_summary(
    db: str,
    ids: str | None = None,
    webenv: str | None = None,
    query_key: str | None = None,
) -> Dict[str, Any]:
    """
    Retrieves summaries of records from the NCBI Entrez database using the E-utilities API
    Args:
        db (str): database to retrieve summaries from
        ids (str | None): comma-separated list of record IDs to retrieve summaries for (optional if webenv and query_key are provided)
        webenv (str | None): WebEnv string from a previous search to retrieve summaries for (optional if ids are provided)
        query_key (str | None): query key from a previous search to retrieve summaries for (optional if ids are provided)

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
    }

    with httpx.Client(transport=transport) as client:
        response = client.get(base, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


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
        return search_results["esearchresult"]["idlist"][0]


def ae2ena(series: str) -> List[str]:
    """
    Retrieves ENA accessions associated with a given series from the EBI BioStudies database
    Args:
        series (str): series identifier to retrieve ENA accessions for (e.g., E-MTAB-10018)

    Returns:
        List[str]: A list of ENA accessions associated with the series
    """
    # Get ERP from idf file
    base = f"https://www.ebi.ac.uk/biostudies/files/{series}/{series}.idf.txt"
    with httpx.Client(transport=transport) as client:
        response = client.get(base, follow_redirects=True, timeout=30)
    response.raise_for_status()

    # Check if there are ERP accessions in the idf file
    erps = []
    for line in response.text.splitlines():
        if line.startswith("Comment[SecondaryAccession]"):
            erps.append(line.split("\t")[1].strip())
    return erps


def ae2biosamples(series: str) -> List[str]:
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
        row.get("Comment[BioSD_SAMPLE]")
        for row in reader
        if "Comment[BioSD_SAMPLE]" in row
    ]
    return biosamples


def ae2(series: str) -> Dict[str, Any]:
    """
    Retrieves ENA accessions or biosample IDs associated with a given series from the EBI BioStudies database
    Args:
        series (str): series identifier to retrieve ENA accessions or biosample IDs for (e.g., E-MTAB-10018)

    Returns:
        Dict[str, Any]: A dictionary containing the retrieved ENA accessions or biosample IDs
    """
    # Get ERP from idf file
    base = f"https://www.ebi.ac.uk/biostudies/files/{series}/{series}.idf.txt"
    with httpx.Client(transport=transport) as client:
        response = client.get(base, follow_redirects=True, timeout=30)
    response.raise_for_status()

    # Check if there are ERP accessions in the idf file
    erps = []
    for line in response.text.splitlines():
        if line.startswith("Comment[SecondaryAccession]"):
            erps.append(line.split("\t")[1].strip())

    if len(erps) >= 0:
        return {"ena_accessions": erps}

    # If no ERP accessions are found, get all biosamples from sdrf file
    base = f"https://www.ebi.ac.uk/biostudies/files/{series}/{series}.sdrf.txt"
    with httpx.Client(transport=transport) as client:
        response = client.get(base, follow_redirects=True, timeout=30)
    response.raise_for_status()

    biosamples = []
    reader = csv.DictReader(io.StringIO(response.text), delimiter="\t")
    biosamples = [
        row.get("Comment[BioSD_SAMPLE]")
        for row in reader
        if "Comment[BioSD_SAMPLE]" in row
    ]
    if len(biosamples) == 0:
        return None
    return {"biosamples": biosamples}
