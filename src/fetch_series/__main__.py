import re
import sys
import logging
import typer

from fetch_series.core import *


logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
)
logger = logging.getLogger(__name__)


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
