import logging
import re
import sys

import typer
from dotenv import load_dotenv

from fetch_series.core import eutils_summary, geo_dataset_id
from fetch_series.logging_utils import configure_logging

# Pick up NCBI_API_KEY from a local .env, if there is one. Without a key the
# NCBI rate limit drops from 10 to 3 requests per second.
load_dotenv()
configure_logging(level=logging.WARNING, stream=sys.stderr)
logger = logging.getLogger(__name__)

# Accession prefixes that identify a project registered with NCBI, and so are
# worth asking GEO about. PRJEB/PRJDB projects are EBI/DDBJ-native and have no
# GEO series, so they take a different route entirely.
NCBI_BACKED_BIOPROJECTS = frozenset(
    {"sra-bioproject", "ebi-ncbi-bioproject", "ddbj-ncbi-bioproject"}
)


def check_series(series: str) -> str:
    """
    Checks the format of the series identifier and determines the type of identifier (GSE, Bioproject, E-MTAB, ERP, SRP, etc.)
    Args:
        series (str): series identifier to check (e.g., GSE12345, PRJNA12345, E-MTAB-12345, ERP12345, SRP12345)

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
    # ArrayExpress accessions are hyphenated (E-MTAB-6505); the unhyphenated
    # pattern this used to carry could never match a real one.
    elif re.match(r"E-MTAB-\d+$", series):
        stype = "arrayexpress"
    elif re.match(r"ERP\d+$", series):
        stype = "ena"
    elif re.match(r"SRP\d+$", series):
        stype = "sra"
    else:
        logger.error("Invalid series identifier: %s", series)
        raise typer.BadParameter(f"Invalid series identifier: {series}")
    return stype


def gse_from_bioproject(bioproject: str, stype: str) -> list[str]:
    """
    Retrieves the GEO series accessions associated with a BioProject, by searching
    ``db=gds`` for the BioProject accession directly.

    This is the "direct" BP -> GEO route. Surveyed over 12,114 BioProjects it agrees
    with the ``elink bioproject -> gds`` route on every single accession, so the extra
    ELink round trip buys nothing here.

    Args:
        bioproject (str): BioProject accession (e.g., PRJNA673418)
        stype (str): search type from :func:`check_series`

    Returns:
        list[str]: GEO series accessions, empty if the project has no GEO record
    """
    if stype not in NCBI_BACKED_BIOPROJECTS:
        logger.warning(
            "%s is not an NCBI-backed BioProject (%s); GEO holds no record for it",
            bioproject,
            stype,
        )
        return []

    uid = geo_dataset_id(series=bioproject, stype="bioproject")
    if uid is None:
        return []

    summary = eutils_summary(db="gds", ids=uid)
    # A valid JSON body without "result" is how E-utilities reports an error
    # (for example "Too many UIDs in request"), so treat it as "unresolved",
    # not as a crash.
    if "result" not in summary:
        logger.warning(
            "esummary returned no 'result' for %s (uid %s): %s",
            bioproject,
            uid,
            summary.get("error", "no error message"),
        )
        return []

    accession = summary["result"].get(uid, {}).get("accession")
    return [accession] if accession else []


def cli(
    series: str = typer.Argument(..., help="Series identifier to search for (e.g., GSE12345)"),
) -> None:
    # Check if the series identifier is valid (GSE, Bioproject, E-MTAB, ERP, SRP, etc.)
    stype = check_series(series)

    if stype.endswith("bioproject"):
        for accession in gse_from_bioproject(series, stype):
            typer.echo(accession)
    else:
        raise NotImplementedError(f"Search type {stype} is not implemented yet.")


def main() -> None:
    """Entry point for the CLI."""
    typer.run(cli)


if __name__ == "__main__":
    main()
