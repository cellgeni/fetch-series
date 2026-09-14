"""Live-API checks for the E-utilities wrappers.

Every test here talks to NCBI, so they are all marked ``integration`` and are
excluded from the default run. Use ``uv run pytest -m integration``.

The worked example is the one documented in ``docs/bioproject.md``:
PRJNA988806 -> GSE236084 -> SRP446371 -> SRR25056225-29.
"""

import pytest

from fetch_series.core import eutils_search, eutils_summary, geo_dataset_id

pytestmark = pytest.mark.integration

# GSE160513 and the BioProject it was submitted under. The GEO UID for a series
# is its accession number prefixed with "200".
GSE = "GSE160513"
GSE_UID = "200160513"
BIOPROJECT = "PRJNA673418"
GSE_TITLE = "Leptin receptor marks and targets the hair follicle mesenchymal niche"


def test_eutils_search_gse():
    """esearch on db=gds resolves a GSE accession to exactly one UID."""
    results = eutils_search(query=f"{GSE}[ACCN]+GSE[ETYP]", db="gds")
    assert "esearchresult" in results
    assert int(results["esearchresult"]["count"]) == 1
    assert results["esearchresult"]["idlist"] == [GSE_UID]


def test_eutils_search_bioproject():
    """The same UID is reachable from the BioProject accession, via [ALL]."""
    results = eutils_search(query=f"{BIOPROJECT}[ALL]+GSE[ETYP]", db="gds")
    assert "esearchresult" in results
    assert int(results["esearchresult"]["count"]) == 1
    assert results["esearchresult"]["idlist"] == [GSE_UID]


def test_eutils_summary_by_history():
    """esummary can be driven from the WebEnv/query_key of a prior search."""
    search_results = eutils_search(query=f"{GSE}[ACCN]+GSE[ETYP]", db="gds")
    summary = eutils_summary(
        db="gds",
        webenv=search_results["esearchresult"]["webenv"],
        query_key=search_results["esearchresult"]["querykey"],
    )
    assert "result" in summary
    assert summary["result"][GSE_UID]["title"] == GSE_TITLE


def test_eutils_summary_by_id():
    """esummary can equally be driven from an explicit UID list."""
    summary = eutils_summary(db="gds", ids=GSE_UID)
    assert summary["result"][GSE_UID]["title"] == GSE_TITLE


def test_eutils_summary_rejects_ambiguous_arguments():
    """Passing both, or neither, of ids and history is a programming error."""
    with pytest.raises(ValueError):
        eutils_summary(db="gds")
    with pytest.raises(ValueError):
        eutils_summary(db="gds", ids=GSE_UID, webenv="x", query_key="1")


def test_geo_dataset_id_from_gse():
    """geo_dataset_id returns the bare UID, not the whole esearch envelope."""
    assert geo_dataset_id(series=GSE, stype="gse") == GSE_UID


def test_geo_dataset_id_from_bioproject():
    assert geo_dataset_id(series=BIOPROJECT, stype="bioproject") == GSE_UID


def test_geo_dataset_id_rejects_unknown_stype():
    with pytest.raises(ValueError):
        geo_dataset_id(series=GSE, stype="nonsense")  # type: ignore[arg-type]
