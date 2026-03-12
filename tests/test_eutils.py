import pytest
from fetch_series.__main__ import eutils_search, eutils_summary, geo_dataset_search


def test_eutils_search_gse():
    """Test the eutils_search function with a known GSE identifier."""
    query = "GSE160513[ACCN]+GSE[ETYP]"
    db = "gds"
    results = eutils_search(query=query, db=db)
    assert "esearchresult" in results
    assert "idlist" in results["esearchresult"]
    assert int(results["esearchresult"]["count"]) == 1
    assert results["esearchresult"]["idlist"][0] == "200160513"


def test_eutils_search_bioproject():
    """Test the eutils_search function with a known BioProject identifier."""
    query = "PRJNA673418[ALL]+GSE[ETYP]"
    db = "gds"
    results = eutils_search(query=query, db=db)
    assert "esearchresult" in results
    assert "idlist" in results["esearchresult"]
    assert int(results["esearchresult"]["count"]) == 1
    assert results["esearchresult"]["idlist"][0] == "200160513"


def test_eutils_summary():
    """Test the eutils_summary function with a known GSE identifier."""
    query = "GSE160513[ACCN]+GSE[ETYP]"
    db = "gds"
    search_results = eutils_search(query=query, db=db)
    webenv = search_results["esearchresult"]["webenv"]
    query_key = search_results["esearchresult"]["querykey"]
    summary_results = eutils_summary(webenv=webenv, query_key=query_key, db=db)
    assert "result" in summary_results
    assert "200160513" in summary_results["result"]
    assert (
        summary_results["result"]["200160513"]["title"]
        == "Leptin receptor marks and targets the hair follicle mesenchymal niche"
    )


def test_geo_dataset_search_gse():
    """Test the geo_dataset_search function with a known GSE identifier."""
    series = "GSE160513"
    results = geo_dataset_search(series=series, stype="gse")
    assert "esearchresult" in results
    assert "idlist" in results["esearchresult"]
    assert int(results["esearchresult"]["count"]) == 1
    assert results["esearchresult"]["idlist"][0] == "200160513"


def test_geo_dataset_search_bioproject():
    """Test the geo_dataset_search function with a known BioProject identifier."""
    series = "PRJNA673418"
    results = geo_dataset_search(series=series, stype="bioproject")
    assert "esearchresult" in results
    assert "idlist" in results["esearchresult"]
    assert int(results["esearchresult"]["count"]) == 1
    assert results["esearchresult"]["idlist"][0] == "200160513"
