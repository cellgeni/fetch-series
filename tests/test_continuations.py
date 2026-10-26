"""Document -> record links: the second half of the request that fetched the document."""

from __future__ import annotations

from fetch_series.catalogue import CATALOGUE, NodeKind
from fetch_series.catalogue.continuations import DOCUMENT_READERS, continuations
from fetch_series.export.graph import route_graph


def test_a_soft_family_names_its_samples():
    """The pipeline reads a series' samples out of its SOFT family file."""
    found = {(c.document, c.target, c.fetched_by.mechanism) for c in continuations()}
    assert ("metadata_geo", "geo_sample", "geo_soft_family") in found
    assert ("metadata_ae", "ae_sample", "biostudies_sdrf") in found


def test_a_continuation_is_never_a_catalogue_link():
    """The catalogue declares each request once; a continuation adds no link to count."""
    assert not any(CATALOGUE.nodes[link.source].kind is NodeKind.METADATA for link in CATALOGUE)


def test_a_reader_reads_the_same_document_its_fetcher_fetched():
    for fetcher, readers in DOCUMENT_READERS.items():
        endpoint = CATALOGUE.mechanisms[fetcher].endpoint.split("?")[0]
        for reader in readers:
            assert CATALOGUE.mechanisms[reader].endpoint.split("?")[0] == endpoint, (
                fetcher,
                reader,
            )


def test_continuations_follow_their_record_links_into_the_graph():
    graph = route_graph()
    continued = [e for e in graph["edges"] if e.get("continues")]
    assert continued and all(e["source"].startswith("metadata_") for e in continued)
    assert ["geo_series", "metadata_geo", "biosample"] in graph["paths_surveyed"]["geo_series"][
        "biosample"
    ]
