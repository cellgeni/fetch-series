"""Regressions for request-size and paging limits.

Every bug here is the same shape: an archive quietly caps a response or rejects
an over-long request, and the naive call reports a truncated or absent answer as
a success. They are gathered in one file because the pattern matters more than
any single instance -- it has now cost this project five separate defects.
"""

from __future__ import annotations

import pytest

from fetch_series.providers import sra_be
from fetch_series.survey.client import MalformedResponseError


class TestRuninfoHeaderValidation:
    """A header-only or error response must not read as 'no runs'."""

    def test_an_empty_body_is_a_legitimate_empty(self):
        assert sra_be.parse_runinfo("") == []
        assert sra_be.parse_runinfo("   \n") == []

    def test_a_proper_runinfo_table_parses(self):
        rows = sra_be.parse_runinfo("Run,Experiment\nSRR1,SRX1\nSRR2,SRX1\n")
        assert sra_be.column(rows, "Run") == ["SRR1", "SRR2"]

    def test_a_header_only_response_is_still_valid(self):
        """Headers with no data rows is how the backend says 'nothing here'."""
        assert sra_be.parse_runinfo("Run,Experiment\n") == []

    def test_a_one_line_error_page_raises_rather_than_reading_as_empty(self):
        """The bug. DictReader yields no rows for a single-line body, so a
        rows-based check passed it through as a genuine true negative."""
        with pytest.raises(MalformedResponseError, match="no Run column"):
            sra_be.parse_runinfo("Error: your query could not be processed")

    def test_a_wrong_table_raises_even_with_rows(self):
        with pytest.raises(MalformedResponseError, match="no Run column"):
            sra_be.parse_runinfo("Accession,Title\nSRX1,something\n")

    def test_column_deduplicates(self):
        """824,216 CGI rows held only 754,277 distinct runs."""
        rows = sra_be.parse_runinfo("Run,Experiment\nSRR1,SRX1\nSRR1,SRX1\nSRR2,SRX1\n")
        assert sra_be.column(rows, "Run") == ["SRR1", "SRR2"]


class TestGeoSampleSize:
    """A corpus name is persisted as evidence provenance, so it must be true."""

    @pytest.mark.slow
    @pytest.mark.parametrize("n", [1, 7, 15, 23, 100, 999])
    def test_returns_exactly_the_requested_size(self, n: int):
        from fetch_series.survey.corpora import geo_sample

        try:
            corpus = geo_sample(n)
        except FileNotFoundError:
            pytest.skip("GSE list not present (Git LFS object not fetched)")
        assert len(corpus) == n, f"geo-sample-{n} holds {len(corpus)}"
        assert corpus.name == f"geo-sample-{n}"

    @pytest.mark.slow
    def test_the_existing_survey_draw_is_unchanged(self):
        """Fixing the size must not silently redraw a corpus already surveyed."""
        from fetch_series.survey.corpora import geo_sample

        try:
            drawn = set(geo_sample(1000).values)
        except FileNotFoundError:
            pytest.skip("GSE list not present")
        # Accessions recorded against geo-sample-1000 in the 2026-09 survey.
        previously_surveyed = {"GSE27679", "GSE29158", "GSE34359", "GSE49732", "GSE49780"}
        assert previously_surveyed <= drawn


class TestBioStudiesSearchWindow:
    """BioStudies serves the first 20,000 hits and then answers HTTP 500.

    Not 400, and not a short page: a permanent condition wearing a status code
    that every retry policy reads as transient. The ArrayExpress collection has
    20,693 studies with an ENA link, so a plain enumeration fails at 96.6% --
    late enough that a run looks healthy until it does not.
    """

    @pytest.fixture
    def paging_client(self):
        class _Response:
            def __init__(self, payload):
                self._payload = payload

            def json(self):
                return self._payload

        class _Client:
            def __init__(self, total: int, per_page: int = 100):
                self.total = total
                self.per_page = per_page
                self.pages_fetched = 0

            async def get(self, url, params=None, timeout=None):
                self.pages_fetched += 1
                page = int((params or {}).get("page", 1))
                start = (page - 1) * self.per_page
                hits = [
                    {"accession": f"E-MTAB-{i}"}
                    for i in range(start, min(start + self.per_page, self.total))
                ]
                return _Response({"hits": hits, "totalHits": self.total})

        return _Client

    async def test_a_query_inside_the_window_pages_to_exhaustion(self, paging_client):
        from fetch_series.providers import biostudies

        client = paging_client(250)
        found = await biostudies.search(client, timeout=10.0)
        assert len(found) == 250
        assert client.pages_fetched == 3

    async def test_a_query_past_the_window_refuses_instead_of_paging_into_a_500(
        self, paging_client
    ):
        """The bug: paging on regardless reaches page 201 and a server error."""
        from fetch_series.providers import biostudies

        client = paging_client(biostudies.SEARCH_WINDOW + 1)
        with pytest.raises(ValueError, match="serves only the first"):
            await biostudies.search(client, timeout=10.0)
        # It must refuse on the first page, not after 200 wasted requests.
        assert client.pages_fetched == 1

    async def test_max_pages_is_an_explicit_opt_out(self, paging_client):
        """A caller that asks for a bounded slice is not making the mistake."""
        from fetch_series.providers import biostudies

        client = paging_client(biostudies.SEARCH_WINDOW + 1)
        found = await biostudies.search(client, timeout=10.0, max_pages=2)
        assert len(found) == 200

    async def test_year_partitioning_covers_the_collection_exactly(self, paging_client):
        """Every study has exactly one release year, so the partitions are
        disjoint and exhaustive -- the property the workaround rests on."""
        from fetch_series.providers import biostudies

        class _ByYear:
            """Each year holds 10 studies, none of them shared."""

            async def get(self, url, params=None, timeout=None):
                year = int(params["facet.released_year"])
                page = int(params.get("page", 1))
                hits = [{"accession": f"E-MTAB-{year}-{i}"} for i in range(10)] if page == 1 else []

                class _R:
                    def json(self_inner):
                        return {"hits": hits, "totalHits": 10}

                return _R()

        found = await biostudies.search_by_year(_ByYear(), timeout=10.0, through_year=2003)
        years = 2003 - biostudies.FIRST_RELEASE_YEAR + 1
        assert len(found) == 10 * years
        assert len(set(found)) == len(found)
