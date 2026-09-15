"""Regressions for request-size and paging limits.

Every bug here is the same shape: an archive quietly caps a response or rejects
an over-long request, and the naive call reports a truncated or absent answer as
a success. They are gathered in one file because the pattern matters more than
any single instance -- it has now cost this project four separate defects.
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
