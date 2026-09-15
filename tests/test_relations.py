"""Offline tests for the relation table and the run record."""

from __future__ import annotations

import pytest

from fetch_series.entities import MISSING, RELATION_COLUMNS, RunRecord
from fetch_series.relations import sample_to_runs


class TestRunRecord:
    def test_first_route_wins(self):
        """Routes are offered in measured order, so the first answer comes from
        the route the evidence ranks highest."""
        r = RunRecord(run="SRR1")
        r.set("species", "Homo sapiens", "route-a")
        r.set("species", "Mus musculus", "route-b")
        assert r.species == "Homo sapiens"

    def test_a_disagreement_is_recorded_not_discarded(self):
        r = RunRecord(run="SRR1")
        r.set("species", "Homo sapiens", "route-a")
        r.set("species", "Mus musculus", "route-b")
        assert r.conflicts["species"] == ["Homo sapiens", "Mus musculus"]

    def test_agreement_is_not_a_conflict(self):
        r = RunRecord(run="SRR1")
        r.set("species", "Homo sapiens", "route-a")
        r.set("species", "Homo sapiens", "route-b")
        assert not r.conflicts
        assert r.provenance["species"] == ["route-a", "route-b"]

    def test_placeholders_and_blanks_are_not_values(self):
        """`-` is the table's missing marker, never an identifier."""
        r = RunRecord(run="SRR1")
        r.set("species", MISSING, "route-a")
        r.set("experiment", "", "route-a")
        assert r.species is None
        assert r.experiment is None
        assert r.provenance == {}

    def test_row_uses_the_placeholder_for_absent_fields(self):
        """No field may be empty: a run of tabs collapses in shell tooling and
        shifts every column after it."""
        row = RunRecord(run="SRR1").as_row()
        assert len(row) == len(RELATION_COLUMNS)
        assert row[0] == "SRR1"
        assert all(cell for cell in row)
        assert row[RELATION_COLUMNS.index("species")] == MISSING

    @pytest.mark.parametrize("sample_field", ["geo_sample", "sample", "biosample"])
    def test_any_sample_identity_is_enough(self, sample_field: str):
        r = RunRecord(run="SRR1", species="Homo sapiens", **{sample_field: "X1"})
        assert r.is_complete_for_reprocessing
        assert r.missing_for_reprocessing == []

    def test_reports_what_is_missing_rather_than_guessing(self):
        r = RunRecord(run="SRR1")
        assert not r.is_complete_for_reprocessing
        assert r.missing_for_reprocessing == ["sample", "species"]

        no_species = RunRecord(run="SRR1", geo_sample="GSM1")
        assert no_species.missing_for_reprocessing == ["species"]


class TestSampleToRuns:
    def test_prefers_the_geo_sample(self):
        """GEO is the identity a reprocessing pipeline is asked about."""
        records = [RunRecord(run="SRR1", geo_sample="GSM1", sample="SRS1")]
        assert sample_to_runs(records) == {"GSM1": ["SRR1"]}

    def test_falls_back_to_the_insdc_sample(self):
        records = [RunRecord(run="SRR1", sample="SRS1")]
        assert sample_to_runs(records) == {"SRS1": ["SRR1"]}

    def test_groups_several_runs_under_one_sample(self):
        """One GEO sample routinely spans several runs -- CITE-seq submitted
        alongside gene expression is the common case."""
        records = [
            RunRecord(run="SRR2", geo_sample="GSM1"),
            RunRecord(run="SRR1", geo_sample="GSM1"),
            RunRecord(run="SRR3", geo_sample="GSM2"),
        ]
        assert sample_to_runs(records) == {"GSM1": ["SRR1", "SRR2"], "GSM2": ["SRR3"]}

    def test_runs_with_no_sample_identity_are_left_out(self):
        records = [RunRecord(run="SRR1"), RunRecord(run="SRR2", geo_sample="GSM1")]
        assert sample_to_runs(records) == {"GSM1": ["SRR2"]}
