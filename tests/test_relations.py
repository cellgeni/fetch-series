"""Offline tests for the relation table and the run record."""

from __future__ import annotations

from types import SimpleNamespace

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


class TestArrayExpressEntryPoint:
    """ENA's filereport rejects an ArrayExpress accession outright."""

    async def test_the_idf_secondary_study_is_used_as_the_ena_key(self, monkeypatch):
        import fetch_series.relations as rel
        from fetch_series.accession import parse

        asked: list[str] = []

        async def idf(client, accession, timeout):
            return ["ERP122394"]

        async def ena(client, accessions, timeout):
            asked.extend(accessions)
            return {"ERR1": RunRecord(run="ERR1")}

        monkeypatch.setattr(rel.biostudies, "idf_secondary_accessions", idf)
        monkeypatch.setattr(rel, "_ena_records", ena)

        records = await rel.relations(parse("E-MTAB-9221"), client=None)
        assert asked == ["ERP122394"]
        assert records[0].ae_experiment == "E-MTAB-9221"

    async def test_a_study_with_no_secondary_accession_falls_back_to_biosamples(self, monkeypatch):
        """E-MTAB-6505 declares none, and is reachable only this way."""
        import fetch_series.relations as rel
        from fetch_series.accession import parse

        asked: list[str] = []

        async def no_idf(client, accession, timeout):
            return []

        async def sdrf(client, accession, timeout):
            return [{"Comment[BioSD_SAMPLE]": "SAMEA5053920"}]

        async def ena(client, accessions, timeout):
            asked.extend(accessions)
            return {"ERR2861957": RunRecord(run="ERR2861957")}

        monkeypatch.setattr(rel.biostudies, "idf_secondary_accessions", no_idf)
        monkeypatch.setattr(rel.biostudies, "sdrf_rows", sdrf)
        monkeypatch.setattr(rel, "_ena_records", ena)

        records = await rel.relations(parse("E-MTAB-6505"), client=None)
        assert asked == ["SAMEA5053920"]
        assert records[0].run == "ERR2861957"

    async def test_a_study_with_neither_returns_nothing_rather_than_asking_ena(self, monkeypatch):
        """Handing an E-MTAB accession to ENA earns a 400 naming the eight
        shapes it accepts, which the caller would see as 'no runs'."""
        import fetch_series.relations as rel
        from fetch_series.accession import parse

        called = []

        async def nothing(client, accession, timeout):
            return []

        async def ena(client, accessions, timeout):
            called.append(accessions)
            return {}

        monkeypatch.setattr(rel.biostudies, "idf_secondary_accessions", nothing)
        monkeypatch.setattr(rel.biostudies, "sdrf_rows", nothing)
        monkeypatch.setattr(rel, "_ena_records", ena)

        assert await rel.relations(parse("E-MTAB-9216"), client=None) == []
        assert called == []


class TestGsmRecoveryThroughBioSample:
    """GSE135325 and GSE137444 record a BioSample per sample and no SRA relation.

    experiment_to_gsm is empty, so nothing attaches a GSM to any run and every
    row carries the INSDC sample instead -- which is a different identity from
    the one a reprocessing pipeline was asked about. ENA reports the BioSample
    for each run, so the same identity joins them from the other side.
    """

    async def test_a_gsm_is_recovered_from_the_biosample(self, monkeypatch):
        import fetch_series.relations as rel
        from fetch_series.accession import parse

        family = SimpleNamespace(
            bioprojects=["PRJNA555555"],
            sample_relations={"GSM4005486": {"biosample": "SAMN12476461"}},
        )

        async def fetch(client, accession, timeout):
            return "soft"

        async def ena(client, accessions, timeout):
            record = RunRecord(run="SRR9895473")
            record.set("biosample", "SAMN12476461", "ena")
            return {"SRR9895473": record}

        monkeypatch.setattr(rel.geo, "fetch_soft_family", fetch)
        monkeypatch.setattr(rel.geo, "parse_soft_family", lambda a, t: family)
        monkeypatch.setattr(rel, "_ena_records", ena)

        records = await rel.relations(parse("GSE135325"), client=None)
        assert records[0].geo_sample == "GSM4005486"
        assert records[0].provenance["geo_sample"] == [rel.BIOSAMPLE_RECOVERY]

    async def test_the_experiment_relation_still_wins_when_geo_names_one(self, monkeypatch):
        """The recovery is a fallback, not a replacement: where GEO names the
        experiment, that is the authoritative attribution."""
        import fetch_series.relations as rel
        from fetch_series.accession import parse

        family = SimpleNamespace(
            bioprojects=["PRJNA1"],
            sample_relations={
                "GSM1": {"experiment": "SRX1", "biosample": "SAMN1"},
                "GSM2": {"biosample": "SAMN1"},
            },
        )

        async def fetch(client, accession, timeout):
            return "soft"

        async def ena(client, accessions, timeout):
            record = RunRecord(run="SRR1")
            record.set("experiment", "SRX1", "ena")
            record.set("biosample", "SAMN1", "ena")
            return {"SRR1": record}

        monkeypatch.setattr(rel.geo, "fetch_soft_family", fetch)
        monkeypatch.setattr(rel.geo, "parse_soft_family", lambda a, t: family)
        monkeypatch.setattr(rel, "_ena_records", ena)

        records = await rel.relations(parse("GSE1"), client=None)
        assert records[0].geo_sample == "GSM1"
        assert records[0].provenance["geo_sample"] == [rel.SOFT_ROUTE]

    async def test_an_unmatched_biosample_leaves_the_run_unattributed(self, monkeypatch):
        """Inventing an attribution would be worse than admitting there is none."""
        import fetch_series.relations as rel
        from fetch_series.accession import parse

        family = SimpleNamespace(
            bioprojects=["PRJNA1"], sample_relations={"GSM1": {"biosample": "SAMN1"}}
        )

        async def fetch(client, accession, timeout):
            return "soft"

        async def ena(client, accessions, timeout):
            record = RunRecord(run="SRR1")
            record.set("biosample", "SAMN999", "ena")
            return {"SRR1": record}

        monkeypatch.setattr(rel.geo, "fetch_soft_family", fetch)
        monkeypatch.setattr(rel.geo, "parse_soft_family", lambda a, t: family)
        monkeypatch.setattr(rel, "_ena_records", ena)

        records = await rel.relations(parse("GSE1"), client=None)
        assert records[0].geo_sample is None
