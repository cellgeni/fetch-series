"""Reading an accession out of ENA's submitter aliases.

ENA returns the submitter's own name for every run, experiment and sample, and
for a GEO- or ArrayExpress-brokered submission that name is usually the
accession itself. That makes a run resolvable to its GSM in one request, without
the SOFT family file and without a BioSample join.

An alias is free text, so the parsing is anchored and the route is expected to
return nothing often. Guessing would be worse than answering nothing: a wrong
GSM attributes a run to another sample, which for a provenance tool is the
failure mode that matters most.
"""

from __future__ import annotations

import pytest

from fetch_series.providers.ena_portal import AE_IN_ALIAS, GSM_IN_ALIAS, alias_accessions


def row(**kw: str) -> dict[str, str]:
    return kw


class TestGsmFromAlias:
    @pytest.mark.parametrize(
        ("alias", "expected"),
        [
            ("GSM4005486", ["GSM4005486"]),
            ("GSM7518069_r1", ["GSM7518069"]),
            ("GSM8462816_r2", ["GSM8462816"]),
        ],
    )
    def test_a_gsm_alias_resolves(self, alias, expected):
        assert alias_accessions([row(sample_alias=alias)], GSM_IN_ALIAS) == expected

    @pytest.mark.parametrize(
        "alias",
        [
            "E12",  # SRR27190799: the submitter's own sample name
            "SAMD00142600",  # DRR188691: a BioSample, not a GEO sample
            "endometrium, LH5_1, rep1",
            "",
        ],
    )
    def test_an_alias_that_is_not_an_accession_resolves_to_nothing(self, alias):
        assert alias_accessions([row(sample_alias=alias)], GSM_IN_ALIAS) == []

    def test_a_name_merely_starting_with_gsm_is_not_a_gsm(self):
        """`GSM\\d+` unanchored would match `GSM123` inside `GSM123ABC` and
        attribute the run to a sample that may well exist."""
        assert alias_accessions([row(sample_alias="GSM123ABC")], GSM_IN_ALIAS) == []
        assert alias_accessions([row(sample_alias="XGSM4005486")], GSM_IN_ALIAS) == []

    def test_the_sample_alias_is_preferred_over_the_others(self):
        """It is the bare accession where the other two carry an `_r1` suffix,
        and taking the cleanest field first keeps the result exact."""
        found = alias_accessions(
            [row(sample_alias="GSM1", experiment_alias="GSM2_r1", run_alias="GSM3_r1")],
            GSM_IN_ALIAS,
        )
        assert found == ["GSM1"]

    def test_a_later_field_is_used_when_the_sample_alias_has_no_accession(self):
        found = alias_accessions(
            [row(sample_alias="E12", experiment_alias="GSM4005486")], GSM_IN_ALIAS
        )
        assert found == ["GSM4005486"]

    def test_results_are_deduplicated_and_sorted(self):
        rows = [row(sample_alias="GSM2"), row(sample_alias="GSM1"), row(sample_alias="GSM2")]
        assert alias_accessions(rows, GSM_IN_ALIAS) == ["GSM1", "GSM2"]

    def test_a_missing_field_is_not_an_error(self):
        assert alias_accessions([{}], GSM_IN_ALIAS) == []


class TestArrayExpressFromAlias:
    def test_an_arrayexpress_prefixed_alias_resolves(self):
        """ERR2861957's alias is `E-MTAB-6505:Day2_r1`."""
        found = alias_accessions([row(sample_alias="E-MTAB-6505:Day2_r1")], AE_IN_ALIAS)
        assert found == ["E-MTAB-6505"]

    def test_a_gsm_alias_is_not_an_arrayexpress_accession(self):
        assert alias_accessions([row(sample_alias="GSM4005486")], AE_IN_ALIAS) == []

    def test_the_two_patterns_do_not_overlap(self):
        for alias in ("GSM4005486", "E-MTAB-6505:x", "E12", "SAMD00142600"):
            gsm = alias_accessions([row(sample_alias=alias)], GSM_IN_ALIAS)
            ae = alias_accessions([row(sample_alias=alias)], AE_IN_ALIAS)
            assert not (gsm and ae), alias
