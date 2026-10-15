"""What can be measured about this layer, and the circularity guards on it."""

from __future__ import annotations

from datetime import date

from fetch_series.metadata import Claim, Method, SampleMetadata, Term
from fetch_series.metadata.evidence import (
    AGREEMENT_BAND,
    SOURCES,
    ExtractorEvidence,
    ranked,
    source_of,
    unmeasured_fields,
)
from fetch_series.metadata.gold import (
    LABEL_SOURCE,
    Outcome,
    agreement,
    is_independent,
    key_verdicts,
    labels_from_sdrf,
    score,
    summarise,
    unmapped_label_keys,
)
from fetch_series.providers.biostudies import SdrfCharacteristic

ON = date(2026, 9, 16)


def _characteristic(key: str, value: str, curie: str | None = None) -> SdrfCharacteristic:
    return SdrfCharacteristic(
        key=key,
        value=value,
        kind="characteristics",
        term_accession=curie.replace(":", "_") if curie else None,
    )


def _record(sample: str, *claims: Claim, consulted: frozenset[str] | None = None) -> SampleMetadata:
    record = SampleMetadata(
        sample=sample,
        consulted=consulted if consulted is not None else frozenset(s.id for s in SOURCES),
    )
    for claim in claims:
        record.add(claim)
    return record


def _claim(field: str, value: str, source: str, curie: str | None = None) -> Claim:
    return Claim(
        field=field,
        value=value,
        method=Method.ONTOLOGY if curie else Method.ARCHIVE,
        source=source,
        evidence=value,
        term=Term(curie, value, curie.split(":")[0].lower(), "ols:label") if curie else None,
    )


class TestLabels:
    def test_only_declared_fields_become_labels(self) -> None:
        """`Characteristics[clinical history]` describes nothing this set declares."""
        labels = labels_from_sdrf(
            {
                "SAMN1": [
                    _characteristic("organism part", "liver"),
                    _characteristic("clinical history", "three prior lines of therapy"),
                ]
            }
        )
        assert [(label.field, label.value) for label in labels] == [("tissue", "liver")]

    def test_a_curated_term_accession_is_carried(self) -> None:
        labels = labels_from_sdrf(
            {"SAMN1": [_characteristic("organism part", "liver", "UBERON:0002107")]}
        )
        assert labels[0].curie == "UBERON:0002107"


class TestCircularityGuards:
    def test_arrayexpress_is_never_scored_against_itself(self) -> None:
        """It is both a declared source and the label; scoring it returns 100%."""
        labels = labels_from_sdrf({"SAMN1": [_characteristic("organism part", "liver")]})
        record = _record(
            "SAMN1",
            _claim("tissue", "liver", f"{LABEL_SOURCE}[organism part]"),
            consulted=frozenset({LABEL_SOURCE}),
        )
        assert [v.source for v in score([record], labels)] == []

    def test_an_ebi_registered_sample_yields_no_scoreable_evidence(self) -> None:
        """E-MTAB-10553 is all SAMEA: its ENA records come from the same submission
        as its SDRF, so it scores 100% on every field both carry and ranks nothing.
        """
        labels = labels_from_sdrf({"SAMEA1": [_characteristic("organism part", "liver")]})
        record = _record("SAMEA1", _claim("tissue", "liver", "ena:filereport[tissue_type]"))
        verdicts = score([record], labels)
        assert verdicts and all(not v.independent for v in verdicts)
        assert summarise(verdicts, "gold:E-MTAB-10553", ON) == {}

    def test_an_independently_registered_sample_is_scoreable(self) -> None:
        labels = labels_from_sdrf({"SAMN1": [_characteristic("organism part", "liver")]})
        record = _record("SAMN1", _claim("tissue", "liver", "ena:filereport[tissue_type]"))
        evidence = summarise(score([record], labels), "gold:x", ON)
        assert evidence[("ena:filereport", "tissue")].agreements == 1

    def test_registration_authority_decides_independence(self) -> None:
        assert is_independent("SAMN12011225")
        assert is_independent("GSM677627")
        assert not is_independent("SAMEA11294529")


class TestSilenceIsNotError:
    def test_a_source_that_said_nothing_is_silent_not_wrong(self) -> None:
        labels = labels_from_sdrf({"SAMN1": [_characteristic("organism part", "liver")]})
        record = _record("SAMN1", _claim("tissue", "liver", "biosample:attribute[tissue]"))
        outcomes = {v.source: v.outcome for v in score([record], labels)}
        assert outcomes["biosample:attribute"] is Outcome.AGREE_UNGROUNDED
        assert outcomes["ena:filereport"] is Outcome.SILENT

    def test_silence_is_excluded_from_the_agreement_rate(self) -> None:
        """A source answering 5% of samples and always right is not 5% accurate."""
        labels = labels_from_sdrf({"SAMN1": [_characteristic("organism part", "liver")]})
        record = _record("SAMN1", _claim("tissue", "liver", "biosample:attribute[tissue]"))
        evidence = summarise(score([record], labels), "gold:x", ON)
        silent = evidence[("ena:filereport", "tissue")]
        assert silent.samples_answered == 0
        assert silent.agreement_rate == 0.0
        assert silent.answer_rate == 0.0


class TestKeyMappingIsTheOneThingTheGeoImportMeasures:
    def test_a_renamed_key_that_maps_to_the_same_field_agrees(self) -> None:
        """E-GEOD-25789: GEO's `strain` arrives as EBI's `strain or line`.

        The value is copied verbatim, which is why the join is on the value --
        keying on the key would beg the question being asked.
        """
        verdicts = key_verdicts(
            {"GSM677627": [("developmental stage", "late embryo")]},
            {"GSM677627": [_characteristic("developmental stage", "late embryo")]},
        )
        assert verdicts[0].outcome is Outcome.AGREE

    def test_a_key_this_project_maps_to_nothing_is_silent(self) -> None:
        verdicts = key_verdicts(
            {"GSM1": [("strain", "N2")]},
            {"GSM1": [_characteristic("strain or line", "N2")]},
        )
        assert verdicts[0].outcome is Outcome.SILENT
        assert unmapped_label_keys(verdicts) == {"strain or line": 1}

    def test_values_that_do_not_match_are_not_compared(self) -> None:
        verdicts = key_verdicts(
            {"GSM1": [("tissue", "lung")]},
            {"GSM1": [_characteristic("organism part", "liver")]},
        )
        assert verdicts == []


class TestAgreement:
    def test_two_sources_agreeing_is_counted_once(self) -> None:
        record = _record(
            "SAMN1",
            _claim("tissue", "liver", "ena:filereport[tissue_type]"),
            _claim("tissue", "liver", "biosample:attribute[tissue]"),
        )
        [pair] = [a for a in agreement([record]) if a.field == "tissue" and a.agreed]
        assert (pair.left, pair.right) == ("biosample:attribute", "ena:filereport")
        assert pair.agreed == 1 and pair.disagreed == 0

    def test_disagreement_is_counted_as_such(self) -> None:
        record = _record(
            "SAMN1",
            _claim("sex", "male", "ena:filereport[sex]"),
            _claim("sex", "female", "biosample:attribute[sex]"),
        )
        [pair] = [a for a in agreement([record]) if a.field == "sex" and a.disagreed]
        assert pair.disagreed == 1

    def test_a_less_complete_source_is_not_contradicting(self) -> None:
        """`lung` where another says `lung, lymph node` is less complete, not wrong."""
        record = _record(
            "SAMN1",
            _claim("tissue", "lung", "ena:filereport[tissue_type]"),
            _claim("tissue", "lung", "biosample:attribute[tissue]"),
            _claim("tissue", "lymph node", "biosample:attribute[tissue]"),
        )
        [pair] = [a for a in agreement([record]) if a.field == "tissue" and a.agreed]
        assert pair.agreed == 1

    def test_a_source_never_queried_is_not_counted_as_empty(self) -> None:
        """A BioProject with no GEO series was never asked what GEO says.

        Counting that as GEO being silent manufactures a true negative out of a
        request that was never made -- the fourth verdict state.
        """
        record = _record(
            "SAMN1",
            _claim("tissue", "liver", "ena:filereport[tissue_type]"),
            consulted=frozenset({"ena:filereport"}),
        )
        assert [a for a in agreement([record]) if "geo:characteristics" in (a.left, a.right)] == []

    def test_sources_are_compared_by_archive_not_by_column_name(self) -> None:
        """ENA's `tissue_type` and BioSample's `attribute[tissue]` are one question.

        Grouping by the raw provenance string put every pair in a bucket of its
        own and reported no agreement anywhere.
        """
        record = _record(
            "SAMN1",
            _claim("tissue", "liver", "ena:filereport[tissue_type]"),
            _claim("tissue", "liver", "biosample:attribute[tissue]"),
        )
        assert any(a.agreed for a in agreement([record]))


class TestRankingIsDerivedAndNeverWrittenDown:
    def test_nothing_is_measured_yet_and_the_code_says_so(self) -> None:
        """Every declared source carries no evidence. That is the honest state.

        Its consequence is enforced elsewhere: `asserted()` declines to pick
        between two sources that disagree, because there is no measured order
        to justify preferring one.
        """
        assert all(not source.measured for source in SOURCES)
        assert all(gaps for gaps in unmeasured_fields().values())

    def test_unmeasured_sources_sort_by_cost_then_id_for_stability(self) -> None:
        """A source id is recorded as provenance on every claim.

        An unstable order would make two identical queries produce
        differently-attributed answers. Cost precedes the id tie-break, exactly
        as in ``graph.ranked()``, so a free source is consulted first.
        """
        assert [s.id for s in ranked("tissue")] == [
            s.id for s in sorted(SOURCES, key=lambda s: (s.cost, s.id))
        ]

    def test_the_order_is_identical_on_repeated_calls(self) -> None:
        assert [s.id for s in ranked("tissue")] == [s.id for s in ranked("tissue")]

    def test_a_measured_source_outranks_an_unmeasured_one(self) -> None:
        import dataclasses

        measured = dataclasses.replace(
            SOURCES[-1],
            evidence={
                "tissue": ExtractorEvidence(
                    corpus="gold:x",
                    surveyed_on=ON,
                    field="tissue",
                    samples_queried=100,
                    samples_answered=90,
                    agreements=80,
                    disagreements=10,
                )
            },
        )
        order = sorted(
            [*SOURCES[:-1], measured],
            key=lambda s: (0 if s.evidence.get("tissue") else 1, s.id),
        )
        assert order[0].id == measured.id

    def test_the_band_separates_noise_from_substance(self) -> None:
        """Ranking on raw rates put noise above substance twice in the route layer."""
        assert AGREEMENT_BAND == 0.01

    def test_verdict_counts_cannot_exceed_what_was_answered(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="exceed"):
            ExtractorEvidence(
                corpus="gold:x",
                surveyed_on=ON,
                field="tissue",
                samples_queried=10,
                samples_answered=5,
                agreements=4,
                disagreements=4,
            )


class TestProvenanceParsesBackToADeclaredSource:
    def test_every_harvest_source_shape_resolves(self) -> None:
        for provenance, expected in [
            ("ena:filereport[tissue_type]", "ena:filereport"),
            ("biosample:attribute[tissue]", "biosample:attribute"),
            ("biosample:description[title]", "biosample:description"),
            ("geo:characteristics[tissue]", "geo:characteristics"),
            ("geo:soft[sample_title]", "geo:soft"),
            ("ae:characteristics[organism part]", "ae:characteristics"),
        ]:
            found = source_of(provenance)
            assert found is not None and found.id == expected, provenance

    def test_an_undeclared_provenance_is_none_rather_than_its_neighbour(self) -> None:
        assert source_of("something:invented[x]") is None


class TestRepetitionIsNotCorroboration:
    def test_the_same_source_saying_the_same_thing_twice_counts_once(self) -> None:
        """ENA's filereport is per run, not per sample.

        GSE109816's single sample has 880 runs, and before this check the
        filereport offered its tissue once per run -- 2,688 grounded claims for
        one sample, inflating every count derived from them.
        """
        record = _record(
            "SAMN1",
            _claim("tissue", "liver", "ena:filereport[tissue_type]"),
            _claim("tissue", "liver", "ena:filereport[tissue_type]"),
            _claim("tissue", "liver", "ena:filereport[tissue_type]"),
        )
        assert len(record.claims["tissue"]) == 1

    def test_a_different_source_saying_the_same_thing_is_kept(self) -> None:
        """Two independent sources agreeing is the finding, not noise."""
        record = _record(
            "SAMN1",
            _claim("tissue", "liver", "ena:filereport[tissue_type]"),
            _claim("tissue", "liver", "biosample:attribute[tissue]"),
        )
        assert len(record.claims["tissue"]) == 2
        assert any(a.agreed for a in agreement([record]))
