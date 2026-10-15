"""Tier 1: what the rules claim, and what they refuse to."""

from __future__ import annotations

from fetch_series.metadata import Method
from fetch_series.metadata.fields import Scope
from fetch_series.metadata.harvest import Harvest, HarvestedValue
from fetch_series.metadata.rules import propose, unmapped_keys


def _value(source: str, key: str, value: str, **kwargs: object) -> HarvestedValue:
    return HarvestedValue(source=source, key=key, value=value, **kwargs)  # type: ignore[arg-type]


def _harvest(
    sample: str, *values: HarvestedValue, taxon: str = "9606", **kwargs: object
) -> Harvest:
    return Harvest(sample=sample, taxon=taxon, values=list(values), **kwargs)  # type: ignore[arg-type]


class TestScope:
    def test_a_study_scoped_value_never_assigns_a_sample_field(self) -> None:
        """PRJNA434002's study_title names autism for all 56 runs, controls included.

        The per-sample disease column is empty in ENA for every one of them, so
        the only thing a rule could do with the title is label the controls
        diseased.
        """
        harvest = _harvest(
            "SAMN1",
            _value(
                "ena:filereport[study_title]",
                "study_title",
                "Single-nucleus RNA sequencing of brain tissue from Autism Spectrum Disorder",
                scope=Scope.STUDY,
            ),
        )
        [proposal] = propose([harvest])
        assert proposal.claims == []
        assert "study-scoped" in proposal.unmapped[0].reason


class TestProse:
    def test_tier_one_does_not_read_prose(self) -> None:
        """`5864_BA9` is donor 5864, Brodmann area 9 -- and reading that needs
        this study's naming convention, which does not generalise.

        A regex for it would be indistinguishable in the output from a rule
        that generalises, and would inflate tier 1 at tier 3's expense.
        """
        harvest = _harvest("SAMN1", _value("geo:soft[sample_title]", "", "5864_BA9"))
        [proposal] = propose([harvest])
        assert proposal.claims == []
        assert "tier 1 reads keys only" in proposal.unmapped[0].reason

    def test_prose_shared_by_every_sample_is_flagged_as_invariant(self) -> None:
        harvests = [
            _harvest("SAMN1", _value("geo:soft[sample_description]", "", "one protocol")),
            _harvest("SAMN2", _value("geo:soft[sample_description]", "", "one protocol")),
        ]
        proposals = propose(harvests)
        assert proposals[0].invariant_prose == ("geo:soft[sample_description]",)

    def test_varying_prose_is_not_flagged(self) -> None:
        harvests = [
            _harvest("SAMN1", _value("geo:soft[sample_title]", "", "5864_BA9")),
            _harvest("SAMN2", _value("geo:soft[sample_title]", "", "5841_BA9")),
        ]
        assert propose(harvests)[0].invariant_prose == ()

    def test_a_single_sample_study_flags_nothing(self) -> None:
        """Every field is trivially invariant when there is one sample.

        Firing there would make the flag loudest where it says least.
        """
        harvests = [_harvest("SAMN1", _value("geo:soft[sample_title]", "", "x"))]
        assert propose(harvests)[0].invariant_prose == ()


class TestAnInvariantCharacteristicStillAssigns:
    def test_twenty_samples_all_from_lung_are_all_lung(self) -> None:
        """The opposite trap to study scope, and the easier one to fall into.

        A submitter who wrote `tissue: lung` once per sample asserted it once
        per sample. Refusing an invariant value would discard correct metadata
        from every single-tissue study, which is most of them.
        """
        harvests = [
            _harvest(f"SAMN{n}", _value("geo:characteristics[tissue]", "tissue", "lung"))
            for n in range(20)
        ]
        for proposal in propose(harvests):
            assert [c.value for c in proposal.claims] == ["lung"]


class TestMethodRecordsWhichTierActed:
    def test_an_archives_own_spelling_is_archive_not_rule(self) -> None:
        harvest = _harvest("SAMN1", _value("biosample:attribute[tissue]", "tissue", "lung"))
        [proposal] = propose([harvest])
        assert proposal.claims[0].method is Method.ARCHIVE

    def test_a_renamed_key_or_rewritten_value_is_rule(self) -> None:
        """This is what lets a survey say what the synonym tables add."""
        renamed = _harvest("SAMN1", _value("ena:filereport[tissue_type]", "tissue_type", "lung"))
        assert propose([renamed])[0].claims[0].method is Method.RULE
        rewritten = _harvest("SAMN2", _value("biosample:attribute[sex]", "sex", "M"))
        claim = propose([rewritten])[0].claims[0]
        assert claim.method is Method.RULE
        assert claim.value == "male" and claim.raw == "M"


class TestAmbiguityIsReported:
    def test_an_ambiguous_value_is_not_normalised(self) -> None:
        harvest = _harvest("SAMN1", _value("biosample:attribute[disease]", "disease", "Control"))
        [proposal] = propose([harvest])
        assert proposal.claims == []
        assert "SAMN12011228" in proposal.unmapped[0].reason

    def test_an_unmapped_key_names_the_reason_it_is_unmapped(self) -> None:
        harvest = _harvest("SAMN1", _value("geo:characteristics[condition]", "condition", "LPS"))
        [proposal] = propose([harvest])
        assert "treatment arm" in proposal.unmapped[0].reason


class TestContextResolution:
    def test_isolate_becomes_a_donor_under_a_human_package(self) -> None:
        harvest = _harvest(
            "SAMN1",
            _value("biosample:attribute[isolate]", "isolate", "5864", context="Human.1.0"),
        )
        [proposal] = propose([harvest])
        assert proposal.claims[0].field == "donor_id"
        assert proposal.claims[0].value == "5864"

    def test_isolate_stays_unmapped_for_a_microbe(self) -> None:
        harvest = _harvest(
            "SAMN1",
            _value("biosample:attribute[isolate]", "isolate", "DSM 1234", context="Microbe.1.0"),
            taxon="562",
        )
        [proposal] = propose([harvest])
        assert proposal.claims == []
        assert "strain isolate" in proposal.unmapped[0].reason


class TestTheBacklogIsOrderedByWhatItIsWorth:
    def test_keys_are_counted_by_how_many_samples_carry_them(self) -> None:
        """70 of 116 observed keys appeared exactly once; those are not worth declaring."""
        harvests = [
            _harvest(
                f"SAMN{n}",
                _value("geo:characteristics[condition]", "condition", "x"),
                _value("geo:characteristics[weird]", "weird", "y")
                if n == 0
                else _value("geo:characteristics[condition]", "condition", "z"),
            )
            for n in range(5)
        ]
        counts = unmapped_keys(propose(harvests))
        assert next(iter(counts)) == "condition"
        assert counts["weird"] == 1
