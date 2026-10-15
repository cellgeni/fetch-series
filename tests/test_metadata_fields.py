"""The field set is declared data, and the declarations have to hold together."""

from __future__ import annotations

import pytest

from fetch_series.metadata import fields as field_module
from fetch_series.metadata.fields import (
    FIELD_NAMES,
    GROUNDED_FIELDS,
    HUMAN,
    MOUSE,
    Scope,
    by_name,
)


class TestDeclarations:
    def test_every_field_name_is_unique(self) -> None:
        assert len(FIELD_NAMES) == len(set(FIELD_NAMES))

    def test_every_grounded_field_names_at_least_one_ontology(self) -> None:
        for spec in GROUNDED_FIELDS:
            assert spec.permitted_ontologies(), f"{spec.name} is grounded but names no ontology"

    def test_structural_fields_name_no_ontology(self) -> None:
        """A field with no ontology carries a normalised string and no CURIE.

        `donor_id` is an opaque submitter label. Declaring an ontology for it
        would make `ground()` look one up, and anything it found would be a
        false friend.
        """
        for name in ("donor_id", "sample_grouping", "multiplexing", "timepoint", "age"):
            assert not by_name(name).grounded

    def test_by_name_raises_on_a_typo_rather_than_returning_none(self) -> None:
        with pytest.raises(KeyError, match="no such metadata field"):
            by_name("tisue")

    def test_every_field_is_sample_scoped(self) -> None:
        """Every declared field describes a sample, not a study.

        This is what makes the study-scope rule checkable: a source declared
        study-scoped can never legitimately assign any of them. If a
        study-scoped *field* is ever added, `rules.propose` needs a
        corresponding change and this test is where that gets noticed.
        """
        assert all(by_name(n).scope is Scope.SAMPLE for n in FIELD_NAMES)


class TestOntologyByTaxon:
    """Developmental stage is the field whose ontology depends on the organism."""

    def test_human_and_mouse_get_species_specific_ontologies(self) -> None:
        spec = by_name("development_stage")
        assert spec.permitted_ontologies(HUMAN) == ("hsapdv",)
        assert spec.permitted_ontologies(MOUSE) == ("mmusdv",)

    def test_an_unlisted_taxon_falls_back_rather_than_refusing(self) -> None:
        """A zebrafish stage grounds against UBERON or not at all.

        Falling back to the generic ontology is worse than a species-specific
        answer and much better than declining to ground anything for every
        organism this project has not enumerated.
        """
        spec = by_name("development_stage")
        assert spec.permitted_ontologies("7955") == ("uberon",)
        assert spec.permitted_ontologies(None) == ("uberon",)

    def test_sex_grounds_in_pato_and_not_uberon(self) -> None:
        """The measured trap: ZOOMA answers 'male' with UBERON:0003101.

        Constrained to UBERON -- an anatomy ontology -- ZOOMA returns *male
        organism* at confidence GOOD for a phenotypic quality. The field
        declaring PATO is what prevents that being asked in the first place.
        """
        assert by_name("sex").permitted_ontologies() == ("pato",)
        assert "uberon" not in by_name("sex").permitted_ontologies()


class TestTheDeclarationIsData:
    def test_adding_a_field_needs_no_change_to_the_core(self) -> None:
        """A new field is a dataclass instance, not a code change."""
        extra = field_module.Field(
            name="ancestry",
            scope=Scope.SAMPLE,
            summary="Self-reported ancestry.",
            ontologies=("hancestro",),
        )
        assert extra.grounded
        assert extra.permitted_ontologies() == ("hancestro",)
        assert not extra.multivalued
