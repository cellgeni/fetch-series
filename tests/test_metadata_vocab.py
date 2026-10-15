"""Key and value normalisation, and the things it deliberately refuses to do."""

from __future__ import annotations

from fetch_series.metadata.vocab import (
    canonical_field,
    normalise_key,
    normalise_value,
    resolve_with_context,
    split_characteristic,
    split_values,
    why_unmapped,
    why_value_unmapped,
)


class TestOneSpellingPerKey:
    """Four archives, four spellings, one field."""

    def test_the_four_archives_agree_after_normalisation(self) -> None:
        for raw in (
            "tissue",
            "tissue_type",
            "Characteristics[organism part]",
            "Factor Value[organism part]",
            "Tissue/Organ",
            "organism part",
        ):
            assert canonical_field(raw) == "tissue", raw

    def test_the_channel_suffix_is_stripped(self) -> None:
        """GEO writes `_ch1` on almost everything, and it is not part of a name."""
        assert normalise_key("disease_state_ch1") == "disease state"
        assert canonical_field("disease_state_ch1") == "disease"

    def test_a_unit_in_parentheses_is_not_part_of_the_name(self) -> None:
        assert canonical_field("age (years)") == "age"

    def test_an_undeclared_key_maps_to_nothing_rather_than_guessing(self) -> None:
        assert canonical_field("relative humidity") is None


class TestSomeKeysAreDeliberatelyNotMapped:
    def test_condition_is_refused_with_a_reason(self) -> None:
        """`condition` holds a diagnosis in one study and a treatment arm in the next.

        Mapping it to `disease` would label an LPS-treated wild-type control as
        diseased. It is reported with the reason rather than dropped, because a
        corpus that silently discards rows has an unknowable denominator.
        """
        assert canonical_field("condition") is None
        reason = why_unmapped("condition")
        assert reason is not None and "treatment arm" in reason

    def test_source_name_is_refused_because_it_is_composite(self) -> None:
        assert canonical_field("source name") is None
        assert why_unmapped("source_name") is not None

    def test_cell_line_is_refused_rather_than_called_a_tissue(self) -> None:
        assert canonical_field("cell line") is None
        assert why_unmapped("cell line") is not None


class TestValueNormalisation:
    def test_sex_synonyms_collapse(self) -> None:
        for raw in ("M", "male", "Males", "man", "XY"):
            assert normalise_value("sex", raw) == "male", raw
        for raw in ("F", "female", "woman", "XX"):
            assert normalise_value("sex", raw) == "female", raw

    def test_an_explicit_absence_is_unknown_and_not_empty(self) -> None:
        """`not collected` is a statement, and it is not the same as silence."""
        assert normalise_value("sex", "not collected") == "unknown"

    def test_healthy_maps_to_normal_but_bare_control_does_not(self) -> None:
        """SAMN12011228 carries `disease = Control` in an autism study.

        There it means healthy. In a study where every donor is diseased and
        some are untreated, the same string under the same key means "diseased,
        untreated" -- so it is reported with the reason instead.
        """
        assert normalise_value("disease", "healthy control") == "normal"
        assert normalise_value("disease", "Control") == "Control"
        reason = why_value_unmapped("disease", "Control")
        assert reason is not None and "SAMN12011228" in reason

    def test_cleaning_is_conservative_because_the_result_gets_looked_up(self) -> None:
        """An over-eager rewrite turns a findable label into an unfindable one."""
        assert normalise_value("tissue", "  prefrontal   cortex ") == "prefrontal cortex"
        assert normalise_value("tissue", "Brain, prefrontal cortex") == "Brain, prefrontal cortex"


class TestSplitting:
    def test_a_value_naming_two_things_splits_longest_first(self) -> None:
        assert split_values("lung, lymph node") == ("lymph node", "lung")

    def test_a_hierarchical_pair_splits_specific_first(self) -> None:
        """`Brain, prefrontal cortex` is one tissue named at two levels.

        Nothing in the string says whether it is a mixture or a hierarchy, so
        both parts are offered; longest-first means the caller taking the first
        that grounds gets `prefrontal cortex` and not `Brain`.
        """
        assert split_values("Brain, prefrontal cortex")[0] == "prefrontal cortex"

    def test_a_marker_plus_is_not_a_separator(self) -> None:
        """Splitting on `+` turned `CD4+` into a `CD4` no ontology has a term for."""
        assert split_values("CD4+ and CD8+ T cells") == ("CD8+ T cells", "CD4+")

    def test_a_single_value_does_not_split(self) -> None:
        assert split_values("PBMC") == ()

    def test_characteristics_split_on_the_first_colon_only(self) -> None:
        """`diagnosis: stage II, ER+: yes` -- a value with a colon is common."""
        assert split_characteristic("diagnosis: stage II, ER+: yes") == (
            "diagnosis",
            "stage II, ER+: yes",
        )

    def test_a_line_with_no_colon_is_not_a_pair(self) -> None:
        assert split_characteristic("just a bare value") is None


class TestContextDependentKeys:
    """`isolate` is the donor in a human record and a strain in a microbial one."""

    def test_the_package_decides(self) -> None:
        """SAMN12011225 carries `isolate = 5864` beside a title of `5864_BA9`.

        NCBI's Human package defines `isolate` as the individual the sample came
        from, which makes it the donor -- the rarest and most valuable
        structural field in the corpus.
        """
        assert resolve_with_context("isolate", package="Human.1.0") == ("donor_id", None)

    def test_the_taxon_decides_when_the_package_abstains(self) -> None:
        """SAMN12039425 is a mouse sample filed under `Generic.1.0`."""
        assert resolve_with_context("isolate", package="Generic.1.0", taxon="10090") == (
            "donor_id",
            None,
        )

    def test_a_microbial_record_is_refused_with_a_reason(self) -> None:
        name, reason = resolve_with_context("isolate", package="Microbe.1.0", taxon="562")
        assert name is None
        assert reason is not None and "strain isolate" in reason

    def test_the_value_is_never_used_to_decide(self) -> None:
        """`C57BL/6` under `isolate` reads as a plausible donor label.

        Only the archive's own declaration decides, so an undeclared record
        stays unmapped whatever the value looks like.
        """
        assert resolve_with_context("isolate")[0] is None

    def test_an_ordinary_key_is_not_context_dependent(self) -> None:
        assert resolve_with_context("tissue", package="Human.1.0") == (None, None)
