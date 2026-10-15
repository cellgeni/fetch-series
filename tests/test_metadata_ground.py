"""Tier 2: grounding, and the two things it refuses to assert.

Offline. `ols.exact_match` and `zooma.annotate` are monkeypatched on the module
attributes the grounder reads, which is the house pattern -- no cassettes exist
in this project and none are expected.
"""

from __future__ import annotations

from typing import Any

import pytest

from fetch_series.metadata import Claim, Method, SampleMetadata, Term
from fetch_series.metadata import ground as ground_module
from fetch_series.metadata.ground import Grounder, _looks_like_abbreviation
from fetch_series.providers.ols import OlsTerm


def _claim(field: str, value: str, source: str = "biosample:attribute[tissue]") -> Claim:
    return Claim(field=field, value=value, method=Method.ARCHIVE, source=source, evidence=value)


@pytest.fixture
def stub(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """A fake OLS and ZOOMA, recording what they were asked."""
    state: dict[str, Any] = {"ols": {}, "zooma": {}, "curies": {}, "asked": []}

    async def exact_match(client, value, ontologies, timeout):  # type: ignore[no-untyped-def]
        state["asked"].append(("ols", value, ontologies))
        return list(state["ols"].get(value, []))

    async def annotate(client, value, ontologies, timeout, *, property_type=None):  # type: ignore[no-untyped-def]
        state["asked"].append(("zooma", value, ontologies))
        return list(state["zooma"].get(value, []))

    async def lookup_curie(client, curie, timeout):  # type: ignore[no-untyped-def]
        return state["curies"].get(curie)

    monkeypatch.setattr(ground_module.ols, "exact_match", exact_match)
    monkeypatch.setattr(ground_module.ols, "lookup_curie", lookup_curie)
    monkeypatch.setattr(ground_module.zooma, "annotate", annotate)
    return state


class TestExactMatchIsTriedOnPartsBeforeZoomaIsTriedAtAll:
    async def test_the_whole_value_is_tried_first(self, stub: dict[str, Any]) -> None:
        stub["ols"]["prefrontal cortex"] = [
            OlsTerm("UBERON:0000451", "prefrontal cortex", "uberon", "prefrontal cortex", "label")
        ]
        grounder = Grounder(client=None)  # type: ignore[arg-type]
        [claim] = await grounder.ground(_claim("tissue", "prefrontal cortex"))
        assert claim.term is not None and claim.term.curie == "UBERON:0000451"
        assert [kind for kind, *_ in stub["asked"]] == ["ols"]

    async def test_a_part_is_tried_before_the_approximate_step(self, stub: dict[str, Any]) -> None:
        """`Brain, prefrontal cortex` is a real BioSample tissue value.

        It has no exact match. ZOOMA, asked, returns UBERON:0001870 *frontal
        cortex* -- the wrong gyrus -- with the correct term ranked second. The
        split finds the right one deterministically.
        """
        stub["ols"]["prefrontal cortex"] = [
            OlsTerm("UBERON:0000451", "prefrontal cortex", "uberon", "prefrontal cortex", "label")
        ]
        grounder = Grounder(client=None)  # type: ignore[arg-type]
        [claim] = await grounder.ground(_claim("tissue", "Brain, prefrontal cortex"))
        assert claim.term is not None and claim.term.curie == "UBERON:0000451"
        assert "zooma" not in [kind for kind, *_ in stub["asked"]]
        assert grounder.stats.ols_parts == 1

    async def test_the_ontology_label_becomes_the_value(self, stub: dict[str, Any]) -> None:
        """The submitter's string is what a search index cannot use."""
        stub["ols"]["prefrontal cortex"] = [
            OlsTerm("UBERON:0000451", "prefrontal cortex", "uberon", "prefrontal cortex", "label")
        ]
        grounder = Grounder(client=None)  # type: ignore[arg-type]
        [claim] = await grounder.ground(_claim("tissue", "Brain, prefrontal cortex"))
        assert claim.value == "prefrontal cortex"
        assert claim.raw == "Brain, prefrontal cortex"
        assert claim.method is Method.ONTOLOGY


class TestAValueNamingTwoThingsBecomesTwoClaims:
    async def test_a_mixture_is_not_collapsed(self, stub: dict[str, Any]) -> None:
        stub["ols"]["lung, lymph node"] = [
            OlsTerm("UBERON:0002048", "lung", "uberon", "lung", "label"),
            OlsTerm("UBERON:0000029", "lymph node", "uberon", "lymph node", "label"),
        ]
        grounder = Grounder(client=None)  # type: ignore[arg-type]
        claims = await grounder.ground(_claim("tissue", "lung, lymph node"))
        assert {c.term.curie for c in claims if c.term} == {
            "UBERON:0002048",
            "UBERON:0000029",
        }


class TestDecliningIsAResult:
    async def test_an_ungroundable_value_keeps_its_string(self, stub: dict[str, Any]) -> None:
        grounder = Grounder(client=None)  # type: ignore[arg-type]
        [claim] = await grounder.ground(_claim("tissue", "some bespoke prose"))
        assert claim.term is None
        assert claim.value == "some bespoke prose"
        assert grounder.stats.ungrounded == 1

    async def test_it_is_never_an_empty_list(self, stub: dict[str, Any]) -> None:
        """Returning nothing would silently delete the value."""
        grounder = Grounder(client=None)  # type: ignore[arg-type]
        assert len(await grounder.ground(_claim("tissue", "nothing matches this"))) == 1

    async def test_a_structural_field_is_not_looked_up_at_all(self, stub: dict[str, Any]) -> None:
        grounder = Grounder(client=None)  # type: ignore[arg-type]
        [claim] = await grounder.ground(_claim("donor_id", "5864"))
        assert claim.term is None
        assert stub["asked"] == []


class TestAnAbbreviationMatchedBySynonymIsNeverAsserted:
    async def test_asd_is_flagged_not_asserted(self, stub: dict[str, Any]) -> None:
        """SAMN12011225 carries `disease = ASD` in an autism study.

        MONDO holds `ASD` as a synonym of *atrial septal defect* and of nothing
        else -- autism spectrum disorder does not carry it -- so the lookup
        returns exactly one hit and looks unambiguous. It is a different organ
        system.
        """
        stub["ols"]["ASD"] = [
            OlsTerm("MONDO:0006664", "atrial septal defect", "mondo", "ASD", "synonym")
        ]
        grounder = Grounder(client=None)  # type: ignore[arg-type]
        [claim] = await grounder.ground(_claim("disease", "ASD"))
        assert claim.term is not None and claim.term.curie == "MONDO:0006664"
        assert claim.needs_review
        assert "abbreviation" in (claim.review or "")

    async def test_the_study_text_can_corroborate_an_expansion(self, stub: dict[str, Any]) -> None:
        """A genuine annotation is not penalised for being an abbreviation."""
        stub["ols"]["PBMC"] = [
            OlsTerm("CL:2000001", "peripheral blood mononuclear cell", "cl", "PBMC", "synonym")
        ]
        grounder = Grounder(
            client=None,  # type: ignore[arg-type]
            study_text="We sorted peripheral blood mononuclear cell fractions from each donor.",
        )
        [claim] = await grounder.ground(_claim("cell_type", "PBMC"))
        assert not claim.needs_review

    async def test_a_label_match_is_not_flagged_however_short(self, stub: dict[str, Any]) -> None:
        """The risk is in the synonym, not in the brevity."""
        stub["ols"]["male"] = [OlsTerm("PATO:0000384", "male", "pato", "male", "label")]
        grounder = Grounder(client=None)  # type: ignore[arg-type]
        [claim] = await grounder.ground(_claim("sex", "male"))
        assert not claim.needs_review

    def test_the_shape_test_admits_acronyms_and_rejects_words(self) -> None:
        for token in ("ASD", "PBMC", "COPD", "T2D"):
            assert _looks_like_abbreviation(token), token
        for token in ("male", "liver", "CD4+ T cell", "prefrontal cortex", ""):
            assert not _looks_like_abbreviation(token), token


class TestAZoomaMatchIsNeverAsserted:
    async def test_an_approximate_match_is_flagged(self, stub: dict[str, Any]) -> None:
        """GSE111360's `CD45+ cells from a fresh kidney tumor sample`.

        Constrained to CL, ZOOMA returns CL:0001074 -- a rare innate lymphoid
        precursor -- for a pan-leukocyte marker. Not a near miss.
        """

        class _Hit:
            curie = "CL:0001074"
            confidence = "GOOD"
            matched_value = "CD45+ cells from a fresh kidney tumor sample"
            prefix = "cl"

        stub["zooma"]["CD45+ cells from a fresh kidney tumor sample"] = [_Hit()]
        stub["curies"]["CL:0001074"] = OlsTerm(
            "CL:0001074",
            "CD34-positive, CD56-positive, CD117-positive common innate lymphoid precursor",
            "cl",
            "",
            "curie",
        )
        grounder = Grounder(client=None)  # type: ignore[arg-type]
        [claim] = await grounder.ground(
            _claim("cell_type", "CD45+ cells from a fresh kidney tumor sample")
        )
        assert claim.needs_review
        assert grounder.stats.approximate == 1

    async def test_the_label_comes_from_ols_not_from_zooma(self, stub: dict[str, Any]) -> None:
        """ZOOMA echoes the *queried* string back as the annotated property.

        Recording that as the term's label printed a CURIE labelled with the
        prose it does not mean, which hid the error above entirely.
        """

        class _Hit:
            curie = "CL:0001074"
            confidence = "GOOD"
            matched_value = "CD45+ cells from a fresh kidney tumor sample"
            prefix = "cl"

        stub["zooma"]["CD45+ cells"] = [_Hit()]
        stub["curies"]["CL:0001074"] = OlsTerm(
            "CL:0001074", "common innate lymphoid precursor", "cl", "", "curie"
        )
        grounder = Grounder(client=None)  # type: ignore[arg-type]
        [claim] = await grounder.ground(_claim("cell_type", "CD45+ cells"))
        assert claim.term is not None
        assert claim.term.label == "common innate lymphoid precursor"

    async def test_a_curie_that_does_not_resolve_is_not_a_grounding(
        self, stub: dict[str, Any]
    ) -> None:
        class _Hit:
            curie = "CL:9999999"
            confidence = "HIGH"
            matched_value = "x"
            prefix = "cl"

        stub["zooma"]["x"] = [_Hit()]
        grounder = Grounder(client=None)  # type: ignore[arg-type]
        [claim] = await grounder.ground(_claim("cell_type", "x"))
        assert claim.term is None


class TestTheMemoIsNotAnOptimisationDetail:
    async def test_one_lookup_serves_every_sample(self, stub: dict[str, Any]) -> None:
        """800 samples from one tissue must ask two free EBI services once."""
        stub["ols"]["lung"] = [OlsTerm("UBERON:0002048", "lung", "uberon", "lung", "label")]
        grounder = Grounder(client=None)  # type: ignore[arg-type]
        for _ in range(50):
            await grounder.ground(_claim("tissue", "lung"))
        assert len([a for a in stub["asked"] if a[0] == "ols"]) == 1
        assert grounder.stats.attempted == 50
        assert grounder.stats.lookups == 1
        assert grounder.stats.grounded_claims == 50


class TestTaxonSelectsTheOntology:
    async def test_a_mouse_stage_is_not_asked_of_hsapdv(self, stub: dict[str, Any]) -> None:
        grounder = Grounder(client=None)  # type: ignore[arg-type]
        await grounder.ground(_claim("development_stage", "E14.5"), taxon="10090")
        assert next(ontologies for _, _, ontologies in stub["asked"]) == ("mmusdv",)


class TestConflictsSurviveGrounding:
    async def test_two_archives_disagreeing_still_disagree(self, stub: dict[str, Any]) -> None:
        record = SampleMetadata(sample="SAMN1")
        record.add(
            Claim(
                field="sex",
                value="male",
                method=Method.ONTOLOGY,
                source="ena:filereport[sex]",
                evidence="male",
                term=Term("PATO:0000384", "male", "pato", "ols:label"),
            )
        )
        record.add(
            Claim(
                field="sex",
                value="female",
                method=Method.ONTOLOGY,
                source="biosample:attribute[sex]",
                evidence="female",
                term=Term("PATO:0000383", "female", "pato", "ols:label"),
            )
        )
        assert record.ambiguous("sex")
        assert record.asserted("sex") is None
