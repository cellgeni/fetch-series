"""Regression test for ZOOMA returning a term from the wrong kind of ontology.

Inverted: it asserts the behaviour still reproduces. A failure means ZOOMA has
changed what it returns, at which point the grounding rules in
`fetch_series.metadata.ground` should be re-measured before being relaxed.

**The behaviour.** ZOOMA's `ontologies:[...]` filter does not prevent a wrong
answer; it selects which wrong answer you get. Constrained to UBERON -- an
anatomy ontology -- the property value `male` returns `UBERON:0003101` *male
organism*, at confidence `GOOD`, where a phenotypic quality was asked for.
Constrained to PATO it returns the correct `PATO:0000384` at `HIGH`.

The consequence in the code is not a workaround but a declaration: each field
names the ontology that can actually express it, and the returned CURIE is
re-checked against that. The confidence band is recorded and acted on nowhere,
because `GOOD` is the band on the wrong answer here.

Knowledge base: docs/pathologies/zooma-filter-forces-a-term.md
"""

from __future__ import annotations

import pytest

from fetch_series.providers import zooma
from fetch_series.survey import Limits, SurveyClient

pytestmark = pytest.mark.pathology

WRONG_ONTOLOGY_TERM = "UBERON:0003101"
RIGHT_TERM = "PATO:0000384"


def _client() -> SurveyClient:
    return SurveyClient(limits=Limits(rps=3.0, concurrency=2, attempts=2))


class TestTheFilterForcesATerm:
    def test_male_still_grounds_in_uberon_when_uberon_is_asked(
        self,
        run_or_skip,  # type: ignore[no-untyped-def]
    ) -> None:
        async def query() -> list[zooma.ZoomaHit]:
            async with _client() as client:
                return await zooma.annotate(
                    client, "male", ("uberon",), 60.0, property_type="organism part"
                )

        hits = run_or_skip(query())
        assert hits, "ZOOMA returned nothing for 'male' in UBERON; the behaviour has changed"
        assert hits[0].curie == WRONG_ONTOLOGY_TERM, (
            f"ZOOMA no longer answers 'male' with {WRONG_ONTOLOGY_TERM} under an UBERON "
            "filter. Re-measure grounding before relaxing anything in metadata/ground.py, "
            "and update docs/pathologies/zooma-filter-forces-a-term.md."
        )

    def test_the_declared_ontology_gets_the_right_answer(
        self,
        run_or_skip,  # type: ignore[no-untyped-def]
    ) -> None:
        """Not inverted: `sex` declaring PATO is what makes the layer correct."""

        async def query() -> list[zooma.ZoomaHit]:
            async with _client() as client:
                return await zooma.annotate(client, "male", ("pato",), 60.0, property_type="sex")

        hits = run_or_skip(query())
        assert hits and hits[0].curie == RIGHT_TERM

    def test_a_confidence_band_does_not_order_by_correctness(
        self,
        run_or_skip,  # type: ignore[no-untyped-def]
    ) -> None:
        """The reason no threshold ships in providers/zooma.py.

        The wrong answer above and the right answer below are both in the top
        two bands. A cutoff would let the wrong one through and would look
        principled doing it.
        """

        async def query(ontology: str) -> list[zooma.ZoomaHit]:
            async with _client() as client:
                return await zooma.annotate(client, "male", (ontology,), 60.0)

        wrong = run_or_skip(query("uberon"))
        assert wrong and wrong[0].confidence in {"HIGH", "GOOD"}
