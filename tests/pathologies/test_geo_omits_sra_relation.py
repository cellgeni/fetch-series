"""Regression test for GEO omitting the sample-level SRA relation.

Inverted, like the rest of this suite: it asserts the bug still reproduces. A
failure means GEO has added the relations -- update the knowledge-base page and
re-rank the route.

Knowledge base: docs/pathologies/geo-omits-sample-sra-relation.md
"""

from __future__ import annotations

import os

import pytest
from dotenv import load_dotenv

from fetch_series.providers import geo
from fetch_series.routes import gse_to_experiment_elink, gse_to_experiment_soft
from fetch_series.survey import Limits, SurveyClient

pytestmark = pytest.mark.pathology

load_dotenv()

# GEO series that record a BioSample relation per sample and no SRA relation,
# despite having experiments that ELink can reach.
OMITS_SRA_RELATION = {
    "GSE135325": 6,
    "GSE137444": 12,
}


def _client() -> SurveyClient:
    return SurveyClient(
        limits=Limits(rps=3.0, concurrency=2, attempts=3),
        api_key=os.getenv("NCBI_API_KEY") or os.getenv("NCBI_KEY"),
    )


@pytest.mark.parametrize(("series", "expected_via_elink"), sorted(OMITS_SRA_RELATION.items()))
def test_soft_still_omits_the_sra_relation(
    series: str, expected_via_elink: int, run_or_skip
) -> None:
    """The SOFT file names no experiments; ELink finds them."""

    async def check() -> tuple[list[str], list[str], dict[str, dict[str, str]]]:
        async with _client() as client:
            soft = await gse_to_experiment_soft(series, client, 60)
            elink = await gse_to_experiment_elink(series, client, 60)
            text = await geo.fetch_soft_family(client, series, 60)
            return soft, elink, geo.parse_soft_family(series, text).sample_relations

    soft, elink, relations = run_or_skip(check())

    # Premise: the series still exists and still has samples.
    assert relations, f"{series} SOFT file lists no samples at all; the series has changed"
    # Premise: the experiments are still reachable the other way.
    assert len(elink) >= expected_via_elink, (
        f"ELink now finds only {len(elink)} experiments for {series}, was {expected_via_elink}"
    )

    assert soft == [], (
        f"GEO now records SRA relations for {series} ({len(soft)} experiments). The "
        "defect in docs/pathologies/geo-omits-sample-sra-relation.md appears to be "
        "FIXED -- update the page and remove this test."
    )
    # The samples are present, just without the SRA half of their relations.
    assert any("biosample" in r for r in relations.values()), (
        f"{series} records neither SRA nor BioSample relations; the pathology has changed shape"
    )
