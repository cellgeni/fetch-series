"""Regression test for `efetch db=biosample` substituting a different sample.

Inverted, like the rest of this suite: it asserts the bug still reproduces. A
failure means NCBI has fixed it -- at which point the extra `esearch` request
in `fetch_biosamples` can be dropped and the knowledge-base page updated.

**The bug.** `efetch db=biosample` appears to accept accessions. It does not:
it strips the alphabetic prefix and treats the remainder as a UID. For an
NCBI-registered sample the number happens to *be* the UID -- SAMN12011228 is
UID 12011228 -- so nothing looks wrong on the majority of samples. For an
EBI- or DDBJ-registered one the number is a different, valid UID, and a
**different organism's** attributes come back with HTTP 200 and no warning.

Knowledge base: docs/pathologies/biosample-efetch-ignores-accession-prefix.md
"""

from __future__ import annotations

import pytest
from dotenv import load_dotenv

from fetch_series.core import default_api_key
from fetch_series.providers import eutils
from fetch_series.providers.eutils import BASE, parse_biosample_set
from fetch_series.survey import Limits, SurveyClient

pytestmark = pytest.mark.pathology

load_dotenv()

# An EBI-registered sample and the NCBI-registered one its number collides
# with. SAMEA11294529 is a human liver sample in E-MTAB-10553; SAMN11294529 is
# a rumen bacterium.
COLLIDING = "SAMEA11294529"
RETURNED_INSTEAD = "SAMN11294529"
EXPECTED_ORGANISM = "Homo sapiens"


def _client() -> SurveyClient:
    return SurveyClient(
        limits=Limits(rps=3.0, concurrency=2, attempts=2),
        api_key=default_api_key(),
    )


class TestEfetchIgnoresTheAccessionPrefix:
    def test_a_raw_accession_returns_a_different_sample(self, run_or_skip) -> None:  # type: ignore[no-untyped-def]
        """The bug itself. A failure here is good news."""

        async def query() -> str:
            async with _client() as client:
                response = await client.get(
                    f"{BASE}/efetch.fcgi",
                    params={
                        "db": "biosample",
                        "id": COLLIDING,
                        "retmode": "xml",
                        "api_key": client.api_key,
                    },
                    timeout=60.0,
                )
                return response.text

        records = parse_biosample_set(run_or_skip(query()))
        assert records, "efetch returned no BioSample at all; not the verdict this test makes"
        accessions = [record.accession for record in records]
        assert COLLIDING not in accessions, (
            f"efetch now honours the accession prefix and returned {COLLIDING}. "
            "NCBI has fixed this: drop the esearch hop in fetch_biosamples, update "
            "docs/pathologies/biosample-efetch-ignores-accession-prefix.md, and delete this test."
        )
        assert RETURNED_INSTEAD in accessions

    def test_the_number_alone_returns_the_same_wrong_record(self, run_or_skip) -> None:  # type: ignore[no-untyped-def]
        """Confirms the mechanism, rather than only the symptom.

        If `id=11294529` and `id=SAMEA11294529` return the same record, the
        prefix is being discarded and the remainder read as a UID. Any other
        explanation would have to account for that coincidence.
        """

        async def query(identifier: str) -> str:
            async with _client() as client:
                response = await client.get(
                    f"{BASE}/efetch.fcgi",
                    params={
                        "db": "biosample",
                        "id": identifier,
                        "retmode": "xml",
                        "api_key": client.api_key,
                    },
                    timeout=60.0,
                )
                return response.text

        by_accession = parse_biosample_set(run_or_skip(query(COLLIDING)))
        by_number = parse_biosample_set(run_or_skip(query("11294529")))
        assert [r.accession for r in by_accession] == [r.accession for r in by_number]


class TestTheWorkaroundStillWorks:
    def test_resolving_through_esearch_returns_the_sample_asked_for(
        self,
        run_or_skip,  # type: ignore[no-untyped-def]
    ) -> None:
        """Not inverted: this must keep passing for the layer to be correct.

        It is here rather than in the offline suite because it is the only
        check that the workaround still defeats the live bug.
        """

        async def query() -> list[eutils.BioSampleRecord]:
            async with _client() as client:
                return await eutils.fetch_biosamples(client, [COLLIDING], 60.0)

        records = run_or_skip(query())
        assert [r.accession for r in records] == [COLLIDING]
        assert records[0].organism == EXPECTED_ORGANISM
