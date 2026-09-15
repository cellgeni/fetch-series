"""Regression test for a known NCBI defect.

**This suite is inverted.** Each test asserts that a documented archive bug
*still reproduces*. A failure here is good news: the archive fixed something.
When one fails, update the knowledge-base page, re-rank the affected route, and
delete the test.

Knowledge base: docs/pathologies/elink-gds-sra-missing-links.md
"""

from __future__ import annotations

import os

import pytest
from dotenv import load_dotenv

from fetch_series.providers import ena_portal, geo
from fetch_series.providers.eutils import elink_uids, esearch
from fetch_series.survey import Limits, SurveyClient

pytestmark = pytest.mark.pathology

load_dotenv()

# GEO series whose SOFT family file names SRA experiments that ELink cannot
# reach. Value is one experiment the SOFT file names, used to prove the data
# really is in SRA.
UNREACHABLE = {
    "GSE27679": "SRX046652",
    "GSE29158": "SRX014928",
    "GSE34359": "SRX111797",
    "GSE35884": "SRX117991",
    "GSE49732": "SRX466511",
    "GSE49780": "SRX467063",
}


def _client() -> SurveyClient:
    return SurveyClient(
        limits=Limits(rps=3.0, concurrency=2, attempts=3),
        api_key=os.getenv("NCBI_API_KEY") or os.getenv("NCBI_KEY"),
    )


@pytest.mark.parametrize(("series", "experiment"), sorted(UNREACHABLE.items()))
def test_elink_still_returns_no_sra_links(series: str, experiment: str, run_or_skip) -> None:
    """ELink reports no SRA neighbours, while the experiment plainly exists."""

    async def check() -> tuple[list[str], str | None, list[str]]:
        async with _client() as client:
            uid = await geo.gds_uid(client, series, timeout=30)
            assert uid is not None, f"{series} has no gds UID; the series itself has changed"
            links = await elink_uids(client, dbfrom="gds", db="sra", uids=[uid], timeout=30)
            found = await esearch(client, db="sra", term=f"{experiment}[ACCN]", timeout=30)
            rows = await ena_portal.read_run_report(client, experiment, timeout=30)
            return links, found.get("count"), ena_portal.column(rows, "run_accession")

    links, sra_count, runs = run_or_skip(check())

    # The data exists. If these two ever fail, the premise is gone, not the bug.
    assert sra_count == "1", f"{experiment} is no longer in db=sra"
    assert runs, f"{experiment} has no runs in ENA"

    assert links == [], (
        f"NCBI now links {series} to {len(links)} SRA records. The defect in "
        "docs/pathologies/elink-gds-sra-missing-links.md appears to be FIXED -- "
        "update the page, re-rank gse->experiment:elink_gds_sra, and remove this test."
    )
