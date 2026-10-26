"""Regression test for EFetch answering fewer runs than ESearch counted.

**This suite is inverted.** Each test asserts that a documented archive behaviour
*still reproduces*. A failure here is good news: the archive changed something.
When one fails, update the knowledge-base page and delete the test.

Knowledge base: docs/pathologies/efetch-returns-fewer-runs-than-esearch-counts.md
"""

from __future__ import annotations

import pytest

from fetch_series.core import default_api_key
from fetch_series.providers.eutils import BASE, efetch_pages, esearch
from fetch_series.survey import Limits, SurveyClient

pytestmark = pytest.mark.pathology

TIMEOUT = 300.0


def _runs(text: str) -> set[str]:
    return {
        line.split(",", 1)[0] for line in text.splitlines()[1:] if line[:3] in ("SRR", "ERR", "DRR")
    }


async def _one_request(project: str, retmax: int) -> tuple[int, int]:
    """ESearch's count, and the runs one EFetch of its history answers."""
    async with SurveyClient(
        limits=Limits(rps=2.0, concurrency=1, attempts=3), api_key=default_api_key()
    ) as client:
        found = await esearch(client, db="sra", term=f"{project}[GPRJ]", timeout=TIMEOUT)
        response = await client.get(
            f"{BASE}/efetch.fcgi",
            params={
                "db": "sra",
                "WebEnv": found["webenv"],
                "query_key": found["querykey"],
                "retmax": retmax,
                "rettype": "runinfo",
                "retmode": "text",
                "api_key": client.api_key,
            },
            timeout=TIMEOUT,
        )
        return int(found["count"]), len(_runs(response.text))


async def _paged(project: str) -> int:
    async with SurveyClient(
        limits=Limits(rps=2.0, concurrency=1, attempts=3), api_key=default_api_key()
    ) as client:
        found = await esearch(client, db="sra", term=f"{project}[GPRJ]", timeout=TIMEOUT)
        pages = await efetch_pages(
            client,
            db="sra",
            webenv=found["webenv"],
            query_key=found["querykey"],
            count=int(found["count"]),
            timeout=TIMEOUT,
        )
        return len(set().union(*(_runs(page) for page in pages)))


def test_one_efetch_still_stops_at_ten_thousand(run_or_skip):
    """PRJEB14362: 46,085 experiments, and one EFetch answers at most 10,000 runs."""
    count, runs = run_or_skip(_one_request("PRJEB14362", retmax=100_000))
    assert count > 40_000
    assert runs <= 10_000, (
        f"One EFetch now answers {runs:,} runs of PRJEB14362's {count:,} experiments: "
        "the cap moved. Update docs/pathologies/efetch-returns-fewer-runs-than-esearch-counts.md "
        "and eutils.EFETCH_PAGE."
    )


def test_an_experiment_with_no_run_is_still_counted(run_or_skip):
    """PRJNA940674: ESearch counts SRX19556101, which holds no run."""
    count, runs = run_or_skip(_one_request("PRJNA940674", retmax=500))
    assert (count, runs) == (4, 3), f"PRJNA940674 now counts {count} and answers {runs} runs"


def test_paging_still_recovers_every_run(run_or_skip):
    """The workaround: paged by retstart, the same history answers every run."""
    assert run_or_skip(_paged("PRJEB14362")) > 40_000
