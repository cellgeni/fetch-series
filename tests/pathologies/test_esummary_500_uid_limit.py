"""Regression test for a known NCBI E-utilities limit.

**This suite is inverted.** Each test asserts that a documented archive bug
*still reproduces*. A failure here is good news: the archive fixed something.
When one fails, update the knowledge-base page and delete the test.

Knowledge base: docs/pathologies/esummary-500-uid-limit.md
"""

from __future__ import annotations

from typing import Any

import pytest

from fetch_series.core import default_api_key
from fetch_series.providers.eutils import BASE
from fetch_series.survey import Limits, SurveyClient

pytestmark = pytest.mark.pathology


async def _summary(n: int) -> dict[str, Any]:
    ids = ",".join(str(10_000_000 + i) for i in range(n))
    async with SurveyClient(
        limits=Limits(rps=2.0, concurrency=1, attempts=3), api_key=default_api_key()
    ) as client:
        response = await client.post(
            f"{BASE}/esummary.fcgi",
            data={"db": "biosample", "id": ids, "retmode": "json", "api_key": client.api_key},
            timeout=120.0,
        )
        return response.json()  # type: ignore[no-any-return]


def test_501_uids_are_still_refused_inside_a_200(run_or_skip):
    answer = run_or_skip(_summary(501))
    assert "result" not in answer and "Too many UIDs" in str(answer.get("error")), (
        "ESummary now answers 501 UIDs in JSON: the 500 ceiling moved. "
        "Update docs/pathologies/esummary-500-uid-limit.md and MAX_UIDS_PER_SUMMARY."
    )


def test_500_uids_are_still_answered(run_or_skip):
    assert "result" in run_or_skip(_summary(500))
