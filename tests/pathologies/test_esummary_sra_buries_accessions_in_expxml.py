"""Regression test for a known NCBI E-utilities shape.

**This suite is inverted.** Each test asserts that a documented archive bug
*still reproduces*. A failure here is good news: the archive fixed something.
When one fails, update the knowledge-base page and delete the test.

Knowledge base: docs/pathologies/esummary-sra-buries-accessions-in-expxml.md
"""

from __future__ import annotations

from typing import Any

import pytest

from fetch_series.core import default_api_key
from fetch_series.providers.eutils import BASE, expxml_accessions
from fetch_series.survey import Limits, SurveyClient

pytestmark = pytest.mark.pathology

# An SRA experiment of PRJNA940674 (GSM7078657) and its one run.
UID = "26844786"
RUN = "SRR23693882"


async def _record() -> dict[str, Any]:
    async with SurveyClient(
        limits=Limits(rps=2.0, concurrency=1, attempts=3), api_key=default_api_key()
    ) as client:
        response = await client.get(
            f"{BASE}/esummary.fcgi",
            params={"db": "sra", "id": UID, "retmode": "json", "api_key": client.api_key},
        )
        return response.json()["result"][UID]  # type: ignore[no-any-return]


def test_the_run_accession_is_still_only_inside_an_xml_string(run_or_skip):
    record = run_or_skip(_record())
    fields = {k: v for k, v in record.items() if k not in ("expxml", "runs")}
    assert RUN not in str(fields) and RUN in record["runs"], (
        "SRA's ESummary now carries accessions as JSON fields. "
        "Update docs/pathologies/esummary-sra-buries-accessions-in-expxml.md."
    )
    assert expxml_accessions(record)["Run"] == [RUN]
