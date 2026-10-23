"""Regression test for a known GEO/SRA modelling gap.

**This suite is inverted.** Each test asserts that a documented archive bug
*still reproduces*. A failure here is good news: the archive fixed something.
When one fails, update the knowledge-base page and delete the test.

Knowledge base: docs/pathologies/experiments-outside-the-series-bioproject.md
"""

from __future__ import annotations

from typing import Any

import pytest

from fetch_series.providers import ena_portal
from fetch_series.survey import Limits, SurveyClient

pytestmark = pytest.mark.pathology

# GSE234261 names 100 GSMs and one BioProject, which also holds 232 runs submitted
# to SRA directly (aliases T1-... to T6-...).
PROJECT = "PRJNA980643"


async def _rows() -> list[dict[str, Any]]:
    async with SurveyClient(limits=Limits(rps=2.0, concurrency=1, attempts=3)) as client:
        return await ena_portal.read_run_report(
            client, PROJECT, 120.0, fields=("run_accession", "experiment_accession", "sample_alias")
        )


def test_the_project_still_holds_runs_outside_the_series(run_or_skip):
    rows = run_or_skip(_rows())
    outside = [r for r in rows if not str(r.get("sample_alias") or "").startswith("GSM")]
    assert outside, (
        "PRJNA980643 now holds only GSE234261's runs. "
        "Update docs/pathologies/experiments-outside-the-series-bioproject.md."
    )
