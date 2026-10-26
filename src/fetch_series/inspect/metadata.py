"""What a run's metadata names as its kit, read the way the 2026-10 comparison read it.

``docs/benchmarks/metadata-vs-reads-2026-10.md`` measured the assay layer's
``chemistry_of`` on two sources, and this reads the same two for one run, so
that a tie it breaks is broken by the instrument that was measured:

* ENA's ``read_run`` report for the run, with the protocol fields
  (``filesets.ASSAY_FIELDS``) and the sample alias;
* where the alias is a GSM, GEO's record for that sample, whose free-text fields
  are those the SOFT family gives per sample.

Two requests at most. A failure is reported as the reason no call was made,
redacted; it never stops the read report.
"""

from __future__ import annotations

from typing import Any

from fetch_series.assays.chemistry import ChemistryCall, chemistry_of
from fetch_series.filesets import ASSAY_FIELDS
from fetch_series.logging_utils import redact_exception
from fetch_series.providers import ena_portal, geo
from fetch_series.survey.client import SurveyClient


async def chemistry_of_run(
    run: str, client: SurveyClient, timeout: float = 60.0
) -> tuple[ChemistryCall | None, dict[str, Any]]:
    """The run's metadata chemistry call, and where it was read from."""
    try:
        rows = await ena_portal.read_run_report(
            client, run, timeout, fields=(*ASSAY_FIELDS, "sample_alias")
        )
        row: dict[str, Any] = dict(next((r for r in rows if r.get("run_accession") == run), {}))
        if not row:
            return None, {"error": "ENA has no read_run row for the run"}
        sources = ["ena read_run"]
        alias = str(row.get("sample_alias") or "")
        if alias.startswith("GSM"):
            record = await geo.fetch_sample_record(client, alias, timeout)
            row.update(geo.sample_text(record))
            sources.append(f"geo {alias}")
    except Exception as exc:
        return None, {"error": str(redact_exception(exc))}
    call = chemistry_of(row)
    return call, {
        "sources": sources,
        "candidates": list(call.candidates),
        "matches": [str(m) for m in call.matches],
    }


__all__ = ["chemistry_of_run"]
