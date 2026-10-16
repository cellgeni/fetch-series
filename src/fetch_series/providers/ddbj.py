"""DDBJ Search's ``dblink`` API: every record linked to one record.

It indexes NCBI and EBI records as well as DDBJ's own -- PRJNA258286 and
PRJEB15401 both answer -- and links one INSDC level to all the others in a
single request, plus a BioProject to its GEO series.

Used by the link catalogue's probes, and by nothing that resolves an accession.
``docs/decisions/no-ddbj-provider.md`` still stands: ENA already answers every
DDBJ-issued project this project has met, so no route needs this.
"""

from __future__ import annotations

from typing import Any

from fetch_series.survey.client import MalformedResponseError, SurveyClient

DBLINK = "https://ddbj.nig.ac.jp/search/api/dblink"


async def dblink(
    client: SurveyClient, kind: str, accession: str, timeout: float
) -> dict[str, list[str]]:
    """Linked records of ``accession``, by DDBJ record type (``sra-run``, ``geo`` ...)."""
    response = await client.get(f"{DBLINK}/{kind}/{accession}", timeout=timeout)
    return parse_dblink(response.json())


def parse_dblink(payload: Any) -> dict[str, list[str]]:
    """``dbXrefs`` grouped by type."""
    if not isinstance(payload, dict) or not isinstance(payload.get("dbXrefs"), list):
        raise MalformedResponseError("dblink answered without a dbXrefs list")
    found: dict[str, set[str]] = {}
    for entry in payload["dbXrefs"]:
        if isinstance(entry, dict) and entry.get("type") and entry.get("identifier"):
            found.setdefault(str(entry["type"]), set()).add(str(entry["identifier"]))
    return {kind: sorted(values) for kind, values in found.items()}
