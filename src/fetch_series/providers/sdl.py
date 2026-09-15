"""NCBI Storage Data Locator.

SDL is the only route that answers "where are this run's files, actually" for
NCBI-held data. It knows three things no other route does: the submitter's
*original* files (ERR2861957's route to its BAM is SDL, not ENA's fastq
columns), whether a copy costs money to retrieve, and whether it is in cold
storage and must be rehydrated first.

It also batches. One POST carries many accessions, which matters at file-layer
scale where every other route costs a request per run.

The links it hands out are signed and rotate, so they are resolution results,
not durable identifiers -- the incumbent pipeline's own nf-test suite excludes
them from its snapshots for exactly this reason.
"""

from __future__ import annotations

from typing import Any

from fetch_series.survey.client import MalformedResponseError, SurveyClient

RETRIEVE = "https://locate.ncbi.nlm.nih.gov/sdl/2/retrieve"

# SDL accepts a comma-separated accession list. Batching is the reason to use
# it at scale, but an over-long POST body is the failure mode this project has
# now hit five times, so the batch is bounded and the bound is explicit.
MAX_BATCH = 50


async def retrieve(
    client: SurveyClient,
    accessions: list[str],
    timeout: float,
    *,
    accept_charges: str = "aws",
) -> dict[str, list[dict[str, Any]]]:
    """File listings for a batch of run accessions, keyed by accession.

    A bundle that SDL answers with a non-200 ``status`` is omitted rather than
    recorded as an empty file list: "this run has no files" and "SDL would not
    tell us" are different answers, and a caller that cannot distinguish them
    will report a withdrawn run as one with nothing to download.
    """
    if len(accessions) > MAX_BATCH:
        raise ValueError(f"SDL batch of {len(accessions)} exceeds MAX_BATCH={MAX_BATCH}")
    if not accessions:
        return {}

    response = await client.post(
        RETRIEVE,
        data={"acc": ",".join(accessions), "accept-charges": accept_charges},
        timeout=timeout,
    )
    payload = response.json()
    if "result" not in payload:
        raise MalformedResponseError(f"SDL response has no result key: {sorted(payload)}")

    listings: dict[str, list[dict[str, Any]]] = {}
    for bundle in payload["result"]:
        if bundle.get("status") != 200:
            continue
        listings[bundle["bundle"]] = bundle.get("files") or []
    return listings


def links(files: list[dict[str, Any]]) -> list[str]:
    """Every download link in an SDL file listing.

    A file may be offered from several services (S3, GCS, NCBI, EBI); each is a
    separate way to get the same bytes, so each is a separate link.
    """
    found: list[str] = []
    for entry in files:
        for location in entry.get("locations") or []:
            if link := location.get("link"):
                found.append(link)
    return found
