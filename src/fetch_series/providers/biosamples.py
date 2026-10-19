"""EBI BioSamples, the archive of record for every BioSample accession.

Not NCBI's BioSample database, which mirrors it: the EBI service answers for
``SAMN`` accessions too -- SAMN02991530 carries SRS685538 -- but, unlike NCBI's
efetch, it is keyed on the accession itself, so the prefix substitution that
makes ``efetch db=biosample id=SAMEA...`` return a different sample cannot
happen here.

Used by the link catalogue's probes. No route reads it.
"""

from __future__ import annotations

from typing import Any

from fetch_series.survey.client import MalformedResponseError, SurveyClient

SAMPLES = "https://www.ebi.ac.uk/biosamples/samples"

# The characteristic that carries a BioSample's INSDC sample accession. It is a
# submitter-visible attribute rather than a documented field, which is why a
# reverse lookup goes through the generic attribute filter.
SRA_ACCESSION = "SRA accession"


async def sample(client: SurveyClient, accession: str, timeout: float) -> dict[str, Any]:
    """One BioSample record as JSON."""
    response = await client.get(f"{SAMPLES}/{accession}", timeout=timeout)
    payload = response.json()
    if not isinstance(payload, dict):
        raise MalformedResponseError(f"BioSamples answered {accession} with a non-object")
    return payload


def sra_accessions(payload: dict[str, Any]) -> list[str]:
    """The INSDC sample accessions a record names.

    EBI-registered records carry a top-level ``sraAccession``; NCBI-registered
    ones (SAMN02991530) carry only the characteristic, so both are read.
    """
    found = {str(payload["sraAccession"]).strip()} if payload.get("sraAccession") else set()
    for entry in (payload.get("characteristics") or {}).get(SRA_ACCESSION) or []:
        if isinstance(entry, dict) and (text := str(entry.get("text") or "").strip()):
            found.add(text)
    return sorted(found)


async def by_attribute(
    client: SurveyClient, attribute: str, value: str, timeout: float
) -> list[str]:
    """Accessions of the samples whose ``attribute`` is ``value``.

    One page of 100 is enough for the lookups this serves -- an INSDC sample
    accession names one BioSample -- and a result that fills the page is
    refused rather than silently truncated.
    """
    response = await client.get(
        SAMPLES, params={"filter": f"attr:{attribute}:{value}", "size": 100}, timeout=timeout
    )
    return embedded_accessions(response.json())


def embedded_accessions(payload: dict[str, Any]) -> list[str]:
    """Sample accessions from a HAL search page; a full page is an error, not an answer."""
    samples = (payload.get("_embedded") or {}).get("samples") or []
    total = (payload.get("page") or {}).get("totalElements")
    if isinstance(total, int) and total > len(samples):
        raise MalformedResponseError(f"BioSamples matched {total} samples, served {len(samples)}")
    return sorted(
        {str(s["accession"]) for s in samples if isinstance(s, dict) and s.get("accession")}
    )
