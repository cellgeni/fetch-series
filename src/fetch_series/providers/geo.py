"""GEO access: the SOFT family file, and the ``gds`` Entrez database.

GEO exposes the same series through two very different surfaces, and they do not
agree. The SOFT family file is the submitter's record and carries per-sample
relations; ``db=gds`` is NCBI's index of it and carries project-level links. A
series can have SRA relations in one and not the other, which is why both are
surveyed rather than one being assumed authoritative.
"""

from __future__ import annotations

import gzip
import re
from dataclasses import dataclass, field

from fetch_series.accession import parse
from fetch_series.providers.eutils import esearch, esummary_by_ids
from fetch_series.survey.client import MalformedResponseError, SurveyClient

FTP_BASE = "https://ftp.ncbi.nlm.nih.gov/geo/series"
ACC_CGI = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi"

# !Series_relation = BioProject: https://www.ncbi.nlm.nih.gov/bioproject/PRJNA988806
_RELATION = re.compile(r"^!(Series|Sample)_relation\s*=\s*(.+)$", re.MULTILINE)
_SAMPLE_START = re.compile(r"^\^SAMPLE\s*=\s*(GSM\d+)", re.MULTILINE)
_ACCESSION_IN_URL = re.compile(
    r"(PRJ[NED][ABC]?\d+|SAM[NED][A-Z]?\d+|[SED]R[XPRSA]\d+|GSE\d+|GSM\d+)"
)


@dataclass(slots=True)
class SoftFamily:
    """What a parsed SOFT family file says about a series."""

    series: str
    bioprojects: list[str] = field(default_factory=list)
    subseries: list[str] = field(default_factory=list)
    superseries: list[str] = field(default_factory=list)
    samples: list[str] = field(default_factory=list)
    # GSM -> the SRX and BioSample it declares, where it declares them at all.
    sample_relations: dict[str, dict[str, str]] = field(default_factory=dict)


async def fetch_soft_family(client: SurveyClient, series: str, timeout: float) -> str:
    """Download and decompress a series' SOFT family file."""
    accession = parse(series)
    url = f"{FTP_BASE}/{accession.geo_ftp_stem}/{accession.value}/soft/{accession.value}_family.soft.gz"
    response = await client.get(url, timeout=timeout)
    if not response.content:
        raise MalformedResponseError(f"empty SOFT family file for {series}")
    return gzip.decompress(response.content).decode("utf-8", errors="replace")


def parse_soft_family(series: str, text: str) -> SoftFamily:
    """Parse the relations and sample list out of a SOFT family file.

    Sample relations are flushed per ``^SAMPLE`` record rather than accumulated.
    Accumulating until all fields are set drops every sample with a missing
    relation *and* leaves the previous sample's fields set, so the next sample's
    SRX gets paired with the previous sample's BioSample. GEO does not always
    record ``!Sample_relation = SRA:`` -- GSE135325 and GSE137444 list the
    BioSample alone -- so missing fields are the normal case, not the exception.
    """
    family = SoftFamily(series=series)

    for match in _RELATION.finditer(text):
        scope, value = match.group(1), match.group(2).strip()
        found = _ACCESSION_IN_URL.search(value)
        if scope == "Series":
            if value.startswith("BioProject") and found:
                family.bioprojects.append(found.group(1))
            elif "SuperSeries of" in value and found:
                family.subseries.append(found.group(1))
            elif "SubSeries of" in value and found:
                family.superseries.append(found.group(1))

    # Split into per-sample blocks so a missing relation cannot leak sideways.
    starts = [(m.start(), m.group(1)) for m in _SAMPLE_START.finditer(text)]
    for index, (offset, gsm) in enumerate(starts):
        end = starts[index + 1][0] if index + 1 < len(starts) else len(text)
        block = text[offset:end]
        family.samples.append(gsm)

        relations: dict[str, str] = {}
        for match in _RELATION.finditer(block):
            if match.group(1) != "Sample":
                continue
            value = match.group(2).strip()
            found = _ACCESSION_IN_URL.search(value)
            if not found:
                continue
            token = found.group(1)
            if token.startswith(("SRX", "ERX", "DRX")):
                relations["experiment"] = token
            elif token.startswith("SAM"):
                relations["biosample"] = token
        family.sample_relations[gsm] = relations

    return family


async def gds_uid(client: SurveyClient, series: str, timeout: float) -> str | None:
    """Resolve a GEO accession to its ``gds`` UID.

    The UID of a series is its number prefixed with 200, but that is an
    observation rather than a contract, so it is looked up.
    """
    result = await esearch(client, db="gds", term=f"{series}[ACCN] AND GSE[ETYP]", timeout=timeout)
    uids = result.get("idlist") or []
    return str(uids[0]) if uids else None


async def gds_summary(client: SurveyClient, series: str, timeout: float) -> dict[str, object]:
    """ESummary record for a GEO series, or an empty dict if it has none."""
    uid = await gds_uid(client, series, timeout)
    if uid is None:
        return {}
    summaries = await esummary_by_ids(client, db="gds", uids=[uid], timeout=timeout)
    record = summaries.get(uid, {})
    return record if isinstance(record, dict) else {}
