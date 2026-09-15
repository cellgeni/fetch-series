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
    # GSM -> the free-text fields that say what the library actually is. GEO
    # carries assay information ENA's library_construction_protocol often does
    # not: GSM5659253 names CellRanger in !Sample_data_processing while its ENA
    # protocol describes only the tissue dissociation.
    sample_text: dict[str, dict[str, str]] = field(default_factory=dict)


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
        family.sample_text[gsm] = _sample_text(block)

    return family


# The GEO fields that describe what the library is, mapped onto the names the
# assay layer scans. GEO repeats a key across several lines rather than wrapping
# one, so the values are joined instead of overwritten -- keeping only the last
# line of !Sample_data_processing would throw away the line naming CellRanger,
# which is usually not the last one.
_TEXT_FIELDS = {
    "!Sample_title": "sample_title",
    "!Sample_data_processing": "sample_data_processing",
    "!Sample_library_construction_protocol": "sample_library_construction_protocol",
    "!Sample_extract_protocol_ch1": "sample_extract_protocol",
    "!Sample_growth_protocol_ch1": "sample_extract_protocol",
    "!Sample_instrument_model": "instrument_model",
}
_TEXT_LINE = re.compile(r"^(![A-Za-z_0-9]+)\s*=\s*(.*)$", re.MULTILINE)


def _sample_text(block: str) -> dict[str, str]:
    """The free-text fields of one sample block, joined per key."""
    collected: dict[str, list[str]] = {}
    for match in _TEXT_LINE.finditer(block):
        name = _TEXT_FIELDS.get(match.group(1))
        if name and (value := match.group(2).strip()):
            collected.setdefault(name, []).append(value)
    return {name: " ".join(values) for name, values in collected.items()}


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


@dataclass(slots=True)
class SampleRecord:
    """What GEO's own record for a single sample says."""

    sample: str
    experiment: str | None = None
    biosample: str | None = None
    # A sample can belong to more than one series: GSM7518069 is in both
    # GSE236084 and GSE236087. Routing from a sample to "its" series has to
    # cope with there being several.
    series: list[str] = field(default_factory=list)
    organism: str | None = None


_SERIES_ID = re.compile(r"^!Sample_series_id\s*=\s*(GSE\d+)", re.MULTILINE)
_ORGANISM = re.compile(r"^!Sample_organism_ch1\s*=\s*(.+)$", re.MULTILINE)


async def fetch_sample_record(client: SurveyClient, sample: str, timeout: float) -> str:
    """Download GEO's text record for one sample.

    ``acc.cgi`` is a web endpoint rather than an API, and ``view=brief`` still
    returns the submitter's full protocol prose -- tens of kilobytes for one
    sample. It is the only place the per-sample SRA relation can be had without
    downloading the whole series family file, which is far larger again.
    """
    response = await client.get(
        ACC_CGI,
        params={"acc": sample, "targ": "self", "form": "text", "view": "brief"},
        timeout=timeout,
    )
    text = response.text
    if not text.lstrip().startswith("^SAMPLE"):
        raise MalformedResponseError(
            f"acc.cgi returned no SAMPLE record for {sample}: {text[:120]!r}"
        )
    return text


def parse_sample_record(sample: str, text: str) -> SampleRecord:
    """Parse the relations out of a single-sample SOFT record."""
    record = SampleRecord(sample=sample)
    # The relation lines have the same shape as in a family file, so reuse that
    # parser rather than writing a second one that can drift from it.
    family = parse_soft_family(sample, text)
    relations = family.sample_relations.get(sample, {})
    record.experiment = relations.get("experiment")
    record.biosample = relations.get("biosample")
    record.series = sorted(set(_SERIES_ID.findall(text)))
    organism = _ORGANISM.search(text)
    record.organism = organism.group(1).strip() if organism else None
    return record
