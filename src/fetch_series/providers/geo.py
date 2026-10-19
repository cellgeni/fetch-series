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
    # !Series_relation = SRA: the series' SRA study, as GEO states it.
    sra_studies: list[str] = field(default_factory=list)
    # ^PLATFORM records and every platform id a sample names.
    platforms: list[str] = field(default_factory=list)
    # !Series_supplementary_file and !Sample_supplementary_file: processed data
    # on GEO's FTP server, not reads.
    supplementary_files: list[str] = field(default_factory=list)
    # GSM -> the SRX and BioSample it declares, where it declares them at all.
    sample_relations: dict[str, dict[str, str]] = field(default_factory=dict)
    # GSM -> the free-text fields that say what the library actually is. GEO
    # carries assay information ENA's library_construction_protocol often does
    # not: GSM5659253 names CellRanger in !Sample_data_processing while its ENA
    # protocol describes only the tissue dissociation.
    sample_text: dict[str, dict[str, str]] = field(default_factory=dict)
    # GSM -> the submitter's own characteristics pairs, in declaration order.
    #
    # A list of pairs rather than a dict: !Sample_characteristics_ch1 repeats,
    # once per attribute, and a submitter may legitimately repeat a *key* too
    # ("tissue: lung" then "tissue: lymph node" for a mixed sample). Reading
    # these through a dict keeps the last value only -- the same trap
    # csv.DictReader sets for SDRF columns, with the same silent result.
    sample_characteristics: dict[str, list[tuple[str, str]]] = field(default_factory=dict)
    # Fields that describe the *series*. Kept separate from sample_text rather
    # than merged into it, because they are study-scoped: GEO's !Series_summary
    # is one paragraph shared by every sample, so a disease named in it applies
    # to the study's design and not to any particular sample.
    series_text: dict[str, str] = field(default_factory=dict)


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
            elif value.startswith("SRA") and found:
                family.sra_studies.append(found.group(1))
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
        family.sample_characteristics[gsm] = _characteristics(block)

    family.series_text = _series_text(text)
    family.platforms = sorted(set(_PLATFORM.findall(text)))
    family.supplementary_files = sorted(
        {value for value in _SUPPLEMENTARY.findall(text) if value.strip().upper() != "NONE"}
    )
    return family


# A platform appears as its own ^PLATFORM record in a family file, and as an
# id on every series and sample line that names it.
_PLATFORM = re.compile(
    r"^(?:\^PLATFORM\s*=|!(?:Series|Sample)_platform_id\s*=)\s*(GPL\d+)", re.MULTILINE
)
# !Sample_supplementary_file_1, _2, ...: one numbered line per file.
_SUPPLEMENTARY = re.compile(
    r"^!(?:Series|Sample)_supplementary_file(?:_\d+)?\s*=\s*(\S+)", re.MULTILINE
)


# !Sample_characteristics_ch1 = tissue: prefrontal cortex
_CHARACTERISTIC = re.compile(r"^!Sample_characteristics(?:_ch\d+)?\s*=\s*(.+)$", re.MULTILINE)


def _characteristics(block: str) -> list[tuple[str, str]]:
    """The ``key: value`` characteristics of one sample, in declaration order.

    Split on the **first** colon only. ``age: 60-65`` is unaffected either way,
    but ``diagnosis: stage II, ER+: yes`` is, and a value containing a colon is
    far more common than a key containing one.

    A line with no colon is dropped rather than stored under an empty key --
    submitters do write bare values there, and inventing a key for one would
    put an unattributable string into a field set where every value is supposed
    to name what it describes.
    """
    pairs: list[tuple[str, str]] = []
    for match in _CHARACTERISTIC.finditer(block):
        key, separator, value = match.group(1).partition(":")
        if separator and key.strip() and value.strip():
            pairs.append((key.strip(), value.strip()))
    return pairs


# Study-scoped fields. !Series_summary is the abstract; !Series_overall_design
# is where submitters describe the sample groups, which makes it the most
# informative single field for grouping and the most dangerous one to read as
# a per-sample statement.
_SERIES_FIELDS = {
    "!Series_title": "series_title",
    "!Series_summary": "series_summary",
    "!Series_overall_design": "series_overall_design",
}


def _series_text(text: str) -> dict[str, str]:
    """The series-level free text, joined per key.

    Read from the whole document, not from a ``^SERIES`` block: GEO writes the
    series header first and a SuperSeries repeats nothing, so there is no block
    to bound it with. Lines are joined for the same reason sample fields are --
    GEO wraps a long summary across several lines with the key repeated.
    """
    collected: dict[str, list[str]] = {}
    for match in _TEXT_LINE.finditer(text):
        name = _SERIES_FIELDS.get(match.group(1))
        if name and (value := match.group(2).strip()):
            collected.setdefault(name, []).append(value)
    return {name: " ".join(values) for name, values in collected.items()}


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
    # Descriptive rather than assay-related, so the assay layer ignores them,
    # but they are where a tissue often is when there are no characteristics.
    "!Sample_source_name_ch1": "sample_source_name",
    "!Sample_organism_ch1": "sample_organism",
    "!Sample_description": "sample_description",
    "!Sample_treatment_protocol_ch1": "sample_treatment_protocol",
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


async def gds_record(
    client: SurveyClient, accession: str, entry_type: str, timeout: float
) -> dict[str, object]:
    """ESummary record for one GEO accession of ``entry_type`` (GSE, GSM, GPL).

    The entry type is not optional. ``GSM1480291[ACCN]`` alone matches three
    records -- the sample, its series and its platform -- and the first UID is
    the series'.
    """
    result = await esearch(
        client, db="gds", term=f"{accession}[ACCN] AND {entry_type}[ETYP]", timeout=timeout
    )
    uids = result.get("idlist") or []
    if not uids:
        return {}
    uid = str(uids[0])
    summaries = await esummary_by_ids(client, db="gds", uids=[uid], timeout=timeout)
    record = summaries.get(uid, {})
    return record if isinstance(record, dict) else {}


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
    return await fetch_record(client, sample, timeout, kind="SAMPLE")


async def fetch_record(client: SurveyClient, accession: str, timeout: float, *, kind: str) -> str:
    """GEO's own text record for one series, sample or platform.

    ``kind`` is checked against the record that comes back: acc.cgi answers an
    unknown accession with an HTML page and status 200, and parsing that as an
    empty record would report a withdrawn series as one with no links.
    """
    response = await client.get(
        ACC_CGI,
        params={"acc": accession, "targ": "self", "form": "text", "view": "brief"},
        timeout=timeout,
    )
    text = response.text
    if not text.lstrip().startswith(f"^{kind}"):
        raise MalformedResponseError(
            f"acc.cgi returned no {kind} record for {accession}: {text[:120]!r}"
        )
    return text


@dataclass(slots=True)
class RecordLinks:
    """Every linked accession one acc.cgi record names, by the line that names it."""

    accession: str
    #: ``(type, value)`` per relation line: ``("SubSeries of", "GSE236087")``,
    #: ``("BioProject", "https://www.ncbi.nlm.nih.gov/bioproject/PRJNA988806")``.
    relations: list[tuple[str, str]] = field(default_factory=list)
    samples: list[str] = field(default_factory=list)
    series: list[str] = field(default_factory=list)
    platforms: list[str] = field(default_factory=list)
    supplementary_files: list[str] = field(default_factory=list)


_RECORD_LINE = re.compile(r"^!(Series|Sample|Platform)_(\w+?)\s*=\s*(.*\S)\s*$", re.MULTILINE)


def parse_record_links(accession: str, text: str) -> RecordLinks:
    """The linked accessions in a single ``acc.cgi`` record.

    The relation lines are not part of GEO's documented SOFT format -- GEO adds
    them -- and their value is ``<type>: <accession or URL>``, split on the
    first colon only, because the URL has colons of its own.
    """
    record = RecordLinks(accession=accession)
    for match in _RECORD_LINE.finditer(text):
        label, value = match.group(2), match.group(3)
        if label == "relation":
            kind, _, target = value.partition(":")
            record.relations.append((kind.strip(), target.strip()))
        elif label == "sample_id":
            record.samples.append(value)
        elif label == "series_id":
            record.series.append(value)
        elif label == "platform_id":
            record.platforms.append(value)
        elif label.startswith("supplementary_file") and value.upper() != "NONE":
            record.supplementary_files.append(value)
    return record


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
