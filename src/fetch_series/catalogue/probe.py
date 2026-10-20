"""One live request per untested link, to see whether it answers at all.

Each mechanism gets a fetcher: given one accession, it makes the mechanism's
request and returns what each declared field held, keyed by node -- or by the
group of nodes a field fills (``run`` for SRR, ERR and DRR alike, ``file`` for
every read format). One request therefore probes every link a mechanism offers
from that accession.

The values are then **normalised by the target node**, not trusted: an
accession-bearing field keeps only tokens that :func:`fetch_series.accession.parse`
places on the target -- at its entity type *and* its archive, so an ERR found
by a request about an SRP is not a link from the SRP to ENA's runs. A file is
placed by its format. ``SampleName`` holds a GSM for a GEO submission and a
submitter's free text otherwise, and only the first is a link.

**A link into a file or a document is confirmed only by what was found being
usable**, not by its being named:

* a read file must be *downloadable* (a HEAD answers 2xx and serves the size
  the archive published), *verifiable* (the archive publishes an md5 and a
  size) and *complete for 10x* (both the barcode read and the cDNA read are
  there). Each is checked on one run's file set;
* a supplementary file must download and be size-checkable;
* a metadata document must download and parse with the parser the project
  already uses for it.

What a probe establishes is deliberately narrow. It shows the link exists for
one record, on one day. It says nothing about how often the link answers, so it
never becomes ``RouteEvidence`` -- one accession out of one is not a yield.
"""

from __future__ import annotations

import asyncio
import gzip
import io
import random
import re
import tarfile
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from typing import Any
from urllib.parse import urlencode
from xml.etree import ElementTree

import httpx

from fetch_series.accession import RULES, EntityType, parse, try_parse
from fetch_series.catalogue import (
    CATALOGUE,
    PROBE_KEEP,
    Catalogue,
    Check,
    Link,
    Node,
    NodeKind,
    Probe,
)
from fetch_series.catalogue.mechanisms import DBLINK_TYPES, INSDC_FIELDS, RUNINFO_FIELDS
from fetch_series.catalogue.nodes import SPLIT, TEN_X_RUNS, node_for, read_format
from fetch_series.files import (
    FileRecord,
    classify,
    from_ena_row,
    from_sdl_files,
    mate_of,
    request_url,
)
from fetch_series.filesets import verify
from fetch_series.logging_utils import redact, redact_exception
from fetch_series.providers import (
    biosamples,
    biostudies,
    ddbj,
    ena_portal,
    eutils,
    geo,
    sdl,
    sra_be,
)
from fetch_series.results import Outcome
from fetch_series.routes import ae_file_records
from fetch_series.survey.client import (
    Limits,
    MalformedResponseError,
    SurveyClient,
    is_retryable,
)

Found = dict[str, list[str]]


@dataclass(slots=True)
class Document:
    """A metadata document a probe fetched, and whether it parsed."""

    url: str
    parses: bool


@dataclass(slots=True)
class Answer:
    """What one request said: values by node or group, and what they name.

    ``files`` carries the published size and md5 of every file named, so the
    files can be checked rather than merely listed; ``documents`` the metadata
    documents fetched, by node.
    """

    values: Found
    files: list[FileRecord] = field(default_factory=list)
    documents: dict[str, Document] = field(default_factory=dict)


Fetcher = Callable[[SurveyClient, str, float], Awaitable["Found | Answer"]]


class RecordAbsent(LookupError):
    """The archive's own search holds no record of the input accession.

    That is an answer, not a failure: nothing links from a record the archive
    does not have, so every link the request would have answered is EMPTY.
    ``PRJNA1136968`` is absent from BioProject; ``PRJNA857927`` was withdrawn.
    Recorded with the reason, so an empty from absence can be told from an
    empty from a record that links nowhere.
    """


# Any accession the registry knows, found anywhere in a value: a relation
# line's URL, an alias with a suffix (`GSM1480291_r1`), a run range
# (`ERR1630013-ERR1633526`). The registry's patterns are mutually exclusive, so
# one alternation of them finds each token once. Bounded on both sides by
# anything but a letter or digit, as ena_portal's alias patterns are: a
# submitter name that merely starts with an accession is not one.
_TOKEN = re.compile(
    "(?<![A-Za-z0-9])(?:"
    + "|".join(f"(?:{rule.pattern.pattern})" for rule in RULES)
    + ")(?![A-Za-z0-9])"
)


def group_of(node: Node) -> str:
    """The key a fetcher files a node's values under, where it does not use the node id."""
    if node.kind is NodeKind.READ_FORMAT:
        return "file"
    if node.insdc_object and node.entity is not None:
        return str(node.entity)
    return node.id


def lookup(found: Mapping[str, list[str]], node: Node) -> list[str]:
    return found.get(node.id) or found.get(group_of(node)) or []


def normalise(node: Node, values: Iterable[str]) -> list[str]:
    """What ``values`` say about ``node``, and nothing else, sorted."""
    if node.kind is NodeKind.READ_FORMAT:
        return sorted({v.strip() for v in values if v.strip() and read_format(v) == node.id})
    if node.entity is None:
        return sorted({part.strip() for v in values for part in v.split(";") if part.strip()})
    found: set[str] = set()
    for value in values:
        for match in _TOKEN.finditer(value):
            parsed = try_parse(match.group(0))
            if parsed is not None and node_for(parsed) == node.id:
                found.add(parsed.value)
    return sorted(found)


def _input_group(accession: str) -> str:
    """The group an accession is an instance of, for fetchers that branch on it."""
    parsed = try_parse(accession)
    node = node_for(parsed) if parsed else None
    if parsed is None or node is None:
        raise ValueError(f"{accession} is not an accession of any one node")
    return str(parsed.entity) if parsed.entity in SPLIT else node


def _split_sample(address: str) -> tuple[str, str | None]:
    """``E-MTAB-5061:AZ_A1`` -> (study, Source Name); a bare study has no row."""
    study, _, row = address.partition(":")
    return study, row or None


def https(url: str) -> str:
    """A download link as something a HEAD can ask: the archives' FTP hosts serve HTTPS.

    A ``#`` is escaped by :func:`~fetch_series.files.request_url`: a Sanger
    CRAM's name holds one, and unescaped it is read as a fragment.
    """
    url = request_url(url.strip())
    if url.startswith("ftp://"):
        return "https://" + url.removeprefix("ftp://")
    if "://" not in url:
        return f"https://{url}"
    return url


def _record(
    run: str, url: str, source: str, *, name: str | None = None, **known: Any
) -> FileRecord:
    name = name or url.rsplit("/", 1)[-1]
    return FileRecord(
        run=run, url=url, name=name, kind=classify(name), source=source, mate=mate_of(name), **known
    )


# --------------------------------------------------------------------- ENA


async def _filereport(client: SurveyClient, accession: str, timeout: float) -> Found:
    rows = await ena_portal.read_run_report(
        client, accession, timeout, fields=tuple(INSDC_FIELDS.values())
    )
    return {group: ena_portal.column(rows, column) for group, column in INSDC_FIELDS.items()}


async def _aliases(client: SurveyClient, accession: str, timeout: float) -> Found:
    fields = ("study_alias", *ena_portal.ALIAS_FIELDS)
    rows = await ena_portal.read_run_report(client, accession, timeout, fields=fields)
    ae = [
        match.group(1)
        for row in rows
        for name in fields
        if (match := ena_portal.AE_IN_ALIAS.match((row.get(name) or "").strip()))
    ]
    return {
        "geo_series": ena_portal.column(rows, "study_alias"),
        "geo_sample": ena_portal.alias_accessions(rows, ena_portal.GSM_IN_ALIAS),
        "ae_experiment": ae,
    }


def _file_column(prefix: str, mechanism: str, result: str = "read_run") -> Fetcher:
    """ENA's ``<prefix>_ftp`` with its md5 and size, as records."""

    async def fetch(client: SurveyClient, accession: str, timeout: float) -> Answer:
        key = "analysis_accession" if result == "analysis" else "run_accession"
        fields: tuple[str, ...] = (key, f"{prefix}_ftp", f"{prefix}_md5", f"{prefix}_bytes")
        if result == "read_run":
            fields += ("library_layout",)
        rows = await ena_portal.read_run_report(
            client, accession, timeout, fields=fields, result=result
        )
        records = [
            record
            for row in rows
            for record in from_ena_row({**row, "run_accession": row.get(key)}, prefix, mechanism)
        ]
        return Answer({"file": [r.url for r in records]}, files=records)

    return fetch


ANALYSIS_FIELDS = {
    "analysis": "analysis_accession",
    "bioproject": "study_accession",
    "study": "secondary_study_accession",
    "biosample": "sample_accession",
    "sample": "secondary_sample_accession",
    "experiment": "experiment_accession",
    "run": "run_accession",
    "submission": "submission_accession",
}


async def _analysis(client: SurveyClient, accession: str, timeout: float) -> Found:
    rows = await ena_portal.read_run_report(
        client, accession, timeout, fields=tuple(ANALYSIS_FIELDS.values()), result="analysis"
    )
    return {group: ena_portal.listed(rows, column) for group, column in ANALYSIS_FIELDS.items()}


_STUDY_QUERY = {
    "bioproject": "study_accession",
    "study": "secondary_study_accession",
    "geo_series": "geo_accession",
}


async def _search_study(client: SurveyClient, accession: str, timeout: float) -> Found:
    column = _STUDY_QUERY[_input_group(accession)]
    rows = await ena_portal.search(
        client,
        "study",
        f'{column}="{accession}"',
        ("study_accession", "secondary_study_accession", "geo_accession"),
        timeout,
    )
    return {
        "bioproject": ena_portal.column(rows, "study_accession"),
        "study": ena_portal.column(rows, "secondary_study_accession"),
        "geo_series": ena_portal.column(rows, "geo_accession"),
    }


async def _search_parent(client: SurveyClient, accession: str, timeout: float) -> Found:
    rows = await ena_portal.search(
        client, "study", f'study_accession="{accession}"', ("parent_study_accession",), timeout
    )
    return {"bioproject": ena_portal.listed(rows, "parent_study_accession")}


async def _search_sample(client: SurveyClient, accession: str, timeout: float) -> Found:
    column = {"biosample": "sample_accession", "sample": "secondary_sample_accession"}[
        _input_group(accession)
    ]
    rows = await ena_portal.search(
        client,
        "sample",
        f'{column}="{accession}"',
        (
            "sample_accession",
            "secondary_sample_accession",
            "study_accession",
            "submission_accession",
        ),
        timeout,
    )
    return {
        "biosample": ena_portal.column(rows, "sample_accession"),
        "sample": ena_portal.column(rows, "secondary_sample_accession"),
        "bioproject": ena_portal.listed(rows, "study_accession"),
        "submission": ena_portal.column(rows, "submission_accession"),
    }


async def _links(client: SurveyClient, accession: str, timeout: float) -> Found:
    source = _input_group(accession)
    data_type = {"bioproject": "study", "biosample": "sample"}[source]
    results = {"run": "read_run", "experiment": "read_experiment", "analysis": "analysis"}
    if source == "bioproject":
        results["biosample"] = "sample"
    return {
        group: await ena_portal.links(client, data_type, accession, result, timeout)
        for group, result in results.items()
    }


async def _xref_source(client: SurveyClient, accession: str, timeout: float) -> Found:
    source = "GEO" if _input_group(accession) == "geo_series" else "ArrayExpress"
    entries = await ena_portal.xref(client, timeout, source=source, source_accession=accession)
    return {
        "bioproject": ena_portal.xref_targets(entries, "Target Primary Accession"),
        "study": ena_portal.xref_targets(entries, "Target Secondary Accession"),
    }


async def _xref_target(client: SurveyClient, accession: str, timeout: float) -> Found:
    entries = await ena_portal.xref(client, timeout, accession=accession)
    return {
        "geo_series": ena_portal.xref_sources(entries, "GEO"),
        "ae_experiment": ena_portal.xref_sources(entries, "ArrayExpress"),
    }


async def _browser_xml(client: SurveyClient, accession: str, timeout: float) -> Found:
    refs = ena_portal.browser_cross_references(
        await ena_portal.browser_xml(client, accession, timeout)
    )
    return {
        "geo_series": refs.get("GEO", []),
        "ae_experiment": [url for url in refs.get("url", []) if "arrayexpress" in url.lower()],
    }


def _names(rows: list[dict[str, Any]], accession: str) -> bool:
    """Whether a metadata table has a row about the record that was asked for."""
    return any(accession in row.values() for row in rows)


async def _filereport_document(client: SurveyClient, accession: str, timeout: float) -> Answer:
    fields = ("run_accession", "experiment_accession", *ena_portal.METADATA_FIELDS[:8])
    rows = await ena_portal.read_run_report(client, accession, timeout, fields=fields)
    url = f"{ena_portal.FILEREPORT}?" + urlencode(
        {"accession": accession, "result": "read_run", "fields": ",".join(fields)}
    )
    return Answer(
        {"metadata_ebi": [url]},
        documents={"metadata_ebi": Document(url, _names(rows, accession))},
    )


def _xml_names(text: str, accession: str) -> bool:
    """Whether a document parses as XML and names the record somewhere in it."""
    try:
        ElementTree.fromstring(text)
    except ElementTree.ParseError:
        return False
    return accession in text


async def _browser_xml_document(client: SurveyClient, accession: str, timeout: float) -> Answer:
    text = await ena_portal.browser_xml(client, accession, timeout)
    try:
        ena_portal.browser_cross_references(text)
        parses = accession in text
    except MalformedResponseError:
        parses = False
    url = f"{ena_portal.BROWSER_XML}/{accession}"
    return Answer({"metadata_ebi": [url]}, documents={"metadata_ebi": Document(url, parses)})


# -------------------------------------------------------------------- NCBI

# How to turn an input accession into a UID of each database, and how an
# ESummary record of each database names its accession.
_UID_TERMS = {
    ("bioproject", "bioproject"): "{acc}",
    ("biosample", "biosample"): "{acc}[accession]",
    ("experiment", "sra"): "{acc}",
    ("geo_series", "gds"): "{acc}[ACCN] AND GSE[ETYP]",
    ("geo_sample", "gds"): "{acc}[ACCN] AND GSM[ETYP]",
}


def _summary_accessions(db: str, record: dict[str, Any]) -> list[str]:
    if db == "sra":
        return eutils.expxml_accessions(record).get("Experiment", [])
    for key in ("accession", "project_acc", "Project_Acc"):
        if value := record.get(key):
            return [str(value)]
    return []


def _elink(linkname: str) -> Fetcher:
    dbfrom, _, rest = linkname.partition("_")
    db = rest.split("_")[0]

    async def fetch(client: SurveyClient, accession: str, timeout: float) -> Found:
        source = _input_group(accession)
        term = _UID_TERMS[(source, dbfrom)].format(acc=accession)
        uids = (await eutils.esearch(client, dbfrom, term, timeout, usehistory=False)).get(
            "idlist"
        ) or []
        if not uids:
            raise RecordAbsent(f"esearch db={dbfrom} found no UID for {accession}")
        linked = await eutils.elink_uids(
            client, dbfrom, db, [str(u) for u in uids], timeout, linkname=linkname
        )
        summaries = await eutils.esummary_by_ids(client, db, linked, timeout) if linked else {}
        values = [
            found
            for record in summaries.values()
            if isinstance(record, dict)
            for found in _summary_accessions(db, record)
        ]
        mechanism = CATALOGUE.mechanisms[f"elink_{linkname}"]
        return dict.fromkeys(mechanism.outputs, values)

    return fetch


async def _runinfo(client: SurveyClient, accession: str, timeout: float) -> Found:
    rows = await sra_be.runinfo_by_accession(client, accession, timeout)
    return {group: sra_be.column(rows, column) for group, column in RUNINFO_FIELDS.items()}


async def _runinfo_download(client: SurveyClient, accession: str, timeout: float) -> Answer:
    rows = await sra_be.runinfo_by_accession(client, accession, timeout)
    records = [
        _record(row["Run"], row["download_path"], "sra_be_runinfo_download")
        for row in rows
        if row.get("download_path")
    ]
    return Answer({"file": [r.url for r in records]}, files=records)


async def _runinfo_document(client: SurveyClient, accession: str, timeout: float) -> Answer:
    rows = await sra_be.runinfo_by_accession(client, accession, timeout)
    url = f"{sra_be.RUNINFO}?acc={accession}"
    return Answer(
        {"metadata_ncbi": [url]},
        documents={"metadata_ncbi": Document(url, _names(rows, accession))},
    )


_EXPXML = {
    "experiment": "Experiment",
    "run": "Run",
    "study": "Study",
    "sample": "Sample",
    "bioproject": "Bioproject",
    "biosample": "Biosample",
    "submission": "Submitter",
}


async def _esummary_sra(client: SurveyClient, accession: str, timeout: float) -> Found:
    uids = await eutils.esearch_all_uids(client, "sra", accession, timeout)
    summaries = await eutils.esummary_by_ids(client, "sra", uids, timeout) if uids else {}
    found: Found = {group: [] for group in _EXPXML}
    for record in summaries.values():
        if isinstance(record, dict):
            accessions = eutils.expxml_accessions(record)
            for group, element in _EXPXML.items():
                found[group].extend(accessions.get(element, []))
    return found


def _sdl(**params: str) -> Fetcher:
    """SDL with extra request parameters: none, ``filetype=sralite``, or the originals."""
    originals = params.get("accept-alternate-locations") == "yes"

    async def fetch(client: SurveyClient, accession: str, timeout: float) -> Answer:
        response = await client.post(
            sdl.RETRIEVE,
            data={"acc": accession, "accept-charges": "aws", **params},
            timeout=timeout,
        )
        payload = response.json()
        if "result" not in payload:
            raise MalformedResponseError(f"SDL response has no result key: {sorted(payload)}")
        records: list[FileRecord] = []
        for bundle in payload["result"]:
            if bundle.get("status") != 200:
                continue
            files = bundle.get("files") or []
            if originals:
                # The submitter's files are listed with an empty link: in cold
                # storage, so named but not downloadable. A record keeps the
                # name, and an empty URL that the download check refuses.
                for entry in files:
                    if entry.get("type") == "sra":
                        continue
                    links = [loc.get("link") for loc in entry.get("locations") or []]
                    records.append(
                        _record(
                            str(entry.get("accession") or bundle.get("bundle")),
                            next((link for link in links if link), ""),
                            "sdl_original",
                            name=str(entry.get("name") or ""),
                            size=entry.get("size"),
                            md5=entry.get("md5"),
                        )
                    )
                continue
            for entry in files:
                for record in from_sdl_files(
                    str(entry.get("accession") or bundle.get("bundle")), [entry], "sdl"
                ):
                    # Prefer the free copy: a pay-required location is the same
                    # bytes behind a bill, and the check is of the free one.
                    if record.is_free:
                        records.append(record)
        values = [r.url or r.name for r in records]
        return Answer({"file": values}, files=records)

    return fetch


async def _sra_xml(client: SurveyClient, accession: str, timeout: float) -> str:
    uids = await eutils.esearch_all_uids(client, "sra", accession, timeout)
    if not uids:
        raise RecordAbsent(f"esearch db=sra found no UID for {accession}")
    return await eutils.efetch_text(
        client, "sra", timeout, uids=uids[:20], rettype="full", retmode="xml"
    )


async def _sra_xml_document(client: SurveyClient, accession: str, timeout: float) -> Answer:
    text = await _sra_xml(client, accession, timeout)
    url = f"{eutils.BASE}/efetch.fcgi?db=sra&rettype=full&retmode=xml&term={accession}"
    return Answer(
        {"metadata_ncbi": [url]},
        documents={"metadata_ncbi": Document(url, _xml_names(text, accession))},
    )


_SEMANTIC = {"SRA Normalized": "sra_normalized", "SRA Lite": "sra_lite"}


async def _sra_xml_files(client: SurveyClient, accession: str, timeout: float) -> Answer:
    root = ElementTree.fromstring(await _sra_xml(client, accession, timeout))
    records: list[FileRecord] = []
    for run in root.iter("RUN"):
        for entry in run.iter("SRAFile"):
            if entry.get("semantic_name") not in _SEMANTIC or not entry.get("url"):
                continue
            size = entry.get("size")
            records.append(
                _record(
                    run.get("accession") or "",
                    entry.get("url") or "",
                    "efetch_sra_files",
                    name=entry.get("filename"),
                    size=int(size) if size and size.isdigit() else None,
                    md5=entry.get("md5"),
                )
            )
    return Answer({"file": [r.url for r in records]}, files=records)


async def read_lengths(client: SurveyClient, run: str, timeout: float) -> list[int] | None:
    """The average length of each read of a run's spots, from its SRA XML.

    ``RUN/Statistics/Read[@average]``: SRR25056225 says 28 and 91, the barcode
    read and the cDNA read. ``None`` where the document says nothing.
    """
    root = ElementTree.fromstring(await _sra_xml(client, run, timeout))
    for element in root.iter("RUN"):
        if element.get("accession") != run:
            continue
        for statistics in element.iter("Statistics"):
            reads = statistics.findall("Read")
            if reads:
                return [round(float(r.get("average") or 0)) for r in reads]
    return None


async def _biosample_record(client: SurveyClient, accession: str, timeout: float) -> Answer:
    records = await eutils.fetch_biosamples(client, [accession], timeout)
    links = [link for record in records for link in record.cross_links]
    url = f"{eutils.BASE}/efetch.fcgi?db=biosample&term={accession}[accession]"
    return Answer(
        {
            "sample": [value for db, value in links if db == "SRA"],
            "geo_sample": [value for db, value in links if db == "GEO"],
            "bioproject": [project for record in records for project in record.bioprojects],
            "metadata_biosample": [url],
        },
        documents={
            "metadata_biosample": Document(url, any(r.accession == accession for r in records))
        },
    )


# --------------------------------------------------------------------- GEO


def _supplementary(urls: Iterable[str], mechanism: str) -> list[FileRecord]:
    return [_record("", https(url), mechanism) for url in urls if url]


def _soft_url(accession: str) -> str:
    parsed = parse(accession)
    return f"{geo.FTP_BASE}/{parsed.geo_ftp_stem}/{parsed.value}/soft/{parsed.value}_family.soft.gz"


async def _soft(client: SurveyClient, accession: str, timeout: float) -> Answer:
    family = geo.parse_soft_family(
        accession, await geo.fetch_soft_family(client, accession, timeout)
    )
    url = _soft_url(accession)
    files = _supplementary(family.supplementary_files, "geo_soft_family")
    return Answer(
        {
            "geo_sample": family.samples,
            "geo_platform": family.platforms,
            "geo_supplementary": [f.url for f in files],
            "metadata_geo": [url],
        },
        files=files,
        documents={"metadata_geo": Document(url, bool(family.samples))},
    )


async def _soft_relations(client: SurveyClient, accession: str, timeout: float) -> Found:
    family = geo.parse_soft_family(
        accession, await geo.fetch_soft_family(client, accession, timeout)
    )
    relations = family.sample_relations.values()
    return {
        "bioproject": family.bioprojects,
        "study": family.sra_studies,
        "geo_series": family.subseries + family.superseries,
        "experiment": [r["experiment"] for r in relations if "experiment" in r],
        "biosample": [r["biosample"] for r in relations if "biosample" in r],
    }


def _geo_file(accession: str, directory: str, name: str) -> str:
    parsed = parse(accession)
    return f"{geo.FTP_BASE}/{parsed.geo_ftp_stem}/{parsed.value}/{directory}/{name}"


async def _miniml(client: SurveyClient, accession: str, timeout: float) -> Answer:
    url = _geo_file(accession, "miniml", f"{accession}_family.xml.tgz")
    response = await client.get(url, timeout=timeout)
    parses = False
    with tarfile.open(fileobj=io.BytesIO(response.content), mode="r:gz") as archive:
        for member in archive.getmembers():
            if member.name == f"{accession}_family.xml" and (handle := archive.extractfile(member)):
                parses = _xml_names(handle.read().decode("utf-8", errors="replace"), accession)
    return Answer({"metadata_geo": [url]}, documents={"metadata_geo": Document(url, parses)})


_MATRIX_FILE = re.compile(r'href="([^"/]+_series_matrix\.txt\.gz)"')


async def _series_matrix(client: SurveyClient, accession: str, timeout: float) -> Answer:
    """The series matrix, found by listing its directory.

    A series on more than one platform has one matrix per platform, named
    ``GSE120926-GPL11154_series_matrix.txt.gz``; the single-platform name
    ``GSE120926_series_matrix.txt.gz`` does not exist for it, and asking for it
    reported a matrix that is there as a 404. The first file, sorted, is the
    one checked.
    """
    directory = _geo_file(accession, "matrix", "")
    listing = await client.get(directory, timeout=timeout)
    names = sorted(set(_MATRIX_FILE.findall(listing.text)))
    if not names:
        return Answer({"metadata_geo": []})
    url = directory + names[0]
    response = await client.get(url, timeout=timeout)
    text = gzip.decompress(response.content).decode("utf-8", errors="replace")
    parses = f'!Series_geo_accession\t"{accession}"' in text
    return Answer(
        {"metadata_geo": [directory + name for name in names]},
        documents={"metadata_geo": Document(url, parses)},
    )


def _relations(record: geo.RecordLinks, *kinds: str) -> list[str]:
    return [value for kind, value in record.relations if kind in kinds]


def _acc_url(accession: str) -> str:
    return f"{geo.ACC_CGI}?acc={accession}&targ=self&form=text&view=brief"


async def _acc_series(client: SurveyClient, accession: str, timeout: float) -> Answer:
    record = geo.parse_record_links(
        accession, await geo.fetch_record(client, accession, timeout, kind="SERIES")
    )
    files = _supplementary(record.supplementary_files, "geo_acc_series")
    url = _acc_url(accession)
    return Answer(
        {
            "geo_sample": record.samples,
            "geo_platform": record.platforms,
            "bioproject": _relations(record, "BioProject"),
            "study": _relations(record, "SRA"),
            "geo_series": _relations(record, "SuperSeries of", "SubSeries of"),
            "geo_supplementary": [f.url for f in files],
            "metadata_geo": [url],
        },
        files=files,
        documents={"metadata_geo": Document(url, bool(record.samples))},
    )


async def _acc_sample(client: SurveyClient, accession: str, timeout: float) -> Answer:
    record = geo.parse_record_links(
        accession, await geo.fetch_record(client, accession, timeout, kind="SAMPLE")
    )
    files = _supplementary(record.supplementary_files, "geo_acc_sample")
    url = _acc_url(accession)
    return Answer(
        {
            "experiment": _relations(record, "SRA"),
            "biosample": _relations(record, "BioSample"),
            "geo_series": record.series,
            "geo_platform": record.platforms,
            "geo_supplementary": [f.url for f in files],
            "metadata_geo": [url],
        },
        files=files,
        documents={"metadata_geo": Document(url, bool(record.series))},
    )


async def _acc_platform(client: SurveyClient, accession: str, timeout: float) -> Found:
    record = geo.parse_record_links(
        accession, await geo.fetch_record(client, accession, timeout, kind="PLATFORM")
    )
    return {"geo_series": record.series, "geo_sample": record.samples}


def _numbered(prefix: str, value: object) -> list[str]:
    """``gse: "154051;82916"`` -> GSE154051, GSE82916: gds stores bare numbers."""
    return [f"{prefix}{part.strip()}" for part in str(value or "").split(";") if part.strip()]


def _entries(value: object) -> list[dict[str, Any]]:
    """The dict entries of a list-valued ESummary field; anything else is none."""
    return [entry for entry in value if isinstance(entry, dict)] if isinstance(value, list) else []


def _sra_relations(record: dict[str, object]) -> list[str]:
    return [
        str(r["targetobject"])
        for r in _entries(record.get("extrelations"))
        if r.get("relationtype") == "SRA" and r.get("targetobject")
    ]


async def _gds_series(client: SurveyClient, accession: str, timeout: float) -> Found:
    record = await geo.gds_record(client, accession, "GSE", timeout)
    return {
        "bioproject": [str(record.get("bioproject") or "")],
        "geo_sample": [
            str(s["accession"]) for s in _entries(record.get("samples")) if s.get("accession")
        ],
        "geo_platform": _numbered("GPL", record.get("gpl")),
        "study": _sra_relations(record),
    }


async def _gds_sample(client: SurveyClient, accession: str, timeout: float) -> Found:
    record = await geo.gds_record(client, accession, "GSM", timeout)
    return {
        "geo_series": _numbered("GSE", record.get("gse")),
        "geo_platform": _numbered("GPL", record.get("gpl")),
        "experiment": _sra_relations(record),
    }


async def _gds_platform(client: SurveyClient, accession: str, timeout: float) -> Found:
    record = await geo.gds_record(client, accession, "GPL", timeout)
    return {"geo_series": _numbered("GSE", record.get("gse"))}


# ------------------------------------------------------- ArrayExpress, EBI


async def _idf(client: SurveyClient, accession: str, timeout: float) -> Answer:
    comments = biostudies.idf_comments(await biostudies.fetch_idf(client, accession, timeout))
    secondary = comments.get("SecondaryAccession", [])
    url = f"{biostudies.FILES}/{accession}/{accession}.idf.txt"
    return Answer(
        {
            "study": secondary,
            "geo_series": secondary,
            "ae_experiment": comments.get("RelatedExperiment", []),
            "run": comments.get("SequenceDataURI", []),
            "metadata_ae": [url],
        },
        documents={"metadata_ae": Document(url, bool(comments))},
    )


async def _sdrf(client: SurveyClient, address: str, timeout: float) -> Answer:
    study, row = _split_sample(address)
    rows = await biostudies.sdrf_raw_rows(client, study, timeout)
    values = biostudies.sdrf_values(
        rows, ("Source Name", "Derived Array Data File"), source_name=row
    )
    files = [
        _record("", f"{biostudies.FILES}/{study}/{name}", "biostudies_sdrf")
        for name in values["Derived Array Data File"]
    ]
    url = f"{biostudies.FILES}/{study}/{study}.sdrf.txt"
    return Answer(
        {
            "ae_sample": [f"{study}:{name}" for name in values["Source Name"]],
            "ae_supplementary": [f.url for f in files],
            "metadata_ae": [url],
        },
        files=files,
        documents={"metadata_ae": Document(url, len(rows) > 1 and bool(values["Source Name"]))},
    )


_SDRF_COMMENTS = {
    "biosample": "Comment[BioSD_SAMPLE]",
    "sample": "Comment[ENA_SAMPLE]",
    "experiment": "Comment[ENA_EXPERIMENT]",
    "run": "Comment[ENA_RUN]",
    "geo_sample": "Source Name",
}


async def _sdrf_comments(client: SurveyClient, address: str, timeout: float) -> Found:
    study, row = _split_sample(address)
    rows = await biostudies.sdrf_raw_rows(client, study, timeout)
    values = biostudies.sdrf_values(rows, tuple(_SDRF_COMMENTS.values()), source_name=row)
    return {group: values[column] for group, column in _SDRF_COMMENTS.items()}


async def _sdrf_fastq(client: SurveyClient, address: str, timeout: float) -> Answer:
    """The measured route's own reading, guard and all, narrowed to one row if asked."""
    study, row = _split_sample(address)
    records = await ae_file_records(study, client, timeout)
    if row is not None:
        rows = await biostudies.sdrf_raw_rows(client, study, timeout)
        runs = set(
            biostudies.sdrf_values(rows, ("Comment[ENA_RUN]",), source_name=row)["Comment[ENA_RUN]"]
        )
        records = [r for r in records if r.run in runs]
    return Answer({"file": [r.url for r in records]}, files=records)


async def _files(client: SurveyClient, accession: str, timeout: float) -> Answer:
    """Processed data a study registers: its own section, or its own description."""
    entries = await biostudies.registered_files(client, accession, timeout)
    files = [
        _record(
            "",
            f"{biostudies.FILES}/{accession}/{entry['path']}",
            "biostudies_files",
            size=entry.get("Size") if isinstance(entry.get("Size"), int) else None,
        )
        for entry in entries
        if entry.get("path")
        and (
            entry.get("Section") == "processed-data" or entry.get("Description") == "Processed Data"
        )
    ]
    return Answer({"ae_supplementary": [f.url for f in files]}, files=files)


async def _study_json(client: SurveyClient, accession: str, timeout: float) -> Found:
    urls = [
        url for url, _ in biostudies.study_links(await biostudies.study(client, accession, timeout))
    ]
    return dict.fromkeys(("study", "bioproject", "ae_experiment", "geo_series"), urls)


def _biostudies_search(collection: str | None, key: str) -> Fetcher:
    async def fetch(client: SurveyClient, accession: str, timeout: float) -> Found:
        hits = await biostudies.search(
            client, timeout, query=accession, collection=collection, max_pages=3
        )
        return {key: hits}

    return fetch


async def _biosamples_record(client: SurveyClient, accession: str, timeout: float) -> Answer:
    payload = await biosamples.sample(client, accession, timeout)
    url = f"{biosamples.SAMPLES}/{accession}"
    return Answer(
        {"sample": biosamples.sra_accessions(payload), "metadata_biosample": [url]},
        documents={"metadata_biosample": Document(url, payload.get("accession") == accession)},
    )


async def _biosamples_filter(client: SurveyClient, accession: str, timeout: float) -> Found:
    hits = await biosamples.by_attribute(client, biosamples.SRA_ACCESSION, accession, timeout)
    return {"biosample": hits}


async def _dblink(client: SurveyClient, accession: str, timeout: float) -> Found:
    linked = await ddbj.dblink(client, DBLINK_TYPES[_input_group(accession)], accession, timeout)
    return {group: linked.get(kind, []) for group, kind in DBLINK_TYPES.items()}


# -------------------------------------------------------------------- DDBJ

DDBJ_ENTRY = "https://ddbj.nig.ac.jp/search/entry"


async def _entry(
    client: SurveyClient, accession: str, timeout: float
) -> tuple[str, dict[str, Any]]:
    kind = {"run": "sra-run", "experiment": "sra-experiment"}[_input_group(accession)]
    url = f"{DDBJ_ENTRY}/{kind}/{accession}.json"
    response = await client.get(url, timeout=timeout)
    payload = response.json()
    if not isinstance(payload, dict):
        raise MalformedResponseError(f"DDBJ entry for {accession} is not an object")
    return url, payload


def _distribution(payload: dict[str, Any], kind: str) -> list[str]:
    return [
        str(d["contentUrl"])
        for d in payload.get("distribution") or []
        if isinstance(d, dict) and d.get("encodingFormat") == kind and d.get("contentUrl")
    ]


async def _ddbj_entry_sra(client: SurveyClient, accession: str, timeout: float) -> Answer:
    _, payload = await _entry(client, accession, timeout)
    records = [_record(accession, url, "ddbj_entry_sra") for url in _distribution(payload, "SRA")]
    return Answer({"file": [r.url for r in records]}, files=records)


# An nginx index: a link per file, then its date and its size, rounded.
_INDEX_ROW = re.compile(r'<a href="([^"/?][^"]*)">')


async def _ddbj_entry_fastq(client: SurveyClient, accession: str, timeout: float) -> Answer:
    _, payload = await _entry(client, accession, timeout)
    records: list[FileRecord] = []
    for directory in _distribution(payload, "FASTQ"):
        listing = (await client.get(directory, timeout=timeout)).text
        for name in _INDEX_ROW.findall(listing):
            if name.startswith(accession):
                records.append(
                    _record(accession, directory.rstrip("/") + "/" + name, "ddbj_entry_fastq")
                )
    return Answer({"file": [r.url for r in records]}, files=records)


async def _ddbj_entry_document(client: SurveyClient, accession: str, timeout: float) -> Answer:
    url, payload = await _entry(client, accession, timeout)
    return Answer(
        {"metadata_ddbj": [url]},
        documents={"metadata_ddbj": Document(url, payload.get("identifier") == accession)},
    )


async def _ddbj_dra_xml(client: SurveyClient, accession: str, timeout: float) -> Answer:
    _, payload = await _entry(client, accession, timeout)
    found: list[str] = []
    documents: dict[str, Document] = {}
    for url in _distribution(payload, "XML")[:1]:
        text = (await client.get(url, timeout=timeout)).text
        found.append(url)
        documents["metadata_ddbj"] = Document(url, _xml_names(text, accession))
    return Answer({"metadata_ddbj": found}, documents=documents)


# ---------------------------------------------------------------- registry


@dataclass(frozen=True, slots=True)
class Prober:
    """How to probe one mechanism: which host bucket's limits apply, and the fetch.

    ``retry`` narrows the client's retry rule for this mechanism, given the
    input accession: a request known never to succeed should fail at once
    rather than spend five attempts and two minutes proving it again.
    """

    host: str
    fetch: Fetcher
    retry: Callable[[str, BaseException], bool] | None = None


def _sdl_retry(accession: str, exc: BaseException) -> bool:
    """SDL answers a project or a study with a gateway 504, every time.

    ``sdl/2/retrieve acc=PRJNA495040`` (and ``ERP110462``) times out at the
    gateway after ~10 s on every attempt, and 504 is retryable everywhere else.
    Only a run, or an experiment, is worth a second attempt after a 504.
    """
    parsed = try_parse(accession)
    asks_for_runs = parsed is not None and parsed.entity in (EntityType.RUN, EntityType.EXPERIMENT)
    if (
        not asks_for_runs
        and isinstance(exc, httpx.HTTPStatusError)
        and exc.response.status_code == 504
    ):
        return False
    return is_retryable(exc)


_ELINKS = (
    "bioproject_biosample_all",
    "bioproject_sra_all",
    "bioproject_gds",
    "bioproject_bioproject_u2d",
    "bioproject_bioproject_d2u",
    "biosample_bioproject",
    "biosample_sra",
    "biosample_gds",
    "sra_bioproject",
    "sra_biosample",
    "sra_gds",
    "gds_bioproject",
    "gds_biosample",
    "gds_sra",
)

PROBERS: dict[str, Prober] = {
    "ena_filereport": Prober("ena_portal", _filereport),
    "ena_filereport_aliases": Prober("ena_portal", _aliases),
    "ena_filereport_fastq": Prober("ena_portal", _file_column("fastq", "ena_filereport_fastq")),
    "ena_filereport_submitted": Prober(
        "ena_portal", _file_column("submitted", "ena_filereport_submitted")
    ),
    "ena_filereport_bam": Prober("ena_portal", _file_column("bam", "ena_filereport_bam")),
    "ena_filereport_sra": Prober("ena_portal", _file_column("sra", "ena_filereport_sra")),
    "ena_filereport_analysis": Prober("ena_portal", _analysis),
    "ena_filereport_analysis_files": Prober(
        "ena_portal", _file_column("submitted", "ena_filereport_analysis_files", result="analysis")
    ),
    "ena_search_study": Prober("ena_search", _search_study),
    "ena_search_study_parent": Prober("ena_search", _search_parent),
    "ena_search_sample": Prober("ena_search", _search_sample),
    "ena_links": Prober("ena_portal", _links),
    "ena_xref_source": Prober("ena_search", _xref_source),
    "ena_xref_target": Prober("ena_search", _xref_target),
    "ena_browser_xml": Prober("ena_search", _browser_xml),
    "ena_filereport_document": Prober("ena_portal", _filereport_document),
    "ena_browser_xml_document": Prober("ena_search", _browser_xml_document),
    **{f"elink_{name}": Prober("ncbi_eutils", _elink(name)) for name in _ELINKS},
    "sra_be_runinfo": Prober("ncbi_web", _runinfo),
    "sra_be_runinfo_download": Prober("ncbi_web", _runinfo_download),
    "sra_be_runinfo_document": Prober("ncbi_web", _runinfo_document),
    "esummary_sra": Prober("ncbi_eutils", _esummary_sra),
    "sdl": Prober("ncbi_web", _sdl(), retry=_sdl_retry),
    "sdl_lite": Prober("ncbi_web", _sdl(filetype="sralite"), retry=_sdl_retry),
    "sdl_original": Prober(
        "ncbi_web", _sdl(**{"accept-alternate-locations": "yes"}), retry=_sdl_retry
    ),
    "efetch_sra_xml": Prober("ncbi_eutils", _sra_xml_document),
    "efetch_sra_files": Prober("ncbi_eutils", _sra_xml_files),
    "biosample_record": Prober("ncbi_eutils", _biosample_record),
    "geo_soft_family": Prober("ncbi_web", _soft),
    "geo_soft_family_relations": Prober("ncbi_web", _soft_relations),
    "geo_miniml": Prober("ncbi_web", _miniml),
    "geo_series_matrix": Prober("ncbi_web", _series_matrix),
    "geo_acc_series": Prober("ncbi_web", _acc_series),
    "geo_acc_sample": Prober("ncbi_web", _acc_sample),
    "geo_acc_platform": Prober("ncbi_web", _acc_platform),
    "gds_esummary_series": Prober("ncbi_eutils", _gds_series),
    "gds_esummary_sample": Prober("ncbi_eutils", _gds_sample),
    "gds_esummary_platform": Prober("ncbi_eutils", _gds_platform),
    "biostudies_idf": Prober("biostudies", _idf),
    "biostudies_sdrf": Prober("biostudies", _sdrf),
    "biostudies_sdrf_comments": Prober("biostudies", _sdrf_comments),
    "biostudies_sdrf_fastq": Prober("biostudies", _sdrf_fastq),
    "biostudies_files": Prober("biostudies", _files),
    "biostudies_study": Prober("biostudies", _study_json),
    "biostudies_search": Prober("biostudies", _biostudies_search("arrayexpress", "ae_experiment")),
    "biostudies_search_all": Prober("biostudies", _biostudies_search(None, "biostudy")),
    "biosamples_record": Prober("biosamples", _biosamples_record),
    "biosamples_filter": Prober("biosamples", _biosamples_filter),
    "ddbj_dblink": Prober("ddbj", _dblink),
    "ddbj_entry_sra": Prober("ddbj", _ddbj_entry_sra),
    "ddbj_entry_fastq": Prober("ddbj", _ddbj_entry_fastq),
    "ddbj_entry_document": Prober("ddbj", _ddbj_entry_document),
    "ddbj_dra_xml": Prober("ddbj", _ddbj_dra_xml),
}


# One rate-limited client per bucket: a host, or a service of one whose limits
# are its own. ENA's portal answers filereport at 8 rps and answers a burst of
# `search` with an HTML 500, so the two are separate buckets of one host.
#
# The ceilings: EBI's APIs at 15 rps in total (ena_portal + ena_search +
# biostudies + biosamples), and E-utilities at 10 with a key, which leaves
# headroom for retries. NCBI's other services -- GEO, sra-db-be, SDL -- are not
# E-utilities and are paced on their own.
BUCKETS = (
    "ncbi_eutils",
    "ncbi_web",
    "ena_portal",
    "ena_search",
    "biostudies",
    "biosamples",
    "ddbj",
    "files",
)


def host_limits(has_api_key: bool) -> dict[str, Limits]:
    """Per-bucket limits, well under each archive's ceiling.

    ``files`` is every host a download link points at that is not an archive's
    API -- ENA's and NCBI's file servers, S3 and Google buckets -- asked with a
    HEAD and nothing more.
    """

    def at(rps: float, concurrency: int) -> Limits:
        return Limits(rps=rps, concurrency=concurrency, base_timeout=60.0)

    return {
        "ncbi_eutils": at(8.0 if has_api_key else 2.5, 4),
        "ncbi_web": at(4.0, 4),
        "ena_portal": at(8.0, 6),
        "ena_search": at(2.0, 2),
        "biostudies": at(3.0, 3),
        "biosamples": at(2.0, 2),
        "ddbj": at(1.5, 2),
        "files": at(4.0, 4),
    }


def host_of(url: str) -> str:
    """Which bucket a download link's HEAD is made under: always ``files``.

    A HEAD on a file server is not an API request, and pacing it under the API
    it was listed by would let a run of fastq checks starve the survey of the
    API it is measuring.
    """
    del url
    return "files"


# ------------------------------------------------------------------- checks

# A 10x barcode read is the cell barcode and the UMI: 16 + 10 bases on v2,
# 16 + 12 on v3. An i7 index read is 8 or 10. So a run whose spots carry two
# reads of at least 26 bases carries the barcode read and the cDNA read; one
# carrying a single such read has lost one of them.
BARCODE_READ_MIN = 26

# The sequencer's own names, which a submitter's files keep and the archives'
# `_1`/`_2` convention does not use: TSF2_S35_L002_R1_001.fastq.gz.
_ILLUMINA_READ = re.compile(r"_([RI])([12])(?:_\d{3})?\.f(?:ast)?q", re.IGNORECASE)


def read_of(name: str) -> int | None:
    """Which read of a spot a fastq is: ``mate_of``'s answer, or the sequencer's name.

    ``files.mate_of`` reads the archives' `_1`/`_2` and deliberately nothing
    else, because ``links.tsv`` reproduces the incumbent's reading. A
    submitter's file keeps the sequencer's ``_R1_001``, which is the same read
    by another name; the 10x check reads both rather than call it unpaired.
    """
    bare = re.sub(r"\.\d+$", "", name.rsplit("/", 1)[-1])
    if (mate := mate_of(bare)) is not None:
        return mate
    match = _ILLUMINA_READ.search(bare)
    if match is None:
        return None
    kind, number = match.group(1).upper(), int(match.group(2))
    return number if kind == "R" else (0 if number == 1 else -1)


def _complete_fastq(files: list[FileRecord]) -> bool:
    """Both reads of the pair, the barcode read and the cDNA read, are files of the set.

    This is ``Candidate.is_paired`` with the sequencer's names read too, and it
    subsumes ``demonstrably_incomplete``: a library the archive declares paired
    and publishes one file for -- the set ENA dumps without the barcode read --
    has one read, not two.
    """
    return {1, 2} <= {read_of(f.name or f.url) for f in files}


# Given the runs a link found files for, sorted, which one's set to check.
RunPicker = Callable[[list[str]], str]


def prefer_runs(*preferred: str) -> RunPicker:
    """A probe's picker: the first of ``preferred`` that is there, else the first run.

    Right for a probe, which checks one fixed accession's family on purpose.
    Wrong for a survey: "the run that sorts first" is the oldest submission of
    every BioProject, and a check made only on oldest runs measures the oldest.
    """

    def pick(runs: list[str]) -> str:
        for run in preferred:
            if run in runs:
                return run
        return runs[0]

    return pick


def seeded_runs(seed: int, accession: str) -> RunPicker:
    """A survey's picker: the accession's own run if it is one, else a seeded choice.

    Seeded per accession (``Random(f"{seed}:{accession}")``), so a resumed or
    repeated survey checks the same run, and two accessions' choices do not
    depend on the order they were visited in.
    """
    rng = random.Random(f"{seed}:{accession}")

    def pick(runs: list[str]) -> str:
        return accession if accession in runs else rng.choice(runs)

    return pick


def _set_of(records: list[FileRecord], pick_run: RunPicker) -> tuple[str, list[FileRecord]]:
    """One run's set out of everything found, the run chosen by ``pick_run``."""
    by_run: dict[str, list[FileRecord]] = {}
    for record in records:
        by_run.setdefault(record.run, []).append(record)
    run = pick_run(sorted(by_run))
    return run, by_run[run]


async def _downloadable(files: list[FileRecord], clients: Mapping[str, SurveyClient]) -> bool:
    """Every file answers a HEAD with 2xx, serving the size the archive published."""
    if not files or any(not f.url for f in files):
        return False
    for record in files:
        url = https(record.url)
        client = clients.get(host_of(url)) or next(iter(clients.values()))
        verdict = await verify(replace(record, url=url), client)
        if not verdict.resolves or verdict.size_matches is False:
            return False
    return True


async def check_reads(
    node: Node,
    records: list[FileRecord],
    clients: Mapping[str, SurveyClient],
    pick_run: RunPicker,
) -> tuple[tuple[Check, ...], str]:
    """Downloadable, verifiable and complete for 10x, on one run's set of ``node``'s format."""
    run, files = _set_of(records, pick_run)
    data = [f for f in files if f.is_data]
    downloadable = await _downloadable(data, clients)
    verifiable = bool(data) and all(f.md5 and f.size for f in data)
    complete: bool | None
    if node.id == "fastq":
        complete = _complete_fastq(data)
    elif node.id in ("bam", "cram"):
        # One file holds every read of a spot; the barcode is in its tags.
        complete = len(data) == 1
    else:
        complete = None
        if "ncbi_eutils" in clients and run:
            try:
                lengths = await read_lengths(clients["ncbi_eutils"], run, 60.0)
            except Exception:
                lengths = None
            if lengths is not None:
                complete = sum(length >= BARCODE_READ_MIN for length in lengths) >= 2
    checks = (("downloadable", downloadable), ("verifiable", verifiable), ("complete", complete))
    return checks, f"{run or 'the set'}: {len(data)} file{'s' if len(data) != 1 else ''}"


async def check_supplementary(
    records: list[FileRecord], clients: Mapping[str, SurveyClient]
) -> tuple[tuple[Check, ...], str]:
    """Downloadable, and a size known before downloading that the server agrees with."""
    record = sorted(records, key=lambda r: r.url)[0]
    url = https(record.url)
    client = clients.get(host_of(url)) or next(iter(clients.values()))
    verdict = await verify(replace(record, url=url), client)
    checkable = verdict.served_bytes is not None and verdict.size_matches is not False
    return (("downloadable", verdict.resolves), ("size-checkable", checkable)), record.name


@dataclass(frozen=True, slots=True)
class Finding:
    """What one request said about one link from one accession, in full.

    A probe and a link survey share this. A probe keeps a sample of ``values``
    (:meth:`probe`); a survey records every one, because the distinct count is
    the measurement.
    """

    link: str
    accession: str
    outcome: Outcome
    values: tuple[str, ...] = ()
    error: str | None = None
    checks: tuple[Check, ...] = ()
    checked: str | None = None
    # Why an EMPTY is empty, where the reason is known: the record is absent.
    note: str | None = None
    # The file records behind ``values``, for a link into a read format: what a
    # content survey reads. Never written to probes.json or an export.
    files: tuple[FileRecord, ...] = ()

    def probe(self, checked_on: date) -> Probe:
        return Probe(
            link=self.link,
            accession=self.accession,
            checked_on=checked_on,
            outcome=self.outcome,
            found=self.values[:PROBE_KEEP],
            found_count=len(self.values),
            error=self.error,
            checks=self.checks,
            checked=self.checked,
        )


async def answer_links(
    mechanism: str,
    accession: str,
    members: Iterable[Link],
    clients: Mapping[str, SurveyClient],
    *,
    catalogue: Catalogue = CATALOGUE,
    pick_run: RunPicker | None = None,
    retry: Callable[[BaseException], bool] | None = None,
) -> list[Finding]:
    """Make ``mechanism``'s request for ``accession`` and say what it found for each link.

    One request answers every link in ``members``. A request that fails after
    its retries fails every one of them, with the redacted error recorded:
    "the request did not complete" must stay distinguishable from "the field
    was empty".

    Args:
        pick_run: which run's file set to check where a link finds several
            runs' files. A probe prefers its own accession and the 10x family;
            a survey passes :func:`seeded_runs`.
        retry: whether a failure is worth another attempt, where it is narrower
            than the client's own rule -- a request known never to succeed
            should not spend the retry budget proving it again.
    """
    links = list(members)
    prober = PROBERS[mechanism]
    client = clients[prober.host]
    picker = pick_run or prefer_runs(accession, *TEN_X_RUNS)
    if retry is None and prober.retry is not None:
        rule = prober.retry

        def retry(exc: BaseException) -> bool:
            return rule(accession, exc)

    try:
        raw = await client.with_retry(
            lambda timeout: prober.fetch(client, accession, timeout),
            label=f"{mechanism} {accession}",
            **({"retryable": retry} if retry is not None else {}),
        )
    except RecordAbsent as exc:
        note = redact(str(exc))[:300]
        return [Finding(link.id, accession, Outcome.EMPTY, note=note) for link in links]
    except Exception as exc:
        error = redact_exception(exc)
        message = redact(f"{type(error).__name__}: {error}")[:300]
        return [Finding(link.id, accession, Outcome.FAILED, error=message) for link in links]
    answer = raw if isinstance(raw, Answer) else Answer(raw)
    findings = []
    for link in links:
        node = catalogue.nodes[link.target]
        values = normalise(node, lookup(answer.values, node))
        if link.source == link.target:
            # A self link names a different record of the same kind -- a
            # SubSeries, a related experiment. E-MTAB-5061's own JSON links
            # E-MTAB-5061 (its Expression Atlas page), which is no link.
            values = [value for value in values if value != accession]
        checks: tuple[Check, ...] = ()
        checked = None
        if values:
            checks, checked = await _check(node, values, answer, picker, clients)
        wanted = set(values)
        findings.append(
            Finding(
                link=link.id,
                accession=accession,
                outcome=Outcome.RESOLVED if values else Outcome.EMPTY,
                values=tuple(values),
                checks=checks,
                checked=checked,
                files=tuple(r for r in answer.files if (r.url or r.name) in wanted),
            )
        )
    return findings


async def probe_links(
    links: Iterable[Link],
    clients: dict[str, SurveyClient],
    *,
    catalogue: Catalogue = CATALOGUE,
) -> list[Probe]:
    """Probe ``links``, one request per mechanism and accession.

    :func:`answer_links` truncated to :data:`PROBE_KEEP`: a probe shows what
    the field holds, it does not copy the archive.
    """
    groups: dict[tuple[str, str], list[Link]] = {}
    for link in links:
        groups.setdefault((link.mechanism, catalogue.probe_accession(link)), []).append(link)
    checked_on = datetime.now(UTC).date()

    async def one(mechanism: str, accession: str, members: list[Link]) -> list[Probe]:
        findings = await answer_links(mechanism, accession, members, clients, catalogue=catalogue)
        return [finding.probe(checked_on) for finding in findings]

    results = await asyncio.gather(
        *(one(mechanism, accession, members) for (mechanism, accession), members in groups.items())
    )
    return [probe for batch in results for probe in batch]


async def _check(
    node: Node,
    values: list[str],
    answer: Answer,
    pick_run: RunPicker,
    clients: Mapping[str, SurveyClient],
) -> tuple[tuple[Check, ...], str | None]:
    """The checks a found file or document must pass; none for an accession."""
    if node.kind is NodeKind.METADATA:
        document = answer.documents.get(node.id)
        if document is None:
            return (("downloads", True), ("parses", None)), None
        return (("downloads", True), ("parses", document.parses)), document.url
    wanted = set(values)
    records = [r for r in answer.files if (r.url or r.name) in wanted]
    if node.kind is NodeKind.READ_FORMAT:
        if not records:
            return (("downloadable", None), ("verifiable", None), ("complete", None)), None
        try:
            return await check_reads(node, records, clients, pick_run)
        except Exception as exc:
            error = redact_exception(exc)
            return (("downloadable", None), ("verifiable", None), ("complete", None)), redact(
                f"check failed: {type(error).__name__}"
            )
    if node.kind is NodeKind.SUPPLEMENTARY:
        if not records:
            return (("downloadable", None), ("size-checkable", None)), None
        try:
            return await check_supplementary(records, clients)
        except Exception as exc:
            error = redact_exception(exc)
            return (("downloadable", None), ("size-checkable", None)), redact(
                f"check failed: {type(error).__name__}"
            )
    return (), None


__all__ = [
    "BUCKETS",
    "PROBERS",
    "Answer",
    "Document",
    "Finding",
    "Prober",
    "RecordAbsent",
    "RunPicker",
    "answer_links",
    "host_limits",
    "normalise",
    "prefer_runs",
    "probe_links",
    "read_of",
    "seeded_runs",
]
