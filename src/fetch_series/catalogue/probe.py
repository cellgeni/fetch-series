"""One live request per untested link, to see whether it answers at all.

Each mechanism gets a fetcher: given one accession, it makes the mechanism's
request and returns what each declared field held, keyed by target node. One
request therefore probes every link a mechanism offers from that accession.

The values are then **normalised by the target node**, not trusted: an
accession-bearing field keeps only tokens that :func:`fetch_series.accession.parse`
places at the target's entity type. ``SampleName`` holds a GSM for a GEO
submission and a submitter's free text otherwise, and only the first is a link.

What a probe establishes is deliberately narrow. It shows the link exists for
one record, on one day. It says nothing about how often the link answers, so it
never becomes ``RouteEvidence`` -- one accession out of one is not a yield.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from fetch_series.accession import RULES, EntityType, try_parse
from fetch_series.catalogue import CATALOGUE, PROBE_KEEP, Catalogue, Link, Node, Probe
from fetch_series.catalogue.mechanisms import DBLINK_TYPES, INSDC_OUTPUTS, RUNINFO_OUTPUTS
from fetch_series.logging_utils import redact
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
from fetch_series.survey.client import Limits, SurveyClient, redact_exception

Found = dict[str, list[str]]
Fetcher = Callable[[SurveyClient, str, float], Awaitable[Found]]

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


def normalise(node: Node, values: Iterable[str]) -> list[str]:
    """What ``values`` say about ``node``, and nothing else, sorted."""
    if node.entity is EntityType.FILE or node.entity is None:
        return sorted({part.strip() for v in values for part in v.split(";") if part.strip()})
    found: set[str] = set()
    for value in values:
        for match in _TOKEN.finditer(value):
            parsed = try_parse(match.group(0))
            if parsed is not None and parsed.entity is node.entity:
                found.add(parsed.value)
    return sorted(found)


def _input_node(accession: str) -> str:
    """The node an accession is an instance of, for fetchers that branch on it."""
    parsed = try_parse(accession)
    node = CATALOGUE.node_of(parsed.entity) if parsed else None
    if node is None:
        raise ValueError(f"{accession} is not an accession of any one node")
    return node.id


def _split_sample(address: str) -> tuple[str, str | None]:
    """``E-MTAB-5061:AZ_A1`` -> (study, Source Name); a bare study has no row."""
    study, _, row = address.partition(":")
    return study, row or None


# --------------------------------------------------------------------- ENA


async def _filereport(client: SurveyClient, accession: str, timeout: float) -> Found:
    rows = await ena_portal.read_run_report(
        client, accession, timeout, fields=tuple(INSDC_OUTPUTS.values())
    )
    return {node: ena_portal.column(rows, field) for node, field in INSDC_OUTPUTS.items()}


async def _aliases(client: SurveyClient, accession: str, timeout: float) -> Found:
    fields = ("study_alias", *ena_portal.ALIAS_FIELDS)
    rows = await ena_portal.read_run_report(client, accession, timeout, fields=fields)
    ae = [
        match.group(1)
        for row in rows
        for field in fields
        if (match := ena_portal.AE_IN_ALIAS.match((row.get(field) or "").strip()))
    ]
    return {
        "geo_series": ena_portal.column(rows, "study_alias"),
        "geo_sample": ena_portal.alias_accessions(rows, ena_portal.GSM_IN_ALIAS),
        "ae_experiment": ae,
    }


def _file_column(field: str) -> Fetcher:
    async def fetch(client: SurveyClient, accession: str, timeout: float) -> Found:
        rows = await ena_portal.read_run_report(client, accession, timeout, fields=(field,))
        return {"ena_files": ena_portal.listed(rows, field)}

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
    fields = (*ANALYSIS_FIELDS.values(), "submitted_ftp", "generated_ftp")
    rows = await ena_portal.read_run_report(
        client, accession, timeout, fields=fields, result="analysis"
    )
    found = {node: ena_portal.listed(rows, field) for node, field in ANALYSIS_FIELDS.items()}
    found["ena_files"] = ena_portal.listed(rows, "submitted_ftp") + ena_portal.listed(
        rows, "generated_ftp"
    )
    return found


_STUDY_QUERY = {
    "bioproject": "study_accession",
    "study": "secondary_study_accession",
    "geo_series": "geo_accession",
}


async def _search_study(client: SurveyClient, accession: str, timeout: float) -> Found:
    field = _STUDY_QUERY[_input_node(accession)]
    rows = await ena_portal.search(
        client,
        "study",
        f'{field}="{accession}"',
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
    field = {"biosample": "sample_accession", "sample": "secondary_sample_accession"}[
        _input_node(accession)
    ]
    rows = await ena_portal.search(
        client,
        "sample",
        f'{field}="{accession}"',
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
    source = _input_node(accession)
    data_type = {"bioproject": "study", "biosample": "sample"}[source]
    results = {"run": "read_run", "experiment": "read_experiment", "analysis": "analysis"}
    if source == "bioproject":
        results["biosample"] = "sample"
    return {
        node: await ena_portal.links(client, data_type, accession, result, timeout)
        for node, result in results.items()
    }


async def _xref_source(client: SurveyClient, accession: str, timeout: float) -> Found:
    source = "GEO" if _input_node(accession) == "geo_series" else "ArrayExpress"
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
        source = _input_node(accession)
        term = _UID_TERMS[(source, dbfrom)].format(acc=accession)
        uids = (await eutils.esearch(client, dbfrom, term, timeout, usehistory=False)).get(
            "idlist"
        ) or []
        if not uids:
            raise ValueError(f"esearch db={dbfrom} found no UID for {accession}")
        linked = await eutils.elink_uids(
            client, dbfrom, db, [str(u) for u in uids], timeout, linkname=linkname
        )
        summaries = await eutils.esummary_by_ids(client, db, linked, timeout) if linked else {}
        values = [
            accession
            for record in summaries.values()
            if isinstance(record, dict)
            for accession in _summary_accessions(db, record)
        ]
        mechanism = CATALOGUE.mechanisms[f"elink_{linkname}"]
        return dict.fromkeys(mechanism.outputs, values)

    return fetch


async def _runinfo(client: SurveyClient, accession: str, timeout: float) -> Found:
    rows = await sra_be.runinfo_by_accession(client, accession, timeout)
    return {node: sra_be.column(rows, column) for node, column in RUNINFO_OUTPUTS.items()}


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
    found: Found = {node: [] for node in _EXPXML}
    for record in summaries.values():
        if isinstance(record, dict):
            accessions = eutils.expxml_accessions(record)
            for node, element in _EXPXML.items():
                found[node].extend(accessions.get(element, []))
    return found


async def _sdl(client: SurveyClient, accession: str, timeout: float) -> Found:
    listings = await sdl.retrieve(client, [accession], timeout)
    return {"sra_files": [link for files in listings.values() for link in sdl.links(files)]}


async def _biosample_record(client: SurveyClient, accession: str, timeout: float) -> Found:
    records = await eutils.fetch_biosamples(client, [accession], timeout)
    links = [link for record in records for link in record.cross_links]
    return {
        "sample": [value for db, value in links if db == "SRA"],
        "geo_sample": [value for db, value in links if db == "GEO"],
        "bioproject": [project for record in records for project in record.bioprojects],
    }


# --------------------------------------------------------------------- GEO


async def _soft(client: SurveyClient, accession: str, timeout: float) -> Found:
    family = geo.parse_soft_family(
        accession, await geo.fetch_soft_family(client, accession, timeout)
    )
    return {
        "geo_sample": family.samples,
        "geo_platform": family.platforms,
        "geo_files": family.supplementary_files,
    }


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


def _relations(record: geo.RecordLinks, *kinds: str) -> list[str]:
    return [value for kind, value in record.relations if kind in kinds]


async def _acc_series(client: SurveyClient, accession: str, timeout: float) -> Found:
    record = geo.parse_record_links(
        accession, await geo.fetch_record(client, accession, timeout, kind="SERIES")
    )
    return {
        "geo_sample": record.samples,
        "geo_platform": record.platforms,
        "bioproject": _relations(record, "BioProject"),
        "study": _relations(record, "SRA"),
        "geo_series": _relations(record, "SuperSeries of", "SubSeries of"),
        "geo_files": record.supplementary_files,
    }


async def _acc_sample(client: SurveyClient, accession: str, timeout: float) -> Found:
    record = geo.parse_record_links(
        accession, await geo.fetch_record(client, accession, timeout, kind="SAMPLE")
    )
    return {
        "experiment": _relations(record, "SRA"),
        "biosample": _relations(record, "BioSample"),
        "geo_series": record.series,
        "geo_platform": record.platforms,
        "geo_files": record.supplementary_files,
    }


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


async def _idf(client: SurveyClient, accession: str, timeout: float) -> Found:
    comments = biostudies.idf_comments(await biostudies.fetch_idf(client, accession, timeout))
    secondary = comments.get("SecondaryAccession", [])
    return {
        "study": secondary,
        "geo_series": secondary,
        "ae_experiment": comments.get("RelatedExperiment", []),
        "run": comments.get("SequenceDataURI", []),
    }


async def _sdrf(client: SurveyClient, address: str, timeout: float) -> Found:
    study, row = _split_sample(address)
    rows = await biostudies.sdrf_raw_rows(client, study, timeout)
    values = biostudies.sdrf_values(
        rows, ("Source Name", "Array Data File", "Derived Array Data File"), source_name=row
    )
    return {
        "ae_sample": [f"{study}:{name}" for name in values["Source Name"]],
        "ae_files": values["Array Data File"] + values["Derived Array Data File"],
    }


_SDRF_COMMENTS = {
    "biosample": "Comment[BioSD_SAMPLE]",
    "sample": "Comment[ENA_SAMPLE]",
    "experiment": "Comment[ENA_EXPERIMENT]",
    "run": "Comment[ENA_RUN]",
    "ae_files": "Comment[FASTQ_URI]",
    "geo_sample": "Source Name",
}


async def _sdrf_comments(client: SurveyClient, address: str, timeout: float) -> Found:
    study, row = _split_sample(address)
    rows = await biostudies.sdrf_raw_rows(client, study, timeout)
    values = biostudies.sdrf_values(rows, tuple(_SDRF_COMMENTS.values()), source_name=row)
    return {node: values[column] for node, column in _SDRF_COMMENTS.items()}


async def _files(client: SurveyClient, accession: str, timeout: float) -> Found:
    files = await biostudies.registered_files(client, accession, timeout)
    return {"ae_files": sorted(str(f.get("path")) for f in files if f.get("path"))}


async def _study_json(client: SurveyClient, accession: str, timeout: float) -> Found:
    urls = [
        url for url, _ in biostudies.study_links(await biostudies.study(client, accession, timeout))
    ]
    return dict.fromkeys(("study", "bioproject", "ae_experiment", "geo_series"), urls)


def _biostudies_search(collection: str | None, node: str) -> Fetcher:
    async def fetch(client: SurveyClient, accession: str, timeout: float) -> Found:
        hits = await biostudies.search(
            client, timeout, query=accession, collection=collection, max_pages=3
        )
        return {node: hits}

    return fetch


async def _biosamples_record(client: SurveyClient, accession: str, timeout: float) -> Found:
    return {
        "sample": biosamples.sra_accessions(await biosamples.sample(client, accession, timeout))
    }


async def _biosamples_filter(client: SurveyClient, accession: str, timeout: float) -> Found:
    hits = await biosamples.by_attribute(client, biosamples.SRA_ACCESSION, accession, timeout)
    return {"biosample": hits}


async def _dblink(client: SurveyClient, accession: str, timeout: float) -> Found:
    linked = await ddbj.dblink(client, DBLINK_TYPES[_input_node(accession)], accession, timeout)
    return {node: linked.get(kind, []) for node, kind in DBLINK_TYPES.items()}


# ---------------------------------------------------------------- registry


@dataclass(frozen=True, slots=True)
class Prober:
    """How to probe one mechanism: which host's limits apply, and the fetch."""

    host: str
    fetch: Fetcher


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
    "ena_filereport": Prober("ebi", _filereport),
    "ena_filereport_aliases": Prober("ebi", _aliases),
    "ena_filereport_fastq": Prober("ebi", _file_column("fastq_ftp")),
    "ena_filereport_submitted": Prober("ebi", _file_column("submitted_ftp")),
    "ena_filereport_sra": Prober("ebi", _file_column("sra_ftp")),
    "ena_filereport_analysis": Prober("ebi", _analysis),
    "ena_search_study": Prober("ebi", _search_study),
    "ena_search_study_parent": Prober("ebi", _search_parent),
    "ena_search_sample": Prober("ebi", _search_sample),
    "ena_links": Prober("ebi", _links),
    "ena_xref_source": Prober("ebi", _xref_source),
    "ena_xref_target": Prober("ebi", _xref_target),
    "ena_browser_xml": Prober("ebi", _browser_xml),
    **{f"elink_{name}": Prober("ncbi", _elink(name)) for name in _ELINKS},
    "sra_be_runinfo": Prober("ncbi", _runinfo),
    "esummary_sra": Prober("ncbi", _esummary_sra),
    "sdl": Prober("ncbi", _sdl),
    "biosample_record": Prober("ncbi", _biosample_record),
    "geo_soft_family": Prober("ncbi", _soft),
    "geo_soft_family_relations": Prober("ncbi", _soft_relations),
    "geo_acc_series": Prober("ncbi", _acc_series),
    "geo_acc_sample": Prober("ncbi", _acc_sample),
    "geo_acc_platform": Prober("ncbi", _acc_platform),
    "gds_esummary_series": Prober("ncbi", _gds_series),
    "gds_esummary_sample": Prober("ncbi", _gds_sample),
    "gds_esummary_platform": Prober("ncbi", _gds_platform),
    "biostudies_idf": Prober("ebi", _idf),
    "biostudies_sdrf": Prober("ebi", _sdrf),
    "biostudies_sdrf_comments": Prober("ebi", _sdrf_comments),
    "biostudies_files": Prober("ebi", _files),
    "biostudies_study": Prober("ebi", _study_json),
    "biostudies_search": Prober("ebi", _biostudies_search("arrayexpress", "ae_experiment")),
    "biostudies_search_all": Prober("ebi", _biostudies_search(None, "biostudy")),
    "biosamples_record": Prober("ebi", _biosamples_record),
    "biosamples_filter": Prober("ebi", _biosamples_filter),
    "ddbj_dblink": Prober("ddbj", _dblink),
}


def host_limits(has_api_key: bool) -> dict[str, Limits]:
    """Per-host limits, well under each archive's ceiling.

    ENA is the one that matters: it answers a burst of portal searches with an
    HTML 500, so EBI runs at two requests a second with retries that back off.
    """
    return {
        "ncbi": Limits(rps=7.0 if has_api_key else 2.5, concurrency=3, base_timeout=60.0),
        "ebi": Limits(rps=2.0, concurrency=2, base_timeout=60.0),
        "ddbj": Limits(rps=1.0, concurrency=1, base_timeout=60.0),
    }


async def probe_links(
    links: Iterable[Link],
    clients: dict[str, SurveyClient],
    *,
    catalogue: Catalogue = CATALOGUE,
) -> list[Probe]:
    """Probe ``links``, one request per mechanism and accession.

    A request that fails after its retries fails every link it would have
    answered, with the redacted error recorded: "the request did not complete"
    must stay distinguishable from "the field was empty".
    """
    groups: dict[tuple[str, str], list[Link]] = {}
    for link in links:
        groups.setdefault((link.mechanism, catalogue.probe_accession(link)), []).append(link)
    checked_on = datetime.now(UTC).date()

    async def one(mechanism: str, accession: str, members: list[Link]) -> list[Probe]:
        prober = PROBERS[mechanism]
        client = clients[prober.host]
        try:
            found = await client.with_retry(
                lambda timeout: prober.fetch(client, accession, timeout),
                label=f"probe {mechanism} {accession}",
            )
        except Exception as exc:
            error = redact_exception(exc)
            message = redact(f"{type(error).__name__}: {error}")[:300]
            return [
                Probe(link.id, accession, checked_on, Outcome.FAILED, error=message)
                for link in members
            ]
        probes = []
        for link in members:
            values = normalise(catalogue.nodes[link.target], found.get(link.target, []))
            if link.source == link.target:
                # A self link names a different record of the same kind -- a
                # SubSeries, a related experiment. E-MTAB-5061's own JSON links
                # E-MTAB-5061 (its Expression Atlas page), which is no link.
                values = [value for value in values if value != accession]
            probes.append(
                Probe(
                    link=link.id,
                    accession=accession,
                    checked_on=checked_on,
                    outcome=Outcome.RESOLVED if values else Outcome.EMPTY,
                    found=tuple(values[:PROBE_KEEP]),
                    found_count=len(values),
                )
            )
        return probes

    results = await asyncio.gather(
        *(one(mechanism, accession, members) for (mechanism, accession), members in groups.items())
    )
    return [probe for batch in results for probe in batch]
