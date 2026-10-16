"""ENA portal API.

The single most complete route measured so far: over 12,756 BioProjects it
returned 784,026 unique runs, including 111,665 that NCBI's ``efetch`` never
does, while only about 1,800 runs were missing from it.

It also accepts almost any accession as the ``accession`` parameter -- project,
study, sample, experiment, run -- which makes one endpoint serve a large part of
the graph.
"""

from __future__ import annotations

import csv
import io
import re

# Parsed with the standard library. ENA is a trusted source and its record XML
# declares no entities, but ElementTree expands internal ones, so this is not
# the parser to point at an arbitrary URL.
import xml.etree.ElementTree as ElementTree
from typing import Any

from fetch_series.survey.client import MalformedResponseError, SurveyClient

FILEREPORT = "https://www.ebi.ac.uk/ena/portal/api/filereport"
LINKS = "https://www.ebi.ac.uk/ena/portal/api/links"
SEARCH = "https://www.ebi.ac.uk/ena/portal/api/search"
XREF = "https://www.ebi.ac.uk/ena/xref/rest/json/search"
BROWSER_XML = "https://www.ebi.ac.uk/ena/browser/api/xml"

# The identity columns. File columns (fastq_ftp, submitted_ftp, sra_ftp,
# fastq_md5) belong to the file layer and are deliberately not requested here.
IDENTITY_FIELDS = (
    "study_accession",
    "secondary_study_accession",
    "sample_accession",
    "secondary_sample_accession",
    "experiment_accession",
    "run_accession",
    "tax_id",
    "scientific_name",
    "library_strategy",
    "library_source",
)


# The descriptive columns, kept apart from the identity ones for the same
# reason the file columns are: a caller resolving relations should not pay for
# metadata it is not going to read, and `read_run_report` takes the field list
# as a parameter precisely so the two can be asked for separately or together.
#
# ENA *defines* tissue_type, cell_type, disease, sex and dev_stage, and
# populates them less often than the schema suggests: PRJNA434002 fills
# tissue_type and sex, leaves disease empty for every run, and states the
# disease in study_title instead -- where it applies to the cases and not to
# the controls. So these are a source, not the source.
METADATA_FIELDS = (
    "run_accession",
    "sample_accession",
    "secondary_sample_accession",
    "tax_id",
    "scientific_name",
    # Structured descriptive columns.
    "tissue_type",
    "cell_type",
    "cell_line",
    "dev_stage",
    "disease",
    "sex",
    "age",
    "strain",
    "sub_species",
    "isolation_source",
    "host",
    "host_sex",
    "host_status",
    "host_body_site",
    # Free text, for the rule layer and for stage 2's digest. `sample_title`
    # and `sample_description` are per-sample; `study_title` is per-study and
    # must never assign a per-sample field.
    "sample_title",
    "sample_description",
    "sample_alias",
    "description",
    "study_title",
    "experiment_title",
    "library_construction_protocol",
)

# Which of the above describe the study rather than the sample. A value from one
# of these cannot assign a sample-scoped field, however confidently it reads.
STUDY_SCOPED_FIELDS = frozenset(
    {"study_title", "experiment_title", "library_construction_protocol"}
)


# ENA carries the submitter's own names for a run, experiment and sample, and
# for a GEO-brokered submission those names are usually the GEO accession
# itself. That makes them a route from a run to its GSM that needs neither the
# SOFT family file nor a BioSample join -- and it works on GSE135325 and
# GSE137444, the two series where GEO records no SRA relation at all.
#
# `sample_alias` first: it is the GSM exactly in most cases, where the other two
# carry a `_r1` suffix. Anchored, and required to end at a non-alphanumeric, so
# a submitter name that merely starts with those letters cannot be mistaken for
# an accession. An alias is free text -- SRR27190799's is `E12` and
# DRR188691's is `SAMD00142600` -- so this route is expected to return nothing
# often, and must never guess.
ALIAS_FIELDS = ("sample_alias", "experiment_alias", "run_alias")
GSM_IN_ALIAS = re.compile(r"^(GSM\d+)(?![A-Za-z0-9])")
AE_IN_ALIAS = re.compile(r"^(E-[A-Z]{4}-\d+)(?![A-Za-z0-9])")


def alias_accessions(rows: list[dict[str, Any]], pattern: re.Pattern[str]) -> list[str]:
    """Accessions the submitter's own aliases carry, where they carry one."""
    found: set[str] = set()
    for row in rows:
        for field in ALIAS_FIELDS:
            if match := pattern.match((row.get(field) or "").strip()):
                found.add(match.group(1))
                break
    return sorted(found)


async def read_run_report(
    client: SurveyClient,
    accession: str,
    timeout: float,
    *,
    fields: tuple[str, ...] = IDENTITY_FIELDS,
    result: str = "read_run",
) -> list[dict[str, Any]]:
    """Fetch a filereport as a list of row dicts.

    ``limit=0`` means no limit. Omitting it returns a truncated report without
    saying so.
    """
    response = await client.get(
        FILEREPORT,
        params={
            "accession": accession,
            "result": result,
            "fields": ",".join(fields),
            "format": "tsv",
            "limit": 0,
        },
        timeout=timeout,
    )
    return parse_tsv(response.text)


def parse_tsv(text: str) -> list[dict[str, Any]]:
    """Rows of an ENA TSV answer; an empty body is no rows, not an error."""
    body = text.strip()
    if not body:
        return []
    return list(csv.DictReader(io.StringIO(body), delimiter="\t"))


def column(rows: list[dict[str, Any]], field: str) -> list[str]:
    """Distinct non-empty values of one column, sorted.

    ``csv.DictReader`` fills a short row's missing trailing cells with None, not
    an empty string, and ENA omits trailing empty cells: a sample search whose
    last requested field is empty for every row crashed here before this read
    the cell defensively.
    """
    return sorted({value for row in rows if (value := (row.get(field) or "").strip())})


def listed(rows: list[dict[str, Any]], field: str) -> list[str]:
    """Distinct values of a column that may hold a semicolon-separated list.

    The analysis report lists every run an analysis drew on in one cell --
    ERZ10183054's ``run_accession`` is four runs joined by ``;`` -- and the file
    columns do the same for every file of a run.
    """
    return sorted(
        {part.strip() for row in rows for part in (row.get(field) or "").split(";") if part.strip()}
    )


# --------------------------------------------------------------------------
# Search, links and cross-references
#
# Used by the link catalogue's probes rather than by any route. Each answers a
# link the filereport does not: a GEO series from its BioProject (search on
# geo_accession, or xref), an umbrella project (parent_study_accession), an
# analysis from its project (links).
# --------------------------------------------------------------------------


async def search(
    client: SurveyClient,
    result: str,
    query: str,
    fields: tuple[str, ...],
    timeout: float,
) -> list[dict[str, Any]]:
    """Rows of a portal search, e.g. ``result=study`` with ``geo_accession="GSE60450"``.

    ENA answers a burst of these with an HTML page and status 500 rather than a
    429, so the caller's retry policy is what gets a burst through.
    """
    response = await client.get(
        SEARCH,
        params={
            "result": result,
            "query": query,
            "fields": ",".join(fields),
            "format": "tsv",
            "limit": 0,
        },
        timeout=timeout,
    )
    return parse_tsv(response.text)


async def links(
    client: SurveyClient, data_type: str, accession: str, result: str, timeout: float
) -> list[str]:
    """Accessions of type ``result`` linked to a project (``study``) or ``sample``.

    Only the primary accession works. ``ERP017126`` and ``ERS1348472`` are
    answered with a header and no rows -- an answer indistinguishable from "no
    links" -- where ``PRJEB15401`` and ``SAMEA4437023`` return their records.
    """
    # No `limit`. On this endpoint `limit=0` means zero rows -- a header and
    # nothing else, for PRJEB44468's 106 analyses -- where on filereport and
    # search the same parameter means no limit at all.
    response = await client.get(
        f"{LINKS}/{data_type}",
        params={"accession": accession, "result": result, "format": "tsv"},
        timeout=timeout,
    )
    return column(parse_tsv(response.text), "accession")


async def xref(
    client: SurveyClient,
    timeout: float,
    *,
    accession: str | None = None,
    source: str | None = None,
    source_accession: str | None = None,
) -> list[dict[str, Any]]:
    """Cross-reference records, either side of a link between ENA and elsewhere.

    ``accession=`` takes an ENA record and returns what points at it;
    ``source=`` with ``source_accession=`` takes the external record. The second
    form needs both: ``source_accession=GSE60450`` alone finds nothing.
    """
    response = await client.get(
        XREF,
        params={"accession": accession, "source": source, "source_accession": source_accession},
        timeout=timeout,
    )
    payload = response.json()
    if not isinstance(payload, list):
        raise MalformedResponseError(f"xref answered with {type(payload).__name__}, not a list")
    return [entry for entry in payload if isinstance(entry, dict)]


def xref_sources(entries: list[dict[str, Any]], source: str) -> list[str]:
    """Primary accessions of the external records from ``source`` (GEO, ArrayExpress)."""
    return sorted(
        {
            str(entry.get("Source Primary Accession") or "").strip()
            for entry in entries
            if entry.get("Source") == source and entry.get("Source Primary Accession")
        }
    )


def xref_targets(entries: list[dict[str, Any]], key: str) -> list[str]:
    """ENA accessions an external record points at: ``Target Primary/Secondary Accession``."""
    return sorted({str(entry.get(key) or "").strip() for entry in entries if entry.get(key)})


async def browser_xml(client: SurveyClient, accession: str, timeout: float) -> str:
    """ENA's XML record for one accession."""
    response = await client.get(f"{BROWSER_XML}/{accession}", timeout=timeout)
    return response.text


def browser_cross_references(xml: str) -> dict[str, list[str]]:
    """The external identifiers and links a record's XML carries.

    ``EXTERNAL_ID`` by namespace (``GEO``, ``BioSample``), plus ``URL_LINK``
    targets under ``url``: PRJNA258286 names GSE60450 as an EXTERNAL_ID, and
    PRJEB15401 names E-MTAB-5061 only as a URL to ArrayExpress.
    """
    try:
        root = ElementTree.fromstring(xml)
    except ElementTree.ParseError as exc:
        raise MalformedResponseError(f"ENA browser returned unparseable XML: {exc}") from None
    found: dict[str, set[str]] = {}
    for node in root.iter("EXTERNAL_ID"):
        if (namespace := node.get("namespace")) and node.text and node.text.strip():
            found.setdefault(namespace, set()).add(node.text.strip())
    for node in root.iter("URL_LINK"):
        if (url := node.findtext("URL")) and url.strip():
            found.setdefault("url", set()).add(url.strip())
    return {key: sorted(values) for key, values in found.items()}
