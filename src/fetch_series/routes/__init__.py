"""Route implementations: one async function per declared edge in the graph.

Each function takes an accession, a client and this attempt's deadline, and
returns the target accessions it found. Returning an empty list means the
archive holds no such link -- a real answer. Raising means the route failed to
get an answer at all. Keeping those apart is the whole point of
:class:`~fetch_series.results.Outcome`.

``IMPLEMENTATIONS`` maps a route id from :mod:`fetch_series.graph` to the
function that answers it, so the benchmark harness and the resolver dispatch
through one table and cannot drift.
"""

from __future__ import annotations

from typing import Any

from fetch_series.files import FileRecord, classify, from_ena_row, from_sdl_files, mate_of
from fetch_series.providers import biostudies, ena_portal, geo, sdl, sra_be
from fetch_series.providers.eutils import (
    efetch_text,
    elink_uids,
    epost_history,
    esearch,
    esearch_all_uids,
    esummary_by_ids,
)
from fetch_series.survey.client import SurveyClient
from fetch_series.survey.runner import RouteFn

# --------------------------------------------------------------------------
# GEO series -> BioProject
# --------------------------------------------------------------------------


async def gse_to_bioproject_soft(series: str, client: SurveyClient, timeout: float) -> list[str]:
    """Read ``!Series_relation = BioProject:`` out of the SOFT family file.

    This is what fetch10xmeta does. It is the submitter's own record, so it is
    the most direct statement of the link that exists -- but a SuperSeries
    carries no BioProject of its own, only its subseries do.
    """
    text = await geo.fetch_soft_family(client, series, timeout)
    return sorted(set(geo.parse_soft_family(series, text).bioprojects))


async def gse_to_bioproject_gds(series: str, client: SurveyClient, timeout: float) -> list[str]:
    """Take the ``bioproject`` field of the ``db=gds`` ESummary record."""
    record = await geo.gds_summary(client, series, timeout)
    value = str(record.get("bioproject") or "").strip()
    if not value:
        return []
    # The field holds a URL in some records and a bare accession in others.
    found = geo._ACCESSION_IN_URL.search(value)
    return [found.group(1)] if found else []


# --------------------------------------------------------------------------
# GEO series -> GEO samples
# --------------------------------------------------------------------------


async def gse_to_gsm_soft(series: str, client: SurveyClient, timeout: float) -> list[str]:
    text = await geo.fetch_soft_family(client, series, timeout)
    return sorted(set(geo.parse_soft_family(series, text).samples))


async def gse_to_gsm_gds(series: str, client: SurveyClient, timeout: float) -> list[str]:
    """Take the ``Samples`` array of the ``db=gds`` ESummary record."""
    record = await geo.gds_summary(client, series, timeout)
    samples = record.get("samples") or []
    if not isinstance(samples, list):
        return []
    return sorted(
        {
            str(entry.get("accession", "")).strip()
            for entry in samples
            if isinstance(entry, dict) and entry.get("accession")
        }
    )


# --------------------------------------------------------------------------
# GEO series -> INSDC experiments
# --------------------------------------------------------------------------


async def gse_to_experiment_soft(series: str, client: SurveyClient, timeout: float) -> list[str]:
    """Collect ``!Sample_relation = SRA:`` across every sample in the SOFT file."""
    text = await geo.fetch_soft_family(client, series, timeout)
    family = geo.parse_soft_family(series, text)
    return sorted(
        {
            relations["experiment"]
            for relations in family.sample_relations.values()
            if "experiment" in relations
        }
    )


async def gse_to_experiment_elink(series: str, client: SurveyClient, timeout: float) -> list[str]:
    """Walk ``gds -> sra`` with ELink, then read the experiment accessions.

    Uses ``cmd=neighbor`` rather than a history, so a response with no usable
    ``querykey`` is not mistaken for an absent link.
    """
    uid = await geo.gds_uid(client, series, timeout)
    if uid is None:
        return []
    sra_uids = await elink_uids(client, dbfrom="gds", db="sra", uids=[uid], timeout=timeout)
    if not sra_uids:
        return []
    summaries = await esummary_by_ids(client, db="sra", uids=sra_uids, timeout=timeout)
    experiments: set[str] = set()
    for record in summaries.values():
        if not isinstance(record, dict):
            continue
        # esummary db=sra buries accessions in an XML blob, so match on shape.
        blob = str(record.get("expxml", ""))
        experiments.update(geo._ACCESSION_IN_URL.findall(blob))
    return sorted(e for e in experiments if e[1:3] in {"RX"})


async def gse_to_experiment_soft_bioproject_ena(
    series: str, client: SurveyClient, timeout: float
) -> list[str]:
    """Take the BioProject from the SOFT file, then ask ENA for its experiments.

    The recovery path for series that ``elink gds->sra`` cannot reach. Measured
    on 85 such series, it returned every experiment the SOFT file names for 79
    of them (93%) with no partial recoveries; the remaining 6 record no
    BioProject in SOFT at all, four of them because they sit under a shared
    umbrella project (PRJNA30709) rather than one of their own.

    Its value is that it crosses archives: the GEO half comes from NCBI's FTP
    mirror and the experiment half from EBI, so it shares no failure mode with
    either NCBI Entrez route.
    """
    text = await geo.fetch_soft_family(client, series, timeout)
    bioprojects = sorted(set(geo.parse_soft_family(series, text).bioprojects))
    if not bioprojects:
        return []
    found: set[str] = set()
    for bioproject in bioprojects:
        rows = await ena_portal.read_run_report(client, bioproject, timeout)
        found.update(ena_portal.column(rows, "experiment_accession"))
    return sorted(found)


async def gse_to_run_elink(series: str, client: SurveyClient, timeout: float) -> list[str]:
    """``gds -> sra`` by ELink, then EFetch runinfo for the run accessions."""
    uid = await geo.gds_uid(client, series, timeout)
    if uid is None:
        return []
    sra_uids = await elink_uids(client, dbfrom="gds", db="sra", uids=[uid], timeout=timeout)
    if not sra_uids:
        return []
    text = await efetch_text(client, db="sra", uids=sra_uids, timeout=timeout)
    return _runs_from_runinfo(text)


def _runs_from_runinfo(text: str) -> list[str]:
    """Pull the Run column out of an SRA runinfo CSV."""
    return sorted(sra_be.column(sra_be.parse_runinfo(text), "Run"))


# --------------------------------------------------------------------------
# GEO sample as an entry point
#
# Until these existed a GSM was a dead end: the graph could reach one from a
# series but not leave it, so half the GEO entry points resolved to nothing.
# --------------------------------------------------------------------------


async def gsm_to_experiment(sample: str, client: SurveyClient, timeout: float) -> list[str]:
    """Read !Sample_relation = SRA: from GEO's record for the sample."""
    record = geo.parse_sample_record(sample, await geo.fetch_sample_record(client, sample, timeout))
    return [record.experiment] if record.experiment else []


async def gsm_to_biosample(sample: str, client: SurveyClient, timeout: float) -> list[str]:
    """Read !Sample_relation = BioSample: from GEO's record for the sample."""
    record = geo.parse_sample_record(sample, await geo.fetch_sample_record(client, sample, timeout))
    return [record.biosample] if record.biosample else []


async def gsm_to_series(sample: str, client: SurveyClient, timeout: float) -> list[str]:
    """The series a sample belongs to -- there can be more than one."""
    record = geo.parse_sample_record(sample, await geo.fetch_sample_record(client, sample, timeout))
    return record.series


# --------------------------------------------------------------------------
# ENA portal: one endpoint serves much of the graph
# --------------------------------------------------------------------------


async def _bioproject_sra_history(
    accession: str, client: SurveyClient, timeout: float, *, via_elink: bool
) -> tuple[str, str] | None:
    """Get an ESearch history over the SRA records of a BioProject.

    Two ways in, and the survey says they differ. ``via_elink`` searches
    db=bioproject and walks the link; the direct form searches db=sra for
    ``[GPRJ]``, which is one request cheaper but unreliable for renamed or
    merged projects -- the term falls through to ``[All Fields]``.
    """
    if not via_elink:
        result = await esearch(client, db="sra", term=f"{accession}[GPRJ]", timeout=timeout)
        if result.get("count") == "0":
            return None
        return result["webenv"], result["querykey"]

    project = await esearch(client, db="bioproject", term=f"{accession}[PRJA]", timeout=timeout)
    uids = project.get("idlist") or []
    if not uids:
        return None
    sra_uids = await elink_uids(client, dbfrom="bioproject", db="sra", uids=uids, timeout=timeout)
    if not sra_uids:
        return None
    # Re-establish a history over the linked UIDs so the backend can be driven
    # from it; ELink's own history is the one that sometimes has no querykey.
    #
    # By EPost, not by joining the UIDs into an ESearch term. A project with
    # hundreds of SRA records would make that term many kilobytes of GET URL and
    # earn a non-retryable 414 -- failing on precisely the large projects worth
    # not failing on, which is exactly what the same mistake cost at ESummary.
    return await epost_history(client, db="sra", uids=sra_uids, timeout=timeout)


def _bioproject_run_route(*, backend: str, via_elink: bool) -> RouteFn:
    async def route(accession: str, client: SurveyClient, timeout: float) -> list[str]:
        history = await _bioproject_sra_history(accession, client, timeout, via_elink=via_elink)
        if history is None:
            return []
        webenv, query_key = history
        if backend == "sra_be":
            rows = await sra_be.runinfo_by_history(client, webenv, query_key, timeout)
            return sra_be.column(rows, "Run")
        text = await efetch_text(
            client, db="sra", webenv=webenv, query_key=query_key, timeout=timeout
        )
        return _runs_from_runinfo(text)

    return route


async def bioproject_to_geo_gds_direct(
    accession: str, client: SurveyClient, timeout: float
) -> list[str]:
    """Search db=gds for the BioProject accession directly.

    Paged. ESearch returns 20 UIDs when asked for no particular number, so a
    project with more GDS records than that would have returned a truncated
    set -- as a successful resolution, which the resolver then stops on,
    never trying the ELink route that would have found the rest.
    """
    uids = await esearch_all_uids(client, db="gds", term=f"{accession}[ALL]", timeout=timeout)
    if not uids:
        return []
    summaries = await esummary_by_ids(client, db="gds", uids=uids, timeout=timeout)
    return _geo_series_from_summaries(summaries)


async def bioproject_to_geo_elink(
    accession: str, client: SurveyClient, timeout: float
) -> list[str]:
    """Walk bioproject -> gds with ELink, using cmd=neighbor."""
    project = await esearch(client, db="bioproject", term=f"{accession}[PRJA]", timeout=timeout)
    uids = project.get("idlist") or []
    if not uids:
        return []
    gds_uids = await elink_uids(client, dbfrom="bioproject", db="gds", uids=uids, timeout=timeout)
    if not gds_uids:
        return []
    summaries = await esummary_by_ids(client, db="gds", uids=gds_uids, timeout=timeout)
    return _geo_series_from_summaries(summaries)


def _geo_series_from_summaries(summaries: dict[str, Any]) -> list[str]:
    """Series accessions from gds ESummary records.

    A gds record can be a GSE or a GSM; only series are wanted here.
    """
    found = set()
    for record in summaries.values():
        if not isinstance(record, dict):
            continue
        accession = str(record.get("accession", "")).strip()
        if accession.startswith("GSE"):
            found.add(accession)
    return sorted(found)


async def bioproject_to_biosample_elink(
    accession: str, client: SurveyClient, timeout: float
) -> list[str]:
    """Walk bioproject -> biosample with ELink, paging the summaries.

    The survey script this replaces did not page its ESummary call, so any
    project with more than 500 BioSamples hit the UID ceiling and was recorded
    as a failure. That accounts for its 330 failures, ten times any SRA route.
    """
    project = await esearch(client, db="bioproject", term=f"{accession}[PRJA]", timeout=timeout)
    uids = project.get("idlist") or []
    if not uids:
        return []
    sample_uids = await elink_uids(
        client, dbfrom="bioproject", db="biosample", uids=uids, timeout=timeout
    )
    if not sample_uids:
        return []
    summaries = await esummary_by_ids(client, db="biosample", uids=sample_uids, timeout=timeout)
    return sorted(
        {
            str(record["accession"]).strip()
            for record in summaries.values()
            if isinstance(record, dict) and record.get("accession")
        }
    )


def _ena_route(field: str) -> RouteFn:
    async def route(accession: str, client: SurveyClient, timeout: float) -> list[str]:
        rows = await ena_portal.read_run_report(client, accession, timeout)
        return ena_portal.column(rows, field)

    return route


# --------------------------------------------------------------------------
# ArrayExpress
# --------------------------------------------------------------------------


async def ae_to_study_idf(accession: str, client: SurveyClient, timeout: float) -> list[str]:
    return await biostudies.idf_secondary_accessions(client, accession, timeout)


async def ae_to_biosample_sdrf(accession: str, client: SurveyClient, timeout: float) -> list[str]:
    rows = await biostudies.sdrf_rows(client, accession, timeout)
    return biostudies.sdrf_column(rows, "Comment[BioSD_SAMPLE]")


async def study_to_ae_experiment(accession: str, client: SurveyClient, timeout: float) -> list[str]:
    """The ArrayExpress experiment that declares this study as its secondary.

    BioStudies has no field query for secondary accessions, so this is a
    free-text search of the ArrayExpress collection. The search index covers the
    IDF, so a study named there is found -- but free text also matches a study
    accession that merely appears in a description, so results are filtered to
    ArrayExpress accessions and the caller should treat more than one hit as a
    fact about the archive rather than a bug here.
    """
    hits = await biostudies.search(client, timeout, query=accession)
    return sorted({hit for hit in hits if hit.startswith("E-")})


# --------------------------------------------------------------------------
# The file layer
#
# A file route answers with URLs rather than accessions. That is deliberate:
# the survey harness compares routes by what they return, and comparing the URL
# sets two providers offer for the same run is exactly the question the file
# layer exists to ask. The typed FileRecord view lives alongside, for callers
# that need the checksum and not just the link.
# --------------------------------------------------------------------------

# ENA packs a run's files into parallel `;`-joined columns, one family per
# prefix. `fastq_*` are ENA's own derivations, `submitted_*` are the
# submitter's original deposit, `sra_*` the NCBI-format archive object.
ENA_FILE_FIELDS = (
    "run_accession",
    # Not a file field, but the one that says whether the file set is complete:
    # ENA declares ERP129702 PAIRED and publishes one fastq per run.
    "library_layout",
    "fastq_ftp",
    "fastq_md5",
    "fastq_bytes",
    "submitted_ftp",
    "submitted_md5",
    "submitted_bytes",
    "submitted_format",
    "sra_ftp",
    "sra_md5",
    "sra_bytes",
)


ENA_FILE_COLUMNS = ("fastq", "submitted", "sra")


async def ena_file_records(
    accession: str,
    client: SurveyClient,
    timeout: float,
    column: str | None = None,
) -> list[FileRecord]:
    """Typed file records from ENA's file columns.

    All three families come from one row, so ``column=None`` returns records for
    all of them at the cost of a single request. Asking for each in turn issued
    three identical filereport calls per run and discarded two thirds of each
    response -- which at file-layer scale is three times the work and three
    times the load on EBI for the same answer.
    """
    rows = await ena_portal.read_run_report(client, accession, timeout, fields=ENA_FILE_FIELDS)
    wanted = (column,) if column is not None else ENA_FILE_COLUMNS
    records: list[FileRecord] = []
    for row in rows:
        for name in wanted:
            records.extend(from_ena_row(row, name, f"run->file:ena_{name}"))
    return records


def _ena_file_route(column: str) -> RouteFn:
    async def route(accession: str, client: SurveyClient, timeout: float) -> list[str]:
        records = await ena_file_records(accession, client, timeout, column)
        return sorted({record.url for record in records})

    return route


async def sdl_file_records(
    accession: str, client: SurveyClient, timeout: float
) -> list[FileRecord]:
    """Typed file records from NCBI's Storage Data Locator.

    SDL batches, but a route is called per accession, so this asks for one
    bundle. Batching belongs to the caller that has many runs in hand.
    """
    listings = await sdl.retrieve(client, [accession], timeout)
    return from_sdl_files(accession, listings.get(accession, []), "run->file:sdl")


async def run_to_file_sdl(accession: str, client: SurveyClient, timeout: float) -> list[str]:
    records = await sdl_file_records(accession, client, timeout)
    return sorted({record.url for record in records})


async def ae_file_records(accession: str, client: SurveyClient, timeout: float) -> list[FileRecord]:
    """Submitter fastq files an ArrayExpress SDRF names, guarded by registration.

    The guard is the whole point, and it is the one piece of file-layer
    knowledge the incumbent bash module already had: a SDRF URI under the
    decommissioned pre-BioStudies mirror is believed only when the study still
    registers a file of that name. E-MTAB-8060 and E-MTAB-9221 are
    indistinguishable without it.

    A failure to fetch the file list yields None, not an empty set, so the URIs
    are kept as written. An unreachable API must not be able to reroute a whole
    study to its BAMs.
    """
    rows = await biostudies.sdrf_raw_rows(client, accession, timeout)
    registered: set[str] | None
    try:
        registered = biostudies.registered_names(
            await biostudies.registered_files(client, accession, timeout)
        )
    except Exception:
        registered = None

    records: list[FileRecord] = []
    for run, uris in biostudies.sdrf_fastq_uris(rows, accession, registered).items():
        for uri in uris:
            name = uri.rsplit("/", 1)[-1]
            records.append(
                FileRecord(
                    run=run,
                    url=uri,
                    name=name,
                    kind=classify(name),
                    source="ae_experiment->file:sdrf",
                    mate=mate_of(name),
                )
            )
    return records


async def ae_to_file_sdrf(accession: str, client: SurveyClient, timeout: float) -> list[str]:
    records = await ae_file_records(accession, client, timeout)
    return sorted({record.url for record in records})


IMPLEMENTATIONS: dict[str, RouteFn] = {
    "gse->bioproject:soft_family": gse_to_bioproject_soft,
    "gse->bioproject:gds_summary": gse_to_bioproject_gds,
    "gse->geo_sample:soft_family": gse_to_gsm_soft,
    "geo_sample->experiment:acc_cgi": gsm_to_experiment,
    "geo_sample->biosample:acc_cgi": gsm_to_biosample,
    "geo_sample->geo_series:acc_cgi": gsm_to_series,
    "gse->geo_sample:gds_summary": gse_to_gsm_gds,
    "gse->experiment:soft_family": gse_to_experiment_soft,
    "gse->experiment:elink_gds_sra": gse_to_experiment_elink,
    "gse->experiment:soft_bioproject_ena": gse_to_experiment_soft_bioproject_ena,
    "gse->run:elink_gds_sra": gse_to_run_elink,
    "bioproject->run:ena_filereport": _ena_route("run_accession"),
    "bioproject->run:sra_be_direct_cgi": _bioproject_run_route(backend="sra_be", via_elink=False),
    "bioproject->run:sra_be_elink": _bioproject_run_route(backend="sra_be", via_elink=True),
    "bioproject->run:efetch_direct": _bioproject_run_route(backend="efetch", via_elink=False),
    "bioproject->run:efetch_elink": _bioproject_run_route(backend="efetch", via_elink=True),
    "bioproject->geo_series:gds_direct": bioproject_to_geo_gds_direct,
    "bioproject->geo_series:elink": bioproject_to_geo_elink,
    "bioproject->biosample:elink": bioproject_to_biosample_elink,
    "bioproject->study:ena_filereport": _ena_route("secondary_study_accession"),
    "bioproject->experiment:ena_filereport": _ena_route("experiment_accession"),
    "bioproject->biosample:ena_filereport": _ena_route("sample_accession"),
    "biosample->run:ena_filereport": _ena_route("run_accession"),
    "study->bioproject:ena_filereport": _ena_route("study_accession"),
    "study->run:ena_filereport": _ena_route("run_accession"),
    "experiment->run:ena_filereport": _ena_route("run_accession"),
    "experiment->biosample:ena_filereport": _ena_route("sample_accession"),
    "sample->run:ena_filereport": _ena_route("run_accession"),
    "run->experiment:ena_filereport": _ena_route("experiment_accession"),
    "run->biosample:ena_filereport": _ena_route("sample_accession"),
    "ae_experiment->study:idf_secondary": ae_to_study_idf,
    "ae_experiment->biosample:sdrf": ae_to_biosample_sdrf,
    "study->ae_experiment:biostudies_search": study_to_ae_experiment,
    "run->file:ena_fastq": _ena_file_route("fastq"),
    "run->file:ena_submitted": _ena_file_route("submitted"),
    "run->file:ena_sra": _ena_file_route("sra"),
    "run->file:sdl": run_to_file_sdl,
    "ae_experiment->file:sdrf": ae_to_file_sdrf,
}
