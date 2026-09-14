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

from fetch_series.providers import biostudies, ena_portal, geo
from fetch_series.providers.eutils import efetch_text, elink_uids, esummary_by_ids
from fetch_series.survey.client import MalformedResponseError, SurveyClient
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
    import csv
    import io

    rows = list(csv.DictReader(io.StringIO(text)))
    if rows and "Run" not in rows[0]:
        raise MalformedResponseError("runinfo response has no Run column")
    return sorted({row["Run"].strip() for row in rows if row.get("Run", "").strip()})


# --------------------------------------------------------------------------
# ENA portal: one endpoint serves much of the graph
# --------------------------------------------------------------------------


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


IMPLEMENTATIONS: dict[str, RouteFn] = {
    "gse->bioproject:soft_family": gse_to_bioproject_soft,
    "gse->bioproject:gds_summary": gse_to_bioproject_gds,
    "gse->geo_sample:soft_family": gse_to_gsm_soft,
    "gse->geo_sample:gds_summary": gse_to_gsm_gds,
    "gse->experiment:soft_family": gse_to_experiment_soft,
    "gse->experiment:elink_gds_sra": gse_to_experiment_elink,
    "gse->experiment:soft_bioproject_ena": gse_to_experiment_soft_bioproject_ena,
    "gse->run:elink_gds_sra": gse_to_run_elink,
    "bioproject->run:ena_filereport": _ena_route("run_accession"),
    "bioproject->study:ena_filereport": _ena_route("secondary_study_accession"),
    "bioproject->experiment:ena_filereport": _ena_route("experiment_accession"),
    "bioproject->biosample:ena_filereport": _ena_route("sample_accession"),
    "biosample->run:ena_filereport": _ena_route("run_accession"),
    "study->bioproject:ena_filereport": _ena_route("study_accession"),
    "study->run:ena_filereport": _ena_route("run_accession"),
    "run->experiment:ena_filereport": _ena_route("experiment_accession"),
    "run->biosample:ena_filereport": _ena_route("sample_accession"),
    "ae_experiment->study:idf_secondary": ae_to_study_idf,
    "ae_experiment->biosample:sdrf": ae_to_biosample_sdrf,
}
