"""Build the cross-archive relation table for a series or project.

Each archive holds a different part of the chain and only one holds each part:

* **GEO** is the only place that knows ``GSM -> SRX``. Nothing in INSDC records
  which GEO sample an experiment belongs to.
* **ENA** returns run, experiment, sample, BioSample, study and species in one
  request, for almost any accession you key it on.
* **ArrayExpress** holds its own accession for studies that have one.

So the table is a join, not a lookup, and the join key between GEO and INSDC is
the experiment accession.
"""

from __future__ import annotations

import logging

from fetch_series.accession import Accession, EntityType, parse, try_parse
from fetch_series.entities import RunRecord
from fetch_series.providers import biostudies, ena_portal, geo
from fetch_series.survey.client import SurveyClient

logger = logging.getLogger(__name__)

ENA_ROUTE = "ena:filereport"
SOFT_ROUTE = "geo:soft_family"

# What each ENA filereport column contributes to a RunRecord.
_ENA_TO_FIELD = {
    "run_accession": "run",
    "experiment_accession": "experiment",
    "secondary_sample_accession": "sample",
    "sample_accession": "biosample",
    "secondary_study_accession": "study",
    "study_accession": "bioproject",
    "scientific_name": "species",
    "library_strategy": "library_strategy",
}


async def relations(
    accession: Accession, client: SurveyClient, timeout: float = 120.0
) -> list[RunRecord]:
    """Resolve ``accession`` into one record per run.

    ENA's filereport does not accept GEO accessions -- it answers a GSE with a
    400 -- so a GEO series is resolved through its BioProject first, which is
    recorded in the SOFT family file. That is the same chain the
    ``gse->experiment:soft_bioproject_ena`` route measures, and it resolved
    12,551 of 13,045 series.
    """
    if accession.entity is EntityType.GEO_SERIES:
        return await _relations_from_geo(accession, client, timeout)

    if accession.entity is EntityType.AE_EXPERIMENT:
        return await _relations_from_arrayexpress(accession, client, timeout)

    records = await _ena_records(client, [accession.value], timeout)
    return sorted(records.values(), key=lambda r: r.run)


async def _relations_from_arrayexpress(
    accession: Accession, client: SurveyClient, timeout: float
) -> list[RunRecord]:
    """Resolve an ArrayExpress experiment through the accessions ENA accepts.

    ENA's filereport rejects an ArrayExpress accession outright -- it names the
    eight accession shapes it takes and E-MTAB is not among them -- so the study
    has to be found first. The IDF's ``Comment[SecondaryAccession]`` is the
    direct route; E-MTAB-6505 declares none and is reachable only through the
    BioSamples its SDRF names, which is why the fallback is not optional.
    """
    keys: list[str] = []
    try:
        keys = await biostudies.idf_secondary_accessions(client, accession.value, timeout)
    except Exception as exc:
        logger.warning("IDF fetch failed for %s: %s", accession.value, exc)

    if not keys:
        try:
            rows = await biostudies.sdrf_rows(client, accession.value, timeout)
            keys = biostudies.sdrf_column(rows, "Comment[BioSD_SAMPLE]")
        except Exception as exc:
            logger.warning("SDRF fetch failed for %s: %s", accession.value, exc)

    if not keys:
        logger.warning("%s declares neither a secondary study nor a BioSample", accession.value)
        return []

    records = await _ena_records(client, keys, timeout)
    for record in records.values():
        record.set("ae_experiment", accession.value, "input")
    return sorted(records.values(), key=lambda r: r.run)


async def _ena_records(
    client: SurveyClient, accessions: list[str], timeout: float
) -> dict[str, RunRecord]:
    """One filereport per accession, merged into records keyed by run."""
    records: dict[str, RunRecord] = {}
    for value in accessions:
        try:
            rows = await ena_portal.read_run_report(client, value, timeout)
        except Exception as exc:
            logger.warning("ENA filereport failed for %s: %s", value, exc)
            continue
        for row in rows:
            run = (row.get("run_accession") or "").strip()
            if not run:
                continue
            record = records.setdefault(run, RunRecord(run=run))
            for column, name in _ENA_TO_FIELD.items():
                record.set(name, (row.get(column) or "").strip(), ENA_ROUTE)
    return records


async def _relations_from_geo(
    accession: Accession, client: SurveyClient, timeout: float
) -> list[RunRecord]:
    """Resolve a GEO series: SOFT for the GEO half, ENA for the INSDC half."""
    family = geo.parse_soft_family(
        accession.value, await geo.fetch_soft_family(client, accession.value, timeout)
    )

    experiment_to_gsm = {
        rel["experiment"]: gsm
        for gsm, rel in family.sample_relations.items()
        if "experiment" in rel
    }
    gsm_to_biosample = {
        gsm: rel["biosample"] for gsm, rel in family.sample_relations.items() if "biosample" in rel
    }

    # Ask ENA about the project if the series names one; about the experiments
    # directly if it does not. Six of the 13,045 reprocessed series record SRA
    # relations but no BioProject -- four of them because they sit under a shared
    # umbrella project rather than one of their own.
    if family.bioprojects:
        keys = sorted(set(family.bioprojects))
    else:
        keys = sorted(experiment_to_gsm)
        if keys:
            logger.info("%s records no BioProject; querying ENA per experiment instead", accession)

    records = await _ena_records(client, keys, timeout)
    geo_named_experiments = bool(experiment_to_gsm)

    # Runs ENA reports under the project but belonging to other series are not
    # this series' runs, so filter on the experiments GEO attributes to it --
    # but only when GEO gave us any, and only when the filter leaves something.
    #
    # If it leaves nothing, GEO's experiment list is stale: none of the
    # accessions it names appears in what the project actually holds. GSE150508
    # is the case in point -- its SOFT file names SRX7571191, which has no runs,
    # while the project holds SRX9670669, which has two. Discarding the real
    # data because the stale record disagrees would be the worst of both
    # answers, so the rows are kept and the divergence is reported.
    if experiment_to_gsm:
        attributed = {
            run: record for run, record in records.items() if record.experiment in experiment_to_gsm
        }
        if attributed:
            records = attributed
        elif records:
            logger.warning(
                "%s: none of the %d experiments its SOFT file names appears in the "
                "project's runs -- the GEO record is stale. Reporting the %d runs the "
                "project actually holds; see "
                "docs/pathologies/soft-names-experiments-with-no-runs.md",
                accession,
                len(experiment_to_gsm),
                len(records),
            )
            experiment_to_gsm = {}

    for record in records.values():
        record.set("geo_series", accession.value, SOFT_ROUTE)
        for bioproject in family.bioprojects:
            record.set("bioproject", bioproject, SOFT_ROUTE)
        gsm = experiment_to_gsm.get(record.experiment or "")
        if gsm:
            record.set("geo_sample", gsm, SOFT_ROUTE)
            record.set("biosample", gsm_to_biosample.get(gsm), SOFT_ROUTE)

    # Only when GEO genuinely named nothing. A series whose experiment list was
    # stale did name relations -- saying otherwise would misdescribe the defect.
    if not geo_named_experiments and records:
        logger.warning(
            "%s: the SOFT file names no SRA relation for any sample, so no run could be "
            "attributed to a GEO sample; rows carry the INSDC sample only",
            accession,
        )
    return sorted(records.values(), key=lambda r: r.run)


def sample_to_runs(records: list[RunRecord]) -> dict[str, list[str]]:
    """Group runs by sample, preferring the GEO sample where there is one.

    GEO is preferred because that is the identity a reprocessing pipeline is
    asked about, and because one GEO sample can span several INSDC experiments --
    CITE-seq submitted alongside gene expression is the common case.
    """
    grouped: dict[str, list[str]] = {}
    for record in records:
        key = record.geo_sample or record.sample or record.biosample
        if key is None:
            continue
        grouped.setdefault(key, []).append(record.run)
    return {key: sorted(runs) for key, runs in sorted(grouped.items())}


def resolve_input(raw: str) -> Accession:
    """Parse an entry accession, with a useful error for near-misses."""
    parsed = try_parse(raw)
    if parsed is not None:
        return parsed
    return parse(raw)  # raises UnknownAccessionError with the full namespace list
