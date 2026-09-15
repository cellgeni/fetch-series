"""Assemble a run's files from every route that offers any, and recommend one set.

This is the point of the whole project: an accession in, verified download links
out. Everything before it -- accession parsing, the route graph, the resolver --
exists to get from whatever the user has to a list of runs, and this module
takes it from there.

It asks *every* file route rather than stopping at the first that answers. That
is a deliberate departure from how the resolver treats identity routes, where
the first good answer is the answer. Here the routes do not answer the same
question: ENA's ``fastq_*`` columns hold ENA's own derived fastqs, ``submitted_*``
holds what the submitter deposited, and SDL holds NCBI's view of both. A run can
have fastqs and no submitted files, submitted files and no fastqs, or -- as the
two-run tier-one check found -- exactly one of the two, with the other route
returning empty and meaning it.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from fetch_series.accession import Accession, EntityType
from fetch_series.export.links import LinkRow, links_for
from fetch_series.files import FileRecord, FileSet, Recommendation, recommend
from fetch_series.graph import REGISTRY, RouteRegistry
from fetch_series.relations import relations
from fetch_series.resolver import resolve_path
from fetch_series.routes import (
    IMPLEMENTATIONS,
    ae_file_records,
    ena_file_records,
    sdl_file_records,
)
from fetch_series.survey.client import SurveyClient
from fetch_series.survey.runner import RouteFn

# Each file route paired with the call that returns typed records rather than
# bare URLs. The route ids are the same ones the graph declares and the survey
# measures, so a route's coverage figure and its use here cannot drift apart.
FILE_RECORD_SOURCES: Mapping[str, str] = {
    "run->file:ena_fastq": "fastq",
    "run->file:ena_submitted": "submitted",
    "run->file:ena_sra": "sra",
}


async def files_for_run(
    run: str,
    client: SurveyClient,
    timeout: float = 60.0,
    *,
    include_sdl: bool = True,
) -> FileSet:
    """Every file every route offers for one run.

    A route that fails is not allowed to take the others down with it: a failed
    route contributes nothing and the set says so by its ``sources``, which is
    recoverable, whereas an exception here loses the routes that did answer.
    """

    async def ena(column: str) -> list[FileRecord]:
        return await ena_file_records(run, client, timeout, column)

    tasks = [ena(column) for column in FILE_RECORD_SOURCES.values()]
    if include_sdl:
        tasks.append(sdl_file_records(run, client, timeout))

    records: list[FileRecord] = []
    for outcome in await asyncio.gather(*tasks, return_exceptions=True):
        if isinstance(outcome, BaseException):
            continue
        records.extend(outcome)
    return FileSet(run=run, records=tuple(records))


async def runs_of(
    accession: Accession,
    client: SurveyClient,
    *,
    registry: RouteRegistry = REGISTRY,
    implementations: Mapping[str, RouteFn] = IMPLEMENTATIONS,
) -> list[str]:
    """The runs an accession resolves to, or itself if it is already one."""
    if accession.entity is EntityType.RUN:
        return [accession.value]
    resolution = await resolve_path(
        accession, EntityType.RUN, client, registry=registry, implementations=implementations
    )
    return list(resolution.values)


async def recommendations_for(
    accession: Accession,
    client: SurveyClient,
    *,
    limit: int | None = None,
    include_sdl: bool = True,
) -> list[Recommendation]:
    """Resolve an accession to runs and recommend a file set for each.

    ``limit`` caps how many runs are asked about. A 880-run series costs four
    requests per run, and a user checking what a series holds should not have to
    spend 3,520 requests to find out.
    """
    runs = await runs_of(accession, client)
    if limit is not None:
        runs = runs[:limit]
    sets = await asyncio.gather(
        *(files_for_run(run, client, include_sdl=include_sdl) for run in runs)
    )
    return [recommend(fileset) for fileset in sets]


@dataclass(frozen=True, slots=True)
class Verdict:
    """What a HEAD request said about one published download link.

    ``size_matches`` is None when the archive published no size, or the server
    returned no ``Content-Length`` -- the check did not fail, it could not run,
    and conflating the two would report every chunked response as a corrupt
    file.
    """

    record: FileRecord
    status: int | None
    served_bytes: int | None
    error: str | None = None

    @property
    def resolves(self) -> bool:
        return self.status is not None and 200 <= self.status < 300

    @property
    def size_matches(self) -> bool | None:
        if self.record.size is None or self.served_bytes is None:
            return None
        return self.record.size == self.served_bytes

    def describe(self) -> str:
        if self.error:
            return f"{self.record.url} -- {self.error}"
        if not self.resolves:
            return f"{self.record.url} -- HTTP {self.status}"
        if self.size_matches is False:
            return (
                f"{self.record.url} -- archive published {self.record.size:,} bytes, "
                f"server serves {self.served_bytes:,}"
            )
        if self.size_matches is None:
            return f"{self.record.url} -- resolves; size not checkable"
        return f"{self.record.url} -- resolves, size matches"


async def verify(record: FileRecord, client: SurveyClient, timeout: float = 30.0) -> Verdict:
    """Check that a published link resolves and serves the size it claims.

    A HEAD, not a download: the md5 the archive publishes cannot be checked
    without fetching the bytes, and at file-layer scale that is not a survey, it
    is a mirror. Size and resolvability are what can be checked cheaply, and
    they catch the two failures that matter most -- a link to a decommissioned
    mirror, and a file that was replaced without its metadata being updated.
    """
    try:
        response = await client.head(record.url, timeout=timeout)
    except Exception as exc:  # a failed request is itself the verdict
        return Verdict(record=record, status=None, served_bytes=None, error=type(exc).__name__)
    length = response.headers.get("content-length")
    return Verdict(
        record=record,
        status=response.status_code,
        served_bytes=int(length) if length and length.isdigit() else None,
    )


async def verify_all(
    records: Iterable[FileRecord], client: SurveyClient, timeout: float = 30.0
) -> list[Verdict]:
    """Verify many links concurrently, at the client's own rate limit."""
    return list(await asyncio.gather(*(verify(r, client, timeout) for r in records)))


async def links_table(
    accession: Accession,
    client: SurveyClient,
    *,
    limit: int | None = None,
) -> list[LinkRow]:
    """The incumbent ``links.tsv`` for everything an accession resolves to.

    Builds the relation table first, because the sample column is the one thing
    the file routes cannot supply: for a GEO series it is the GSM, which only
    GEO knows, and for a bare SRA submission it is the SRS.

    The ArrayExpress SDRF route is asked once per *study*, not once per run --
    it is keyed on the experiment accession and returns every run at once -- so
    a series with 200 runs costs one SDRF request rather than 200.
    """
    records = await relations(accession, client)
    if limit is not None:
        records = records[:limit]
    runs = [record.run for record in records]

    sdrf: dict[str, list[FileRecord]] = {}
    ae_experiments = {r.ae_experiment for r in records if r.ae_experiment}
    if accession.entity is EntityType.AE_EXPERIMENT:
        ae_experiments.add(accession.value)
    for experiment in sorted(ae_experiments):
        try:
            for record in await ae_file_records(experiment, client, 60.0):
                sdrf.setdefault(record.run, []).append(record)
        except Exception:
            continue  # the SDRF is one source among several; losing it is not fatal

    sets = await asyncio.gather(*(files_for_run(run, client) for run in runs))
    filesets = {
        fileset.run: FileSet(
            run=fileset.run, records=fileset.records + tuple(sdrf.get(fileset.run, ()))
        )
        for fileset in sets
    }
    return links_for(
        filesets,
        species={r.run: r.species or "UNKNOWN" for r in records},
        # GEO calls the sample a GSM and SRA calls it an SRS. The incumbent
        # emits whichever the entry point implies, so the GEO sample wins when
        # there is one.
        samples={r.run: (r.geo_sample or r.sample or "NA") for r in records},
    )
