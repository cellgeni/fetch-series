"""Surveying promoted routes: a catalogue mechanism's request, as a route, at ``sample:3000``.

A link survey drew 300 accessions per node and recorded ``link:`` verdicts that
never become evidence. This records route verdicts -- under the route's own id,
as every route survey does -- over a seeded draw of 3,000 of the source entity
type's population, so what it measures can become a ``RouteEvidence`` block.

One request answers every target a mechanism reads, so the unit is a mechanism
from one source entity type, and each accession is asked once for all of its
routes (``routes.catalogue.fanout``). The gate comes first, as for any route:
``probe-families`` must not fail, and ``hard-cases`` must have a verdict for
every accession of the source type it holds.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime

from fetch_series.accession import EntityType
from fetch_series.cache import SurveyCache
from fetch_series.catalogue.populations import ENA_BIOSAMPLES, ENA_STUDIES, population
from fetch_series.graph import (
    EBI_RPS,
    NCBI_EUTILS_RPS,
    REGISTRY,
    Promoted,
    Route,
    RouteEvidence,
    RouteRegistry,
)
from fetch_series.results import Outcome
from fetch_series.routes import IMPLEMENTATIONS
from fetch_series.routes.catalogue import Candidate, fanout, host_of
from fetch_series.survey import corpora
from fetch_series.survey.client import SurveyClient
from fetch_series.survey.directions import draw_verdicts
from fetch_series.survey.runner import FanoutRun, SurveyRun, run_fanout, run_route

DRAW = 3_000
GATES = ("probe-families", "hard-cases")

# The population a route from each entity type is drawn from: the same ones the
# declared routes and the link surveys use, before any draw.
POPULATIONS: dict[EntityType, str] = {
    EntityType.BIOPROJECT: "reprocessed-prj",
    EntityType.BIOSAMPLE: ENA_BIOSAMPLES,
    EntityType.STUDY: ENA_STUDIES,
    EntityType.SAMPLE: "reprocessed-srs",
    EntityType.EXPERIMENT: "reprocessed-srx",
    EntityType.RUN: "reprocessed-srr",
    EntityType.GEO_SERIES: "reprocessed-gse",
    EntityType.GEO_SAMPLE: "reprocessed-sample",
    EntityType.AE_EXPERIMENT: "arrayexpress-ena",
}


def standard_corpus(source: EntityType) -> str:
    """``sample:3000`` of the source's population, or the whole of it where smaller."""
    if source is EntityType.BIOSTUDY:
        name = population("biostudy")
    else:
        name = POPULATIONS[source]
    return name if len(corpora.load(name).of_type(source)) <= DRAW else f"sample:{DRAW}@{name}"


@dataclass(frozen=True, slots=True)
class Unit:
    """One mechanism from one source entity type: the routes one request answers."""

    mechanism: str
    source: EntityType
    routes: tuple[Candidate, ...]

    @property
    def id(self) -> str:
        return f"{self.mechanism}@{self.source.value}"

    @property
    def route_ids(self) -> list[str]:
        return [c.id for c in self.routes]


def units_of(candidates: Iterable[Candidate]) -> list[Unit]:
    grouped: dict[tuple[str, EntityType], list[Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.mechanism, candidate.source), []).append(candidate)
    return [
        Unit(mechanism, source, tuple(sorted(members, key=lambda c: c.id)))
        for (mechanism, source), members in sorted(
            grouped.items(), key=lambda item: (item[0][1].value, item[0][0])
        )
    ]


@dataclass(frozen=True, slots=True)
class Job:
    unit: Unit
    corpus: str
    accessions: tuple[str, ...]

    @property
    def bucket(self) -> str:
        return host_of(self.unit.mechanism)


@dataclass(slots=True)
class Plan:
    jobs: list[Job] = field(default_factory=list)
    skipped: list[tuple[Unit, str]] = field(default_factory=list)


def plan(units: Iterable[Unit], corpora_for: Callable[[Unit], list[str]]) -> Plan:
    loaded: dict[str, corpora.Corpus] = {}
    out = Plan()
    for unit in units:
        for name in corpora_for(unit):
            if name not in loaded:
                loaded[name] = corpora.load(name)
            accessions = loaded[name].of_type(unit.source)
            if accessions:
                out.jobs.append(Job(unit, name, accessions))
            else:
                out.skipped.append((unit, f"{name} holds no {unit.source.value}"))
    return out


async def run_jobs(
    jobs: Sequence[Job],
    clients: Mapping[str, SurveyClient],
    cache: SurveyCache,
    *,
    retry_failed: bool = False,
    on_done: Callable[[Job, FanoutRun], None] | None = None,
) -> None:
    """Each bucket's jobs in order, the buckets side by side, as the link survey runs them."""
    by_bucket: dict[str, list[Job]] = {}
    for job in jobs:
        by_bucket.setdefault(job.bucket, []).append(job)

    async def drain(bucket: str, queue: list[Job]) -> None:
        workers = clients[bucket].limits.concurrency
        for job in sorted(queue, key=lambda j: (j.unit.source.value, j.corpus, j.unit.mechanism)):
            fn = fanout(job.unit.mechanism, job.unit.routes, clients, job.corpus)
            run = await run_fanout(
                job.unit.route_ids,
                job.accessions,
                fn,
                cache,
                job.corpus,
                workers=workers,
                retry_failed=retry_failed,
            )
            if on_done is not None:
                on_done(job, run)

    await asyncio.gather(*(drain(bucket, queue) for bucket, queue in by_bucket.items()))


def failed_elsewhere(unit: Unit, cache: SurveyCache) -> list[str]:
    """Routes of the unit's request, into targets outside the unit, that failed its gate.

    One request answers every target a mechanism reads from a source, and a
    failure is the request's (``routes.catalogue.combine``), so a gate failed
    for one target is failed for all. SDL asked for a project or study answers
    504 every time (2026-10-26); gating it again for a second target would
    spend a retry budget learning that twice.
    """
    prefix, suffix = f"{unit.source.value}->", f":{unit.mechanism}"
    own = set(unit.route_ids)
    return sorted(
        route_id
        for _, route_id in cache.routes_surveyed(GATES[0])
        if route_id.startswith(prefix)
        and route_id.endswith(suffix)
        and route_id not in own
        and any(r.outcome is Outcome.FAILED for r in cache.results(GATES[0], route_id))
    )


def gate_passed(unit: Unit, cache: SurveyCache) -> bool | None:
    """``None`` if ungated; else whether no route of the unit failed on ``probe-families``.

    A unit whose request failed the gate for another target has failed it too
    (:func:`failed_elsewhere`).
    """
    if failed_elsewhere(unit, cache):
        return False
    answered = failed = 0
    for gate in GATES:
        for route_id in unit.route_ids:
            for result in cache.results(gate, route_id):
                answered += 1
                failed += gate == "probe-families" and result.outcome is Outcome.FAILED
    if not answered:
        return None
    return failed == 0


# The host bucket a declared route's requests are paced under: the link survey's
# buckets (``catalogue.probe.host_limits``), by the route's provider.
_BUCKET_OF_PROVIDER = {
    "eutils": "ncbi_eutils",
    "ena_portal": "ena_portal",
    "biostudies": "biostudies",
    "biosamples": "biosamples",
    "ddbj": "ddbj",
}


def bucket_of(route: Route) -> str:
    """NCBI's services other than E-utilities -- GEO, sra-db-be, SDL -- share ``ncbi_web``."""
    return _BUCKET_OF_PROVIDER.get(route.provider.split("+", 1)[0], "ncbi_web")


@dataclass(frozen=True, slots=True)
class RouteJob:
    """A declared route surveyed on the draw its direction's candidates are surveyed on."""

    route: Route
    corpus: str
    accessions: tuple[str, ...]

    @property
    def bucket(self) -> str:
        return bucket_of(self.route)


def directions_of(units: Iterable[Unit]) -> list[tuple[EntityType, EntityType]]:
    """Every (source, target) the units' routes answer, once each, in a stable order."""
    found = {(c.source, c.target) for unit in units for c in unit.routes}
    return sorted(found, key=lambda d: (d[0].value, d[1].value))


def declared_jobs(
    directions: Iterable[tuple[EntityType, EntityType]],
    cache: SurveyCache,
    *,
    since: datetime | None = None,
    registry: RouteRegistry = REGISTRY,
) -> list[RouteJob]:
    """Each declared route of each direction, over the draw's accessions it has no verdict on.

    A second route is compared with the routes already in its direction on the
    same accessions, so those need verdicts on the draw too
    (:func:`survey.directions.draw_verdicts` finds them under the population's
    name as well). With ``since``, a verdict recorded before it counts as
    missing: a project gains runs between two surveys, and a route asked later
    would be credited with them.
    """
    jobs: list[RouteJob] = []
    for source, target in directions:
        corpus = standard_corpus(source)
        accessions = corpora.load(corpus).of_type(source)
        for route in registry.ranked(source, target):
            held = draw_verdicts(route.id, corpus, accessions, cache)
            owed = tuple(
                a
                for a in accessions
                if a not in held or (since is not None and held[a].fetched_at < since)
            )
            if owed:
                jobs.append(RouteJob(route, corpus, owed))
    return jobs


async def run_route_jobs(
    jobs: Sequence[RouteJob],
    clients: Mapping[str, SurveyClient],
    cache: SurveyCache,
    *,
    on_done: Callable[[RouteJob, SurveyRun], None] | None = None,
) -> None:
    """Each bucket's declared routes in order, the buckets side by side.

    The owed accessions are exactly the ones to ask, so nothing is resumed past:
    a verdict older than ``since`` is asked again and replaced.
    """
    by_bucket: dict[str, list[RouteJob]] = {}
    for job in jobs:
        by_bucket.setdefault(job.bucket, []).append(job)

    async def drain(queue: list[RouteJob]) -> None:
        for job in sorted(queue, key=lambda j: (j.corpus, j.route.id)):
            run = await run_route(
                job.route,
                job.accessions,
                IMPLEMENTATIONS[job.route.id],
                cache,
                job.corpus,
                clients[job.bucket],
                resume=False,
            )
            if on_done is not None:
                on_done(job, run)

    await asyncio.gather(*(drain(queue) for queue in by_bucket.values()))


# What one call costs where it is more than one request, as the link survey counts it.
_CALL_COST = {"elink_": 3, "esearch_sra_runinfo": 2, "esummary_sra": 2, "biosample_record": 2}
_PROVIDERS = {
    "ena_portal": ("ena_portal", EBI_RPS),
    "ena_search": ("ena_portal", EBI_RPS),
    "ncbi_eutils": ("eutils", NCBI_EUTILS_RPS),
    "ncbi_web": ("ncbi_web", NCBI_EUTILS_RPS),
    "biostudies": ("biostudies", EBI_RPS),
    "biosamples": ("biosamples", EBI_RPS),
    "ddbj": ("ddbj", 1.5),
}
# Pathologies a mechanism's request is known to exhibit, each with its page.
PATHOLOGIES: dict[str, tuple[str, ...]] = {
    "esummary_sra": ("esummary-500-uid-limit", "esummary-sra-buries-accessions-in-expxml"),
    "sra_be_runinfo": ("sra-be-history-caps-at-5000-runs",),
    "esearch_sra_runinfo": ("efetch-returns-fewer-runs-than-esearch-counts",),
    "biostudies_idf": ("biostudies-files-404s-registered-files",),
    **{
        mechanism: ("biostudies-files-404s-registered-files", "ae-sdrf-split-across-files")
        for mechanism in ("biostudies_sdrf", "biostudies_sdrf_comments")
    },
}


def evidence_row(candidate: Candidate, corpus: str, cache: SurveyCache) -> Promoted | None:
    """The ``PROMOTED`` row a surveyed candidate becomes, or ``None`` before its survey."""
    results = list(cache.results(corpus, candidate.id))
    if not results:
        return None
    counts = {o: sum(r.outcome is o for r in results) for o in Outcome}
    distinct = {value for r in results for value in r.results}
    provider, rps = _PROVIDERS[host_of(candidate.mechanism)]
    cost = next(
        (n for prefix, n in _CALL_COST.items() if candidate.mechanism.startswith(prefix)), 1
    )
    latest = max(r.fetched_at for r in results).date()
    return Promoted(
        mechanism=candidate.mechanism,
        source=candidate.source,
        target=candidate.target,
        provider=provider,
        requests=cost,
        rate_limit_rps=rps,
        source_archives=candidate.source_archives,
        known_pathologies=PATHOLOGIES.get(candidate.mechanism, ())
        + (("megalink-backend-flakiness",) if candidate.mechanism.startswith("elink_") else ()),
        proves_data_exists=False,
        evidence=RouteEvidence(
            corpus=corpus,
            surveyed_on=latest,
            accessions_queried=len(results),
            accessions_failed=counts[Outcome.FAILED],
            accessions_empty=counts[Outcome.EMPTY],
            unique_results=len(distinct),
            notes=(
                f"{counts[Outcome.RESOLVED]:,} resolved, {counts[Outcome.EMPTY]:,} empty. "
                "A catalogue mechanism surveyed as a route."
            ),
        ),
    )


def as_source(row: Promoted) -> str:
    """A ``PROMOTED`` row as the Python that declares it in ``graph.py``."""
    e = row.evidence
    archives = ", ".join(f"Archive.{a.name}" for a in row.source_archives)
    pathologies = "".join(f'"{p}", ' for p in row.known_pathologies)
    return f"""    Promoted(
        mechanism="{row.mechanism}",
        source=EntityType.{row.source.name},
        target=EntityType.{row.target.name},
        provider="{row.provider}",
        requests={row.requests},
        rate_limit_rps={row.rate_limit_rps},
        source_archives=({archives}{"," if len(row.source_archives) == 1 else ""}),
        known_pathologies=({pathologies.rstrip(", ")}{"," if len(row.known_pathologies) == 1 else ""}),
        evidence=RouteEvidence(
            corpus="{e.corpus}",
            surveyed_on=date({e.surveyed_on.year}, {e.surveyed_on.month}, {e.surveyed_on.day}),
            accessions_queried={e.accessions_queried:_},
            accessions_failed={e.accessions_failed:_},
            accessions_empty={(e.accessions_empty or 0):_},
            unique_results={(e.unique_results or 0):_},
            notes="{e.notes}",
        ),
    ),"""


def answered(row: Promoted) -> bool:
    """A survey that resolved nothing measured an absence: it stays a link, not a route."""
    e = row.evidence
    return e.accessions_queried - e.accessions_failed - (e.accessions_empty or 0) > 0


__all__ = [
    "DRAW",
    "GATES",
    "POPULATIONS",
    "Job",
    "Plan",
    "RouteJob",
    "Unit",
    "answered",
    "as_source",
    "bucket_of",
    "declared_jobs",
    "directions_of",
    "evidence_row",
    "failed_elsewhere",
    "gate_passed",
    "plan",
    "run_jobs",
    "run_route_jobs",
    "standard_corpus",
    "units_of",
]
