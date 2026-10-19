"""Surveying catalogue links: the probe's request, over a corpus instead of one accession.

A survey unit is one mechanism from one source node. Every link in it is
answered by the same request -- ENA ``filereport`` on an SRR names that run's
experiment, sample, study and BioProject at once -- so the unit makes one
request per accession and records one verdict per link, under ``link:<link id>``
(:mod:`fetch_series.units`).

What this produces is measurement of whether a link answers, over a seeded
draw. It is not a ranking and never becomes ``RouteEvidence``: a draw of 300 is
too coarse to separate two links whose yields differ by less than the ranking's
1% band, and promoting a link to a route means surveying it as a route.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field

from fetch_series.cache import SurveyCache
from fetch_series.catalogue import CATALOGUE, Catalogue, Link, NodeKind, Probe
from fetch_series.catalogue.nodes import node_for
from fetch_series.catalogue.probe import (
    DDBJ_ENTRY,
    PROBERS,
    Finding,
    answer_links,
    seeded_runs,
)
from fetch_series.providers import biostudies, eutils, geo, sra_be
from fetch_series.results import Outcome, RouteResult
from fetch_series.survey import corpora
from fetch_series.survey.client import SurveyClient
from fetch_series.survey.runner import SHUFFLE_SEED, FanoutFn, FanoutRun, run_fanout
from fetch_series.units import link_unit, parse_unit

logger = logging.getLogger(__name__)

GATES = ("probe-families", "hard-cases")


@dataclass(frozen=True, slots=True)
class SurveyUnit:
    """One mechanism from one source node: the links one request answers."""

    mechanism: str
    source: str
    links: tuple[Link, ...]

    @property
    def id(self) -> str:
        return f"{self.mechanism}@{self.source}"

    @property
    def unit_ids(self) -> list[str]:
        return [link_unit(link.id) for link in self.links]


def units_of(links: Iterable[Link]) -> list[SurveyUnit]:
    """Group links into survey units, sorted so a plan reads the same twice."""
    grouped: dict[tuple[str, str], list[Link]] = {}
    for link in links:
        grouped.setdefault((link.mechanism, link.source), []).append(link)
    return [
        SurveyUnit(mechanism, source, tuple(sorted(members, key=lambda link: link.id)))
        for (mechanism, source), members in sorted(grouped.items())
    ]


def memoizable(method: str, url: str, params: Mapping[str, object]) -> bool:
    """The repeated requests a link survey answers from memory, and nothing else.

    sra-db-be runinfo (three mechanisms), an IDF or SDRF (three), a SOFT family
    file (two), an SRA XML efetch by UID (two), a DDBJ entry (three). Each is a
    stateless document fetch, so the second answer is the first answer. ESearch
    and ELink are deliberately absent: they return a WebEnv, and a WebEnv
    answered from memory names a session the server has since dropped.
    """
    if method == "GET":
        return (
            url == sra_be.RUNINFO
            or (url.startswith(biostudies.FILES) and url.endswith((".sdrf.txt", ".idf.txt")))
            or (url.startswith(geo.FTP_BASE) and url.endswith("_family.soft.gz"))
            or url.startswith(DDBJ_ENTRY)
        )
    return (
        method == "POST"
        and url == f"{eutils.BASE}/efetch.fcgi"
        and params.get("db") == "sra"
        and bool(params.get("id"))
        and "WebEnv" not in params
    )


def as_result(finding: Finding, corpus: str, latency_ms: int) -> RouteResult:
    """A finding as the verdict the cache stores, its checks in ``detail``."""
    unit = link_unit(finding.link)
    if finding.outcome is Outcome.FAILED:
        error_class, _, message = (finding.error or "").partition(": ")
        return RouteResult(
            accession=finding.accession,
            route_id=unit,
            corpus=corpus,
            outcome=Outcome.FAILED,
            error_class=error_class or None,
            error_message=message or finding.error,
            latency_ms=latency_ms,
        )
    detail = (
        {"checks": [list(check) for check in finding.checks], "checked": finding.checked}
        if finding.checks
        else None
    )
    return RouteResult.from_results(
        accession=finding.accession,
        route_id=unit,
        corpus=corpus,
        results=list(finding.values),
        latency_ms=latency_ms,
        detail=detail,
    )


def link_fanout(
    unit: SurveyUnit,
    clients: Mapping[str, SurveyClient],
    corpus: str,
    *,
    catalogue: Catalogue = CATALOGUE,
    seed: int = SHUFFLE_SEED,
    retry: Callable[[BaseException], bool] | None = None,
) -> FanoutFn:
    """The fan-out function for one unit: one request, a verdict per owed link.

    The run a link's file set is checked on is chosen by seed, per accession,
    never "the run that sorts first" -- which is every BioProject's oldest.
    """
    by_unit = {link_unit(link.id): link for link in unit.links}

    async def fn(accession: str, owed: frozenset[str]) -> list[RouteResult]:
        members = [by_unit[u] for u in sorted(owed) if parse_unit(u).kind == "link"]
        started = time.monotonic()
        findings = await answer_links(
            unit.mechanism,
            accession,
            members,
            clients,
            catalogue=catalogue,
            pick_run=seeded_runs(seed, accession),
            retry=retry,
        )
        latency = int((time.monotonic() - started) * 1000)
        return [as_result(finding, corpus, latency) for finding in findings]

    return fn


# ------------------------------------------------------------- selection


class SelectionError(ValueError):
    """The links asked for cannot be surveyed as links."""


def select_links(
    *,
    link_ids: Sequence[str] = (),
    mechanism: str | None = None,
    source: str | None = None,
    every: bool = False,
    catalogue: Catalogue = CATALOGUE,
) -> list[Link]:
    """The links a survey run covers. Never a link a route's survey measured.

    A measured link's evidence is its route's. Surveying it again as a link
    would put a second, smaller measurement beside the route's -- under a
    different id, on a different corpus -- and invite reading the two as
    independent confirmations.
    """
    chosen = sum(bool(x) for x in (link_ids, mechanism, every))
    if chosen != 1:
        raise SelectionError("Choose exactly one of --link, --mechanism or --all")
    if source is not None and mechanism is None:
        raise SelectionError("--source narrows --mechanism")
    if link_ids:
        try:
            links = [catalogue.link(link_id) for link_id in link_ids]
        except KeyError as exc:
            raise SelectionError(f"Unknown link: {exc.args[0]}") from None
        measured = [link.id for link in links if catalogue.split(link) is not None]
        if measured:
            raise SelectionError(
                f"Measured by a route's survey, so never surveyed as a link: {', '.join(measured)}"
            )
        return links
    unmeasured = catalogue.untested
    if every:
        return unmeasured
    if mechanism not in catalogue.mechanisms:
        raise SelectionError(f"Unknown mechanism: {mechanism}")
    links = [
        link
        for link in unmeasured
        if link.mechanism == mechanism and (source is None or link.source == source)
    ]
    if not links:
        raise SelectionError(f"No unmeasured link of {mechanism} from {source or 'any node'}")
    return links


# ---------------------------------------------------------------- jobs


@dataclass(frozen=True, slots=True)
class Job:
    """One unit over one corpus: what a fan-out run is."""

    unit: SurveyUnit
    corpus: str
    accessions: tuple[str, ...]

    @property
    def bucket(self) -> str:
        return PROBERS[self.unit.mechanism].host


def accessions_for(source: str, corpus: corpora.Corpus) -> tuple[str, ...]:
    """The corpus's instances of ``source``: what the unit's request accepts."""
    if source == "ae_sample":
        return corpus.addresses
    return tuple(a.value for a in corpus.accessions if node_for(a) == source)


@dataclass(slots=True)
class Plan:
    jobs: list[Job] = field(default_factory=list)
    skipped: list[tuple[SurveyUnit, str]] = field(default_factory=list)


def plan(
    units: Iterable[SurveyUnit],
    corpora_for: Callable[[SurveyUnit], list[str]],
    *,
    load: Callable[[str], corpora.Corpus] = corpora.load,
) -> Plan:
    """Each unit against each of its corpora, or the reason it cannot be."""
    loaded: dict[str, corpora.Corpus] = {}
    out = Plan()
    for unit in units:
        try:
            names = corpora_for(unit)
        except LookupError as exc:
            out.skipped.append((unit, str(exc)))
            continue
        for name in names:
            if name not in loaded:
                loaded[name] = load(name)
            accessions = accessions_for(unit.source, loaded[name])
            if accessions:
                out.jobs.append(Job(unit, name, accessions))
            else:
                out.skipped.append((unit, f"{name} holds no {unit.source}"))
    return out


# What one call costs, where it is more than one request: an ELink is an
# ESearch, the link and an ESummary; an efetch by accession needs its UIDs first.
_CALL_COST = {
    "elink_": 3,
    "efetch_sra": 2,
    "biosample_record": 2,
    "gds_esummary": 2,
    "geo_acc": 1,
}


def estimate(job: Job, catalogue: Catalogue = CATALOGUE) -> dict[str, int]:
    """Requests per bucket, before memoisation: a plan, not a promise.

    The mechanism's own call, plus the checks on what it finds: a HEAD per
    file of one run's set (two, for a pair), one for a supplementary file, and
    the SRA XML that says whether an SRA object holds the barcode read.
    """
    per_call = next(
        (cost for prefix, cost in _CALL_COST.items() if job.unit.mechanism.startswith(prefix)), 1
    )
    n = len(job.accessions)
    cost = {job.bucket: per_call * n}
    for link in job.unit.links:
        kind = catalogue.nodes[link.target].kind
        if kind is NodeKind.READ_FORMAT:
            cost["files"] = cost.get("files", 0) + 2 * n
            if link.target.startswith("sra_"):
                cost["ncbi_eutils"] = cost.get("ncbi_eutils", 0) + 2 * n
        elif kind is NodeKind.SUPPLEMENTARY:
            cost["files"] = cost.get("files", 0) + n
    return cost


async def run_jobs(
    jobs: Sequence[Job],
    clients: Mapping[str, SurveyClient],
    cache: SurveyCache,
    *,
    retry_failed: bool = False,
    on_done: Callable[[Job, FanoutRun], None] | None = None,
    catalogue: Catalogue = CATALOGUE,
) -> list[tuple[Job, FanoutRun]]:
    """Run jobs, one bucket's in order and the buckets side by side.

    Within a bucket, order is the plan's -- sorted by source and mechanism, so
    the three runinfo mechanisms over one corpus run back to back and the memo
    answers the second and third from the first. Across buckets nothing is
    shared but the file checks, which have their own bucket.
    """
    by_bucket: dict[str, list[Job]] = {}
    for job in jobs:
        by_bucket.setdefault(job.bucket, []).append(job)
    done: list[tuple[Job, FanoutRun]] = []

    async def drain(bucket: str, queue: list[Job]) -> None:
        workers = clients[bucket].limits.concurrency
        for job in sorted(queue, key=lambda j: (j.unit.source, j.corpus, j.unit.mechanism)):
            fn = link_fanout(job.unit, clients, job.corpus, catalogue=catalogue)
            run = await run_fanout(
                job.unit.unit_ids,
                job.accessions,
                fn,
                cache,
                job.corpus,
                workers=workers,
                retry_failed=retry_failed,
            )
            done.append((job, run))
            if on_done is not None:
                on_done(job, run)

    await asyncio.gather(*(drain(bucket, queue) for bucket, queue in by_bucket.items()))
    return done


# ---------------------------------------------------------------- gate


@dataclass(frozen=True, slots=True)
class GateRow:
    """One link's gate: did every gate accession get a verdict, and does the probe agree."""

    link: str
    failed: int
    answered: int
    probe_outcome: str | None
    gate_outcome: str | None

    @property
    def agrees(self) -> bool | None:
        if self.probe_outcome is None or self.gate_outcome is None:
            return None
        return self.probe_outcome == self.gate_outcome


def gate_report(
    units: Iterable[SurveyUnit],
    cache: SurveyCache,
    probes: Mapping[str, Probe],
    catalogue: Catalogue = CATALOGUE,
) -> list[GateRow]:
    """Per link: gate failures, and whether the probe accession's verdict matches its probe.

    The comparison is on outcome. The probe was one request on one day, and a
    gate verdict that disagrees with it is either the archive moving or the
    survey's fetch differing from the probe's -- the second is a bug, and this
    is where it shows.
    """
    rows = []
    for unit in units:
        for link in unit.links:
            unit_id = link_unit(link.id)
            failed = answered = 0
            for gate in GATES:
                for result in cache.results(gate, unit_id):
                    answered += 1
                    failed += result.outcome is Outcome.FAILED
            probe = probes.get(link.id)
            gate_outcome = None
            if probe is not None:
                accession = catalogue.probe_accession(link)
                gate_outcome = next(
                    (
                        r.outcome.value
                        for r in cache.results("probe-families", unit_id)
                        if r.accession == accession
                    ),
                    None,
                )
            rows.append(
                GateRow(
                    link=link.id,
                    failed=failed,
                    answered=answered,
                    probe_outcome=probe.outcome.value if probe else None,
                    gate_outcome=gate_outcome,
                )
            )
    return rows


def gate_passed(unit: SurveyUnit, cache: SurveyCache) -> bool | None:
    """Whether a unit may proceed to a draw: gated, and not one FAILED verdict.

    ``None`` when it has not been gated at all. ``hard-cases`` holds few
    accessions of most nodes, so a unit is gated if *either* gate gave it any
    verdict.
    """
    answered = failed = 0
    for gate in GATES:
        for unit_id in unit.unit_ids:
            for result in cache.results(gate, unit_id):
                answered += 1
                failed += result.outcome is Outcome.FAILED
    if not answered:
        return None
    return failed == 0


__all__ = [
    "GATES",
    "GateRow",
    "Job",
    "Plan",
    "SelectionError",
    "SurveyUnit",
    "accessions_for",
    "as_result",
    "estimate",
    "gate_passed",
    "gate_report",
    "link_fanout",
    "memoizable",
    "plan",
    "run_jobs",
    "select_links",
    "units_of",
]
