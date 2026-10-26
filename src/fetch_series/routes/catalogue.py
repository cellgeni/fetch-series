"""Routes read from a catalogue mechanism: the request a link survey measured, promoted.

A link survey measures whether a request answers, over a draw of 300, and never
becomes evidence. Promoting a link means surveying it as a *route*: at
``sample:3000``, with a function that answers for an accession and a
``RouteEvidence`` block. This module is the function, for the routes whose
request is already a catalogue mechanism.

The promoted route reads exactly what its link survey measured. It makes the
mechanism's own request (``catalogue.probe.PROBERS``) and reads it with the
probe's own extraction (``probe.answer_links``), restricted to the catalogue
links from the accession's node into every node of the target entity type. There
is no second implementation of any request to drift from the first.

A route from an entity type answers for whichever of its nodes the accession is:
``run->study:ena_filereport`` from an SRR reads the links ``run_ncbi ->
study_ncbi`` of that mechanism, from an ERR ``run_ebi -> study_ebi``.

Which routes are candidates is the catalogue's to say (:func:`candidates`): a
surveyed link in a direction no declared route covers. A direction that
already has a measured route gets no second one here; whether a second earns its
place is a question for its own measurement.
"""

from __future__ import annotations

import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from fetch_series.accession import Archive, EntityType, try_parse
from fetch_series.catalogue import CATALOGUE, Catalogue, Link, Probe, Status, load_probes
from fetch_series.catalogue.nodes import node_for
from fetch_series.catalogue.probe import PROBERS, Finding, answer_links
from fetch_series.graph import REGISTRY, RouteRegistry
from fetch_series.logging_utils import redact
from fetch_series.results import Outcome, RouteResult
from fetch_series.survey.client import SurveyClient
from fetch_series.survey.runner import FanoutFn

# Records about the data rather than levels of it: they stay catalogue links.
NOT_PROMOTED = frozenset({EntityType.SUBMISSION, EntityType.ANALYSIS, EntityType.GEO_PLATFORM})


def route_id(mechanism: str, source: EntityType, target: EntityType) -> str:
    """``bioproject->sample:sra_be_runinfo``: the scheme every route id follows."""
    return f"{source.value}->{target.value}:{mechanism}"


@dataclass(frozen=True, slots=True)
class Candidate:
    """One promotable route: a mechanism, read from one entity type into another."""

    mechanism: str
    source: EntityType
    target: EntityType
    links: tuple[Link, ...]

    @property
    def id(self) -> str:
        return route_id(self.mechanism, self.source, self.target)

    @property
    def source_archives(self) -> tuple[Archive, ...]:
        """The issuing archives its links answer from; empty where the source is shared."""
        archives = {CATALOGUE.nodes[link.source].archive for link in self.links}
        if None in archives:
            return ()
        return tuple(sorted(a for a in archives if a is not None))


def candidates(
    catalogue: Catalogue = CATALOGUE,
    registry: RouteRegistry = REGISTRY,
    *,
    probes: Mapping[str, Probe] | None = None,
) -> list[Candidate]:
    """Every surveyed or probed link no route covers, in a direction no route covers.

    Grouped by (mechanism, source entity, target entity): one route each.
    """
    known = probes if probes is not None else load_probes()
    covered = {(route.source, route.target) for route in registry}
    grouped: dict[tuple[str, EntityType, EntityType], list[Link]] = {}
    for link in catalogue.links:
        if link.route_id is not None:
            continue
        if catalogue.status(link, known) not in (Status.SURVEYED, Status.UNTESTED):
            continue
        source = catalogue.nodes[link.source].entity
        target = catalogue.nodes[link.target].entity
        if source is None or target is None or {source, target} & NOT_PROMOTED:
            continue
        if (source, target) in covered:
            continue
        grouped.setdefault((link.mechanism, source, target), []).append(link)
    return [
        Candidate(mechanism, source, target, tuple(sorted(links, key=lambda link: link.id)))
        for (mechanism, source, target), links in sorted(
            grouped.items(), key=lambda item: (item[0][1].value, item[0][2].value, item[0][0])
        )
    ]


def links_from(
    mechanism: str,
    accession: str,
    targets: Iterable[EntityType],
    catalogue: Catalogue = CATALOGUE,
) -> list[Link]:
    """The mechanism's links from the accession's own node into every node of ``targets``."""
    parsed = try_parse(accession)
    node = node_for(parsed) if parsed is not None else None
    wanted = set(targets)
    return [
        link
        for link in catalogue.links
        if link.mechanism == mechanism
        and link.source == node
        and catalogue.nodes[link.target].entity in wanted
    ]


class _Every(dict[str, SurveyClient]):
    """One client for every host bucket: what the resolver hands a route."""

    def __init__(self, client: SurveyClient) -> None:
        super().__init__()
        self._client = client

    def __missing__(self, key: str) -> SurveyClient:
        return self._client


@dataclass(frozen=True, slots=True)
class Verdict:
    """What one request said about one target entity type: the union over its nodes."""

    outcome: Outcome
    values: tuple[str, ...] = ()
    error: str | None = None
    note: str | None = None


def combine(findings: Iterable[Finding]) -> Verdict:
    """Findings for several nodes of one entity type as one route's verdict.

    Any value makes it resolved. Otherwise one failed finding makes it failed:
    one request answers them all, so a failure is the request's, and an empty
    from it would be a fabricated true negative.
    """
    found = list(findings)
    values = sorted({v for f in found for v in f.values})
    if values:
        return Verdict(Outcome.RESOLVED, tuple(values))
    failed = next((f for f in found if f.outcome is Outcome.FAILED), None)
    if failed is not None:
        return Verdict(Outcome.FAILED, error=failed.error)
    note = next((f.note for f in found if f.note), None)
    return Verdict(Outcome.EMPTY, note=note)


async def answer(
    mechanism: str,
    accession: str,
    targets: Iterable[EntityType],
    clients: Mapping[str, SurveyClient],
    catalogue: Catalogue = CATALOGUE,
) -> dict[EntityType, Verdict]:
    """One request, read for every target entity type: a verdict each."""
    wanted = list(dict.fromkeys(targets))
    links = links_from(mechanism, accession, wanted, catalogue)
    findings = (
        await answer_links(
            mechanism, accession, links, clients, catalogue=catalogue, make_checks=False
        )
        if links
        else []
    )
    by_target: dict[EntityType, list[Finding]] = {target: [] for target in wanted}
    by_link = {link.id: link for link in links}
    for finding in findings:
        target = catalogue.nodes[by_link[finding.link].target].entity
        if target is not None:
            by_target[target].append(finding)
    return {target: combine(found) for target, found in by_target.items()}


class RouteFailed(RuntimeError):
    """The request behind a promoted route did not complete. Already redacted."""


async def answer_route(
    mechanism: str,
    source: EntityType,
    target: EntityType,
    accession: str,
    client: SurveyClient,
    timeout: float,
) -> list[str]:
    """A promoted route as the resolver calls it: values, an empty list, or a raise."""
    del source, timeout  # the accession says its node; the prober keeps its own deadline
    verdict = (await answer(mechanism, accession, [target], _Every(client)))[target]
    if verdict.outcome is Outcome.FAILED:
        raise RouteFailed(redact(verdict.error or f"{mechanism} failed for {accession}"))
    return list(verdict.values)


def fanout(
    mechanism: str,
    routes: Iterable[Candidate],
    clients: Mapping[str, SurveyClient],
    corpus: str,
    catalogue: Catalogue = CATALOGUE,
) -> FanoutFn:
    """One request per accession, a route verdict for every promoted target it answers."""
    by_id = {candidate.id: candidate for candidate in routes}

    async def fn(accession: str, owed: frozenset[str]) -> list[RouteResult]:
        wanted = [by_id[unit] for unit in sorted(owed) if unit in by_id]
        started = time.monotonic()
        verdicts = await answer(
            mechanism, accession, [c.target for c in wanted], clients, catalogue
        )
        latency = int((time.monotonic() - started) * 1000)
        out = []
        for candidate in wanted:
            verdict = verdicts[candidate.target]
            if verdict.outcome is Outcome.FAILED:
                error_class, _, message = (verdict.error or "").partition(": ")
                out.append(
                    RouteResult(
                        accession=accession,
                        route_id=candidate.id,
                        corpus=corpus,
                        outcome=Outcome.FAILED,
                        error_class=error_class or None,
                        error_message=message or verdict.error,
                        latency_ms=latency,
                    )
                )
                continue
            out.append(
                RouteResult.from_results(
                    accession=accession,
                    route_id=candidate.id,
                    corpus=corpus,
                    results=list(verdict.values),
                    latency_ms=latency,
                    detail={"absent": verdict.note} if verdict.note else None,
                )
            )
        return out

    return fn


def host_of(mechanism: str) -> str:
    """The host bucket whose limits a mechanism's request runs under."""
    return PROBERS[mechanism].host


__all__ = [
    "NOT_PROMOTED",
    "Candidate",
    "RouteFailed",
    "Verdict",
    "answer",
    "answer_route",
    "candidates",
    "combine",
    "fanout",
    "host_of",
    "links_from",
    "route_id",
]
