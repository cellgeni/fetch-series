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

import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass

from fetch_series.catalogue import CATALOGUE, Catalogue, Link
from fetch_series.catalogue.probe import Finding, answer_links, seeded_runs
from fetch_series.results import Outcome, RouteResult
from fetch_series.survey.client import SurveyClient
from fetch_series.survey.runner import SHUFFLE_SEED, FanoutFn
from fetch_series.units import link_unit, parse_unit


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


__all__ = ["SurveyUnit", "as_result", "link_fanout", "units_of"]
