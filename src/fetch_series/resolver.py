"""The fallback engine: try ranked routes, merge, and record where each answer came from.

The design is not a matter of taste; each part of it answers something a census
found.

**Union, not a fallback chain.** The two GEO routes fail on disjoint
populations: `elink gds->sra` returns nothing for 2,293 of 13,045 series, while
GEO omits the sample-level SRA relation for 16. No ordering of them answers
every series, so :attr:`Mode.UNION` asks every applicable route and merges.

**Provenance per value, not per record.** When routes disagree the resolver must
be able to say which route produced what. GSE114373 is the reason: its two routes
return equal-sized, completely disjoint sets of experiments, and only one side
has data. A merged set with no provenance cannot express that.

**Confirmed versus unconfirmed.** An experiment accession is not evidence that
data exists -- roughly 4,238 of the 308,639 experiments GEO's SOFT files name
have no runs. A route flagged :attr:`Route.proves_data_exists` only ever returns
entities that carry data, so its agreement is a confirmation.

**Archive-aware selection.** NCBI's index resolved BioProject -> BioSample for
99.6% of NCBI-issued projects and 1.4% of EBI-issued ones, so a route that
cannot answer for an accession's issuing archive is never called.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

from fetch_series.accession import Accession, EntityType
from fetch_series.graph import REGISTRY, Route, RouteRegistry
from fetch_series.routes import IMPLEMENTATIONS
from fetch_series.survey.client import SurveyClient
from fetch_series.survey.runner import RouteFn

logger = logging.getLogger(__name__)


class Mode(StrEnum):
    """How hard to look."""

    FIRST = "first"
    """Stop at the first route that returns anything. Cheapest, and wrong for
    any direction where routes are known to disagree."""

    UNION = "union"
    """Ask every applicable route and merge. The default, because for the one
    direction measured in full no single route was sufficient."""


@dataclass(frozen=True, slots=True)
class Attribution:
    """Which route produced a value, and when."""

    route_id: str
    proves_data_exists: bool
    fetched_at: datetime


@dataclass(slots=True)
class Resolution:
    """The merged answer, with every value's origin recorded."""

    source: Accession
    target: EntityType
    mode: Mode
    attributions: dict[str, list[Attribution]] = field(default_factory=dict)
    routes_resolved: dict[str, int] = field(default_factory=dict)
    routes_empty: list[str] = field(default_factory=list)
    routes_failed: dict[str, str] = field(default_factory=dict)
    routes_skipped: dict[str, str] = field(default_factory=dict)

    @property
    def values(self) -> list[str]:
        """Every accession found, by any route."""
        return sorted(self.attributions)

    @property
    def confirmed(self) -> list[str]:
        """Values a data-proving route vouched for.

        These carry runs. Everything else may or may not.
        """
        return sorted(
            value
            for value, attrs in self.attributions.items()
            if any(a.proves_data_exists for a in attrs)
        )

    @property
    def unconfirmed(self) -> list[str]:
        """Values no data-proving route returned.

        Not necessarily wrong -- a data-proving route may simply not have been
        applicable, or may key its query on something that misses them. But
        these are the values that can silently download nothing.
        """
        return sorted(set(self.attributions) - set(self.confirmed))

    @property
    def agreed(self) -> list[str]:
        """Values that more than one route returned."""
        return sorted(v for v, attrs in self.attributions.items() if len(attrs) > 1)

    @property
    def single_route_only(self) -> dict[str, str]:
        """Values only one route found, and which route that was.

        The interesting column when two routes disagree: a value nobody
        corroborates is either a gap in the other routes or a stale record in
        this one, and the resolver cannot tell which.
        """
        return {
            value: attrs[0].route_id
            for value, attrs in self.attributions.items()
            if len(attrs) == 1
        }

    @property
    def disagreed(self) -> bool:
        """Whether the routes that answered returned different sets."""
        answering = [r for r, n in self.routes_resolved.items() if n]
        if len(answering) < 2:
            return False
        per_route = {
            route: {
                v
                for v, attrs in self.attributions.items()
                if any(a.route_id == route for a in attrs)
            }
            for route in answering
        }
        return len(set(map(frozenset, per_route.values()))) > 1

    def explain(self) -> list[str]:
        """Human-readable account of how the answer was reached."""
        lines = [f"{self.source} -> {self.target} ({self.mode})"]
        for route, reason in self.routes_skipped.items():
            lines.append(f"  skipped  {route}: {reason}")
        for route, n in self.routes_resolved.items():
            lines.append(f"  resolved {route}: {n} results")
        for route in self.routes_empty:
            lines.append(f"  empty    {route}: the archive holds no such link")
        for route, error in self.routes_failed.items():
            lines.append(f"  failed   {route}: {error}")
        lines.append(
            f"  -> {len(self.values)} values, {len(self.confirmed)} confirmed to carry data"
        )
        if self.unconfirmed:
            lines.append(f"  -> {len(self.unconfirmed)} unconfirmed; these may resolve to no files")
        if self.disagreed:
            lines.append("  -> routes disagreed; see single_route_only")
        return lines


async def resolve(
    accession: Accession,
    target: EntityType,
    client: SurveyClient,
    *,
    mode: Mode = Mode.UNION,
    registry: RouteRegistry = REGISTRY,
    implementations: Mapping[str, RouteFn] = IMPLEMENTATIONS,
) -> Resolution:
    """Resolve ``accession`` to ``target``, trying routes in measured order."""
    resolution = Resolution(source=accession, target=target, mode=mode)

    candidates: list[Route] = []
    for route in registry.ranked(accession.entity, target):
        if not route.accepts(accession):
            resolution.routes_skipped[route.id] = (
                f"provider does not index {accession.archive}-issued accessions"
            )
        elif route.id not in implementations:
            resolution.routes_skipped[route.id] = "declared but not implemented"
        else:
            candidates.append(route)

    if not candidates:
        return resolution

    async def run(route: Route) -> tuple[Route, list[str] | BaseException]:
        fn = implementations[route.id]

        async def attempt(timeout: float) -> list[str]:
            return await fn(accession.value, client, timeout)

        try:
            return route, await client.with_retry(attempt, label=f"{route.id} {accession}")
        except Exception as exc:
            return route, exc

    outcomes: list[tuple[Route, list[str] | BaseException]]
    if mode is Mode.UNION:
        outcomes = list(await asyncio.gather(*(run(route) for route in candidates)))
    else:
        outcomes = []
        for route in candidates:
            attempted = await run(route)
            outcomes.append(attempted)
            if isinstance(attempted[1], list) and attempted[1]:
                break

    now = datetime.now(UTC)
    for route, outcome in outcomes:
        if isinstance(outcome, BaseException):
            resolution.routes_failed[route.id] = f"{type(outcome).__name__}: {outcome}"
            continue
        if not outcome:
            resolution.routes_empty.append(route.id)
            continue
        resolution.routes_resolved[route.id] = len(outcome)
        for value in outcome:
            resolution.attributions.setdefault(value, []).append(
                Attribution(
                    route_id=route.id,
                    proves_data_exists=route.proves_data_exists,
                    fetched_at=now,
                )
            )
    return resolution
