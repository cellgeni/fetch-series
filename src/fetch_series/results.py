"""The verdict a route returns for one accession."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum


class Outcome(StrEnum):
    """What happened when a route was asked about one accession.

    Three states, not two. The survey scripts this replaces carried a boolean
    ``success``, which forced a genuine "this BioProject has no GEO record" into
    the same bucket as "the request blew up". Most BioProjects link only to SRA
    and BioSample, so :attr:`EMPTY` is the common, correct answer -- and
    reporting it as failure is what made the BioProject -> GEO route look far
    worse than it is.

    The inverse mistake is equally easy: an empty result is never evidence that
    a route *worked*. :attr:`EMPTY` is a real answer about the archive, not a
    licence to stop looking -- the resolver still falls through to the next
    route.
    """

    RESOLVED = "resolved"
    EMPTY = "empty"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class RouteResult:
    """One cell of the survey matrix: (corpus, route, accession) -> verdict."""

    accession: str
    route_id: str
    corpus: str
    outcome: Outcome
    results: tuple[str, ...] = ()
    error_class: str | None = None
    error_message: str | None = None
    latency_ms: int = 0
    fetched_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def n_results(self) -> int:
        return len(self.results)

    @classmethod
    def from_results(
        cls,
        accession: str,
        route_id: str,
        corpus: str,
        results: list[str],
        latency_ms: int,
    ) -> RouteResult:
        """Build a verdict from a successful call, choosing RESOLVED vs EMPTY."""
        unique = tuple(sorted(set(results)))
        return cls(
            accession=accession,
            route_id=route_id,
            corpus=corpus,
            outcome=Outcome.RESOLVED if unique else Outcome.EMPTY,
            results=unique,
            latency_ms=latency_ms,
        )

    @classmethod
    def from_error(
        cls,
        accession: str,
        route_id: str,
        corpus: str,
        error: BaseException,
        latency_ms: int,
    ) -> RouteResult:
        """Build a failure verdict.

        A failure is recorded, never dropped: "queried and failed" must stay
        distinguishable from "never queried", or the coverage denominator is a
        guess.
        """
        return cls(
            accession=accession,
            route_id=route_id,
            corpus=corpus,
            outcome=Outcome.FAILED,
            error_class=type(error).__name__,
            error_message=str(error)[:500],
            latency_ms=latency_ms,
        )
