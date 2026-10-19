"""The verdict a route returns for one accession."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from fetch_series.logging_utils import redact, redact_exception

# What a check on a found file or document said. ``None`` is "could not be
# determined", which is not a pass: a file whose completeness is unknown is not
# known to be valid. Here rather than in the catalogue because a survey verdict
# carries checks too, and the cache must not import the catalogue to store one.
Check = tuple[str, bool | None]


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
    """One cell of the survey matrix: (corpus, route, accession) -> verdict.

    ``route_id`` is a unit id, which for a declared route is the route id
    itself. A link survey records under ``link:<link id>`` and a content
    survey under ``screen:<reads>:<link id>``: a link id can be *identical* to a
    route id (``bioproject->biosample:ena_filereport`` is both), and the cache
    is keyed on (corpus, unit, accession), so an unprefixed link verdict would
    overwrite the route's.

    ``detail`` is structured measurement beside the verdict -- the checks a
    found file passed, what they were made on. Never sequences: it is
    committed with the export.
    """

    accession: str
    route_id: str
    corpus: str
    outcome: Outcome
    results: tuple[str, ...] = ()
    error_class: str | None = None
    error_message: str | None = None
    latency_ms: int = 0
    fetched_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    detail: dict[str, Any] | None = None

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
        detail: dict[str, Any] | None = None,
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
            detail=detail,
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

        The message is redacted here, not by the caller. It is stored in the
        cache and exported to a public repository, and httpx puts the request
        URL -- API key included -- into the message of most errors. A rule that
        every caller must remember is a rule one caller forgets.
        """
        return cls(
            accession=accession,
            route_id=route_id,
            corpus=corpus,
            outcome=Outcome.FAILED,
            error_class=type(error).__name__,
            error_message=redact(str(redact_exception(error)))[:500],
            latency_ms=latency_ms,
        )
