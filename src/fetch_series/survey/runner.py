"""Run one route over one corpus, resumably."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any

from fetch_series.cache import SurveyCache
from fetch_series.graph import Route
from fetch_series.results import Outcome, RouteResult
from fetch_series.survey.client import SurveyClient

logger = logging.getLogger(__name__)

# Fixed so a census visits accessions in the same order every time.
SHUFFLE_SEED = 20260915

# Given an accession, a client and this attempt's deadline, return the target
# accessions the route found. Returning an empty list is a legitimate answer
# (the archive holds no link), not an error -- raise to report a failure.
RouteFn = Callable[[str, SurveyClient, float], Awaitable[list[str]]]


def shuffled(accessions: Sequence[str]) -> list[str]:
    """The corpus in its seeded visit order.

    Exposed because slicing a corpus *before* shuffling is a trap. Corpora
    arrive sorted, so taking the first N of one yields the N lowest accession
    numbers -- the oldest submissions -- not a sample. That bias is what made
    six ELink failures in an early 150-series run all look like old accessions,
    and prompted an age hypothesis the full census then disproved.
    """
    ordered = list(accessions)
    random.Random(SHUFFLE_SEED).shuffle(ordered)
    return ordered


@dataclass(frozen=True, slots=True)
class SurveyRun:
    """What a completed run of one route over one corpus produced."""

    route_id: str
    corpus: str
    queried: int
    skipped: int
    resolved: int
    empty: int
    failed: int
    unique_results: int
    elapsed_s: float

    @property
    def failure_rate(self) -> float:
        answered = self.resolved + self.empty + self.failed
        return self.failed / answered if answered else 1.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "route_id": self.route_id,
            "corpus": self.corpus,
            "queried": self.queried,
            "skipped": self.skipped,
            "resolved": self.resolved,
            "empty": self.empty,
            "failed": self.failed,
            "unique_results": self.unique_results,
            "failure_rate": round(self.failure_rate, 6),
            "elapsed_s": round(self.elapsed_s, 1),
        }


async def run_route(
    route: Route,
    accessions: Sequence[str],
    fn: RouteFn,
    cache: SurveyCache,
    corpus: str,
    client: SurveyClient,
    *,
    resume: bool = True,
    retry_failed: bool = False,
    on_result: Callable[[RouteResult], None] | None = None,
) -> SurveyRun:
    """Query every accession in ``accessions`` over ``route``.

    Args:
        resume: skip accessions that already carry a verdict for this
            (corpus, route) pair. A survey of this size will be interrupted.
        retry_failed: also re-query accessions whose stored verdict is a
            failure. Off by default on purpose -- a survey that quietly retried
            its own failures would report a coverage figure no single run ever
            achieved, which is not something you can cite in a route ranking.

    Every accession gets a verdict recorded before the next one is attempted, so
    an interrupted run loses at most the work in flight.
    """
    started = time.monotonic()

    # Shuffled, deterministically. Corpora arrive in sorted order, which puts
    # every PRJDB and PRJEB accession before the first PRJNA -- so the opening
    # thousand results of a BioProject census are all non-NCBI projects and its
    # interim empty rate reads as 91% when the true figure is nothing like it.
    # A fixed seed keeps the order reproducible; resume is keyed on the
    # accession set, so shuffling cannot disturb it.
    order = shuffled(accessions)
    todo = order
    skipped = 0
    if resume:
        done = cache.answered(corpus, route.id)
        if retry_failed:
            done -= cache.failed(corpus, route.id)
        # Filter the *shuffled* list. This line once read `accessions`, so every
        # resumed run silently went back to sorted order and spent its remaining
        # budget on the oldest submissions -- the bias the shuffle exists to stop.
        todo = [a for a in order if a not in done]
        skipped = len(order) - len(todo)
        if skipped:
            logger.info("%s: skipping %d already-answered accessions", route.id, skipped)

    counts = {Outcome.RESOLVED: 0, Outcome.EMPTY: 0, Outcome.FAILED: 0}
    distinct: set[str] = set()
    lock = asyncio.Lock()

    async def query(accession: str) -> None:
        call_started = time.monotonic()
        try:
            found = await client.with_retry(
                lambda timeout: fn(accession, client, timeout), label=f"{route.id} {accession}"
            )
            result = RouteResult.from_results(
                accession=accession,
                route_id=route.id,
                corpus=corpus,
                results=found,
                latency_ms=int((time.monotonic() - call_started) * 1000),
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            result = RouteResult.from_error(
                accession=accession,
                route_id=route.id,
                corpus=corpus,
                error=exc,
                latency_ms=int((time.monotonic() - call_started) * 1000),
            )
            logger.warning("%s failed for %s: %s", route.id, accession, exc)

        # SQLite connections are not safe to share across concurrent writers.
        async with lock:
            cache.record(result)
            counts[result.outcome] += 1
            distinct.update(result.results)
        if on_result is not None:
            on_result(result)

    # A worker pool, not `gather` over the whole corpus. The scripts this
    # replaces created one coroutine per accession and handed all 12,756 to
    # gather at once; here at most `concurrency` accessions are ever in flight,
    # so a slow project holds up one worker rather than sharing a deadline with
    # thousands of queued peers.
    queue: asyncio.Queue[str] = asyncio.Queue()
    for accession in todo:
        queue.put_nowait(accession)

    async def worker() -> None:
        while True:
            try:
                accession = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                await query(accession)
            finally:
                queue.task_done()

    workers = min(client.limits.concurrency, len(todo)) or 1
    await asyncio.gather(*(worker() for _ in range(workers)))

    return SurveyRun(
        route_id=route.id,
        corpus=corpus,
        queried=len(todo),
        skipped=skipped,
        resolved=counts[Outcome.RESOLVED],
        empty=counts[Outcome.EMPTY],
        failed=counts[Outcome.FAILED],
        unique_results=len(distinct),
        elapsed_s=time.monotonic() - started,
    )


# Given an accession and the unit ids it still owes, make the one request those
# units share and return one verdict per owed id. A FAILED verdict is a verdict;
# raising fails every owed id with the error recorded.
FanoutFn = Callable[[str, frozenset[str]], Awaitable[list[RouteResult]]]


@dataclass(frozen=True, slots=True)
class FanoutRun:
    """What one fan-out run produced: accessions visited, verdicts by unit."""

    corpus: str
    queried: int
    skipped: int
    units: dict[str, dict[str, int]]
    elapsed_s: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "corpus": self.corpus,
            "queried": self.queried,
            "skipped": self.skipped,
            "units": self.units,
            "elapsed_s": round(self.elapsed_s, 1),
        }


async def run_fanout(
    unit_ids: Sequence[str],
    accessions: Sequence[str],
    fn: FanoutFn,
    cache: SurveyCache,
    corpus: str,
    *,
    workers: int,
    resume: bool = True,
    retry_failed: bool = False,
    on_result: Callable[[RouteResult], None] | None = None,
) -> FanoutRun:
    """Ask every accession for every unit, one request per accession.

    One request answers several links: an ENA ``filereport`` on a run names its
    experiment, sample, study and BioProject at once. A unit-at-a-time runner
    would make that request once per link, which is what made the plan's
    260,000 requests a million.

    An accession is done only when every owed unit has a verdict. One that is
    partly answered -- a run interrupted between two writes, a unit added to the
    survey later -- is asked again for only what it is missing, and its verdicts
    are written in one transaction, so a half-written accession cannot occur.

    Args:
        workers: accessions in flight at once. The client's own limits pace the
            requests; this bounds the coroutines.
        retry_failed: re-ask units whose stored verdict is a failure. Off by
            default for the reason ``run_route`` gives.
    """
    started = time.monotonic()
    units = list(dict.fromkeys(unit_ids))
    wanted = frozenset(units)
    if not wanted:
        raise ValueError("A fan-out survey needs at least one unit id")

    # Seeded order, as run_route: a sorted corpus puts every PRJDB before the
    # first PRJNA, and resume filters this list, never the sorted one.
    order = shuffled(accessions)
    have: dict[str, set[str]] = {}
    if resume:
        have = cache.answered_by(corpus, wanted)
        if retry_failed:
            for accession, failed in cache.answered_by(corpus, wanted, failed_only=True).items():
                have[accession] -= failed
    todo = [(a, wanted - have.get(a, set())) for a in order]
    todo = [(a, owed) for a, owed in todo if owed]
    skipped = len(order) - len(todo)
    if skipped:
        logger.info("fan-out over %s: skipping %d fully answered accessions", corpus, skipped)

    counts: dict[str, dict[str, int]] = {
        unit: {o.value: 0 for o in Outcome} | {"distinct": 0} for unit in units
    }
    distinct: dict[str, set[str]] = {unit: set() for unit in units}
    lock = asyncio.Lock()

    async def query(accession: str, owed: frozenset[str]) -> None:
        call_started = time.monotonic()
        try:
            found = await fn(accession, owed)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            latency = int((time.monotonic() - call_started) * 1000)
            found = [RouteResult.from_error(accession, unit, corpus, exc, latency) for unit in owed]
            logger.warning("fan-out failed for %s: %s", accession, found[0].error_message)
        by_unit = {result.route_id: result for result in found}
        unexpected = set(by_unit) - owed
        if unexpected or len(by_unit) != len(found):
            # A verdict for a unit nobody asked about, or two for one unit, is a
            # bug in the fan-out function, and recording it would attribute an
            # answer to the wrong link. Fail loudly.
            raise ValueError(
                f"fan-out for {accession} returned unowed or duplicate units: {sorted(unexpected)}"
            )
        latency = int((time.monotonic() - call_started) * 1000)
        for unit in owed - set(by_unit):
            # Owed and not answered is recorded as a failure, never left absent:
            # absence means "never queried", which this accession was.
            by_unit[unit] = RouteResult.from_error(
                accession, unit, corpus, LookupError("the fan-out returned no verdict"), latency
            )
        results = [
            RouteResult(
                accession=accession,
                route_id=r.route_id,
                corpus=corpus,
                outcome=r.outcome,
                results=r.results,
                error_class=r.error_class,
                error_message=r.error_message,
                latency_ms=r.latency_ms,
                fetched_at=r.fetched_at,
                detail=r.detail,
            )
            for r in sorted(by_unit.values(), key=lambda r: r.route_id)
        ]
        async with lock:
            cache.record_all(results)
            for result in results:
                counts[result.route_id][result.outcome.value] += 1
                distinct[result.route_id].update(result.results)
        if on_result is not None:
            for result in results:
                on_result(result)

    queue: asyncio.Queue[tuple[str, frozenset[str]]] = asyncio.Queue()
    for accession, owed in todo:
        queue.put_nowait((accession, frozenset(owed)))

    async def worker() -> None:
        while True:
            try:
                accession, owed = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                await query(accession, owed)
            finally:
                queue.task_done()

    await asyncio.gather(*(worker() for _ in range(min(workers, len(todo)) or 1)))
    for unit in units:
        counts[unit]["distinct"] = len(distinct[unit])
    return FanoutRun(
        corpus=corpus,
        queried=len(todo),
        skipped=skipped,
        units=counts,
        elapsed_s=time.monotonic() - started,
    )
