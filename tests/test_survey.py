"""Offline tests for the survey harness."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from fetch_series.accession import EntityType
from fetch_series.cache import SurveyCache
from fetch_series.graph import Route, RouteCost
from fetch_series.results import Outcome, RouteResult
from fetch_series.survey import (
    Limits,
    MalformedResponseError,
    SurveyClient,
    is_retryable,
    run_route,
)
from fetch_series.survey.client import RateLimiter
from fetch_series.survey.corpora import hard_cases

ROUTE = Route(
    id="test->run:fake",
    source=EntityType.BIOPROJECT,
    target=EntityType.RUN,
    provider="fake",
    summary="a fake route",
    kb_page="routes/fake.md",
    cost=RouteCost(requests=1, rate_limit_rps=100.0),
)


@pytest.fixture
def cache(tmp_path) -> SurveyCache:
    with SurveyCache(tmp_path / "c.sqlite") as c:
        yield c


@pytest.fixture
def client() -> SurveyClient:
    return SurveyClient(limits=Limits(rps=0, concurrency=4, attempts=2, base_timeout=1.0))


class TestOutcome:
    def test_results_make_a_resolved_verdict(self):
        r = RouteResult.from_results("PRJNA1", "r", "c", ["SRR1", "SRR2"], 10)
        assert r.outcome is Outcome.RESOLVED
        assert r.n_results == 2

    def test_no_results_is_empty_not_failed(self):
        """Most BioProjects link only to SRA; 'no GEO record' is a true negative."""
        r = RouteResult.from_results("PRJNA1", "r", "c", [], 10)
        assert r.outcome is Outcome.EMPTY
        assert r.error_class is None

    def test_results_are_deduplicated_and_sorted(self):
        r = RouteResult.from_results("PRJNA1", "r", "c", ["SRR2", "SRR1", "SRR2"], 10)
        assert r.results == ("SRR1", "SRR2")

    def test_errors_keep_their_class(self):
        r = RouteResult.from_error("PRJNA1", "r", "c", KeyError("result"), 10)
        assert r.outcome is Outcome.FAILED
        assert r.error_class == "KeyError"


class TestRetryClassification:
    @pytest.mark.parametrize(
        "exc",
        [
            httpx.ConnectError("boom"),
            httpx.ReadTimeout("slow"),
            MalformedResponseError("no result key"),
            KeyError("result"),
        ],
    )
    def test_transient_failures_are_retryable(self, exc):
        assert is_retryable(exc)

    @pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
    def test_retryable_statuses(self, status):
        response = httpx.Response(status, request=httpx.Request("GET", "https://x"))
        assert is_retryable(httpx.HTTPStatusError("e", request=response.request, response=response))

    @pytest.mark.parametrize("status", [400, 401, 403, 404])
    def test_client_errors_are_not_retryable(self, status):
        """A 400 from an empty api_key is a bug to fix, not a blip to retry."""
        response = httpx.Response(status, request=httpx.Request("GET", "https://x"))
        assert not is_retryable(
            httpx.HTTPStatusError("e", request=response.request, response=response)
        )

    def test_value_errors_are_not_retryable(self):
        assert not is_retryable(ValueError("bad input"))


class TestLimits:
    def test_timeout_grows_with_each_attempt(self):
        """PRJEB9859 is not flaky, it is big. Retrying the same deadline cannot help."""
        limits = Limits(base_timeout=10.0, timeout_growth=2.0, max_timeout=60.0)
        assert limits.timeout_for(1) == 10.0
        assert limits.timeout_for(2) == 20.0
        assert limits.timeout_for(3) == 40.0

    def test_timeout_is_capped(self):
        limits = Limits(base_timeout=10.0, timeout_growth=2.0, max_timeout=25.0)
        assert limits.timeout_for(5) == 25.0

    async def test_rate_limiter_spaces_requests(self):
        limiter = RateLimiter(rps=50.0)  # 20 ms apart
        loop = asyncio.get_running_loop()
        start = loop.time()
        for _ in range(4):
            await limiter.acquire()
        # Three gaps of 20 ms; the first acquire is free.
        assert loop.time() - start >= 0.055

    async def test_rate_limiter_does_not_sleep_when_idle(self):
        """The old implementation slept the full interval on every call."""
        limiter = RateLimiter(rps=10.0)
        loop = asyncio.get_running_loop()
        await limiter.acquire()
        await asyncio.sleep(0.15)
        start = loop.time()
        await limiter.acquire()
        assert loop.time() - start < 0.05


class TestRunner:
    async def test_records_a_verdict_for_every_accession(self, cache, client):
        async def fn(accession, _client, _timeout):
            return [f"SRR{accession[-1]}"]

        run = await run_route(ROUTE, ["PRJNA1", "PRJNA2"], fn, cache, "c", client)
        assert (run.resolved, run.empty, run.failed) == (2, 0, 0)
        assert run.unique_results == 2
        assert cache.answered("c", ROUTE.id) == {"PRJNA1", "PRJNA2"}

    async def test_failures_are_recorded_not_dropped(self, cache, client):
        """'Queried and failed' must stay distinguishable from 'never queried',
        or the coverage denominator is a guess."""

        async def fn(accession, _client, _timeout):
            if accession == "PRJNA2":
                raise ValueError("no such project")
            return ["SRR1"]

        run = await run_route(ROUTE, ["PRJNA1", "PRJNA2"], fn, cache, "c", client)
        assert (run.resolved, run.failed) == (1, 1)
        assert cache.failed("c", ROUTE.id) == {"PRJNA2"}
        assert cache.answered("c", ROUTE.id) == {"PRJNA1", "PRJNA2"}

    async def test_empty_is_distinguished_from_failed(self, cache, client):
        async def fn(accession, _client, _timeout):
            return []

        run = await run_route(ROUTE, ["PRJNA1"], fn, cache, "c", client)
        assert (run.empty, run.failed) == (1, 0)
        assert run.failure_rate == 0.0

    async def test_resume_skips_answered_accessions(self, cache, client):
        calls: list[str] = []

        async def fn(accession, _client, _timeout):
            calls.append(accession)
            return ["SRR1"]

        await run_route(ROUTE, ["PRJNA1"], fn, cache, "c", client)
        run = await run_route(ROUTE, ["PRJNA1", "PRJNA2"], fn, cache, "c", client)
        assert calls == ["PRJNA1", "PRJNA2"]
        assert run.skipped == 1
        assert run.queried == 1

    async def test_failures_are_not_retried_by_default(self, cache, client):
        """A survey that quietly retried its failures would report a coverage
        figure no single run ever achieved."""
        attempts: list[str] = []

        async def fn(accession, _client, _timeout):
            attempts.append(accession)
            raise ValueError("nope")

        await run_route(ROUTE, ["PRJNA1"], fn, cache, "c", client)
        run = await run_route(ROUTE, ["PRJNA1"], fn, cache, "c", client)
        assert len(attempts) == 1
        assert run.skipped == 1

    async def test_retry_failed_reruns_only_failures(self, cache, client):
        attempts: list[str] = []

        async def fn(accession, _client, _timeout):
            attempts.append(accession)
            if accession == "PRJNA2":
                raise ValueError("nope")
            return ["SRR1"]

        await run_route(ROUTE, ["PRJNA1", "PRJNA2"], fn, cache, "c", client)
        attempts.clear()
        await run_route(ROUTE, ["PRJNA1", "PRJNA2"], fn, cache, "c", client, retry_failed=True)
        assert attempts == ["PRJNA2"]

    async def test_concurrency_is_bounded(self, cache):
        """At most `concurrency` accessions in flight, whatever the corpus size."""
        client = SurveyClient(limits=Limits(rps=0, concurrency=3, attempts=1))
        in_flight = 0
        peak = 0

        async def fn(accession, _client, _timeout):
            nonlocal in_flight, peak
            in_flight += 1
            peak = max(peak, in_flight)
            await asyncio.sleep(0.01)
            in_flight -= 1
            return ["SRR1"]

        await run_route(ROUTE, [f"PRJNA{i}" for i in range(30)], fn, cache, "c", client)
        assert peak <= 3

    async def test_retries_a_transient_failure(self, cache):
        client = SurveyClient(limits=Limits(rps=0, concurrency=1, attempts=3, base_timeout=0.1))
        attempts = 0

        async def fn(accession, _client, _timeout):
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise MalformedResponseError("no result key")
            return ["SRR1"]

        run = await run_route(ROUTE, ["PRJNA1"], fn, cache, "c", client)
        assert attempts == 3
        assert run.resolved == 1


class TestCache:
    def test_summary_counts_by_outcome(self, cache):
        for accession, results in (("A", ["SRR1"]), ("B", []), ("C", ["SRR1", "SRR2"])):
            cache.record(RouteResult.from_results(accession, "r", "c", results, 1))
        cache.record(RouteResult.from_error("D", "r", "c", ValueError("x"), 1))

        summary = cache.summary("c", "r")
        assert summary["resolved"] == 2
        assert summary["empty"] == 1
        assert summary["failed"] == 1
        assert summary["queried"] == 4
        # Deduplicated across accessions -- the number a route ranking cites.
        assert summary["unique_results"] == 2

    def test_recording_the_same_cell_twice_replaces_it(self, cache):
        cache.record(RouteResult.from_error("A", "r", "c", ValueError("x"), 1))
        cache.record(RouteResult.from_results("A", "r", "c", ["SRR1"], 1))
        assert cache.summary("c", "r")["failed"] == 0
        assert cache.summary("c", "r")["resolved"] == 1

    def test_diff_detects_a_re_created_project(self, tmp_path):
        """PRJNA644294 -> PRJNA644462 was found by hand; it should not have to be."""
        with (
            SurveyCache(tmp_path / "before.sqlite") as before,
            SurveyCache(tmp_path / "after.sqlite") as after,
        ):
            before.record(RouteResult.from_results("PRJNA644294", "r", "c", ["GSE153824"], 1))
            before.record(RouteResult.from_results("PRJNA1", "r", "c", ["GSE1"], 1))
            after.record(RouteResult.from_results("PRJNA644294", "r", "c", [], 1))
            after.record(RouteResult.from_results("PRJNA1", "r", "c", ["GSE1"], 1))

            diff = after.diff("c", "r", before)
            assert set(diff["changed"]) == {"PRJNA644294"}
            entry = diff["changed"]["PRJNA644294"]
            assert entry["before"] == ["GSE153824"]
            assert entry["after"] == []
            assert entry["after_outcome"] == "empty"

    def test_roundtrips_results(self, cache):
        stored = RouteResult(
            accession="A",
            route_id="r",
            corpus="c",
            outcome=Outcome.RESOLVED,
            results=("SRR1",),
            latency_ms=42,
            fetched_at=datetime.now(UTC) - timedelta(days=1),
        )
        cache.record(stored)
        loaded = next(cache.results("c", "r"))
        assert loaded == stored

    def test_response_cache_roundtrip(self, cache):
        cache.store_response("k", "eutils", "https://x", 200, b"body")
        assert cache.load_response("k") == (200, b"body")
        assert cache.load_response("missing") is None


class TestCorpora:
    def test_hard_cases_all_parse(self):
        corpus = hard_cases()
        assert corpus.unparsed == ()
        assert len(corpus) == len(corpus.values)

    def test_hard_cases_span_several_entity_types(self):
        """A tier-one corpus made only of BioProjects would not exercise the graph."""
        corpus = hard_cases()
        present = {a.entity for a in corpus.accessions}
        assert EntityType.GEO_SERIES in present
        assert EntityType.BIOPROJECT in present
        assert EntityType.AE_EXPERIMENT in present
        assert EntityType.SAMPLE in present

    def test_of_type_filters(self):
        corpus = hard_cases()
        assert all(v.startswith("GSE") for v in corpus.of_type(EntityType.GEO_SERIES))

    def test_every_hard_case_says_why_it_is_hard(self):
        from fetch_series.survey.corpora import HARD_CASES

        assert all(len(reason) > 20 for reason in HARD_CASES.values())


class TestVisitOrder:
    """Slicing a corpus before shuffling is a biased sample dressed as a limit."""

    def test_shuffled_is_deterministic(self):
        from fetch_series.survey.runner import shuffled

        values = [f"GSE{i}" for i in range(500)]
        assert shuffled(values) == shuffled(values)

    def test_shuffled_preserves_membership(self):
        from fetch_series.survey.runner import shuffled

        values = [f"GSE{i}" for i in range(500)]
        assert sorted(shuffled(values)) == sorted(values)

    def test_a_prefix_of_the_shuffle_is_not_a_prefix_of_the_sort(self):
        """The bug this guards.

        Corpora arrive sorted, so the first N of one are the N lowest accession
        numbers -- the oldest submissions. An early 150-series run sliced that
        way covered only the oldest fifth of GEO, which made six ELink failures
        all look like old accessions and produced an age hypothesis the full
        census then disproved.
        """
        from fetch_series.survey.runner import shuffled

        values = sorted((f"GSE{i}" for i in range(1000)), key=lambda a: int(a[3:]))
        sorted_prefix = [int(a[3:]) for a in values[:100]]
        visit_prefix = [int(a[3:]) for a in shuffled(values)[:100]]
        assert max(sorted_prefix) < 200  # the sorted prefix is all low numbers
        assert max(visit_prefix) > 800  # the visit prefix spans the range


class TestSurveyDerivedCorpora:
    """The results of one survey are the inputs of the next."""

    def test_builds_a_corpus_from_recorded_results(self, tmp_path):
        from fetch_series.cache import SurveyCache
        from fetch_series.survey.corpora import from_survey

        path = tmp_path / "c.sqlite"
        with SurveyCache(path) as cache:
            cache.record(RouteResult.from_results("GSE1", "r", "c", ["SRX1", "SRX2"], 1))
            cache.record(RouteResult.from_results("GSE2", "r", "c", ["SRX2", "SRX3"], 1))

        corpus = from_survey("r", "c", cache_path=path)
        assert corpus.values == ("SRX1", "SRX2", "SRX3")
        assert "r" in corpus.source and "c" in corpus.source

    def test_refuses_to_build_from_nothing(self, tmp_path):
        """An empty corpus would survey zero accessions and report success."""
        from fetch_series.survey.corpora import from_survey

        with pytest.raises(ValueError, match="No recorded results"):
            from_survey("missing", "c", cache_path=tmp_path / "c.sqlite")

    def test_name_round_trips_through_load(self, tmp_path):
        from fetch_series.cache import SurveyCache
        from fetch_series.survey.corpora import from_survey

        path = tmp_path / "c.sqlite"
        with SurveyCache(path) as cache:
            cache.record(RouteResult.from_results("GSE1", "a->b:x", "corp", ["SRX1"], 1))
        corpus = from_survey("a->b:x", "corp", cache_path=path)
        # The name records exactly which survey produced the population, so a
        # result can be traced back to the question that generated its inputs.
        assert corpus.name == "results-of:a->b:x@corp"
