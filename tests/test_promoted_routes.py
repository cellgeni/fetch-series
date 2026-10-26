"""Offline tests for promoting catalogue links to routes (``routes.catalogue``)."""

from __future__ import annotations

import pytest

from fetch_series.accession import EntityType
from fetch_series.cache import SurveyCache
from fetch_series.catalogue.probe import PROBERS, Prober, RecordAbsent
from fetch_series.graph import PROMOTED, REGISTRY
from fetch_series.results import Outcome
from fetch_series.routes import IMPLEMENTATIONS
from fetch_series.routes.catalogue import (
    NOT_PROMOTED,
    RouteFailed,
    answer,
    answer_route,
    candidates,
    fanout,
)
from fetch_series.survey.runner import run_fanout

SAMPLE = EntityType.SAMPLE
STUDY = EntityType.STUDY


class _Client:
    async def with_retry(self, operation, *, label="", retryable=None):  # type: ignore[no-untyped-def]
        return await operation(1.0)


@pytest.fixture
def runinfo(monkeypatch):
    """sra-db-be runinfo answering from a fixed table, counting its requests."""
    calls: list[str] = []

    async def fetch(client, accession, timeout):
        calls.append(accession)
        if accession == "PRJNA404":
            raise RecordAbsent("withdrawn")
        if accession == "PRJNA500":
            raise RuntimeError("backend answered 500")
        return {
            "run": ["SRR1", "ERR9"],
            "sample": ["SRS1", "ERS9"],
            "study": ["SRP1"],
            "experiment": ["SRX1"],
        }

    monkeypatch.setitem(PROBERS, "sra_be_runinfo", Prober("ncbi_web", fetch))
    return calls


class TestCandidates:
    def test_only_directions_no_route_covers(self):
        covered = {(route.source, route.target) for route in REGISTRY}
        found = candidates()
        assert found and all((c.source, c.target) not in covered for c in found)

    def test_records_about_the_data_stay_links(self):
        assert not any({c.source, c.target} & NOT_PROMOTED for c in candidates())

    def test_ids_are_unique_and_follow_the_scheme(self):
        ids = [c.id for c in candidates()]
        assert len(ids) == len(set(ids))
        assert "bioproject->sample:sra_be_runinfo" in ids

    def test_a_route_from_a_shared_record_answers_for_any_archive(self):
        (c,) = [c for c in candidates() if c.id == "bioproject->sample:sra_be_runinfo"]
        assert c.source_archives == ()


class TestAnswer:
    async def test_a_shared_source_reads_every_archives_node(self, runinfo):
        verdicts = await answer(
            "sra_be_runinfo", "PRJNA1", [SAMPLE, STUDY], {"ncbi_web": _Client()}
        )
        assert verdicts[SAMPLE].values == ("ERS9", "SRS1")
        assert verdicts[STUDY].values == ("SRP1",)
        assert runinfo == ["PRJNA1"], "one request answers every target"

    async def test_an_archive_split_source_reads_its_own_archive(self, runinfo):
        verdicts = await answer("sra_be_runinfo", "SRR1", [SAMPLE], {"ncbi_web": _Client()})
        assert verdicts[SAMPLE].values == ("SRS1",), "an SRR links to SRSs, never to ERSs"

    async def test_an_absent_record_is_empty_and_says_why(self, runinfo):
        verdict = (await answer("sra_be_runinfo", "PRJNA404", [SAMPLE], {"ncbi_web": _Client()}))[
            SAMPLE
        ]
        assert verdict.outcome is Outcome.EMPTY and verdict.note

    async def test_a_failed_request_is_failed_never_empty(self, runinfo):
        verdict = (await answer("sra_be_runinfo", "PRJNA500", [SAMPLE], {"ncbi_web": _Client()}))[
            SAMPLE
        ]
        assert verdict.outcome is Outcome.FAILED and "500" in (verdict.error or "")


class TestAsTheResolverCallsIt:
    async def test_values(self, runinfo):
        got = await answer_route(
            "sra_be_runinfo", EntityType.BIOPROJECT, SAMPLE, "PRJNA1", _Client(), 1.0
        )
        assert got == ["ERS9", "SRS1"]

    async def test_a_failure_raises(self, runinfo):
        with pytest.raises(RouteFailed):
            await answer_route(
                "sra_be_runinfo", EntityType.BIOPROJECT, SAMPLE, "PRJNA500", _Client(), 1.0
            )

    def test_every_promoted_row_is_implemented_and_registered(self):
        for row in PROMOTED:
            assert row.id in IMPLEMENTATIONS and row.id in REGISTRY


class TestFanout:
    async def test_one_request_a_verdict_per_route(self, runinfo, tmp_path):
        routes = [
            c
            for c in candidates()
            if c.mechanism == "sra_be_runinfo" and c.source is EntityType.BIOPROJECT
        ]
        fn = fanout("sra_be_runinfo", routes, {"ncbi_web": _Client()}, "c")
        with SurveyCache(tmp_path / "c.sqlite") as cache:
            await run_fanout(
                [c.id for c in routes], ["PRJNA1", "PRJNA404"], fn, cache, "c", workers=1
            )
            sample = {
                r.accession: r for r in cache.results("c", "bioproject->sample:sra_be_runinfo")
            }
        assert sorted(runinfo) == ["PRJNA1", "PRJNA404"]
        assert sample["PRJNA1"].results == ("ERS9", "SRS1")
        assert sample["PRJNA404"].outcome is Outcome.EMPTY
