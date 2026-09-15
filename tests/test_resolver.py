"""Offline tests for the fallback engine.

Each test corresponds to something a census found, named in the docstring.
"""

from __future__ import annotations

import pytest

from fetch_series.accession import Archive, EntityType, parse
from fetch_series.graph import Route, RouteCost, RouteRegistry
from fetch_series.resolver import Mode, resolve
from fetch_series.survey import Limits, SurveyClient


def _route(route_id: str, **kwargs) -> Route:
    return Route(
        id=route_id,
        source=kwargs.pop("source", EntityType.GEO_SERIES),
        target=kwargs.pop("target", EntityType.EXPERIMENT),
        provider="test",
        summary="test",
        kb_page="routes/gse-to-experiment/index.md",
        cost=kwargs.pop("cost", RouteCost(requests=1, rate_limit_rps=100.0)),
        **kwargs,
    )


def _fn(results: list[str] | type[Exception]):
    async def route(accession, client, timeout):
        if isinstance(results, type):
            raise results("boom")
        return list(results)

    return route


@pytest.fixture
def client() -> SurveyClient:
    return SurveyClient(limits=Limits(rps=0, concurrency=4, attempts=1))


GSE = parse("GSE1")


class TestUnion:
    async def test_merges_disjoint_answers(self, client):
        """GSE135325: SOFT empty, ELink resolves. No ordering answers every series."""
        registry = RouteRegistry([_route("a"), _route("b")])
        impls = {"a": _fn([]), "b": _fn(["SRX1", "SRX2"])}
        r = await resolve(
            GSE, EntityType.EXPERIMENT, client, registry=registry, implementations=impls
        )
        assert r.values == ["SRX1", "SRX2"]
        assert r.routes_empty == ["a"]

    async def test_records_which_route_produced_each_value(self, client):
        registry = RouteRegistry([_route("a"), _route("b")])
        impls = {"a": _fn(["SRX1"]), "b": _fn(["SRX1", "SRX2"])}
        r = await resolve(
            GSE, EntityType.EXPERIMENT, client, registry=registry, implementations=impls
        )
        assert r.agreed == ["SRX1"]
        assert r.single_route_only == {"SRX2": "b"}

    async def test_a_failure_does_not_lose_the_other_routes(self, client):
        registry = RouteRegistry([_route("a"), _route("b")])
        impls = {"a": _fn(RuntimeError), "b": _fn(["SRX1"])}
        r = await resolve(
            GSE, EntityType.EXPERIMENT, client, registry=registry, implementations=impls
        )
        assert r.values == ["SRX1"]
        assert "a" in r.routes_failed


class TestConfirmation:
    async def test_a_data_proving_route_confirms_values(self, client):
        """An accession is not evidence that data exists: ~4,238 of the 308,639
        experiments GEO's SOFT files name have no runs."""
        registry = RouteRegistry([_route("soft"), _route("ena", proves_data_exists=True)])
        impls = {"soft": _fn(["SRX_stale", "SRX_live"]), "ena": _fn(["SRX_live"])}
        r = await resolve(
            GSE, EntityType.EXPERIMENT, client, registry=registry, implementations=impls
        )
        assert r.confirmed == ["SRX_live"]
        assert r.unconfirmed == ["SRX_stale"]

    async def test_nothing_is_confirmed_without_a_data_proving_route(self, client):
        registry = RouteRegistry([_route("soft")])
        impls = {"soft": _fn(["SRX1"])}
        r = await resolve(
            GSE, EntityType.EXPERIMENT, client, registry=registry, implementations=impls
        )
        assert r.values == ["SRX1"]
        assert r.confirmed == []
        assert r.unconfirmed == ["SRX1"]

    async def test_the_gse150508_shape(self, client):
        """Two routes, equal-sized disjoint sets, only one side carrying data."""
        registry = RouteRegistry([_route("soft"), _route("ena", proves_data_exists=True)])
        impls = {"soft": _fn(["SRX7571191"]), "ena": _fn(["SRX9670669"])}
        r = await resolve(
            GSE, EntityType.EXPERIMENT, client, registry=registry, implementations=impls
        )
        assert r.disagreed
        assert r.confirmed == ["SRX9670669"]
        assert r.unconfirmed == ["SRX7571191"]


class TestArchiveAwareness:
    async def test_skips_a_route_that_cannot_index_the_archive(self, client):
        """NCBI's index resolved 1.4% of EBI-issued BioProjects. Asking it is
        three requests spent to learn nothing."""
        registry = RouteRegistry(
            [
                _route(
                    "ncbi",
                    source=EntityType.BIOPROJECT,
                    target=EntityType.BIOSAMPLE,
                    source_archives=(Archive.NCBI,),
                ),
                _route("ena", source=EntityType.BIOPROJECT, target=EntityType.BIOSAMPLE),
            ]
        )
        called: list[str] = []

        def tracking(name, results):
            async def route(accession, c, timeout):
                called.append(name)
                return list(results)

            return route

        impls = {"ncbi": tracking("ncbi", ["SAMN1"]), "ena": tracking("ena", ["SAMEA1"])}
        r = await resolve(
            parse("PRJEB1"), EntityType.BIOSAMPLE, client, registry=registry, implementations=impls
        )
        assert called == ["ena"]
        assert "ncbi" in r.routes_skipped
        assert r.values == ["SAMEA1"]


class TestFirstMode:
    async def test_stops_at_the_first_route_that_answers(self, client):
        registry = RouteRegistry([_route("a"), _route("b")])
        called: list[str] = []

        def tracking(name, results):
            async def route(accession, c, timeout):
                called.append(name)
                return list(results)

            return route

        impls = {"a": tracking("a", ["SRX1"]), "b": tracking("b", ["SRX2"])}
        r = await resolve(
            GSE,
            EntityType.EXPERIMENT,
            client,
            mode=Mode.FIRST,
            registry=registry,
            implementations=impls,
        )
        assert called == ["a"]
        assert r.values == ["SRX1"]

    async def test_continues_past_an_empty_route(self, client):
        registry = RouteRegistry([_route("a"), _route("b")])
        impls = {"a": _fn([]), "b": _fn(["SRX2"])}
        r = await resolve(
            GSE,
            EntityType.EXPERIMENT,
            client,
            mode=Mode.FIRST,
            registry=registry,
            implementations=impls,
        )
        assert r.values == ["SRX2"]


class TestNoRoute:
    async def test_unimplemented_routes_are_reported_as_skipped(self, client):
        registry = RouteRegistry([_route("declared-only")])
        r = await resolve(GSE, EntityType.EXPERIMENT, client, registry=registry, implementations={})
        assert r.values == []
        assert r.routes_skipped == {"declared-only": "declared but not implemented"}

    async def test_explain_always_says_something(self, client):
        registry = RouteRegistry([_route("a")])
        impls = {"a": _fn(["SRX1"])}
        r = await resolve(
            GSE, EntityType.EXPERIMENT, client, registry=registry, implementations=impls
        )
        assert any("GSE1 -> experiment" in line for line in r.explain())
