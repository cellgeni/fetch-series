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


class TestMultiHop:
    """A GSM has no direct route to a run, so the graph must be walked."""

    @staticmethod
    def _registry() -> RouteRegistry:
        return RouteRegistry(
            [
                _route("gsm-to-srx", source=EntityType.GEO_SAMPLE, target=EntityType.EXPERIMENT),
                _route(
                    "srx-to-srr",
                    source=EntityType.EXPERIMENT,
                    target=EntityType.RUN,
                    proves_data_exists=True,
                ),
            ]
        )

    async def test_walks_a_two_hop_path(self, client):
        from fetch_series.resolver import resolve_path

        impls = {"gsm-to-srx": _fn(["SRX1"]), "srx-to-srr": _fn(["SRR1", "SRR2"])}
        r = await resolve_path(
            parse("GSM1"),
            EntityType.RUN,
            client,
            registry=self._registry(),
            implementations=impls,
        )
        assert r.values == ["SRR1", "SRR2"]
        assert r.path == (EntityType.GEO_SAMPLE, EntityType.EXPERIMENT, EntityType.RUN)

    async def test_records_the_intermediate_a_value_came_through(self):
        """A chained answer nobody can trace back is not much better than a guess."""
        from fetch_series.resolver import resolve_path

        c = SurveyClient(limits=Limits(rps=0, concurrency=2, attempts=1))
        impls = {"gsm-to-srx": _fn(["SRX1"]), "srx-to-srr": _fn(["SRR1"])}
        r = await resolve_path(
            parse("GSM1"),
            EntityType.RUN,
            c,
            registry=self._registry(),
            implementations=impls,
        )
        assert r.attributions["SRR1"][0].via == "SRX1"

    async def test_a_direct_route_is_preferred(self, client):
        from fetch_series.resolver import resolve_path

        registry = RouteRegistry(
            [
                _route("direct", source=EntityType.GEO_SAMPLE, target=EntityType.RUN),
                _route("gsm-to-srx", source=EntityType.GEO_SAMPLE, target=EntityType.EXPERIMENT),
                _route("srx-to-srr", source=EntityType.EXPERIMENT, target=EntityType.RUN),
            ]
        )
        impls = {
            "direct": _fn(["SRR_direct"]),
            "gsm-to-srx": _fn(["SRX1"]),
            "srx-to-srr": _fn(["SRR_chained"]),
        }
        r = await resolve_path(
            parse("GSM1"), EntityType.RUN, client, registry=registry, implementations=impls
        )
        assert r.values == ["SRR_direct"]
        assert r.path == ()

    async def test_a_dead_hop_yields_nothing_rather_than_a_partial_lie(self, client):
        from fetch_series.resolver import resolve_path

        impls = {"gsm-to-srx": _fn([]), "srx-to-srr": _fn(["SRR1"])}
        r = await resolve_path(
            parse("GSM1"),
            EntityType.RUN,
            client,
            registry=self._registry(),
            implementations=impls,
        )
        assert r.values == []

    async def test_fanout_is_capped_and_says_so(self, client):
        """A 2,396-sample series walked hop by hop would issue 2,396 requests
        for the second hop alone."""
        from fetch_series.resolver import resolve_path

        many = [f"SRX{i}" for i in range(20)]
        impls = {"gsm-to-srx": _fn(many), "srx-to-srr": _fn(["SRR1"])}
        r = await resolve_path(
            parse("GSM1"),
            EntityType.RUN,
            client,
            registry=self._registry(),
            implementations=impls,
            max_fanout=5,
        )
        assert any("fan-out capped" in reason for reason in r.routes_skipped.values())

    async def test_hops_are_not_reported_as_disagreeing(self, client):
        """Hops answer different questions; counting them as rival routes would
        mark every chained resolution a disagreement."""
        from fetch_series.resolver import resolve_path

        impls = {"gsm-to-srx": _fn(["SRX1"]), "srx-to-srr": _fn(["SRR1"])}
        r = await resolve_path(
            parse("GSM1"),
            EntityType.RUN,
            client,
            registry=self._registry(),
            implementations=impls,
        )
        assert not r.disagreed


class TestKnownDefectsAreSurfaced:
    """A route that answered is the one whose defects matter to the caller."""

    def test_explain_names_the_pathology_of_a_contributing_route(self):
        from fetch_series.accession import parse
        from fetch_series.resolver import Resolution

        resolution = Resolution(
            source=parse("GSE78298"), target=EntityType.BIOPROJECT, mode=Mode.UNION
        )
        resolution.routes_resolved["gse->bioproject:gds_summary"] = 1
        text = "\n".join(resolution.explain())
        assert "series-under-shared-umbrella-bioproject" in text

    def test_a_route_that_answered_nothing_does_not_warn(self):
        """Listing every catalogued defect would bury the one that applies."""
        from fetch_series.accession import parse
        from fetch_series.resolver import Resolution

        resolution = Resolution(
            source=parse("GSE78298"), target=EntityType.BIOPROJECT, mode=Mode.UNION
        )
        resolution.routes_empty.append("gse->bioproject:gds_summary")
        assert "known defect" not in "\n".join(resolution.explain())

    def test_an_unknown_route_id_does_not_raise(self):
        """Resolutions can be built from stored survey results, whose route ids
        may name a route that has since been renamed or removed."""
        from fetch_series.accession import parse
        from fetch_series.resolver import Resolution

        resolution = Resolution(
            source=parse("GSE78298"), target=EntityType.BIOPROJECT, mode=Mode.UNION
        )
        resolution.routes_resolved["nonexistent->route:x"] = 1
        assert "nonexistent->route:x" in "\n".join(resolution.explain())
