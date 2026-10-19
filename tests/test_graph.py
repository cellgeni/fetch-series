"""Offline tests for the route registry and ranking."""

import re
from datetime import UTC, date, datetime

import pytest

from fetch_series.accession import Archive, EntityType, parse
from fetch_series.graph import (
    REGISTRY,
    Route,
    RouteCost,
    RouteEvidence,
    RouteRegistry,
)


def _route(route_id: str, source: EntityType, target: EntityType, **kwargs) -> Route:
    return Route(
        id=route_id,
        source=source,
        target=target,
        provider=kwargs.pop("provider", "test"),
        summary="test route",
        kb_page=f"routes/{route_id}.md",
        cost=kwargs.pop("cost", RouteCost(requests=1, rate_limit_rps=10.0)),
        **kwargs,
    )


def _evidence(failed: int, results: int, queried: int = 1000) -> RouteEvidence:
    return RouteEvidence(
        corpus="test",
        surveyed_on=date(2026, 5, 12),
        accessions_queried=queried,
        accessions_failed=failed,
        unique_results=results,
    )


class TestRouteEvidence:
    def test_resolved_is_what_neither_failed_nor_came_back_empty(self):
        evidence = RouteEvidence(
            corpus="test",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=13_045,
            accessions_failed=7,
            accessions_empty=16,
        )
        assert evidence.accessions_resolved == 13_022

    def test_an_unrecorded_split_is_unknown_not_zero(self):
        """None must not read as "nothing came back empty"."""
        assert _evidence(failed=16, results=754_277).accessions_resolved is None

    def test_rejects_more_verdicts_than_accessions(self):
        with pytest.raises(ValueError, match="more failed and empty"):
            RouteEvidence(
                corpus="test",
                surveyed_on=date(2026, 9, 15),
                accessions_queried=10,
                accessions_failed=4,
                accessions_empty=7,
            )

    def test_positional_arguments_keep_their_meaning(self):
        """accessions_empty was added later, so it must not shift older fields.

        Inserted beside accessions_failed, it silently took the place of
        unique_results for any caller passing arguments by position.
        """
        evidence = RouteEvidence("test", date(2026, 5, 12), 12_755, 15, 754_277, "notes")
        assert evidence.unique_results == 754_277
        assert evidence.notes == "notes"
        assert evidence.accessions_empty is None

    def test_rejects_negative_counts(self):
        with pytest.raises(ValueError, match="Negative"):
            RouteEvidence(
                corpus="test",
                surveyed_on=date(2026, 9, 15),
                accessions_queried=10,
                accessions_failed=0,
                accessions_empty=-1,
            )


class TestRegistry:
    def test_rejects_duplicate_route_ids(self):
        registry = RouteRegistry([_route("a", EntityType.RUN, EntityType.STUDY)])
        with pytest.raises(ValueError, match="Duplicate route id"):
            registry.register(_route("a", EntityType.RUN, EntityType.STUDY))

    def test_ranked_filters_by_source_and_target(self):
        registry = RouteRegistry(
            [
                _route("a", EntityType.BIOPROJECT, EntityType.RUN),
                _route("b", EntityType.BIOPROJECT, EntityType.GEO_SERIES),
            ]
        )
        ranked = registry.ranked(EntityType.BIOPROJECT, EntityType.RUN)
        assert [r.id for r in ranked] == ["a"]

    def test_measured_routes_outrank_unmeasured_ones(self):
        registry = RouteRegistry(
            [
                _route("unmeasured", EntityType.BIOPROJECT, EntityType.RUN),
                _route(
                    "measured",
                    EntityType.BIOPROJECT,
                    EntityType.RUN,
                    evidence=_evidence(failed=500, results=10),
                ),
            ]
        )
        ranked = registry.ranked(EntityType.BIOPROJECT, EntityType.RUN)
        # Even a poor measured route beats one nobody has tested.
        assert [r.id for r in ranked] == ["measured", "unmeasured"]

    def test_yield_beats_failure_rate(self):
        """A route that finds more is preferred to one that merely fails less."""
        registry = RouteRegistry(
            [
                _route(
                    "finds-less-fails-less",
                    EntityType.BIOPROJECT,
                    EntityType.RUN,
                    evidence=_evidence(failed=1, results=100),
                ),
                _route(
                    "finds-more",
                    EntityType.BIOPROJECT,
                    EntityType.RUN,
                    evidence=_evidence(failed=20, results=500),
                ),
            ]
        )
        ranked = registry.ranked(EntityType.BIOPROJECT, EntityType.RUN)
        assert [r.id for r in ranked] == ["finds-more", "finds-less-fails-less"]

    def test_indistinguishable_yields_are_separated_by_reliability(self):
        """A 0.007% yield edge must not outrank a doubled failure rate."""
        registry = RouteRegistry(
            [
                _route(
                    "noisy-edge",
                    EntityType.BIOPROJECT,
                    EntityType.RUN,
                    evidence=_evidence(failed=33, results=754_332, queried=12_755),
                    cost=RouteCost(requests=3, rate_limit_rps=10.0),
                ),
                _route(
                    "reliable",
                    EntityType.BIOPROJECT,
                    EntityType.RUN,
                    evidence=_evidence(failed=15, results=754_277, queried=12_755),
                    cost=RouteCost(requests=2, rate_limit_rps=10.0),
                ),
            ]
        )
        ranked = registry.ranked(EntityType.BIOPROJECT, EntityType.RUN)
        assert [r.id for r in ranked] == ["reliable", "noisy-edge"]

    def test_indistinguishable_reliability_is_separated_by_cost(self):
        """Equivalent routes: the one that costs fewer requests wins."""
        registry = RouteRegistry(
            [
                _route(
                    "expensive",
                    EntityType.BIOPROJECT,
                    EntityType.GEO_SERIES,
                    evidence=_evidence(failed=1_078, results=15_180, queried=12_114),
                    cost=RouteCost(requests=3, rate_limit_rps=10.0),
                ),
                _route(
                    "cheap",
                    EntityType.BIOPROJECT,
                    EntityType.GEO_SERIES,
                    evidence=_evidence(failed=1_079, results=15_179, queried=12_114),
                    cost=RouteCost(requests=2, rate_limit_rps=10.0),
                ),
            ]
        )
        ranked = registry.ranked(EntityType.BIOPROJECT, EntityType.GEO_SERIES)
        assert [r.id for r in ranked] == ["cheap", "expensive"]

    def test_a_real_yield_gap_still_wins(self):
        """Banding must not flatten the 12% gap that actually matters."""
        registry = RouteRegistry(
            [
                _route(
                    "complete",
                    EntityType.BIOPROJECT,
                    EntityType.RUN,
                    evidence=_evidence(failed=14, results=784_026, queried=12_755),
                    cost=RouteCost(requests=1, rate_limit_rps=10.0),
                ),
                _route(
                    "incomplete-but-reliable",
                    EntityType.BIOPROJECT,
                    EntityType.RUN,
                    evidence=_evidence(failed=0, results=673_157, queried=12_755),
                    cost=RouteCost(requests=1, rate_limit_rps=10.0),
                ),
            ]
        )
        ranked = registry.ranked(EntityType.BIOPROJECT, EntityType.RUN)
        assert [r.id for r in ranked] == ["complete", "incomplete-but-reliable"]

    def test_ranking_is_stable(self):
        """The chosen order is recorded as provenance, so it must be deterministic."""
        routes = [
            _route(name, EntityType.BIOPROJECT, EntityType.RUN, evidence=_evidence(1, 10))
            for name in ("zzz", "aaa", "mmm")
        ]
        ranked = RouteRegistry(routes).ranked(EntityType.BIOPROJECT, EntityType.RUN)
        assert [r.id for r in ranked] == ["aaa", "mmm", "zzz"]


class TestPathFinding:
    def test_finds_a_direct_edge(self):
        registry = RouteRegistry([_route("a", EntityType.BIOPROJECT, EntityType.RUN)])
        assert registry.find_paths(EntityType.BIOPROJECT, EntityType.RUN) == [
            [EntityType.BIOPROJECT, EntityType.RUN]
        ]

    def test_finds_a_multi_hop_path_when_the_direct_edge_is_missing(self):
        """A BioProject with no project-level GEO link often still reaches GEO
        through its BioSamples."""
        registry = RouteRegistry(
            [
                _route("a", EntityType.BIOPROJECT, EntityType.BIOSAMPLE),
                _route("b", EntityType.BIOSAMPLE, EntityType.GEO_SERIES),
            ]
        )
        paths = registry.find_paths(EntityType.BIOPROJECT, EntityType.GEO_SERIES)
        assert paths == [[EntityType.BIOPROJECT, EntityType.BIOSAMPLE, EntityType.GEO_SERIES]]

    def test_returns_shortest_paths_first(self):
        registry = RouteRegistry(
            [
                _route("direct", EntityType.BIOPROJECT, EntityType.RUN),
                _route("a", EntityType.BIOPROJECT, EntityType.STUDY),
                _route("b", EntityType.STUDY, EntityType.RUN),
            ]
        )
        paths = registry.find_paths(EntityType.BIOPROJECT, EntityType.RUN)
        assert len(paths[0]) == 2
        assert len(paths[1]) == 3

    def test_does_not_cycle(self):
        registry = RouteRegistry(
            [
                _route("a", EntityType.BIOPROJECT, EntityType.STUDY),
                _route("b", EntityType.STUDY, EntityType.BIOPROJECT),
            ]
        )
        assert registry.find_paths(EntityType.BIOPROJECT, EntityType.RUN) == []

    def test_identity_path(self):
        assert REGISTRY.find_paths(EntityType.RUN, EntityType.RUN) == [[EntityType.RUN]]


class TestSeededRegistry:
    def test_route_ids_are_unique(self):
        ids = [r.id for r in REGISTRY]
        assert len(ids) == len(set(ids))

    def test_kb_pages_are_markdown_under_routes(self):
        """Pages need not be unique.

        Several routes deliberately share a direction index: the BioProject ->
        run comparison documents all five of its routes on one page, because
        the point of that page is the comparison between them. Insisting on a
        page per route would force five stubs that each say "see the index".
        """
        pages = [r.kb_page for r in REGISTRY]
        assert all(p.startswith("routes/") and p.endswith(".md") for p in pages)
        # Every direction that has a measured route must have at least one page.
        assert len({p for p in pages}) >= 4

    def test_evidence_is_dated_and_named(self):
        """An undated coverage number is not evidence of anything."""
        for route in REGISTRY:
            if route.evidence is not None:
                assert route.evidence.corpus
                # UTC, not local. A survey run at 00:12 BST is 23:12 UTC the
                # previous day, and a CI runner an hour behind would reject a
                # date this machine considers today.
                assert route.evidence.surveyed_on <= datetime.now(UTC).date()
                assert route.evidence.accessions_failed <= route.evidence.accessions_queried

    def test_evidence_records_the_empty_split(self):
        """Every route separates EMPTY from FAILED, bar the six that never could.

        Those six were surveyed by the 2026-05 scripts, which kept one boolean
        per BioProject. The list is closed: a new route has to record the split,
        and `fetch survey evidence` prints it.
        """
        never_recorded = {
            "bioproject->run:sra_be_direct_cgi",
            "bioproject->run:sra_be_elink",
            "bioproject->run:efetch_direct",
            "bioproject->run:efetch_elink",
            "bioproject->geo_series:elink",
            "bioproject->geo_series:gds_direct",
        }
        missing = {
            r.id for r in REGISTRY if r.evidence is not None and r.evidence.accessions_empty is None
        }
        assert missing == never_recorded

    def test_empty_counts_agree_with_the_notes(self):
        """The prose and the field were transcribed from the same survey."""
        split = re.compile(r"([\d,]+) resolved, ([\d,]+) empty")
        checked = 0
        for route in REGISTRY:
            evidence = route.evidence
            if evidence is None or not (m := split.search(evidence.notes)):
                continue
            resolved, empty = (int(g.replace(",", "")) for g in m.groups())
            assert (evidence.accessions_resolved, evidence.accessions_empty) == (
                resolved,
                empty,
            ), route.id
            checked += 1
        assert checked >= 30

    def test_bioproject_to_run_prefers_ena_on_the_evidence(self):
        """The measured ordering, which inverts what fetch10xmeta does today.

        fetch10xmeta reaches for the NCBI routes first; the survey says ENA
        returns 111,665 runs that efetch never does.
        """
        ranked = REGISTRY.ranked(EntityType.BIOPROJECT, EntityType.RUN)
        assert [r.id for r in ranked] == [
            "bioproject->run:ena_filereport",
            # Within each yield band the direct variant wins: same results, one
            # fewer request, and no ELink hop to fail.
            "bioproject->run:sra_be_direct_cgi",
            "bioproject->run:sra_be_elink",
            "bioproject->run:efetch_direct",
            "bioproject->run:efetch_elink",
        ]

    def test_bioproject_to_geo_prefers_the_cheaper_of_two_equivalent_routes(self):
        """ELink and direct gds agree on every accession, so cost decides."""
        ranked = REGISTRY.ranked(EntityType.BIOPROJECT, EntityType.GEO_SERIES)
        direct, elink = ranked[0], ranked[1]
        assert direct.id == "bioproject->geo_series:gds_direct"
        assert direct.cost.requests < elink.cost.requests
        assert direct.evidence is not None and elink.evidence is not None
        # One result apart out of ~15,000: the routes are equivalent in practice,
        # so the extra ELink round trip has nothing to buy.
        assert abs(elink.evidence.unique_results - direct.evidence.unique_results) == 1


class TestArchiveRouting:
    """An archive's index generally knows only its own accessions.

    Measured over the reprocessed BioProjects, NCBI's ELink resolved
    BioProject -> BioSample for 100% of NCBI-issued projects, 32.8% of DDBJ's
    and 1.4% of EBI's. A route that declares its archives lets the resolver skip
    what provably cannot answer.
    """

    def test_unrestricted_routes_accept_any_archive(self):
        route = _route("any", EntityType.BIOPROJECT, EntityType.BIOSAMPLE)
        assert route.accepts(parse("PRJNA1"))
        assert route.accepts(parse("PRJEB1"))
        assert route.accepts(parse("PRJDB1"))

    def test_restricted_routes_reject_other_archives(self):
        route = _route(
            "ncbi-only",
            EntityType.BIOPROJECT,
            EntityType.BIOSAMPLE,
            source_archives=(Archive.NCBI,),
        )
        assert route.accepts(parse("PRJNA1"))
        assert not route.accepts(parse("PRJEB1"))
        assert not route.accepts(parse("PRJDB1"))

    def test_routes_reject_the_wrong_entity_type(self):
        route = _route("x", EntityType.BIOPROJECT, EntityType.BIOSAMPLE)
        assert not route.accepts(parse("GSE1"))

    def test_ranked_for_filters_by_archive(self):
        registry = RouteRegistry(
            [
                _route(
                    "ncbi-only",
                    EntityType.BIOPROJECT,
                    EntityType.BIOSAMPLE,
                    source_archives=(Archive.NCBI,),
                    evidence=_evidence(failed=0, results=100),
                ),
                _route("anywhere", EntityType.BIOPROJECT, EntityType.BIOSAMPLE),
            ]
        )
        ncbi = [r.id for r in registry.ranked_for(parse("PRJNA1"), EntityType.BIOSAMPLE)]
        ebi = [r.id for r in registry.ranked_for(parse("PRJEB1"), EntityType.BIOSAMPLE)]
        assert ncbi == ["ncbi-only", "anywhere"]
        assert ebi == ["anywhere"]

    def test_the_seeded_registry_routes_ebi_projects_away_from_ncbi(self):
        ebi = [r.id for r in REGISTRY.ranked_for(parse("PRJEB42537"), EntityType.BIOSAMPLE)]
        assert "bioproject->biosample:elink" not in ebi
        assert "bioproject->biosample:ena_filereport" in ebi
