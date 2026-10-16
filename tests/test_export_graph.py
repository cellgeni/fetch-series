"""The route graph export, which the knowledge base's route-graph page draws."""

from __future__ import annotations

import json
from collections import Counter

from typer.testing import CliRunner

from fetch_series.accession import EntityType
from fetch_series.cli import app
from fetch_series.export.graph import route_graph
from fetch_series.graph import PATHOLOGY_BACKLOG, REGISTRY


def test_every_route_is_one_edge():
    edges = route_graph()["edges"]
    assert Counter(e["id"] for e in edges) == Counter(r.id for r in REGISTRY)


def test_ranks_are_the_registrys_ranking():
    """The page must show the order the resolver uses, not a re-derived one."""
    for edge in route_graph()["edges"]:
        route = REGISTRY[edge["id"]]
        ranked = [r.id for r in REGISTRY.ranked(route.source, route.target)]
        assert edge["rank"] == ranked.index(edge["id"]) + 1
        assert edge["of"] == len(ranked)


def test_outcomes_add_up_where_the_split_is_known():
    for edge in route_graph()["edges"]:
        evidence = edge["evidence"]
        if evidence["empty"] is None:
            # Unknown, and drawn as unknown -- never as zero empties.
            assert evidence["resolved"] is None
            continue
        assert evidence["resolved"] + evidence["empty"] + evidence["failed"] == evidence["queried"]


def test_nodes_cover_every_endpoint():
    graph = route_graph()
    nodes = {n["id"] for n in graph["nodes"]}
    endpoints = {e["source"] for e in graph["edges"]} | {e["target"] for e in graph["edges"]}
    assert nodes == endpoints


def test_paths_are_the_registrys_paths():
    graph = route_graph()
    for source, targets in graph["paths"].items():
        for target, paths in targets.items():
            expected = REGISTRY.find_paths(EntityType(source), EntityType(target))
            assert paths == [[str(t) for t in p] for p in expected]


def test_undocumented_pathologies_are_flagged():
    """A backlog slug has no page, so the page must not link to one."""
    for edge in route_graph()["edges"]:
        for pathology in edge["pathologies"]:
            assert pathology["documented"] is (pathology["slug"] not in PATHOLOGY_BACKLOG)


def test_cli_prints_the_same_graph():
    result = CliRunner().invoke(app, ["routes", "graph"])
    assert result.exit_code == 0
    assert json.loads(result.output) == json.loads(json.dumps(route_graph()))
