"""The graph export, which the knowledge base's route-graph page draws."""

from __future__ import annotations

import json
from collections import Counter
from itertools import pairwise

from typer.testing import CliRunner

from fetch_series.catalogue import CATALOGUE, Status, load_probes, status
from fetch_series.cli import app
from fetch_series.export.graph import route_graph
from fetch_series.graph import PATHOLOGY_BACKLOG, REGISTRY

GRAPH = route_graph()


def _of(kind: str) -> list[dict]:
    return [e for e in GRAPH["edges"] if e["status"] == kind]


class TestMeasuredEdges:
    def test_every_route_is_one_edge(self):
        assert Counter(e["id"] for e in _of("measured")) == Counter(r.id for r in REGISTRY)

    def test_ranks_are_the_registrys_ranking(self):
        """The page must show the order the resolver uses, not a re-derived one."""
        for edge in _of("measured"):
            route = REGISTRY[edge["id"]]
            ranked = [r.id for r in REGISTRY.ranked(route.source, route.target)]
            assert edge["rank"] == ranked.index(edge["id"]) + 1
            assert edge["of"] == len(ranked)

    def test_endpoints_are_the_routes_catalogue_link(self):
        for edge in _of("measured"):
            link = CATALOGUE.route_link(edge["id"])
            assert (edge["source"], edge["target"]) == (link.source, link.target)
            assert edge["link"]["mechanism"] == link.mechanism

    def test_outcomes_add_up_where_the_split_is_known(self):
        for edge in _of("measured"):
            evidence = edge["evidence"]
            if evidence["empty"] is None:
                # Unknown, and drawn as unknown -- never as zero empties.
                assert evidence["resolved"] is None
                continue
            total = evidence["resolved"] + evidence["empty"] + evidence["failed"]
            assert total == evidence["queried"]

    def test_undocumented_pathologies_are_flagged(self):
        """A backlog slug has no page, so the page must not link to one."""
        for edge in _of("measured"):
            for pathology in edge["pathologies"]:
                assert pathology["documented"] is (pathology["slug"] not in PATHOLOGY_BACKLOG)


class TestUntestedEdges:
    def test_one_edge_per_pair_carrying_every_confirmed_link(self):
        probes = load_probes()
        expected: dict[tuple[str, str], set[str]] = {}
        for link in CATALOGUE:
            if status(link, probes) is Status.UNTESTED:
                expected.setdefault((link.source, link.target), set()).add(link.mechanism)
        found = {
            (e["source"], e["target"]): {entry["mechanism"] for entry in e["links"]}
            for e in _of("untested")
        }
        assert found == expected

    def test_an_untested_edge_carries_its_probes_and_no_evidence(self):
        """A probe proves a link exists; it is not a measurement, so it has no counts."""
        for edge in _of("untested"):
            assert "evidence" not in edge and "rank" not in edge
            for entry in edge["links"]:
                assert entry["probe"]["outcome"] == "resolved"
                assert entry["probe"]["found_count"] >= len(entry["probe"]["found"]) > 0

    def test_unconfirmed_links_are_listed_and_never_drawn(self):
        drawn = {
            (e["source"], e["target"], entry["mechanism"])
            for e in GRAPH["edges"]
            for entry in e.get("links", [e.get("link")])
        }
        for group in GRAPH["unconfirmed"]:
            for entry in group["links"]:
                assert entry["probe"]["outcome"] != "resolved"
                assert (group["source"], group["target"], entry["mechanism"]) not in drawn

    def test_every_mechanism_referred_to_is_described(self):
        for edge in [*GRAPH["edges"], *GRAPH["unconfirmed"]]:
            for entry in edge.get("links") or [edge["link"]]:
                assert entry["mechanism"] in GRAPH["mechanisms"]


class TestNodesAndPaths:
    def test_nodes_are_the_catalogue(self):
        assert [n["id"] for n in GRAPH["nodes"]] == list(CATALOGUE.nodes)

    def test_every_endpoint_is_a_node(self):
        nodes = {n["id"] for n in GRAPH["nodes"]}
        assert {e["source"] for e in GRAPH["edges"]} | {
            e["target"] for e in GRAPH["edges"]
        } <= nodes

    def test_measured_paths_use_only_measured_edges(self):
        measured = {(e["source"], e["target"]) for e in _of("measured")}
        for targets in GRAPH["paths_measured"].values():
            for paths in targets.values():
                for path in paths:
                    assert all((a, b) in measured for a, b in pairwise(path))

    def test_all_paths_use_only_drawn_edges_and_start_with_the_shortest(self):
        drawn = {(e["source"], e["target"]) for e in GRAPH["edges"]}
        for targets in GRAPH["paths_all"].values():
            for paths in targets.values():
                assert [len(p) for p in paths] == sorted(len(p) for p in paths)
                for path in paths:
                    assert all((a, b) in drawn for a, b in pairwise(path))

    def test_a_gap_is_a_pair_with_no_drawn_edge(self):
        drawn = {(e["source"], e["target"]) for e in GRAPH["edges"]}
        for source, missing in GRAPH["unreachable"].items():
            for target in CATALOGUE.nodes:
                if target != source:
                    assert ((source, target) not in drawn) == (target in missing)

    def test_nodes_carry_their_accession_patterns(self):
        nodes = {n["id"]: n for n in GRAPH["nodes"]}
        assert any(a["pattern"] == r"[SED]RR\d+" for a in nodes["run"]["accessions"])
        assert nodes["ena_files"]["accessions"] == []
        assert nodes["ae_sample"]["accessions"] == [] and nodes["ae_sample"]["note"]


def test_cli_prints_the_same_graph():
    result = CliRunner().invoke(app, ["routes", "graph"])
    assert result.exit_code == 0
    assert json.loads(result.output) == json.loads(json.dumps(GRAPH))
