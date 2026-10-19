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
    def test_one_edge_per_route_and_measured_pair(self):
        """A route is drawn once per node pair its survey reached: SRR, ERR and DRR apart."""
        probes = load_probes()
        expected = Counter(
            link.route_id for link in CATALOGUE if status(link, probes) is Status.MEASURED
        )
        assert Counter(e["route"] for e in _of("measured")) == expected
        assert len({e["id"] for e in _of("measured")}) == len(_of("measured"))

    def test_every_route_is_drawn_or_listed_as_never_answering(self):
        """run->file:ena_sra answered for none of its 3,000 runs: listed, with no line."""
        drawn = {e["route"] for e in _of("measured")}
        none = {r["route"] for group in GRAPH["measured_none"] for r in group["routes"]}
        assert drawn | none == {r.id for r in REGISTRY}
        assert "run->file:ena_sra" in none - drawn

    def test_ranks_are_the_registrys_ranking(self):
        """The page must show the order the resolver uses, not a re-derived one."""
        for edge in _of("measured"):
            route = REGISTRY[edge["route"]]
            ranked = [r.id for r in REGISTRY.ranked(route.source, route.target)]
            assert edge["rank"] == ranked.index(edge["route"]) + 1
            assert edge["of"] == len(ranked)

    def test_endpoints_are_one_of_the_routes_catalogue_links(self):
        for edge in _of("measured"):
            pairs = {
                (link.source, link.target, link.mechanism)
                for link in CATALOGUE.route_links(edge["route"])
            }
            assert (edge["source"], edge["target"], edge["link"]["mechanism"]) in pairs

    def test_a_measured_edge_carries_its_split_and_it_reached_the_target(self):
        for edge in _of("measured"):
            split = edge["split"]
            assert 0 < split["reached"] <= split["queried"]
            if split["resolved"] is None:
                assert split["share"] is None
            else:
                assert split["share"] == split["resolved"] / split["queried"]

    def test_a_route_that_never_reached_a_target_is_listed_not_drawn(self):
        drawn = {(e["source"], e["target"], e["route"]) for e in _of("measured")}
        for group in GRAPH["measured_none"]:
            for entry in group["routes"]:
                assert entry["split"]["reached"] == 0 < entry["split"]["queried"]
                assert (group["source"], group["target"], entry["route"]) not in drawn

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
            assert "evidence" not in edge and "rank" not in edge and "split" not in edge
            for entry in edge["links"]:
                assert entry["probe"]["outcome"] == "resolved"
                assert entry["probe"]["found_count"] >= len(entry["probe"]["found"]) > 0
                assert all(check["passed"] is True for check in entry["probe"]["checks"])

    def test_a_line_into_a_read_format_says_who_made_its_files(self):
        nodes = {n["id"]: n for n in GRAPH["nodes"]}
        for edge in GRAPH["edges"]:
            if nodes[edge["target"]]["kind"] == "read-format":
                for entry in edge.get("links") or [edge["link"]]:
                    assert entry["origin"], edge["id"]

    def test_unconfirmed_links_are_listed_and_never_drawn(self):
        drawn = {
            (e["source"], e["target"], entry["mechanism"])
            for e in GRAPH["edges"]
            for entry in e.get("links", [e.get("link")])
        }
        for group in GRAPH["unconfirmed"]:
            for entry in group["links"]:
                probe = entry["probe"]
                # Found nothing, or found a file or document that failed a check.
                assert probe["outcome"] != "resolved" or any(
                    check["passed"] is not True for check in probe["checks"]
                )
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
        assert any(a["pattern"] == r"[SED]RR\d+" for a in nodes["run_ebi"]["accessions"])
        assert nodes["fastq"]["accessions"] == []
        assert nodes["ae_sample"]["accessions"] == [] and nodes["ae_sample"]["note"]

    def test_every_node_has_a_column_the_page_draws(self):
        for node in GRAPH["nodes"]:
            assert node["column"] in (*GRAPH["columns"], "insdc"), node["id"]


def test_cli_prints_the_same_graph():
    result = CliRunner().invoke(app, ["routes", "graph"])
    assert result.exit_code == 0
    assert json.loads(result.output) == json.loads(json.dumps(GRAPH))
