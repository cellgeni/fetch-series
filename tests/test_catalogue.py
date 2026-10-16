"""The link catalogue: declarations that cannot drift from the route registry."""

from __future__ import annotations

import pytest

from fetch_series.accession import EntityType, parse
from fetch_series.catalogue import CATALOGUE, Basis, Catalogue, Level, Mechanism, Node
from fetch_series.catalogue.nodes import NODES, PROBE_ACCESSIONS
from fetch_series.graph import REGISTRY


def _mechanism(**overrides: object) -> Mechanism:
    fields: dict[str, object] = {
        "id": "m",
        "name": "m",
        "api": "api",
        "endpoint": "GET {acc}",
        "citation_url": None,
        "basis": Basis.LIVE_ONLY,
        "inputs": ("a",),
        "outputs": {"b": "field"},
    }
    fields.update(overrides)
    return Mechanism(**fields)  # type: ignore[arg-type]


NODE_A = Node("a", "A", Level.DATASET, "db", "insdc", EntityType.BIOPROJECT)
NODE_B = Node("b", "B", Level.RUN, "db", "insdc", EntityType.RUN)


class TestNodes:
    def test_eighteen_nodes_one_per_level_and_database(self):
        assert len(CATALOGUE.nodes) == 18
        keys = [(n.level, n.database, n.label) for n in NODES]
        assert len(set(keys)) == len(keys)

    def test_the_levels_are_the_planned_rows(self):
        by_level: dict[Level, set[str]] = {}
        for node in NODES:
            by_level.setdefault(node.level, set()).add(node.id)
        assert by_level == {
            Level.DATASET: {"bioproject", "study", "geo_series", "ae_experiment", "biostudy"},
            Level.SAMPLE: {"biosample", "sample", "geo_sample", "ae_sample"},
            Level.LIBRARY: {"experiment"},
            Level.RUN: {"run"},
            Level.FILE: {"ena_files", "sra_files", "ae_files", "geo_files"},
            Level.OTHER: {"submission", "analysis", "geo_platform"},
        }

    def test_a_node_with_its_own_entity_type_is_named_after_it(self):
        """So a route's endpoints land on a node without a lookup table."""
        for node in NODES:
            if node.entity is not None and node.entity is not EntityType.FILE:
                assert node.id == str(node.entity)

    def test_every_entity_type_but_file_has_exactly_one_node(self):
        for entity in EntityType:
            found = CATALOGUE.node_of(entity)
            if entity is EntityType.FILE:
                assert found is None
            else:
                assert found is not None and found.entity is entity

    def test_only_the_arrayexpress_sample_lacks_an_accession(self):
        assert [n.id for n in NODES if n.entity is None] == ["ae_sample"]
        assert CATALOGUE.nodes["ae_sample"].note

    def test_duplicate_node_ids_are_refused(self):
        with pytest.raises(ValueError, match="Duplicate node"):
            Catalogue([NODE_A, NODE_A], [], {})


class TestMechanisms:
    def test_ids_are_unique(self):
        with pytest.raises(ValueError, match="Duplicate mechanism"):
            Catalogue([NODE_A, NODE_B], [_mechanism(), _mechanism()], {})

    def test_a_mechanism_naming_an_unknown_node_is_refused(self):
        with pytest.raises(ValueError, match="unknown nodes"):
            Catalogue([NODE_A], [_mechanism()], {})

    def test_documented_exactly_when_cited(self):
        for mechanism in CATALOGUE.mechanisms.values():
            assert (mechanism.basis is Basis.DOCUMENTED) == (mechanism.citation_url is not None)
            if mechanism.citation_url:
                assert mechanism.citation_url.startswith("https://")

    def test_a_documented_mechanism_without_a_citation_is_refused(self):
        with pytest.raises(ValueError, match="cites"):
            Catalogue([NODE_A, NODE_B], [_mechanism(basis=Basis.DOCUMENTED)], {})

    def test_links_fan_out_from_inputs_to_outputs_but_not_onto_themselves(self):
        mechanism = _mechanism(inputs=("a", "b"), outputs={"a": "x", "b": "y"})
        catalogue = Catalogue([NODE_A, NODE_B], [mechanism], {})
        assert sorted((link.source, link.target) for link in catalogue) == [("a", "b"), ("b", "a")]

    def test_a_self_link_is_drawn_only_when_declared(self):
        mechanism = _mechanism(outputs={"a": "parent"}, self_link=True)
        catalogue = Catalogue([NODE_A, NODE_B], [mechanism], {})
        assert [(link.source, link.target) for link in catalogue] == [("a", "a")]

    def test_every_endpoint_template_names_its_input(self):
        for mechanism in CATALOGUE.mechanisms.values():
            assert "{acc}" in mechanism.endpoint, mechanism.id


class TestRoutesMapOntoLinks:
    def test_every_route_measures_exactly_one_link(self):
        claimed = [link.route_id for link in CATALOGUE if link.route_id]
        assert sorted(claimed) == sorted(route.id for route in REGISTRY)

    def test_a_routes_link_has_the_routes_endpoints(self):
        for route in REGISTRY:
            link = CATALOGUE.route_link(route.id)
            assert CATALOGUE.nodes[link.source].entity is route.source, route.id
            assert CATALOGUE.nodes[link.target].entity is route.target, route.id

    def test_a_route_on_a_link_its_mechanism_does_not_offer_is_refused(self):
        mechanism = _mechanism(routes={("b", "a"): "b->a:route"})
        with pytest.raises(ValueError, match="not its link"):
            Catalogue([NODE_A, NODE_B], [mechanism], {})

    def test_a_route_claimed_twice_is_refused(self):
        first = _mechanism(id="one", routes={("a", "b"): "a->b:route"})
        second = _mechanism(id="two", routes={("a", "b"): "a->b:route"})
        with pytest.raises(ValueError, match="claimed"):
            Catalogue([NODE_A, NODE_B], [first, second], {})

    def test_file_routes_land_on_the_archive_that_lists_the_file(self):
        assert CATALOGUE.route_link("run->file:ena_fastq").target == "ena_files"
        assert CATALOGUE.route_link("run->file:sdl").target == "sra_files"
        assert CATALOGUE.route_link("ae_experiment->file:sdrf").target == "ae_files"

    def test_measured_paths_are_the_registrys_paths(self):
        """Over measured links, the catalogue's search is the registry's.

        File nodes collapse onto the one ``file`` type the registry knows, so
        two catalogue paths ending in ENA's and NCBI's files are one registry
        path; the comparison is between sets for that reason.
        """
        measured = [link for link in CATALOGUE if link.measured]

        def as_types(path: list[str]) -> tuple[EntityType, ...]:
            return tuple(CATALOGUE.nodes[n].entity for n in path)  # type: ignore[misc]

        sources = {CATALOGUE.nodes[link.source].entity for link in measured}
        targets = {CATALOGUE.nodes[link.target].entity for link in measured}
        for source in sources:
            for target in targets:
                if source is target:
                    continue
                expected = {tuple(p) for p in REGISTRY.find_paths(source, target)}  # type: ignore[arg-type]
                found = {
                    as_types(path)
                    for s in CATALOGUE.nodes.values()
                    if s.entity is source
                    for t in CATALOGUE.nodes.values()
                    if t.entity is target
                    for path in CATALOGUE.find_paths(s.id, t.id, measured)
                }
                assert found == expected, (source, target)


class TestProbeAccessions:
    def test_every_source_node_has_a_default(self):
        sources = {link.source for link in CATALOGUE}
        assert sources <= set(PROBE_ACCESSIONS)

    def test_every_probe_accession_is_of_its_source_node(self):
        """A probe asked about the wrong kind of record proves nothing."""
        for link in CATALOGUE.untested:
            accession = CATALOGUE.probe_accession(link)
            node = CATALOGUE.nodes[link.source]
            if node.id == "ae_sample":
                # Addressed as the study plus its row's Source Name.
                study, _, row = accession.partition(":")
                assert parse(study).entity is EntityType.AE_EXPERIMENT and row, link.id
            else:
                assert parse(accession).entity is node.entity, (link.id, accession)

    def test_an_override_for_one_link_beats_one_for_its_source(self):
        mechanism = _mechanism(probe_with={"a": "PRJNA1", "a>b": "PRJEB2"})
        assert mechanism.probe_accession("a", "b", "PRJNA9") == "PRJEB2"
        assert mechanism.probe_accession("a", "c", "PRJNA9") == "PRJNA1"
        assert _mechanism().probe_accession("a", "b", "PRJNA9") == "PRJNA9"
