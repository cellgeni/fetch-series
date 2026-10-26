"""The link catalogue: declarations that cannot drift from the route registry."""

from __future__ import annotations

import json

import pytest

from fetch_series.accession import RULES, Archive, EntityType, parse
from fetch_series.catalogue import (
    CATALOGUE,
    SPLITS_PATH,
    Basis,
    Catalogue,
    Level,
    Mechanism,
    Node,
    NodeKind,
    Origin,
    Split,
    Status,
    load_splits,
)
from fetch_series.catalogue.model import POINTS_AT
from fetch_series.catalogue.nodes import (
    FAMILIES,
    NODES,
    PROBE_ACCESSIONS,
    TEN_X_RUNS,
    is_lite,
    node_for,
    read_format,
)
from fetch_series.catalogue.splits import Verdict, route_splits
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


NODE_A = Node("a", "A", Level.DATASET, "db", "shared", EntityType.BIOPROJECT)
NODE_B = Node("b", "B", Level.RUN, "db", "ncbi", EntityType.RUN, archive=Archive.NCBI)


class TestNodes:
    def test_thirty_nine_nodes(self):
        assert len(CATALOGUE.nodes) == 39
        assert len({(n.level, n.database, n.label) for n in NODES}) == 39

    def test_the_levels_are_the_planned_rows(self):
        count = {level: sum(n.level is level for n in NODES) for level in Level}
        assert count == {
            Level.DATASET: 7,
            Level.SAMPLE: 6,
            Level.LIBRARY: 3,
            Level.RUN: 3,
            Level.OTHER: 7,
            Level.FILE: 5,
            Level.SUPPLEMENTARY: 2,
            Level.METADATA: 6,
        }

    def test_insdc_objects_are_split_by_archive_and_projects_and_samples_are_not(self):
        """The split the routing depends on; BioProject and BioSample are one record."""
        for entity in ("study", "sample", "experiment", "run", "submission", "analysis"):
            found = {n.archive for n in NODES if n.entity == entity}
            assert found == {Archive.NCBI, Archive.EBI, Archive.DDBJ}, entity
        assert [n.id for n in NODES if n.entity is EntityType.BIOPROJECT] == ["bioproject"]
        assert [n.id for n in NODES if n.entity is EntityType.BIOSAMPLE] == ["biosample"]

    def test_a_node_is_named_after_its_entity_and_archive(self):
        """So a parsed accession lands on its node without a lookup table."""
        for node in NODES:
            if node.insdc_object:
                assert node.id == f"{node.entity}_{node.archive}"
            elif node.kind is NodeKind.RECORD and node.entity is not None:
                assert node.id == str(node.entity)

    @pytest.mark.parametrize(
        ("accession", "node"),
        [
            ("SRR1552444", "run_ncbi"),
            ("ERR1630015", "run_ebi"),
            ("DRR231743", "run_ddbj"),
            ("ERP017126", "study_ebi"),
            ("DRA010287", "submission_ddbj"),
            ("SRZ4627089", "analysis_ncbi"),
            ("PRJEB15401", "bioproject"),
            ("SAMD00229074", "biosample"),
            ("GSM1480291", "geo_sample"),
            ("E-MTAB-5061", "ae_experiment"),
        ],
    )
    def test_an_accession_lands_on_its_archives_node(self, accession, node):
        assert node_for(parse(accession)) == node

    def test_every_accession_rule_lands_on_a_node(self):
        examples = {"[SED]": "E", r"\d+": "1", "[A-Z]{4}": "MTAB", "[A-Z]+": "EPMC"}
        for rule in RULES:
            text = rule.pattern.pattern
            for pattern, value in examples.items():
                text = text.replace(pattern, value)
            assert node_for(parse(text)) in CATALOGUE.nodes, rule.name

    def test_only_the_arrayexpress_sample_is_a_record_without_an_accession(self):
        records = [n for n in NODES if n.kind is NodeKind.RECORD and n.entity is None]
        assert [n.id for n in records] == ["ae_sample"] and records[0].note

    def test_every_read_format_is_a_file_and_nothing_else_is(self):
        formats = [n.id for n in NODES if n.kind is NodeKind.READ_FORMAT]
        assert formats == ["fastq", "bam", "cram", "sra_normalized", "sra_lite"]
        assert {n.id for n in NODES if n.entity is EntityType.FILE} == set(formats)

    def test_duplicate_node_ids_are_refused(self):
        with pytest.raises(ValueError, match="Duplicate node"):
            Catalogue([NODE_A, NODE_A], [], {})


class TestReadFormats:
    @pytest.mark.parametrize(
        ("name", "node"),
        [
            ("https://sra-pub-run-odp.s3.amazonaws.com/sra/DRR345778/DRR345778", "sra_normalized"),
            ("https://sra-downloadb.be-md.ncbi.nlm.nih.gov/sos7/x/SRR29360814.2", "sra_normalized"),
            ("SRR25056225.lite", "sra_lite"),
            ("https://example/SRR1552444/SRR1552444.sralite.1", "sra_lite"),
            # DDBJ's mirror directory is named for Lite and mostly holds Normalized.
            (
                "https://ddbj.nig.ac.jp/dra/sralite/ByExp/litesra/ERX1/ERR1630015/ERR1630015.sra",
                "sra_normalized",
            ),
            ("https://sra-pub-src-2.s3.amazonaws.com/ERR13855116/27865_1_3.cram.1", "cram"),
            ("SC01_S2_R1_001.fastq.gz.1", "fastq"),
            ("UPN2_S2_L001.bam", "bam"),
            ("UPN2_S2_L001.bam.bai", None),
            ("pancreas_counts.txt", None),
        ],
    )
    def test_a_file_lands_on_its_format(self, name, node):
        assert read_format(name) == node

    def test_lite_is_read_from_the_name_alone(self):
        assert is_lite("SRR25056225.lite.1") and not is_lite("SRR25056225")


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

    def test_no_link_joins_two_archives_insdc_objects(self):
        """One submission's objects share an archive; the slow test checks the data agrees."""
        for link in CATALOGUE:
            a, b = CATALOGUE.nodes[link.source], CATALOGUE.nodes[link.target]
            if a.insdc_object and b.insdc_object:
                assert a.archive == b.archive, link.id

    def test_a_curated_record_links_only_the_archives_it_points_at(self):
        for link in CATALOGUE:
            for x, y in ((link.source, link.target), (link.target, link.source)):
                curated, other = CATALOGUE.nodes[x], CATALOGUE.nodes[y]
                if curated.archive in POINTS_AT and other.insdc_object:
                    assert other.archive in POINTS_AT[curated.archive], link.id

    def test_the_fan_out_reaches_all_three_archives_from_a_shared_record(self):
        targets = {link.target for link in CATALOGUE.between("bioproject", "run_ncbi")}
        assert targets == {"run_ncbi"}
        for archive in ("ncbi", "ebi", "ddbj"):
            assert CATALOGUE.between("bioproject", f"run_{archive}"), archive

    def test_every_read_format_link_says_who_made_its_files(self):
        for link in CATALOGUE:
            if CATALOGUE.nodes[link.target].kind is NodeKind.READ_FORMAT:
                origin = CATALOGUE.mechanisms[link.mechanism].origin_of(link.target)
                assert isinstance(origin, Origin), link.id

    def test_sdl_names_an_sra_object_and_the_submitters_files_apart(self):
        sdl = CATALOGUE.mechanisms["sdl"]
        assert sdl.origin_of("sra_normalized") is Origin.ARCHIVE_FORMAT
        assert sdl.origin_of("fastq") is Origin.SUBMITTED


class TestRoutesMapOntoLinks:
    def test_every_route_covers_at_least_one_link(self):
        claimed = {link.route_id for link in CATALOGUE if link.route_id}
        assert claimed == {route.id for route in REGISTRY}

    def test_a_routes_links_have_the_routes_endpoints(self):
        for route in REGISTRY:
            for link in CATALOGUE.route_links(route.id):
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

    def test_a_route_spans_every_archive_it_answers_for(self):
        pairs = {
            (link.source, link.target)
            for link in CATALOGUE.route_links("bioproject->run:ena_filereport")
        }
        assert pairs == {
            ("bioproject", "run_ncbi"),
            ("bioproject", "run_ebi"),
            ("bioproject", "run_ddbj"),
        }

    def test_file_routes_land_on_formats(self):
        def targets(route: str) -> set[str]:
            return {link.target for link in CATALOGUE.route_links(route)}

        assert targets("run->file:ena_fastq") == {"fastq"}
        assert targets("run->file:ena_submitted") == {"fastq", "bam", "cram"}
        assert "sra_normalized" in targets("run->file:sdl")
        assert targets("ae_experiment->file:sdrf") == {"fastq"}

    def test_measured_paths_are_registry_paths(self):
        """Over measured links, every node path is a type path the registry finds.

        A node path through two archives' records of one type (an SRP, its
        ArrayExpress experiment, its ERP) has no type path to compare with.

        Not the converse: a route surveyed only on NCBI's experiments measures
        no ERX, so the registry's experiment -> run path exists here for SRX
        alone.
        """
        probes: dict = {}
        measured = [link for link in CATALOGUE if CATALOGUE.status(link, probes) is Status.MEASURED]
        sources = {link.source for link in measured}
        targets = {link.target for link in measured}
        for s in sources:
            for t in targets:
                if s == t:
                    continue
                source = CATALOGUE.nodes[s].entity
                target = CATALOGUE.nodes[t].entity
                assert source is not None and target is not None
                expected = {tuple(p) for p in REGISTRY.find_paths(source, target)}
                for path in CATALOGUE.find_paths(s, t, measured):
                    types = tuple(CATALOGUE.nodes[n].entity for n in path)
                    if len(set(types)) < len(types):
                        continue  # SRP -> E-MTAB -> ERP: two studies, one type
                    assert types in expected, path


class TestSplits:
    """route_splits.json: each route's verdicts per node pair, from the recorded exports."""

    SPLITS = load_splits()

    def test_every_route_has_splits(self):
        assert {s.route for s in self.SPLITS} == {route.id for route in REGISTRY}

    def test_every_split_is_one_of_its_routes_links(self):
        for split in self.SPLITS:
            pairs = {(link.source, link.target) for link in CATALOGUE.route_links(split.route)}
            assert (split.source, split.target) in pairs, split

    def test_a_split_adds_up(self):
        for split in self.SPLITS:
            assert 0 <= split.reached <= split.queried
            if split.resolved is not None:
                assert split.resolved + split.empty + split.failed == split.queried  # type: ignore[operator]
                assert split.reached <= split.resolved

    def test_a_split_accounts_for_every_accession_its_route_queried(self):
        """Filtered measurement: the parts, and what was ruled out, add up to the evidence."""
        recorded = json.loads(SPLITS_PATH.read_text())["routes"]
        for route in REGISTRY:
            evidence = route.evidence
            assert evidence is not None
            queried = {s.source: s.queried for s in self.SPLITS if s.route == route.id}
            excluded = recorded[route.id]["excluded"]
            assert sum(queried.values()) + sum(excluded.values()) == evidence.accessions_queried, (
                route.id
            )

    def test_a_split_accounts_for_every_failure_its_route_recorded(self):
        """The 2026-05 run routes once counted the `-` placeholder's failure; neither does now."""
        for route in REGISTRY:
            evidence = route.evidence
            assert evidence is not None
            failed = {s.source: s.failed for s in self.SPLITS if s.route == route.id}
            assert sum(failed.values()) == evidence.accessions_failed, route.id

    def test_a_placeholder_is_named_and_counted_on_no_link(self):
        """`-` is not a BioProject: sent by the 2026-05 scripts, it failed, and it is not a query."""
        route = REGISTRY["bioproject->run:sra_be_direct_cgi"]
        verdicts = [Verdict("-", "failed"), Verdict("PRJNA434002", "answered", ["SRR6750041"])]
        splits, unparsed, _ = route_splits(CATALOGUE, route, verdicts)
        assert unparsed == ["-"]
        assert {(s.source, s.queried, s.failed) for s in splits} == {("bioproject", 1, 0)}

    def test_a_route_split_by_source_archive_is_the_runs_of_each(self):
        splits = {
            (s.source, s.target): s for s in self.SPLITS if s.route == "run->file:ena_submitted"
        }
        assert splits[("run_ncbi", "fastq")].reached == 0
        assert splits[("run_ebi", "fastq")].reached > 0

    def test_status_follows_the_split(self):
        splits = [
            Split("a->b:r", "a", "b", 10, 9, 1, 0, 9, 9),
        ]
        mechanism = _mechanism(routes={("a", "b"): "a->b:r"})
        catalogue = Catalogue([NODE_A, NODE_B], [mechanism], {}, splits=splits)
        (link,) = catalogue.links
        assert catalogue.status(link, {}) is Status.MEASURED
        none = Catalogue(
            [NODE_A, NODE_B],
            [mechanism],
            {},
            splits=[Split("a->b:r", "a", "b", 10, 0, 10, 0, 0, 0)],
        )
        assert none.status(none.links[0], {}) is Status.MEASURED_NONE
        unsurveyed = Catalogue([NODE_A, NODE_B], [mechanism], {})
        assert unsurveyed.status(unsurveyed.links[0], {}) is Status.UNPROBED
        assert unsurveyed.untested == unsurveyed.links

    def test_a_link_its_routes_survey_never_queried_is_probed(self):
        """experiment->run was surveyed on GEO's experiments: it says nothing about an ERX."""
        link = next(
            link
            for link in CATALOGUE.route_links("experiment->run:ena_filereport")
            if link.source == "experiment_ebi"
        )
        assert CATALOGUE.split(link) is None
        assert link in CATALOGUE.untested


class TestProbeAccessions:
    def test_every_source_node_has_a_default(self):
        sources = {link.source for link in CATALOGUE}
        assert sources <= set(PROBE_ACCESSIONS)

    def test_every_probe_accession_is_of_its_source_node(self):
        """A probe asked about the wrong kind of record proves nothing."""
        for link in CATALOGUE.untested:
            accession = CATALOGUE.probe_accession(link)
            if link.source == "ae_sample":
                # Addressed as the study plus its row's Source Name.
                study, _, row = accession.partition(":")
                assert parse(study).entity is EntityType.AE_EXPERIMENT and row, link.id
            else:
                assert node_for(parse(accession)) == link.source, (link.id, accession)

    def test_a_shared_record_is_probed_with_its_targets_archive(self):
        link = next(iter(CATALOGUE.between("bioproject", "run_ddbj")))
        assert CATALOGUE.probe_accession(link) == FAMILIES[Archive.DDBJ]["bioproject"]

    def test_a_read_file_link_is_probed_on_a_10x_run_family(self):
        for link in CATALOGUE.untested:
            if CATALOGUE.nodes[link.target].kind is NodeKind.READ_FORMAT and link.source.startswith(
                "run_"
            ):
                assert CATALOGUE.probe_accession(link) in TEN_X_RUNS, link.id

    def test_an_override_for_one_link_beats_one_for_its_source(self):
        mechanism = _mechanism(probe_with={"a": "PRJNA1", "a>b": "PRJEB2"})
        assert mechanism.probe_accession("a", "b", "PRJNA9") == "PRJEB2"
        assert mechanism.probe_accession("a", "c", "PRJNA9") == "PRJNA1"
        assert _mechanism().probe_accession("a", "b", "PRJNA9") == "PRJNA9"


class TestPromotedCoverage:
    """A link promoted to a route by its own survey is that route's, like any measured link."""

    def test_a_promoted_route_covers_its_mechanisms_links(self):
        from fetch_series.catalogue import FAMILIES, MECHANISMS, NODES, PROBE_ACCESSIONS, Catalogue

        catalogue = Catalogue(
            NODES,
            MECHANISMS,
            PROBE_ACCESSIONS,
            families=FAMILIES,
            promoted=[
                ("sra_be_runinfo", "bioproject", "sample", "bioproject->sample:sra_be_runinfo")
            ],
        )
        mine = [
            link
            for link in catalogue
            if link.mechanism == "sra_be_runinfo" and link.source == "bioproject"
        ]
        covered = {
            link.target for link in mine if link.route_id == "bioproject->sample:sra_be_runinfo"
        }
        assert covered == {"sample_ncbi", "sample_ebi", "sample_ddbj"}
        assert all(link.route_id is None for link in mine if link.target not in covered)
