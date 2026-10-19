"""Recorded probes, and the runner that makes them.

``probes.json`` is written by ``fetch catalogue probe --write`` from live
requests. These tests check what was recorded, offline: that it covers every
link no route measured, that each found value really is the kind of record the
link claims -- at its archive, too -- that every file and document found was
checked, and that nothing in it could be mistaken for evidence.
"""

from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest
from typer.testing import CliRunner

from fetch_series.accession import Archive, EntityType, parse
from fetch_series.catalogue import (
    CATALOGUE,
    PROBE_KEEP,
    Basis,
    Catalogue,
    Level,
    Mechanism,
    Node,
    NodeKind,
    Origin,
    Probe,
    Status,
    load_probes,
    merge_probes,
    status,
)
from fetch_series.catalogue import probe as probe_module
from fetch_series.catalogue.nodes import node_for, read_format
from fetch_series.catalogue.probe import (
    PROBERS,
    Answer,
    Prober,
    check_reads,
    https,
    normalise,
    prefer_runs,
    probe_links,
    read_of,
)
from fetch_series.cli import app
from fetch_series.files import FileRecord, classify
from fetch_series.filesets import Verdict
from fetch_series.results import Outcome

PROBES = load_probes()

CHECKS = {
    NodeKind.READ_FORMAT: ["downloadable", "verifiable", "complete"],
    NodeKind.SUPPLEMENTARY: ["downloadable", "size-checkable"],
    NodeKind.METADATA: ["downloads", "parses"],
}


class TestRecordedProbes:
    def test_every_link_no_route_measured_has_a_probe(self):
        missing = [link.id for link in CATALOGUE.untested if link.id not in PROBES]
        assert not missing, f"run `fetch catalogue probe --write`; unprobed: {missing[:10]}"

    def test_no_probe_describes_an_undeclared_link(self):
        declared = {link.id for link in CATALOGUE}
        assert set(PROBES) <= declared

    def test_a_measured_link_is_not_probed(self):
        """A measured link has evidence; a probe beside it would read as a second measurement."""
        measured = [link.id for link in CATALOGUE if CATALOGUE.split(link) is not None]
        assert not [link for link in measured if link in PROBES]

    def test_each_probe_used_its_links_fixed_accession(self):
        """A changed probe accession makes the recorded probe stale, and this says so."""
        for link_id, probe in PROBES.items():
            assert probe.accession == CATALOGUE.probe_accession(CATALOGUE.link(link_id)), link_id

    def test_found_values_are_of_the_target_node(self):
        """At its archive as well as its type: an ERR is not a link to NCBI's runs."""
        for link_id, probe in PROBES.items():
            node = CATALOGUE.nodes[CATALOGUE.link(link_id).target]
            for value in probe.found:
                if node.kind is NodeKind.READ_FORMAT:
                    assert read_format(value) == node.id, (link_id, value)
                elif node.kind is not NodeKind.RECORD:
                    assert value.startswith("https://"), (link_id, value)
                elif node.entity is None:
                    study, _, row = value.partition(":")
                    assert parse(study).entity is EntityType.AE_EXPERIMENT and row, value
                else:
                    assert node_for(parse(value)) == node.id, (link_id, value)

    def test_a_self_link_never_finds_its_own_input(self):
        for link_id, probe in PROBES.items():
            link = CATALOGUE.link(link_id)
            if link.source == link.target:
                assert probe.accession not in probe.found, link_id

    def test_outcome_agrees_with_what_was_found(self):
        for link_id, probe in PROBES.items():
            assert (probe.outcome is Outcome.RESOLVED) == (probe.found_count > 0), link_id
            assert len(probe.found) == min(probe.found_count, PROBE_KEEP), link_id
            assert list(probe.found) == sorted(probe.found), link_id
            assert (probe.outcome is Outcome.FAILED) == (probe.error is not None), link_id

    def test_everything_found_in_a_file_or_document_was_checked(self):
        """A link into one is confirmed by its being usable, never by its being named."""
        for link_id, probe in PROBES.items():
            kind = CATALOGUE.nodes[CATALOGUE.link(link_id).target].kind
            if probe.outcome is Outcome.RESOLVED and kind in CHECKS:
                assert [name for name, _ in probe.checks] == CHECKS[kind], link_id
            else:
                assert probe.checks == (), link_id

    def test_a_file_that_failed_a_check_is_not_drawn(self):
        for link_id, probe in PROBES.items():
            if probe.failed_checks:
                assert status(CATALOGUE.link(link_id), PROBES) is Status.UNCONFIRMED, link_id

    def test_probes_are_dated_and_not_in_the_future(self):
        today = datetime.now(UTC).date()
        assert all(probe.checked_on <= today for probe in PROBES.values())

    def test_every_untested_mechanism_can_be_probed(self):
        assert {link.mechanism for link in CATALOGUE.untested} <= set(PROBERS)

    def test_status_follows_the_probe(self):
        statuses = {status(link, PROBES) for link in CATALOGUE}
        assert Status.UNPROBED not in statuses
        for link in CATALOGUE:
            if CATALOGUE.split(link) is not None:
                assert status(link, PROBES) in (Status.MEASURED, Status.MEASURED_NONE)
            elif PROBES[link.id].confirmed:
                assert status(link, PROBES) is Status.UNTESTED
            else:
                assert status(link, PROBES) is Status.UNCONFIRMED

    def test_the_10x_findings_still_stand(self):
        """The probe families were chosen for these; a re-probe that loses one says so."""
        lite = PROBES["run_ebi->sra_lite:sdl_lite"]
        assert ("complete", False) in lite.checks  # ERR4265292's SRA object lost its barcode read
        assert dict(PROBES["run_ncbi->fastq:sdl_original"].checks)["downloadable"] is False
        assert (
            dict(PROBES["run_ncbi->sra_lite:sra_be_runinfo_download"].checks)["verifiable"] is False
        )
        assert PROBES["experiment_ncbi->fastq:ena_filereport_fastq"].confirmed


MEASURED_LINK = "bioproject->run_ncbi:ena_filereport"


class TestWritingProbes:
    """`probe --write` keeps a probe only for a link no route's survey measured."""

    def _probe(self, link: str) -> Probe:
        return Probe(link, "PRJNA258286", datetime.now(UTC).date(), Outcome.EMPTY)

    def test_a_probe_of_a_measured_link_is_never_kept(self):
        assert CATALOGUE.split(CATALOGUE.link(MEASURED_LINK)) is not None
        kept = merge_probes(PROBES, [self._probe(MEASURED_LINK)])
        assert MEASURED_LINK not in {probe.link for probe in kept}

    def test_one_recorded_before_its_link_was_measured_is_dropped_too(self):
        kept = merge_probes({MEASURED_LINK: self._probe(MEASURED_LINK)}, [])
        assert kept == []

    def test_an_undeclared_link_is_dropped_and_the_rest_kept(self):
        kept = merge_probes({"a->b:gone": self._probe("a->b:gone"), **PROBES}, [])
        assert {probe.link for probe in kept} == set(PROBES)

    def test_a_new_probe_replaces_the_recorded_one(self):
        link = next(iter(PROBES))
        fresh = Probe(
            link, PROBES[link].accession, PROBES[link].checked_on, Outcome.FAILED, error="x"
        )
        (kept,) = [probe for probe in merge_probes(PROBES, [fresh]) if probe.link == link]
        assert kept is fresh

    def test_the_cli_refuses_to_probe_a_measured_link(self, monkeypatch):
        """Refused before any request is made."""

        async def no_network(*args: object, **kwargs: object) -> list[Probe]:
            raise AssertionError("a measured link must not be probed")

        monkeypatch.setattr("fetch_series.cli.probe_links", no_network)
        result = CliRunner().invoke(app, ["catalogue", "probe", "--link", MEASURED_LINK, "--write"])
        assert result.exit_code == 2
        assert "never probed" in result.output


def _node(entity: EntityType | None, archive: Archive | None = None) -> Node:
    return Node("n", "N", Level.SAMPLE, "db", "ncbi", entity, archive=archive)


class TestNormalise:
    def test_keeps_only_the_target_entity(self):
        """SampleName is a GSM for a GEO submission and free text otherwise."""
        found = normalise(CATALOGUE.nodes["geo_sample"], ["GSM1480291", "MCL1-LA", "SRS685538"])
        assert found == ["GSM1480291"]

    def test_keeps_only_the_target_archive(self):
        values = ["SRR1552444", "ERR1630015", "DRR231743"]
        assert normalise(CATALOGUE.nodes["run_ebi"], values) == ["ERR1630015"]
        assert normalise(CATALOGUE.nodes["run_ddbj"], values) == ["DRR231743"]

    def test_reads_accessions_out_of_urls_and_suffixes(self):
        values = [
            "https://www.ncbi.nlm.nih.gov/biosample/SAMN02991530",
            "GSM1480291_r1",
            "E-MTAB-5061:AZ_A11",
        ]
        assert normalise(CATALOGUE.nodes["biosample"], values) == ["SAMN02991530"]
        assert normalise(CATALOGUE.nodes["geo_sample"], values) == ["GSM1480291"]
        assert normalise(CATALOGUE.nodes["ae_experiment"], values) == ["E-MTAB-5061"]

    def test_reads_both_ends_of_a_run_range(self):
        uri = "http://www.ebi.ac.uk/ena/data/view/ERR1630013-ERR1633526"
        assert normalise(CATALOGUE.nodes["run_ebi"], [uri]) == ["ERR1630013", "ERR1633526"]

    def test_does_not_read_part_of_a_longer_token(self):
        """A submitter name that merely contains an accession is not one."""
        for value in ("SRR1552444x", "XSRR1552444", "MySRR1552444"):
            assert normalise(CATALOGUE.nodes["run_ncbi"], [value]) == [], value

    def test_files_are_placed_by_format(self):
        values = ["https://h/a_1.fastq.gz", "https://h/a.bam", "https://h/a.bam.bai"]
        assert normalise(CATALOGUE.nodes["fastq"], values) == ["https://h/a_1.fastq.gz"]
        assert normalise(CATALOGUE.nodes["bam"], values) == ["https://h/a.bam"]


class TestTenXChecks:
    @pytest.mark.parametrize(
        ("name", "read"),
        [
            ("SRR25056225_1.fastq.gz", 1),
            ("SRR25056225_2.fastq.gz", 2),
            ("TSF2_S35_L002_R1_001.fastq.gz", 1),
            ("TSF2_S35_L002_R2_001.fastq.gz.1", 2),
            ("x_S1_L001_I1_001.fastq.gz", 0),
            ("SRR29534765.fastq.gz", None),
        ],
    )
    def test_reads_both_the_archives_and_the_sequencers_names(self, name, read):
        assert read_of(name) == read

    def test_a_hash_in_a_file_name_is_not_a_fragment(self):
        """A Sanger CRAM is named 27865_1#3.cram; unescaped, the request is for 27865_1."""
        url = https("ftp.sra.ebi.ac.uk/vol1/run/ERR138/ERR13855116/27865_1#3.cram")
        assert url == "https://ftp.sra.ebi.ac.uk/vol1/run/ERR138/ERR13855116/27865_1%233.cram"
        assert https("ftp://ftp.ncbi.nlm.nih.gov/geo/x.txt.gz").startswith("https://")

    @staticmethod
    def _record(name: str, **known: object) -> FileRecord:
        return FileRecord(
            run="SRR1",
            url=f"https://host/{name}",
            name=name,
            kind=classify(name),
            source="test",
            **known,  # type: ignore[arg-type]
        )

    async def _checks(self, monkeypatch, node: str, records, status: int = 200):
        async def fake_verify(record, client, timeout=30.0):
            return Verdict(record=record, status=status, served_bytes=record.size)

        monkeypatch.setattr(probe_module, "verify", fake_verify)
        checks, _ = await check_reads(
            CATALOGUE.nodes[node],
            records,
            {"ebi": object()},  # type: ignore[dict-item]
            prefer_runs("SRR1"),
        )
        return dict(checks)

    async def test_a_paired_verifiable_fastq_set_is_valid(self, monkeypatch):
        records = [
            self._record("SRR1_1.fastq.gz", md5="a", size=1),
            self._record("SRR1_2.fastq.gz", md5="b", size=2),
        ]
        checks = await self._checks(monkeypatch, "fastq", records)
        assert checks == {"downloadable": True, "verifiable": True, "complete": True}

    async def test_a_lone_fastq_has_lost_the_barcode_read(self, monkeypatch):
        """ENA's single fastq for a paired 10x library: SRR29534765."""
        records = [self._record("SRR1.fastq.gz", md5="a", size=1, declared_paired=True)]
        checks = await self._checks(monkeypatch, "fastq", records)
        assert checks["complete"] is False

    async def test_a_file_without_a_checksum_is_not_verifiable(self, monkeypatch):
        records = [self._record("SRR1_1.fastq.bz2"), self._record("SRR1_2.fastq.bz2")]
        checks = await self._checks(monkeypatch, "fastq", records)
        assert checks["verifiable"] is False and checks["complete"] is True

    async def test_a_file_with_no_link_is_not_downloadable(self, monkeypatch):
        """SDL's originals in cold storage: named, sized, and no link."""
        records = [
            FileRecord(
                run="SRR1", url="", name="x_R1_001.fastq.gz", kind=classify("x.fastq"), source="t"
            )
        ]
        checks = await self._checks(monkeypatch, "fastq", records)
        assert checks["downloadable"] is False

    async def test_an_sra_object_without_a_way_to_read_its_reads_is_undetermined(self, monkeypatch):
        """Never assumed complete: no NCBI client, no read lengths, no verdict."""
        records = [self._record("SRR1", md5="a", size=1)]
        checks = await self._checks(monkeypatch, "sra_normalized", records)
        assert checks["complete"] is None


class TestProbeLinks:
    NODES = (
        Node("bioproject", "BioProject", Level.DATASET, "db", "shared", EntityType.BIOPROJECT),
        Node("run_ncbi", "Run", Level.RUN, "db", "ncbi", EntityType.RUN, archive=Archive.NCBI),
    )
    MECHANISM = Mechanism(
        id="fake",
        name="fake",
        api="api",
        endpoint="GET {acc}",
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=("bioproject",),
        outputs={"run_ncbi": "run_accession", "bioproject": "parent"},
        self_link=True,
    )

    def _catalogue(self) -> Catalogue:
        return Catalogue(self.NODES, [self.MECHANISM], {"bioproject": "PRJNA258286"})

    async def test_one_request_answers_every_link_of_a_mechanism(self, monkeypatch):
        calls: list[str] = []

        async def fetch(client: object, accession: str, timeout: float) -> dict[str, list[str]]:
            calls.append(accession)
            return {"run": ["SRR1552444", "ERR1630015", "not a run"], "bioproject": ["PRJNA258286"]}

        monkeypatch.setitem(PROBERS, "fake", Prober("ebi", fetch))
        catalogue = self._catalogue()
        probes = await probe_links(catalogue.links, {"ebi": _Client()}, catalogue=catalogue)
        assert calls == ["PRJNA258286"]
        by_link = {p.link: p for p in probes}
        # Filed under the group, kept for the node's own archive only.
        assert by_link["bioproject->run_ncbi:fake"].found == ("SRR1552444",)
        # The project's own accession is not a link to itself.
        assert by_link["bioproject->bioproject:fake"].outcome is Outcome.EMPTY

    async def test_an_answer_carries_its_documents_checks(self, monkeypatch):
        nodes = (
            *self.NODES,
            Node("metadata_ncbi", "M", Level.METADATA, "db", "ncbi", None, archive=Archive.NCBI),
        )
        mechanism = Mechanism(
            id="doc",
            name="doc",
            api="api",
            endpoint="GET {acc}",
            citation_url=None,
            basis=Basis.LIVE_ONLY,
            inputs=("run_ncbi",),
            outputs={"metadata_ncbi": "the document"},
        )

        async def fetch(client: object, accession: str, timeout: float) -> Answer:
            url = "https://host/runinfo"
            return Answer(
                {"metadata_ncbi": [url]},
                documents={"metadata_ncbi": probe_module.Document(url, parses=False)},
            )

        monkeypatch.setitem(PROBERS, "doc", Prober("ebi", fetch))
        catalogue = Catalogue(nodes, [mechanism], {"run_ncbi": "SRR1552444"})
        (probe,) = await probe_links(catalogue.links, {"ebi": _Client()}, catalogue=catalogue)
        assert probe.checks == (("downloads", True), ("parses", False))
        assert not probe.confirmed and probe.failed_checks == ["parses"]

    async def test_a_failed_request_fails_every_link_and_redacts_the_error(self, monkeypatch):
        request = httpx.Request("GET", "https://eutils.example/?api_key=abcdefghijklmnopqrstuvwx")

        async def fetch(client: object, accession: str, timeout: float) -> dict[str, list[str]]:
            raise httpx.HTTPStatusError(
                f"400 for {request.url}", request=request, response=httpx.Response(400)
            )

        monkeypatch.setitem(PROBERS, "fake", Prober("ebi", fetch))
        catalogue = self._catalogue()
        probes = await probe_links(catalogue.links, {"ebi": _Client()}, catalogue=catalogue)
        assert {p.outcome for p in probes} == {Outcome.FAILED}
        assert all("abcdefghijklmnop" not in (p.error or "") for p in probes)


class _Client:
    """Just enough of SurveyClient for probe_links: one attempt, no network."""

    async def with_retry(self, operation, *, label: str = ""):  # type: ignore[no-untyped-def]
        return await operation(1.0)


def test_the_module_reads_its_own_catalogue_by_default():
    assert probe_module.CATALOGUE is CATALOGUE


def test_every_origin_is_one_the_page_can_name():
    origins = {
        CATALOGUE.mechanisms[link.mechanism].origin_of(link.target)
        for link in CATALOGUE
        if CATALOGUE.nodes[link.target].kind is NodeKind.READ_FORMAT
    }
    assert origins <= set(Origin)


@pytest.mark.integration
async def test_one_live_probe_still_answers():
    """The cheapest end-to-end check: ENA's filereport for the probe run."""
    from fetch_series.survey.client import SurveyClient

    link = CATALOGUE.link("run_ncbi->sample_ncbi:ena_filereport")
    async with SurveyClient() as client:
        (probe,) = await probe_links([link], {"ena_portal": client})
    assert probe.found == ("SRS685538",)
