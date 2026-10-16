"""Recorded probes, and the runner that makes them.

``probes.json`` is written by ``fetch catalogue probe --write`` from live
requests. These tests check what was recorded, offline: that it covers every
untested link, that each found value really is the kind of record the link
claims, and that nothing in it could be mistaken for evidence.
"""

from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest

from fetch_series.accession import EntityType, parse
from fetch_series.catalogue import (
    CATALOGUE,
    PROBE_KEEP,
    Basis,
    Catalogue,
    Level,
    Mechanism,
    Node,
    Status,
    load_probes,
    status,
)
from fetch_series.catalogue import probe as probe_module
from fetch_series.catalogue.probe import PROBERS, Prober, normalise, probe_links
from fetch_series.results import Outcome

PROBES = load_probes()


class TestRecordedProbes:
    def test_every_untested_link_has_a_probe(self):
        missing = [link.id for link in CATALOGUE.untested if link.id not in PROBES]
        assert not missing, f"run `fetch catalogue probe --write`; unprobed: {missing[:10]}"

    def test_no_probe_describes_an_undeclared_link(self):
        declared = {link.id for link in CATALOGUE}
        assert set(PROBES) <= declared

    def test_a_measured_link_is_not_probed(self):
        """A measured link has evidence; a probe beside it would read as a second measurement."""
        assert not [link.id for link in CATALOGUE if link.measured and link.id in PROBES]

    def test_each_probe_used_its_links_fixed_accession(self):
        """A changed probe accession makes the recorded probe stale, and this says so."""
        for link_id, probe in PROBES.items():
            assert probe.accession == CATALOGUE.probe_accession(CATALOGUE.link(link_id)), link_id

    def test_found_values_are_records_of_the_target_node(self):
        for link_id, probe in PROBES.items():
            node = CATALOGUE.nodes[CATALOGUE.link(link_id).target]
            for value in probe.found:
                if node.entity is EntityType.FILE:
                    assert "/" in value or "." in value, (link_id, value)
                elif node.entity is None:
                    study, _, row = value.partition(":")
                    assert parse(study).entity is EntityType.AE_EXPERIMENT and row, value
                else:
                    assert parse(value).entity is node.entity, (link_id, value)

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

    def test_probes_are_dated_and_not_in_the_future(self):
        today = datetime.now(UTC).date()
        assert all(probe.checked_on <= today for probe in PROBES.values())

    def test_every_untested_mechanism_can_be_probed(self):
        assert {link.mechanism for link in CATALOGUE.untested} <= set(PROBERS)

    def test_status_follows_the_probe(self):
        statuses = {status(link, PROBES) for link in CATALOGUE}
        assert Status.UNPROBED not in statuses
        for link in CATALOGUE:
            if link.measured:
                assert status(link, PROBES) is Status.MEASURED
            elif PROBES[link.id].confirmed:
                assert status(link, PROBES) is Status.UNTESTED
            else:
                assert status(link, PROBES) is Status.UNCONFIRMED


def _node(entity: EntityType | None) -> Node:
    return Node("n", "N", Level.SAMPLE, "db", "insdc", entity)


class TestNormalise:
    def test_keeps_only_the_target_entity(self):
        """SampleName is a GSM for a GEO submission and free text otherwise."""
        found = normalise(_node(EntityType.GEO_SAMPLE), ["GSM1480291", "MCL1-LA", "SRS685538"])
        assert found == ["GSM1480291"]

    def test_reads_accessions_out_of_urls_and_suffixes(self):
        values = [
            "https://www.ncbi.nlm.nih.gov/biosample/SAMN02991530",
            "GSM1480291_r1",
            "E-MTAB-5061:AZ_A11",
        ]
        assert normalise(_node(EntityType.BIOSAMPLE), values) == ["SAMN02991530"]
        assert normalise(_node(EntityType.GEO_SAMPLE), values) == ["GSM1480291"]
        assert normalise(_node(EntityType.AE_EXPERIMENT), values) == ["E-MTAB-5061"]

    def test_reads_both_ends_of_a_run_range(self):
        uri = "http://www.ebi.ac.uk/ena/data/view/ERR1630013-ERR1633526"
        assert normalise(_node(EntityType.RUN), [uri]) == ["ERR1630013", "ERR1633526"]

    def test_does_not_read_part_of_a_longer_token(self):
        """A submitter name that merely contains an accession is not one."""
        for value in ("SRR1552444x", "XSRR1552444", "MySRR1552444"):
            assert normalise(_node(EntityType.RUN), [value]) == [], value

    def test_files_split_on_semicolons(self):
        found = normalise(
            _node(EntityType.FILE), ["ftp.sra.ebi.ac.uk/a.bam;ftp.sra.ebi.ac.uk/a.bai"]
        )
        assert found == ["ftp.sra.ebi.ac.uk/a.bai", "ftp.sra.ebi.ac.uk/a.bam"]


class TestProbeLinks:
    NODES = (
        Node("bioproject", "BioProject", Level.DATASET, "db", "insdc", EntityType.BIOPROJECT),
        Node("run", "Run", Level.RUN, "db", "insdc", EntityType.RUN),
    )
    MECHANISM = Mechanism(
        id="fake",
        name="fake",
        api="api",
        endpoint="GET {acc}",
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=("bioproject",),
        outputs={"run": "run_accession", "bioproject": "parent"},
        self_link=True,
    )

    def _catalogue(self) -> Catalogue:
        return Catalogue(self.NODES, [self.MECHANISM], {"bioproject": "PRJNA258286"})

    async def test_one_request_answers_every_link_of_a_mechanism(self, monkeypatch):
        calls: list[str] = []

        async def fetch(client: object, accession: str, timeout: float) -> dict[str, list[str]]:
            calls.append(accession)
            return {"run": ["SRR1552444", "not a run"], "bioproject": ["PRJNA258286"]}

        monkeypatch.setitem(PROBERS, "fake", Prober("ebi", fetch))
        catalogue = self._catalogue()
        probes = await probe_links(catalogue.links, {"ebi": _Client()}, catalogue=catalogue)
        assert calls == ["PRJNA258286"]
        by_link = {p.link: p for p in probes}
        assert by_link["bioproject->run:fake"].found == ("SRR1552444",)
        # The project's own accession is not a link to itself.
        assert by_link["bioproject->bioproject:fake"].outcome is Outcome.EMPTY

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


@pytest.mark.integration
async def test_one_live_probe_still_answers():
    """The cheapest end-to-end check: ENA's filereport for the probe run."""
    from fetch_series.survey.client import SurveyClient

    link = CATALOGUE.link("run->sample:ena_filereport")
    async with SurveyClient() as client:
        (probe,) = await probe_links([link], {"ebi": client})
    assert probe.found == ("SRS685538",)
