"""Offline tests for L1: populations, host buckets, the retry rule, the memo, the CLI."""

from __future__ import annotations

from datetime import date

import httpx
import pytest
from typer.testing import CliRunner

from fetch_series.cache import SurveyCache
from fetch_series.catalogue import CATALOGUE
from fetch_series.catalogue import populations as pop
from fetch_series.catalogue.probe import BUCKETS, PROBERS, _sdl_retry, host_limits
from fetch_series.catalogue.survey import (
    SelectionError,
    accessions_for,
    estimate,
    gate_passed,
    memoizable,
    plan,
    select_links,
    units_of,
)
from fetch_series.cli import app
from fetch_series.providers import eutils, sra_be
from fetch_series.results import RouteResult
from fetch_series.survey import corpora
from fetch_series.survey.client import Limits, MemoClient

MEASURED = next(link for link in CATALOGUE if CATALOGUE.split(link) is not None)
UNITS = units_of(CATALOGUE.untested)


# ------------------------------------------------------------- populations


class TestPopulations:
    def test_every_source_node_is_accounted_for(self):
        """A node is drawn from, derived, or recorded as having no population."""
        for unit in UNITS:
            assert unit.source in pop.POPULATIONS or unit.source in pop.NO_POPULATION, unit.id

    def test_a_node_without_a_population_says_why_and_when(self):
        for node, (reason, when) in pop.NO_POPULATION.items():
            assert reason and isinstance(when, date), node
            with pytest.raises(LookupError, match="no population"):
                pop.population(node)

    def test_a_shared_node_is_stratified_by_archive(self):
        assert pop.population("bioproject", "ebi") == "node:bioproject/ebi@reprocessed-prj"
        with pytest.raises(ValueError):
            pop.population("bioproject")
        with pytest.raises(ValueError):
            pop.population("run_ncbi", "ncbi")

    def test_a_small_population_is_a_census_and_named_as_one(self, monkeypatch):
        monkeypatch.setattr(pop, "_size", lambda name: 137)
        assert pop.standard_corpus("bioproject", "ddbj") == "node:bioproject/ddbj@reprocessed-prj"

    def test_a_large_population_is_a_seeded_draw(self, monkeypatch):
        monkeypatch.setattr(pop, "_size", lambda name: 12_114)
        assert (
            pop.standard_corpus("bioproject", "ncbi")
            == f"sample:{pop.DRAW}@node:bioproject/ncbi@reprocessed-prj"
        )

    def test_a_derived_population_names_the_survey_it_comes_from(self, monkeypatch):
        monkeypatch.setattr(pop, "_size", lambda name: 5_000)
        name = pop.population("submission_ncbi")
        assert name == (
            "node:submission_ncbi@results-of:link:run_ncbi->submission_ncbi:ena_filereport"
            "@sample:300@node:run_ncbi@reprocessed-srr"
        )

    def test_a_derived_population_waits_for_its_survey(self, monkeypatch):
        def size(name):
            if name.startswith("node:submission_ncbi@results-of:"):
                raise ValueError("No recorded results")
            return 5_000

        monkeypatch.setattr(pop, "_size", size)
        with pytest.raises(pop.PopulationPending, match="ena_filereport"):
            pop.standard_corpus("submission_ncbi")

    def test_every_derivation_is_a_declared_link(self):
        for node, spec in pop.POPULATIONS.items():
            if spec.via is not None:
                link = CATALOGUE.link(spec.via)
                assert link.target == node


# ---------------------------------------------------------------- buckets


class TestBuckets:
    def test_every_mechanism_has_a_bucket(self):
        assert {prober.host for prober in PROBERS.values()} <= set(BUCKETS)

    def test_ebi_stays_within_fifteen_requests_a_second(self):
        limits = host_limits(has_api_key=True)
        ebi = sum(limits[b].rps for b in ("ena_portal", "ena_search", "biostudies", "biosamples"))
        assert ebi <= 15

    @pytest.mark.parametrize(("key", "ceiling"), [(True, 10), (False, 3)])
    def test_eutils_stays_within_ncbis_limit(self, key, ceiling):
        assert host_limits(has_api_key=key)["ncbi_eutils"].rps < ceiling

    def test_eutils_and_ncbis_other_services_are_separate(self):
        assert PROBERS["elink_bioproject_sra_all"].host == "ncbi_eutils"
        assert PROBERS["sra_be_runinfo"].host == "ncbi_web"
        assert PROBERS["geo_soft_family"].host == "ncbi_web"

    def test_enas_search_is_paced_apart_from_its_portal(self):
        """ENA answers a burst of portal searches with an HTML 500."""
        assert PROBERS["ena_search_sample"].host == "ena_search"
        assert PROBERS["ena_filereport"].host == "ena_portal"


# ------------------------------------------------------------ retry rule


def _status(code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "https://locate.ncbi.nlm.nih.gov/sdl/2/retrieve")
    return httpx.HTTPStatusError(str(code), request=request, response=httpx.Response(code))


class TestSdlRetry:
    @pytest.mark.parametrize("accession", ["PRJNA495040", "ERP110462", "SAMN1", "GSE1"])
    def test_a_504_on_anything_but_a_run_is_final(self, accession):
        assert _sdl_retry(accession, _status(504)) is False

    @pytest.mark.parametrize("accession", ["SRR25056225", "SRX1"])
    def test_a_504_on_a_run_is_worth_another_try(self, accession):
        assert _sdl_retry(accession, _status(504)) is True

    def test_a_rate_limit_is_still_retried_for_a_project(self):
        assert _sdl_retry("PRJNA495040", _status(429)) is True

    def test_every_sdl_mechanism_carries_the_rule(self):
        for name in ("sdl", "sdl_lite", "sdl_original"):
            assert PROBERS[name].retry is _sdl_retry


# ------------------------------------------------------------------- memo


class TestMemoizable:
    def test_runinfo_is_remembered(self):
        assert memoizable("GET", sra_be.RUNINFO, {"acc": "SRR1"})

    def test_a_filereport_is_remembered(self):
        """Promoted routes from one source read one filereport, with the same fields."""
        assert memoizable(
            "GET", "https://www.ebi.ac.uk/ena/portal/api/filereport", {"accession": "SRR1"}
        )

    @pytest.mark.parametrize(
        "url",
        [
            "https://www.ebi.ac.uk/biostudies/files/E-MTAB-1/E-MTAB-1.idf.txt",
            "https://ftp.ebi.ac.uk/biostudies/fire/E-MTAB-/001/E-MTAB-1/Files/E-MTAB-1.sdrf.txt",
            "https://www.ebi.ac.uk/biostudies/api/v1/studies/E-MTAB-1/info",
        ],
    )
    def test_an_ae_document_is_remembered_wherever_it_is_read(self, url):
        """Three mechanisms read one IDF or SDRF, through the mirror where the front door 404s."""
        assert memoizable("GET", url, {})

    def test_an_sra_xml_by_uid_is_remembered(self):
        assert memoizable("POST", f"{eutils.BASE}/efetch.fcgi", {"db": "sra", "id": "1,2"})

    @pytest.mark.parametrize(
        ("method", "url", "params"),
        [
            ("POST", f"{eutils.BASE}/esearch.fcgi", {"db": "sra", "term": "SRR1"}),
            ("POST", f"{eutils.BASE}/efetch.fcgi", {"db": "sra", "WebEnv": "x", "id": "1"}),
            ("GET", f"{eutils.BASE}/elink.fcgi", {"db": "sra"}),
            ("GET", "https://www.ebi.ac.uk/ena/portal/api/search", {"query": "x"}),
        ],
    )
    def test_session_state_is_never_remembered(self, method, url, params):
        assert not memoizable(method, url, params)


class TestMemoClient:
    @staticmethod
    def _transport(calls: list[str], status: int = 200, body: bytes = b"run,1\n"):
        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(str(request.url))
            return httpx.Response(status, content=body)

        return httpx.MockTransport(handler)

    async def test_a_repeated_document_is_fetched_once(self):
        calls: list[str] = []
        async with (
            httpx.AsyncClient(transport=self._transport(calls)) as http,
            MemoClient(Limits(rps=0), client=http, memoize=memoizable) as client,
        ):
            first = await client.get(sra_be.RUNINFO, params={"acc": "SRR1"})
            second = await client.get(sra_be.RUNINFO, params={"acc": "SRR1"})
            await client.get(sra_be.RUNINFO, params={"acc": "SRR2"})
        assert first.content == second.content
        assert len(calls) == 2 and client.hits == 1

    async def test_anything_not_admitted_goes_to_the_archive_every_time(self):
        calls: list[str] = []
        async with (
            httpx.AsyncClient(transport=self._transport(calls)) as http,
            MemoClient(Limits(rps=0), client=http, memoize=memoizable) as client,
        ):
            for _ in range(2):
                await client.get("https://www.ebi.ac.uk/ena/portal/api/search")
        assert len(calls) == 2

    async def test_a_failure_is_not_remembered(self):
        calls: list[str] = []
        async with (
            httpx.AsyncClient(transport=self._transport(calls, status=503)) as http,
            MemoClient(Limits(rps=0), client=http, memoize=memoizable) as client,
        ):
            for _ in range(2):
                with pytest.raises(httpx.HTTPStatusError):
                    await client.get(sra_be.RUNINFO, params={"acc": "SRR1"})
        assert len(calls) == 2

    async def test_memory_is_bounded_oldest_first(self):
        calls: list[str] = []
        async with (
            httpx.AsyncClient(transport=self._transport(calls, body=b"x" * 10)) as http,
            MemoClient(Limits(rps=0), client=http, memoize=memoizable, max_bytes=25) as client,
        ):
            for acc in ("SRR1", "SRR2", "SRR3", "SRR1"):
                await client.get(sra_be.RUNINFO, params={"acc": acc})
        # SRR1 was evicted to make room for SRR3, so it is fetched again.
        assert len(calls) == 4

    def test_the_key_never_holds_the_api_key(self):
        key = MemoClient._key("GET", sra_be.RUNINFO, {"acc": "SRR1", "api_key": "secret"})
        assert "secret" not in repr(key)


# -------------------------------------------------------------- selection


class TestSelection:
    def test_a_measured_link_is_refused(self):
        with pytest.raises(SelectionError, match="Measured"):
            select_links(link_ids=[MEASURED.id])

    @pytest.mark.parametrize(
        "kwargs",
        [{}, {"every": True, "mechanism": "ena_filereport"}, {"source": "run_ncbi", "every": True}],
    )
    def test_exactly_one_selector(self, kwargs):
        with pytest.raises(SelectionError):
            select_links(**kwargs)

    def test_all_is_every_unmeasured_link(self):
        assert select_links(every=True) == CATALOGUE.untested

    def test_a_mechanism_narrowed_by_source(self):
        links = select_links(mechanism="ena_filereport", source="run_ncbi")
        assert links and {(link.mechanism, link.source) for link in links} == {
            ("ena_filereport", "run_ncbi")
        }


class TestPlan:
    def test_a_unit_meets_only_its_own_source(self):
        built = corpora._build("x", "", "", ["SRR1", "ERR2", "E-MTAB-1:a"])
        assert accessions_for("run_ebi", built) == ("ERR2",)
        assert accessions_for("ae_sample", built) == ("E-MTAB-1:a",)

    def test_a_corpus_without_the_source_is_skipped_with_its_reason(self):
        built = corpora._build("x", "", "", ["SRR1"])
        (unit,) = units_of(select_links(mechanism="ena_filereport", source="run_ebi"))
        result = plan([unit], lambda u: ["x"], load=lambda name: built)
        assert result.jobs == [] and "holds no run_ebi" in result.skipped[0][1]

    def test_a_pending_population_is_skipped_not_raised(self):
        (unit,) = units_of(select_links(mechanism="ena_filereport", source="run_ebi"))

        def pending(u):
            raise pop.PopulationPending("x", "link:y")

        result = plan([unit], pending)
        assert result.jobs == [] and "survey link:y first" in result.skipped[0][1]

    def test_an_estimate_counts_the_checks_on_found_files(self):
        built = corpora._build("x", "", "", [f"DRR{i}" for i in range(10)])
        # sdl_original from a DDBJ run reads three formats: two HEADs each.
        (unit,) = units_of(select_links(mechanism="sdl_original", source="run_ddbj"))
        (job,) = plan([unit], lambda u: ["x"], load=lambda name: built).jobs
        cost = estimate(job)
        assert cost[job.bucket] == 10 and cost["files"] == 60


class TestGatePassed:
    def test_ungated_gated_and_failed(self, tmp_path):
        (unit,) = units_of(select_links(mechanism="ena_filereport", source="run_ebi"))
        with SurveyCache(tmp_path / "c.sqlite") as cache:
            assert gate_passed(unit, cache) is None
            unit_id = unit.unit_ids[0]
            cache.record(RouteResult.from_results("ERR1", unit_id, "probe-families", ["x"], 1))
            assert gate_passed(unit, cache) is True
            # A hard case failing is the case doing its job: reported, not blocking.
            cache.record(RouteResult.from_error("ERR2", unit_id, "hard-cases", ValueError(), 1))
            assert gate_passed(unit, cache) is True
            # A probe accession failing is a broken fetch, or SDL on a project.
            cache.record(RouteResult.from_error("ERR3", unit_id, "probe-families", ValueError(), 1))
            assert gate_passed(unit, cache) is False


# -------------------------------------------------------------------- CLI


class TestCli:
    def _invoke(self, *args: str):
        return CliRunner().invoke(app, ["catalogue", "survey", *args])

    def test_there_is_no_limit(self):
        """--limit records under the full corpus name; a link survey cannot have one."""
        result = self._invoke("--help")
        assert "--limit" not in result.output.split("Options")[1]

    def test_a_measured_link_is_refused(self):
        result = self._invoke("--link", MEASURED.id, "--gate")
        assert result.exit_code == 2 and "Measured" in result.output

    def test_exactly_one_corpus_choice(self):
        result = self._invoke("--all", "--gate", "--standard")
        assert result.exit_code == 2

    def test_a_plan_queries_nothing(self, monkeypatch, tmp_path):
        built = corpora._build("fake", "", "", [f"ERR{i}" for i in range(7)])
        monkeypatch.setitem(corpora.CORPUS_BUILDERS, "fake", lambda: built)

        def no_network(*args, **kwargs):
            raise AssertionError("a plan made a request")

        monkeypatch.setattr("fetch_series.cli.run_jobs", no_network)
        result = self._invoke(
            "--mechanism",
            "ena_filereport",
            "--source",
            "run_ebi",
            "--corpus",
            "fake",
            "--plan",
            "--cache",
            str(tmp_path / "c.sqlite"),
        )
        assert result.exit_code == 0, result.output
        assert "7 acc" in result.output and "ena_portal 7" in result.output


class TestAnswerLinksUsesTheProbersRule:
    async def test_the_rule_is_applied_with_the_input_accession(self, monkeypatch):
        from fetch_series.catalogue.probe import Prober, answer_links

        seen: list[tuple[str, int]] = []

        def rule(accession: str, exc: BaseException) -> bool:
            seen.append((accession, exc.response.status_code))  # type: ignore[attr-defined]
            return False

        async def fetch(client, accession, timeout):
            raise _status(504)

        class Client:
            async def with_retry(self, operation, *, label="", retryable=None):  # type: ignore[no-untyped-def]
                try:
                    return await operation(1.0)
                except Exception as exc:
                    assert retryable is not None and retryable(exc) is False
                    raise

        link = CATALOGUE.link("bioproject->sra_normalized:sdl")
        monkeypatch.setitem(PROBERS, "sdl", Prober("ncbi_web", fetch, retry=rule))
        (finding,) = await answer_links("sdl", "PRJNA495040", [link], {"ncbi_web": Client()})
        assert finding.outcome.value == "failed"
        assert seen == [("PRJNA495040", 504)]


class TestAbsence:
    async def test_a_record_the_archive_does_not_hold_is_empty_with_its_reason(self, monkeypatch):
        """PRJNA1136968 is absent from BioProject: nothing links from it."""
        from fetch_series.catalogue.probe import Prober, RecordAbsent, answer_links
        from fetch_series.catalogue.survey import as_result

        async def fetch(client, accession, timeout):
            raise RecordAbsent(f"esearch db=bioproject found no UID for {accession}")

        class Client:
            async def with_retry(self, operation, *, label="", retryable=None):  # type: ignore[no-untyped-def]
                return await operation(1.0)

        link = CATALOGUE.link("bioproject->bioproject:elink_bioproject_bioproject_u2d")
        monkeypatch.setitem(PROBERS, link.mechanism, Prober("ncbi_eutils", fetch))
        (finding,) = await answer_links(
            link.mechanism, "PRJNA1136968", [link], {"ncbi_eutils": Client()}
        )
        result = as_result(finding, "c", 1)
        assert result.outcome.value == "empty"
        assert result.detail == {"absent": "esearch db=bioproject found no UID for PRJNA1136968"}


class TestSeriesMatrix:
    async def test_a_multi_platform_series_is_found_by_listing(self):
        """GSE120926 has one matrix per platform and no single-platform file."""
        import gzip

        from fetch_series.catalogue.probe import _series_matrix

        listing = (
            '<a href="GSE120926-GPL20795_series_matrix.txt.gz">'
            '<a href="GSE120926-GPL11154_series_matrix.txt.gz">'
        )
        body = gzip.compress(b'!Series_geo_accession\t"GSE120926"\n')

        class Client:
            async def get(self, url, params=None, timeout=None):  # type: ignore[no-untyped-def]
                if url.endswith("/"):
                    return httpx.Response(200, text=listing)
                assert url.endswith("GSE120926-GPL11154_series_matrix.txt.gz")
                return httpx.Response(200, content=body)

        answer = await _series_matrix(Client(), "GSE120926", 1.0)  # type: ignore[arg-type]
        assert len(answer.values["metadata_geo"]) == 2
        assert answer.documents["metadata_geo"].parses is True
