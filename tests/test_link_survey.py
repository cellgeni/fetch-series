"""Offline tests for the link-survey harness: unit ids, fan-out, cache, export, corpora."""

from __future__ import annotations

import gzip
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from fetch_series.accession import Archive, EntityType
from fetch_series.cache import SurveyCache
from fetch_series.catalogue import CATALOGUE, PROBE_KEEP, Catalogue
from fetch_series.catalogue.model import Basis, Level, Mechanism, Node
from fetch_series.catalogue.probe import (
    PROBERS,
    Prober,
    answer_links,
    prefer_runs,
    seeded_runs,
)
from fetch_series.catalogue.survey import link_fanout, units_of
from fetch_series.graph import REGISTRY
from fetch_series.results import Outcome, RouteResult
from fetch_series.survey import corpora
from fetch_series.survey.export import (
    LINK_COLUMNS,
    SURVEY_COLUMNS,
    export_path,
    format_checks,
    parse_checks,
    read_export,
    write_export,
)
from fetch_series.survey.runner import run_fanout, shuffled
from fetch_series.units import link_unit, parse_unit, screen_unit

KEY = "abcdefghijklmnopqrstuvwxyz0123456789"
WHEN = datetime(2026, 10, 19, 12, 0, tzinfo=UTC)


@pytest.fixture
def cache(tmp_path) -> SurveyCache:
    with SurveyCache(tmp_path / "c.sqlite") as c:
        yield c


# ---------------------------------------------------------------- unit ids


class TestUnitIds:
    def test_no_link_unit_id_is_a_route_id(self):
        """The reason for the prefix: a bare link id can equal a route id, and
        the cache key would then let a link verdict overwrite a route's."""
        route_ids = {route.id for route in REGISTRY}
        assert any(link.id in route_ids for link in CATALOGUE), (
            "no link id collides with a route id any more; the prefix is still "
            "required, but this premise needs rewording"
        )
        for link in CATALOGUE:
            assert link_unit(link.id) not in route_ids
            assert screen_unit(100_000, link.id) not in route_ids

    def test_no_route_id_looks_prefixed(self):
        for route in REGISTRY:
            assert parse_unit(route.id).kind == "route", route.id

    def test_units_round_trip(self):
        link_id = "run_ncbi->fastq:ena_filereport_fastq"
        assert parse_unit(link_unit(link_id)).id == link_id
        unit = parse_unit(screen_unit(200_000, link_id))
        assert (unit.kind, unit.id, unit.reads) == ("screen", link_id, 200_000)

    def test_the_read_count_is_part_of_a_screen_unit(self):
        """100k and 200k reads are different measurements and must not resume
        into each other."""
        link_id = "run_ncbi->fastq:ena_filereport_fastq"
        assert screen_unit(100_000, link_id) != screen_unit(200_000, link_id)

    @pytest.mark.parametrize("bad", ["screen:x:a->b:m", "screen:100", "screen::a->b:m"])
    def test_a_malformed_screen_unit_raises(self, bad):
        with pytest.raises(ValueError):
            parse_unit(bad)

    def test_a_screen_reads_something(self):
        with pytest.raises(ValueError):
            screen_unit(0, "a->b:m")

    def test_the_catalogue_groups_into_321_units(self):
        """1,031 unmeasured links, one unit per (mechanism, source node)."""
        units = units_of(CATALOGUE.untested)
        assert sum(len(u.links) for u in units) == len(CATALOGUE.untested)
        assert len({u.id for u in units}) == len(units)


# ------------------------------------------------------------ RouteResult


class TestFromErrorRedacts:
    def test_the_message_is_redacted_whoever_builds_it(self):
        request = httpx.Request("GET", f"https://x/e.fcgi?db=sra&api_key={KEY}")
        error = httpx.HTTPStatusError(
            f"Client error for url '{request.url}'", request=request, response=httpx.Response(400)
        )
        result = RouteResult.from_error("SRR1", "r", "c", error, 1)
        assert KEY not in (result.error_message or "")
        assert result.error_class == "HTTPStatusError"

    def test_a_plain_message_survives(self):
        result = RouteResult.from_error("SRR1", "r", "c", ValueError("no such run"), 1)
        assert result.error_message == "no such run"


# ------------------------------------------------------------------ cache


class TestCacheDetail:
    def test_an_old_cache_is_migrated_in_place(self, tmp_path):
        """Months of verdicts that cannot be re-collected: add the column, keep the rows."""
        path = tmp_path / "old.sqlite"
        conn = sqlite3.connect(path)
        conn.executescript(
            """
            CREATE TABLE survey_results (
                corpus TEXT NOT NULL, route_id TEXT NOT NULL, accession TEXT NOT NULL,
                outcome TEXT NOT NULL, results TEXT NOT NULL, n_results INTEGER NOT NULL,
                error_class TEXT, error_message TEXT, latency_ms INTEGER NOT NULL,
                fetched_at TEXT NOT NULL, PRIMARY KEY (corpus, route_id, accession));
            INSERT INTO survey_results VALUES
                ('c', 'r', 'PRJNA1', 'resolved', '["SRR1"]', 1, NULL, NULL, 5,
                 '2026-09-15T00:00:00+00:00');
            """
        )
        conn.commit()
        conn.close()
        with SurveyCache(path) as migrated:
            (old,) = list(migrated.results("c", "r"))
            assert old.results == ("SRR1",) and old.detail is None
            migrated.record(
                RouteResult.from_results("PRJNA2", "r", "c", ["SRR2"], 1, detail={"checked": "x"})
            )
            assert {r.accession: r.detail for r in migrated.results("c", "r")} == {
                "PRJNA1": None,
                "PRJNA2": {"checked": "x"},
            }

    def test_detail_round_trips(self, cache):
        detail = {
            "checks": [["downloadable", True], ["complete", None]],
            "checked": "SRR1: 2 files",
        }
        cache.record(RouteResult.from_results("SRR1", "link:a", "c", ["x"], 1, detail=detail))
        (stored,) = list(cache.results("c", "link:a"))
        assert stored.detail == detail

    def test_record_all_is_all_or_nothing(self, cache):
        def verdicts():
            yield RouteResult.from_results("SRR1", "link:a", "c", ["x"], 1)
            raise RuntimeError("interrupted between two writes")

        with pytest.raises(RuntimeError):
            cache.record_all(verdicts())
        assert cache.answered("c", "link:a") == set()

    def test_answered_all_needs_every_unit(self, cache):
        cache.record_all(
            [
                RouteResult.from_results("SRR1", "link:a", "c", ["x"], 1),
                RouteResult.from_results("SRR1", "link:b", "c", [], 1),
                RouteResult.from_results("SRR2", "link:a", "c", ["y"], 1),
            ]
        )
        assert cache.answered_all("c", ["link:a", "link:b"]) == {"SRR1"}
        assert cache.answered_by("c", ["link:a", "link:b"]) == {
            "SRR1": {"link:a", "link:b"},
            "SRR2": {"link:a"},
        }


# ----------------------------------------------------------------- fan-out


UNITS = ["link:a", "link:b", "link:c"]


def _answer_all(calls: list[tuple[str, frozenset[str]]]):
    async def fn(accession: str, owed: frozenset[str]) -> list[RouteResult]:
        calls.append((accession, owed))
        return [
            RouteResult.from_results(accession, u, "ignored", [f"{u}-{accession}"], 1) for u in owed
        ]

    return fn


class TestFanout:
    async def test_one_request_per_accession_and_a_verdict_per_unit(self, cache):
        calls: list[tuple[str, frozenset[str]]] = []
        run = await run_fanout(UNITS, ["SRR1", "SRR2"], _answer_all(calls), cache, "c", workers=2)
        assert sorted(a for a, _ in calls) == ["SRR1", "SRR2"]
        assert all(owed == frozenset(UNITS) for _, owed in calls)
        assert run.queried == 2 and run.units["link:a"]["resolved"] == 2
        assert cache.answered_all("c", UNITS) == {"SRR1", "SRR2"}

    async def test_verdicts_carry_the_survey_corpus_whatever_fn_says(self, cache):
        await run_fanout(UNITS, ["SRR1"], _answer_all([]), cache, "c", workers=1)
        assert all(r.corpus == "c" for r in cache.results("c", "link:a"))

    async def test_a_partly_answered_accession_asks_only_for_what_is_missing(self, cache):
        cache.record(RouteResult.from_results("SRR1", "link:a", "c", ["x"], 1))
        calls: list[tuple[str, frozenset[str]]] = []
        run = await run_fanout(UNITS, ["SRR1"], _answer_all(calls), cache, "c", workers=1)
        assert calls == [("SRR1", frozenset({"link:b", "link:c"}))]
        assert run.skipped == 0
        # The earlier verdict is kept, not overwritten.
        (kept,) = list(cache.results("c", "link:a"))
        assert kept.results == ("x",)

    async def test_a_fully_answered_accession_is_skipped(self, cache):
        await run_fanout(UNITS, ["SRR1"], _answer_all([]), cache, "c", workers=1)
        calls: list[tuple[str, frozenset[str]]] = []
        run = await run_fanout(UNITS, ["SRR1"], _answer_all(calls), cache, "c", workers=1)
        assert calls == [] and run.skipped == 1

    async def test_a_raised_error_fails_every_owed_unit(self, cache):
        async def fn(accession, owed):
            raise httpx.ConnectError(f"no route to host ?api_key={KEY}")

        run = await run_fanout(UNITS, ["SRR1"], fn, cache, "c", workers=1)
        assert {u: run.units[u]["failed"] for u in UNITS} == dict.fromkeys(UNITS, 1)
        for unit in UNITS:
            (verdict,) = list(cache.results("c", unit))
            assert verdict.outcome is Outcome.FAILED
            assert KEY not in (verdict.error_message or "")

    async def test_an_owed_unit_left_unanswered_is_recorded_as_failed(self, cache):
        """Absence means never queried, which this accession was."""

        async def fn(accession, owed):
            return [RouteResult.from_results(accession, "link:a", "c", ["x"], 1)]

        await run_fanout(UNITS, ["SRR1"], fn, cache, "c", workers=1)
        (missing,) = list(cache.results("c", "link:b"))
        assert missing.outcome is Outcome.FAILED and missing.error_class == "LookupError"

    async def test_an_unowed_verdict_is_a_bug_not_a_result(self, cache):
        async def fn(accession, owed):
            return [RouteResult.from_results(accession, "link:z", "c", ["x"], 1)]

        with pytest.raises(ValueError, match="unowed"):
            await run_fanout(UNITS, ["SRR1"], fn, cache, "c", workers=1)

    async def test_retry_failed_re_asks_only_the_failed_units(self, cache):
        cache.record_all(
            [
                RouteResult.from_results("SRR1", "link:a", "c", ["x"], 1),
                RouteResult.from_error("SRR1", "link:b", "c", ValueError("x"), 1),
                RouteResult.from_results("SRR1", "link:c", "c", [], 1),
            ]
        )
        calls: list[tuple[str, frozenset[str]]] = []
        await run_fanout(UNITS, ["SRR1"], _answer_all(calls), cache, "c", workers=1)
        assert calls == []
        await run_fanout(
            UNITS, ["SRR1"], _answer_all(calls), cache, "c", workers=1, retry_failed=True
        )
        assert calls == [("SRR1", frozenset({"link:b"}))]

    async def test_accessions_are_visited_in_the_seeded_order(self, cache):
        corpus = sorted(f"SRR{i:04d}" for i in range(100))
        calls: list[tuple[str, frozenset[str]]] = []
        await run_fanout(UNITS, corpus, _answer_all(calls), cache, "c", workers=1)
        assert [a for a, _ in calls] == shuffled(corpus)

    async def test_a_survey_needs_a_unit(self, cache):
        with pytest.raises(ValueError):
            await run_fanout([], ["SRR1"], _answer_all([]), cache, "c", workers=1)


# ----------------------------------------------------------------- export


def _old_export_body(results: list[RouteResult]) -> str:
    """The exporter as it was before link surveys, kept to pin its bytes."""
    import csv
    import io

    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer)
    writer.writerow(SURVEY_COLUMNS)
    for r in results:
        writer.writerow(
            [
                r.accession,
                r.outcome.value,
                r.n_results,
                ";".join(r.results),
                r.error_class or "",
                (r.error_message or "").replace("\n", " "),
                r.latency_ms,
                r.fetched_at.isoformat(),
            ]
        )
    return buffer.getvalue()


class TestExport:
    RESULTS = (
        RouteResult(
            "PRJNA1",
            "bioproject->run:ena_filereport",
            "c",
            Outcome.RESOLVED,
            ("SRR1", "SRR2"),
            latency_ms=3,
            fetched_at=WHEN,
        ),
        RouteResult(
            "PRJNA2",
            "bioproject->run:ena_filereport",
            "c",
            Outcome.EMPTY,
            (),
            latency_ms=4,
            fetched_at=WHEN,
        ),
        RouteResult(
            "PRJNA3",
            "bioproject->run:ena_filereport",
            "c",
            Outcome.FAILED,
            (),
            "ReadTimeout",
            "timed out\nafter 60 s",
            5,
            WHEN,
        ),
    )

    def test_a_route_export_is_byte_identical(self, tmp_path):
        """RouteEvidence cites these files; adding link surveys must not move a byte."""
        path, rows = write_export(
            tmp_path, "c", "bioproject->run:ena_filereport", list(self.RESULTS)
        )
        assert rows == 3
        with gzip.open(path, "rt", newline="") as fh:
            assert fh.read() == _old_export_body(list(self.RESULTS))

    def test_a_link_export_adds_checks_and_what_they_were_made_on(self, tmp_path):
        result = RouteResult.from_results(
            "SRR1",
            "link:run_ncbi->fastq:ena_filereport_fastq",
            "c",
            ["https://x/SRR1_1.fastq.gz"],
            1,
            detail={
                "checks": [["downloadable", True], ["complete", None]],
                "checked": "SRR1: 1 file",
            },
        )
        path, _ = write_export(tmp_path, "c", result.route_id, [result])
        with gzip.open(path, "rt") as fh:
            header, row = fh.read().splitlines()
        assert header == ",".join(LINK_COLUMNS)
        assert row.endswith(",downloadable=true;complete=,SRR1: 1 file")

    def test_an_export_reads_back(self, tmp_path):
        detail = {"checks": [["downloadable", False]], "checked": "SRR1: 2 files"}
        results = [
            RouteResult.from_results("SRR1", "link:a->b:m", "c", ["x", "y"], 1, detail=detail),
            RouteResult.from_results("SRR2", "link:a->b:m", "c", [], 1),
        ]
        path, _ = write_export(tmp_path / "c", "c", "link:a->b:m", results)
        back = list(read_export(path, "c", "link:a->b:m"))
        assert [(r.accession, r.outcome, r.results, r.detail) for r in back] == [
            ("SRR1", Outcome.RESOLVED, ("x", "y"), detail),
            ("SRR2", Outcome.EMPTY, (), None),
        ]

    def test_checks_round_trip_undetermined_as_undetermined(self):
        checks = (("downloadable", True), ("verifiable", False), ("complete", None))
        assert parse_checks(format_checks(checks)) == checks

    def test_an_lfs_pointer_is_not_data(self, tmp_path):
        path = tmp_path / "x.csv.gz"
        path.write_bytes(b"version https://git-lfs.github.com/spec/v1\noid sha256:0\nsize 1\n")
        with pytest.raises(FileNotFoundError):
            list(read_export(path, "c", "r"))

    def test_an_export_is_found_through_its_manifest(self, tmp_path):
        write_export(tmp_path / "sample_3_at_x", "sample:3@x", "r", [])
        assert export_path("sample:3@x", "r", tmp_path) is None  # wrong slug dir
        directory = tmp_path / "sample_3@x"
        write_export(directory, "sample:3@x", "r", [])
        assert export_path("sample:3@x", "r", tmp_path) == directory / "r.csv.gz"
        (directory / "manifest.txt").write_text("corpus: something else\n")
        assert export_path("sample:3@x", "r", tmp_path) is None

    @pytest.mark.slow
    def test_every_committed_route_export_round_trips_unchanged(self, tmp_path):
        """Reading a committed export and writing it back reproduces it exactly."""
        root = Path("data/surveys")
        checked = 0
        for manifest in sorted(root.glob("*/manifest.txt")):
            if manifest.read_bytes().startswith(b"version https://git-lfs"):
                continue
            corpus = manifest.read_text().strip().removeprefix("corpus: ")
            for path in sorted(manifest.parent.glob("*.csv.gz")):
                if path.read_bytes()[:23] == b"version https://git-lfs":
                    continue
                with gzip.open(path, "rt", newline="") as fh:
                    original = fh.read()
                if not original.startswith(",".join(SURVEY_COLUMNS) + "\r\n"):
                    continue
                # Any unprefixed id round-trips: the unit id is not in the file.
                unit = path.name.removesuffix(".csv.gz")
                rewritten, _ = write_export(
                    tmp_path / manifest.parent.name, corpus, unit, read_export(path, corpus, unit)
                )
                with gzip.open(rewritten, "rt", newline="") as fh:
                    assert fh.read() == original, path
                checked += 1
        if not checked:
            pytest.skip("no committed export fetched (Git LFS)")


# ---------------------------------------------------------------- corpora


def _fake_parent(monkeypatch, raws: list[str], name: str = "fake") -> None:
    built = corpora._build(name, "test parent", "test", raws)
    monkeypatch.setitem(corpora.CORPUS_BUILDERS, name, lambda: built)


class TestNodeFilter:
    RAWS = (
        "PRJNA1",
        "PRJEB2",
        "PRJDB3",
        "SRR1",
        "ERR2",
        "DRR3",
        "ERR4",
        "GSE5",
        "E-MTAB-5061:AZ_A1",
    )

    def test_a_node_keeps_only_its_own_accessions(self, monkeypatch):
        _fake_parent(monkeypatch, list(self.RAWS))
        assert corpora.load("node:run_ebi@fake").values == ("ERR2", "ERR4")

    def test_an_archive_stratifies_a_shared_node(self, monkeypatch):
        _fake_parent(monkeypatch, list(self.RAWS))
        assert corpora.load("node:bioproject/ebi@fake").values == ("PRJEB2",)
        assert len(corpora.load("node:bioproject@fake")) == 3

    def test_the_name_identifies_the_set(self, monkeypatch):
        _fake_parent(monkeypatch, list(self.RAWS))
        assert corpora.load("node:bioproject/ddbj@fake").name == "node:bioproject/ddbj@fake"

    def test_filter_first_then_draw(self, monkeypatch):
        """sample:n@node:… is n of the node, not however many a draw held."""
        _fake_parent(monkeypatch, [f"ERR{i}" for i in range(50)] + [f"SRR{i}" for i in range(500)])
        drawn = corpora.load("sample:20@node:run_ebi@fake")
        assert len(drawn) == 20 and all(v.startswith("ERR") for v in drawn.values)

    def test_ae_samples_are_addresses(self, monkeypatch):
        _fake_parent(monkeypatch, list(self.RAWS))
        assert corpora.load("node:ae_sample@fake").addresses == ("E-MTAB-5061:AZ_A1",)

    @pytest.mark.parametrize(
        "bad", ["node:nonsense@fake", "node:bioproject/mars@fake", "node:@fake"]
    )
    def test_a_bad_spec_raises(self, monkeypatch, bad):
        _fake_parent(monkeypatch, list(self.RAWS))
        with pytest.raises(ValueError):
            corpora.load(bad)

    def test_the_spec_is_documented(self):
        assert any(k.startswith("node:") for k in corpora.KNOWN_CORPORA)


class TestAddresses:
    def test_an_address_is_neither_parsed_nor_junk(self):
        built = corpora._build("x", "", "", ["E-GEOD-60450:GSM1480302 1", "SRR1", "-"])
        assert built.addresses == ("E-GEOD-60450:GSM1480302 1",)  # the space is the submitter's
        assert built.values == ("SRR1",)
        assert built.unparsed == ("-",)
        assert len(built) == 2

    def test_a_draw_without_addresses_is_unchanged(self, monkeypatch):
        """Every sample: corpus surveyed before addresses existed must draw the same set."""
        import random

        raws = [f"SRR{i}" for i in range(400)]
        _fake_parent(monkeypatch, raws)
        expected = random.Random(20260915).sample(list(corpora.load("fake").values), 30)
        assert sorted(corpora.load("sample:30@fake").values) == sorted(expected)


class TestGates:
    def test_probe_families_holds_every_probe_accession(self):
        gate = corpora.load("probe-families")
        held = set(gate.values) | set(gate.addresses)
        assert {CATALOGUE.probe_accession(link) for link in CATALOGUE} <= held
        assert gate.unparsed == ()

    @pytest.mark.parametrize(
        ("name", "gate"),
        [
            ("hard-cases", True),
            ("probe-families", True),
            ("metadata-hard-cases", True),
            ("sample:10@node:run_ebi@probe-families", True),
            ("results-of:a->b:c@hard-cases", True),
            ("sample:3000@reprocessed-srr", False),
            ("node:bioproject/ebi@reprocessed-prj", False),
        ],
    )
    def test_a_gate_stays_a_gate_through_every_derivation(self, name, gate):
        assert corpora.is_gate(name) is gate


class TestResultsOfFallsBackToTheExport:
    def test_a_population_survives_the_cache_not_being_there(self, tmp_path, monkeypatch):
        """The cache is not committed, so CI rebuilds a results-of corpus from the export."""
        from fetch_series.survey import export

        monkeypatch.setattr(export, "SURVEYS_DIR", tmp_path)
        route = "bioproject->run:ena_filereport"
        write_export(
            tmp_path / export.slug("reprocessed-prj"),
            "reprocessed-prj",
            route,
            [RouteResult.from_results("PRJNA1", route, "reprocessed-prj", ["SRR2", "SRR1"], 1)],
        )
        built = corpora.from_survey(route, "reprocessed-prj", cache_path=tmp_path / "absent.sqlite")
        assert built.values == ("SRR1", "SRR2")
        assert built.source.startswith("survey export:")

    def test_nothing_anywhere_still_raises(self, tmp_path, monkeypatch):
        from fetch_series.survey import export

        monkeypatch.setattr(export, "SURVEYS_DIR", tmp_path)
        with pytest.raises(ValueError):
            corpora.from_survey("a->b:c", "x", cache_path=tmp_path / "absent.sqlite")


# ----------------------------------------------------------- answer_links


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
    outputs={"run_ncbi": "run_accession"},
)
MANY_RUNS = [f"SRR{1000 + i}" for i in range(PROBE_KEEP + 8)]


class _Client:
    async def with_retry(self, operation, *, label="", retryable=None):  # type: ignore[no-untyped-def]
        return await operation(1.0)


@pytest.fixture
def fake_mechanism(monkeypatch):
    async def fetch(client, accession, timeout):
        return {"run": MANY_RUNS}

    monkeypatch.setitem(PROBERS, "fake", Prober("ebi", fetch))
    return Catalogue(NODES, [MECHANISM], {"bioproject": "PRJNA1"})


class TestAnswerLinks:
    async def test_a_survey_keeps_every_value(self, fake_mechanism):
        (finding,) = await answer_links(
            "fake", "PRJNA1", fake_mechanism.links, {"ebi": _Client()}, catalogue=fake_mechanism
        )
        assert len(finding.values) == len(MANY_RUNS)

    async def test_a_probe_is_the_finding_truncated(self, fake_mechanism):
        (finding,) = await answer_links(
            "fake", "PRJNA1", fake_mechanism.links, {"ebi": _Client()}, catalogue=fake_mechanism
        )
        probe = finding.probe(WHEN.date())
        assert len(probe.found) == PROBE_KEEP and probe.found_count == len(MANY_RUNS)

    async def test_link_fanout_records_under_prefixed_ids(self, fake_mechanism, cache):
        (unit,) = units_of(fake_mechanism.links)
        fn = link_fanout(unit, {"ebi": _Client()}, "c", catalogue=fake_mechanism)
        run = await run_fanout(unit.unit_ids, ["PRJNA1"], fn, cache, "c", workers=1)
        assert unit.unit_ids == ["link:bioproject->run_ncbi:fake"]
        (verdict,) = list(cache.results("c", "link:bioproject->run_ncbi:fake"))
        assert verdict.n_results == len(MANY_RUNS)
        assert run.units[unit.unit_ids[0]]["distinct"] == len(MANY_RUNS)


class TestRunPickers:
    RUNS = sorted(f"SRR{i}" for i in range(40))

    def test_a_probe_prefers_its_own_run(self):
        assert prefer_runs("SRR7")(self.RUNS) == "SRR7"
        assert prefer_runs("ERR1")(self.RUNS) == self.RUNS[0]

    def test_a_survey_does_not_check_the_oldest_run(self):
        picks = {seeded_runs(20260915, f"PRJNA{i}")(self.RUNS) for i in range(60)}
        assert len(picks) > 10

    def test_a_survey_pick_is_reproducible(self):
        assert seeded_runs(1, "PRJNA1")(self.RUNS) == seeded_runs(1, "PRJNA1")(self.RUNS)

    def test_a_run_accession_checks_itself(self):
        assert seeded_runs(1, "SRR3")(self.RUNS) == "SRR3"


class TestPerCallRetryRule:
    """SDL answers a project with a 504 every time; retrying it spends the budget
    proving it again."""

    async def test_a_narrower_rule_stops_a_hopeless_retry(self):
        from fetch_series.survey.client import Limits, SurveyClient

        attempts = 0
        request = httpx.Request("GET", "https://x")

        async def operation(timeout):
            nonlocal attempts
            attempts += 1
            raise httpx.HTTPStatusError("504", request=request, response=httpx.Response(504))

        client = SurveyClient(limits=Limits(rps=0, attempts=4, base_timeout=0.1))
        with pytest.raises(httpx.HTTPStatusError):
            await client.with_retry(operation, retryable=lambda exc: False)
        assert attempts == 1


class TestExportNeverSilentlyChangesEvidence:
    RESULT = RouteResult.from_results("PRJNA1", "r", "c", ["SRR1"], 1)

    def test_rewriting_the_same_verdicts_is_a_no_op(self, tmp_path):
        from fetch_series.survey.export import write_export

        path, _ = write_export(tmp_path, "c", "r", [self.RESULT])
        before = path.read_bytes()
        write_export(tmp_path, "c", "r", [self.RESULT])
        assert path.read_bytes() == before  # not even the gzip timestamp moved

    def test_changed_verdicts_are_refused_without_replace(self, tmp_path):
        from fetch_series.survey.export import ExportConflict, write_export

        write_export(tmp_path, "c", "r", [self.RESULT])
        moved = RouteResult.from_results("PRJNA1", "r", "c", ["SRR2"], 1)
        with pytest.raises(ExportConflict):
            write_export(tmp_path, "c", "r", [moved])
        write_export(tmp_path, "c", "r", [moved], replace=True)

    def test_a_directory_belongs_to_one_corpus(self, tmp_path):
        from fetch_series.survey.export import ExportConflict, write_export

        write_export(tmp_path, "c", "r", [self.RESULT])
        with pytest.raises(ExportConflict):
            write_export(tmp_path, "another", "r2", [])
