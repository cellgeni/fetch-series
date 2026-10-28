"""Offline tests for measuring a second route in a covered direction (A6).

``candidates(covered=True)`` names the routes, ``promote`` surveys them beside
their direction's declared routes, and ``survey.directions`` reports what each
adds. None of it changes a ranking.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from fetch_series.accession import EntityType
from fetch_series.cache import SurveyCache
from fetch_series.graph import REGISTRY, Route, RouteCost, RouteRegistry
from fetch_series.results import Outcome, RouteResult
from fetch_series.routes.catalogue import EVERY_DIRECTION, NOT_PROMOTED, Candidate, candidates
from fetch_series.survey import directions, promote
from fetch_series.survey.export import write_export

RUN, STUDY, FILE = EntityType.RUN, EntityType.STUDY, EntityType.FILE
WHEN = datetime(2026, 10, 27, tzinfo=UTC)


def _verdict(
    accession: str, route: str, values: tuple[str, ...] = (), *, failed: bool = False, **kw
) -> RouteResult:
    if failed:
        outcome = Outcome.FAILED
    else:
        outcome = Outcome.RESOLVED if values else Outcome.EMPTY
    return RouteResult(
        accession=accession,
        route_id=route,
        corpus=kw.pop("corpus", "draw"),
        outcome=outcome,
        results=values,
        fetched_at=kw.pop("fetched_at", WHEN),
        **kw,
    )


def _route(route_id: str, source: EntityType, target: EntityType, provider: str = "x") -> Route:
    return Route(
        id=route_id,
        source=source,
        target=target,
        provider=provider,
        summary="test route",
        kb_page=f"routes/{route_id}.md",
        cost=RouteCost(requests=1, rate_limit_rps=10.0),
    )


class TestCoveredCandidates:
    def test_every_one_is_in_a_direction_a_route_covers(self):
        covered = {(route.source, route.target) for route in REGISTRY}
        found = candidates(covered=True)
        assert found
        assert all((c.source, c.target) in covered for c in found)

    def test_the_two_halves_meet_only_where_a_mechanism_is_promoted_everywhere(self):
        both = {c.id for c in candidates()} & {c.id for c in candidates(covered=True)}
        assert all(route_id.rsplit(":", 1)[1] in EVERY_DIRECTION for route_id in both)

    def test_none_is_a_declared_route_or_a_record_about_the_data(self):
        found = candidates(covered=True)
        assert not {c.id for c in found} & {route.id for route in REGISTRY}
        assert not any({c.source, c.target} & NOT_PROMOTED for c in found)


class TestGateFailedElsewhere:
    """One request answers every target, so its gate failure is every target's."""

    def _unit(self, mechanism: str, target: EntityType) -> promote.Unit:
        candidate = Candidate(mechanism, EntityType.BIOPROJECT, target, ())
        return promote.Unit(mechanism, EntityType.BIOPROJECT, (candidate,))

    def _record(self, cache: SurveyCache, route: str, *, failed: bool) -> None:
        cache.record(_verdict("PRJNA1", route, ("x",), failed=failed, corpus="probe-families"))

    def test_a_failure_for_another_target_fails_the_unit(self, tmp_path: Path):
        with SurveyCache(tmp_path / "c.sqlite") as cache:
            self._record(cache, "bioproject->file:sdl", failed=True)
            unit = self._unit("sdl", RUN)
            assert promote.failed_elsewhere(unit, cache) == ["bioproject->file:sdl"]
            assert promote.gate_passed(unit, cache) is False

    def test_another_mechanism_sharing_a_prefix_is_not_the_same_request(self, tmp_path: Path):
        with SurveyCache(tmp_path / "c.sqlite") as cache:
            self._record(cache, "bioproject->file:sdl", failed=True)
            assert promote.failed_elsewhere(self._unit("sdl_lite", RUN), cache) == []

    def test_a_pass_elsewhere_does_not_gate_the_unit(self, tmp_path: Path):
        with SurveyCache(tmp_path / "c.sqlite") as cache:
            self._record(cache, "bioproject->file:sdl", failed=False)
            assert promote.gate_passed(self._unit("sdl", RUN), cache) is None


class TestCompare:
    ACCESSIONS = ("A", "B", "C", "D")

    def test_routes_that_agree_add_nothing(self):
        verdicts = {
            r: {a: _verdict(a, r, (f"{a}1",)) for a in self.ACCESSIONS} for r in ("one", "two")
        }
        found = directions.compare(RUN, STUDY, "draw", self.ACCESSIONS, verdicts)
        assert found.agree == 4
        assert all(s.adds_values == s.sole == 0 for s in found.routes)
        assert not any(s.earns(found.compared) for s in found.routes)

    def test_a_value_only_one_route_returns_is_counted_once_per_accession(self):
        verdicts = {
            "one": {a: _verdict(a, "one", ("x",)) for a in self.ACCESSIONS},
            "two": {
                a: _verdict(a, "two", ("x", "y", "z") if a == "A" else ("x",))
                for a in self.ACCESSIONS
            },
        }
        found = directions.compare(RUN, STUDY, "draw", self.ACCESSIONS, verdicts)
        two = next(s for s in found.routes if s.route_id == "two")
        assert (two.adds_values, two.added_values, two.sole) == (1, 2, 0)

    def test_a_sole_resolution_is_one_where_every_other_was_empty_or_failed(self):
        verdicts = {
            "one": {
                "A": _verdict("A", "one"),
                "B": _verdict("B", "one", failed=True),
                "C": _verdict("C", "one", ("c",)),
                "D": _verdict("D", "one", ("d",)),
            },
            "two": {a: _verdict(a, "two", (a.lower(),)) for a in self.ACCESSIONS},
        }
        found = directions.compare(RUN, STUDY, "draw", self.ACCESSIONS, verdicts)
        two = next(s for s in found.routes if s.route_id == "two")
        assert two.sole == 2
        assert two.adds_values == 2

    def test_an_accession_a_route_never_answered_is_not_compared(self):
        """Absence is not emptiness: it would credit the other route with a sole find."""
        verdicts = {
            "one": {a: _verdict(a, "one", ("x",)) for a in self.ACCESSIONS[:3]},
            "two": {a: _verdict(a, "two", ("x",)) for a in self.ACCESSIONS},
        }
        found = directions.compare(RUN, STUDY, "draw", self.ACCESSIONS, verdicts)
        assert found.compared == 3
        assert found.missing == {"one": 1, "two": 0}
        assert all(s.sole == 0 for s in found.routes)

    def test_a_file_is_compared_by_name_not_by_host(self):
        verdicts = {
            "ena": {"A": _verdict("A", "ena", ("ftp.sra.ebi.ac.uk/vol1/run/A_1.fastq.gz",))},
            "ebi": {"A": _verdict("A", "ebi", ("https://www.ebi.ac.uk/files/E-1/A_1.fastq.gz",))},
        }
        found = directions.compare(RUN, FILE, "draw", ("A",), verdicts)
        assert all(s.adds_values == 0 for s in found.routes)

    @pytest.mark.parametrize(("adds", "compared", "earns"), [(3, 3000, True), (2, 3000, False)])
    def test_the_bar_is_a_tenth_of_a_percent_of_what_was_compared(self, adds, compared, earns):
        share = directions.RouteShare("r", False, 0, 0, 0, 0, adds, adds, 0, None, None)
        assert share.earns(compared) is earns

    def test_the_declared_routes_are_marked(self):
        verdicts = {r: {"A": _verdict("A", r, ("x",))} for r in ("one", "two")}
        found = directions.compare(RUN, STUDY, "draw", ("A",), verdicts, declared=["one"])
        assert {s.route_id: s.declared for s in found.routes} == {"one": True, "two": False}


class TestDrawVerdicts:
    def test_the_draw_is_found_in_the_population_export_and_the_newest_wins(self, tmp_path):
        population = "reprocessed-prj"
        draw = f"sample:2@{population}"
        old, new = WHEN - timedelta(days=40), WHEN
        write_export(
            tmp_path / population,
            population,
            "r",
            [
                _verdict("A", "r", ("a",), corpus=population, fetched_at=old, latency_ms=1),
                _verdict("B", "r", ("b",), corpus=population, fetched_at=old, latency_ms=1),
                _verdict("Z", "r", ("z",), corpus=population, fetched_at=old, latency_ms=1),
            ],
        )
        with SurveyCache(tmp_path / "c.sqlite") as cache:
            cache.record(_verdict("B", "r", ("b2",), corpus=draw, fetched_at=new))
            found = directions.draw_verdicts("r", draw, ("A", "B"), cache, root=tmp_path)
        assert set(found) == {"A", "B"}
        assert found["A"].results == ("a",)
        assert found["B"].results == ("b2",)


class TestDeclaredJobs:
    """A direction's declared routes owe verdicts on the draw its candidates are drawn from."""

    @pytest.fixture
    def registry(self, monkeypatch):
        class _Corpus:
            def of_type(self, entity):
                return ("A", "B")

        monkeypatch.setattr(promote, "standard_corpus", lambda source: "draw")
        monkeypatch.setattr(promote.corpora, "load", lambda name: _Corpus())
        return RouteRegistry([_route("run->study:one", RUN, STUDY, "ena_portal")])

    def test_a_route_with_no_verdict_on_the_draw_is_owed_all_of_it(self, registry, tmp_path):
        with SurveyCache(tmp_path / "c.sqlite") as cache:
            jobs = promote.declared_jobs([(RUN, STUDY)], cache, registry=registry)
        assert [(j.route.id, j.accessions, j.bucket) for j in jobs] == [
            ("run->study:one", ("A", "B"), "ena_portal")
        ]

    def test_a_verdict_before_since_is_owed_again(self, registry, tmp_path):
        with SurveyCache(tmp_path / "c.sqlite") as cache:
            cache.record(_verdict("A", "run->study:one", ("x",), fetched_at=WHEN))
            cache.record(_verdict("B", "run->study:one", ("y",), fetched_at=WHEN - timedelta(40)))
            jobs = promote.declared_jobs(
                [(RUN, STUDY)], cache, since=WHEN - timedelta(days=7), registry=registry
            )
        assert [j.accessions for j in jobs] == [("B",)]

    def test_without_since_any_verdict_will_do(self, registry, tmp_path):
        with SurveyCache(tmp_path / "c.sqlite") as cache:
            for a in ("A", "B"):
                cache.record(_verdict(a, "run->study:one", ("x",), fetched_at=WHEN - timedelta(90)))
            assert promote.declared_jobs([(RUN, STUDY)], cache, registry=registry) == []


class TestBuckets:
    @pytest.mark.parametrize(
        ("provider", "bucket"),
        [
            ("eutils", "ncbi_eutils"),
            ("ena_portal", "ena_portal"),
            ("biostudies", "biostudies"),
            ("ncbi_web", "ncbi_web"),
            ("geo_acc_cgi", "ncbi_web"),
            ("sra_be", "ncbi_web"),
        ],
    )
    def test_a_declared_route_is_paced_under_its_providers_bucket(self, provider, bucket):
        assert promote.bucket_of(_route("r", RUN, STUDY, provider)) == bucket
