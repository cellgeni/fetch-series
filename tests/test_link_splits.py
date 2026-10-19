"""Link splits: what a link survey measured, how it sets a link's status, and that it ranks nothing."""

from __future__ import annotations

import ast
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from fetch_series.catalogue import CATALOGUE, DRAWN, Catalogue, Probe, Status, load_splits
from fetch_series.catalogue.link_splits import (
    LINK_SPLITS_PATH,
    LinkSplit,
    LinkSplitError,
    compute,
    load_link_splits,
    save_link_splits,
    split_of,
)
from fetch_series.catalogue.mechanisms import MECHANISMS
from fetch_series.catalogue.nodes import FAMILIES, NODES, PROBE_ACCESSIONS, TEN_X_PROBES
from fetch_series.export.graph import route_graph
from fetch_series.graph import REGISTRY
from fetch_series.results import Outcome, RouteResult
from fetch_series.survey.export import write_export
from fetch_series.units import link_unit

UNMEASURED = CATALOGUE.link("run_ebi->submission_ebi:ena_filereport")
READS = CATALOGUE.link("run_ddbj->bam:ena_filereport_bam")
MEASURED = next(link for link in CATALOGUE if CATALOGUE.split(link) is not None)
DECLARED = {link.id: link for link in CATALOGUE}
MEASURED_IDS = {link.id for link in CATALOGUE if CATALOGUE.split(link) is not None}
CORPUS = "sample:300@node:run_ebi@reprocessed-srr"


def _when(day: int) -> datetime:
    return datetime(2026, 10, day, 23, 30, tzinfo=UTC)


def _ok(accession: str, values: list[str], checks=None, day: int = 20) -> RouteResult:
    detail = {"checks": checks, "checked": "x"} if checks else None
    result = RouteResult.from_results(
        accession, link_unit(UNMEASURED.id), CORPUS, values, 1, detail=detail
    )
    return RouteResult(
        **{**{f: getattr(result, f) for f in result.__slots__}, "fetched_at": _when(day)}
    )


def _split(results, link=UNMEASURED, corpus=CORPUS) -> LinkSplit:
    return split_of(link.id, corpus, results, declared=DECLARED, measured=MEASURED_IDS)


class TestSplitOf:
    def test_three_outcomes_and_distinct_values(self):
        split = _split(
            [
                _ok("ERR1", ["ERA1"]),
                _ok("ERR2", ["ERA1", "ERA2"]),
                _ok("ERR3", []),
                RouteResult.from_error("ERR4", link_unit(UNMEASURED.id), CORPUS, ValueError(), 1),
            ]
        )
        assert (split.queried, split.resolved, split.empty, split.failed) == (4, 2, 1, 1)
        assert split.reached == split.usable == 2 and split.distinct == 2

    def test_usable_needs_every_check_to_pass(self):
        """Reached and not usable: ENA's lone fastq for a paired 10x library."""
        split = _split(
            [
                _ok("ERR1", ["a"], [["downloadable", True], ["complete", True]]),
                _ok("ERR2", ["b"], [["downloadable", True], ["complete", False]]),
                _ok("ERR3", ["c"], [["downloadable", True], ["complete", None]]),
            ],
            link=READS,
        )
        assert split.reached == 3 and split.usable == 1
        assert split.checks["complete"].passed == 1
        assert split.checks["complete"].failed == 1
        assert split.checks["complete"].undetermined == 1

    def test_the_date_is_the_latest_verdict_in_utc(self):
        split = _split([_ok("ERR1", ["a"], day=19), _ok("ERR2", ["b"], day=21)])
        assert split.surveyed_on == date(2026, 10, 21)

    @pytest.mark.parametrize("gate", ["hard-cases", "probe-families", "sample:5@probe-families"])
    def test_a_gate_is_never_evidence(self, gate):
        with pytest.raises(LinkSplitError, match="gate"):
            _split([_ok("ERR1", ["a"])], corpus=gate)

    def test_an_undeclared_link_is_refused(self):
        with pytest.raises(LinkSplitError, match="declared"):
            split_of(
                "a->b:nothing", CORPUS, [_ok("ERR1", ["a"])], declared=DECLARED, measured=set()
            )

    def test_a_route_measured_link_is_refused(self):
        with pytest.raises(LinkSplitError, match="measured"):
            _split([_ok("ERR1", ["a"])], link=MEASURED)

    def test_no_verdicts_is_no_split(self):
        with pytest.raises(LinkSplitError):
            _split([])


def _catalogue(*link_splits: LinkSplit) -> Catalogue:
    return Catalogue(
        NODES,
        MECHANISMS,
        PROBE_ACCESSIONS,
        families=FAMILIES,
        ten_x=TEN_X_PROBES,
        splits=load_splits(),
        link_splits=link_splits,
    )


def _made(link=UNMEASURED, *, resolved=10, empty=5, failed=0, usable=None) -> LinkSplit:
    return LinkSplit(
        link=link.id,
        corpus=CORPUS,
        queried=resolved + empty + failed,
        resolved=resolved,
        empty=empty,
        failed=failed,
        reached=resolved,
        usable=resolved if usable is None else usable,
        distinct=resolved,
        surveyed_on=date(2026, 10, 20),
    )


PROBE = Probe(UNMEASURED.id, "ERR1", date(2026, 10, 1), Outcome.RESOLVED, ("x",), 1)


class TestStatusPrecedence:
    def test_a_link_split_beats_a_probe(self):
        catalogue = _catalogue(_made())
        assert catalogue.status(UNMEASURED, {UNMEASURED.id: PROBE}) is Status.SURVEYED

    def test_nothing_usable_is_surveyed_none_even_beside_a_confirming_probe(self):
        """A real finding: the link exists, and over these records it never answered."""
        catalogue = _catalogue(_made(usable=0))
        assert catalogue.status(UNMEASURED, {UNMEASURED.id: PROBE}) is Status.SURVEYED_NONE
        assert Status.SURVEYED_NONE not in DRAWN

    def test_a_split_that_only_failed_falls_through_to_the_probe(self):
        catalogue = _catalogue(_made(resolved=0, empty=0, failed=300))
        assert catalogue.status(UNMEASURED, {UNMEASURED.id: PROBE}) is Status.UNTESTED

    def test_a_route_split_beats_a_link_split(self):
        """A link split for a measured link is refused at compute time; even if
        one arrived, the route's evidence would still decide."""
        catalogue = _catalogue(_made(MEASURED, usable=0))
        assert catalogue.status(MEASURED, {}) in (Status.MEASURED, Status.MEASURED_NONE)

    def test_any_usable_corpus_draws_the_link(self):
        ncbi = _made(usable=0)
        ebi = LinkSplit(
            **{**{f: getattr(ncbi, f) for f in ncbi.__slots__}, "corpus": "x", "usable": 3}
        )
        assert _catalogue(ncbi, ebi).status(UNMEASURED, {}) is Status.SURVEYED

    def test_surveyed_is_drawn(self):
        assert Status.SURVEYED in DRAWN


class TestNeverRanked:
    def test_ranking_is_identical_with_and_without_link_splits(self):
        """Ranking reads REGISTRY alone; a link split cannot reach it."""
        types = {(r.source, r.target) for r in REGISTRY}
        before = {pair: [r.id for r in REGISTRY.ranked(*pair)] for pair in types}
        _catalogue(*(_made(link) for link in CATALOGUE.untested[:50]))
        after = {pair: [r.id for r in REGISTRY.ranked(*pair)] for pair in types}
        assert before == after

    @pytest.mark.parametrize("module", ["graph.py", "resolver.py"])
    def test_the_ranking_and_the_resolver_never_read_link_splits(self, module):
        tree = ast.parse(Path("src/fetch_series", module).read_text())
        imported = {
            node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
        } | {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        assert not any("link_splits" in name or name.endswith("catalogue") for name in imported)

    def test_a_surveyed_edge_carries_counts_and_no_rank(self):
        catalogue = _catalogue(_made())
        graph = route_graph(catalogue=catalogue, probes={})
        (edge,) = [e for e in graph["edges"] if e["status"] == "surveyed"]
        assert "rank" not in edge and "of" not in edge
        (entry,) = edge["links"]
        assert entry["splits"][0]["usable"] == 10
        assert graph["paths_surveyed"]["run_ebi"]["submission_ebi"] == [
            ["run_ebi", "submission_ebi"]
        ]

    def test_surveyed_none_is_listed_and_not_drawn(self):
        catalogue = _catalogue(_made(usable=0))
        graph = route_graph(catalogue=catalogue, probes={})
        assert not [e for e in graph["edges"] if e["status"] == "surveyed"]
        assert graph["surveyed_none"][0]["links"][0]["splits"][0]["usable"] == 0


class TestCompute:
    def test_a_committed_export_becomes_a_split(self, tmp_path):
        write_export(tmp_path / "x", CORPUS, link_unit(UNMEASURED.id), [_ok("ERR1", ["ERA1"])])
        (split,) = compute(CATALOGUE, tmp_path)
        assert (split.link, split.corpus, split.usable) == (UNMEASURED.id, CORPUS, 1)

    def test_a_gate_export_is_set_aside(self, tmp_path):
        write_export(
            tmp_path / "g", "probe-families", link_unit(UNMEASURED.id), [_ok("ERR1", ["a"])]
        )
        assert compute(CATALOGUE, tmp_path) == []

    def test_a_route_export_is_not_a_link_split(self, tmp_path):
        write_export(tmp_path / "r", CORPUS, "run->experiment:ena_filereport", [])
        assert compute(CATALOGUE, tmp_path) == []

    def test_an_export_naming_no_declared_link_raises(self, tmp_path):
        write_export(tmp_path / "x", CORPUS, link_unit("a->b:nothing"), [_ok("ERR1", ["a"])])
        with pytest.raises(LinkSplitError, match="no declared link"):
            compute(CATALOGUE, tmp_path)

    def test_save_and_load_round_trip(self, tmp_path):
        split = _split(
            [_ok("ERR1", ["a"], [["downloadable", True], ["complete", None]]), _ok("ERR2", [])]
        )
        path = tmp_path / "link_splits.json"
        save_link_splits([split], {"analysis_ncbi": ("none", date(2026, 10, 19))}, path)
        assert load_link_splits(path) == [split]


@pytest.mark.slow
def test_the_committed_link_splits_are_the_exports():
    """link_splits.json is derived; regenerating it from the exports changes nothing."""
    if not LINK_SPLITS_PATH.exists():
        pytest.skip("no link splits committed yet")
    try:
        computed = compute(CATALOGUE)
    except FileNotFoundError:
        pytest.skip("survey exports not present (Git LFS objects not fetched)")
    assert computed == load_link_splits()
