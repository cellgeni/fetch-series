"""Each route's recorded verdicts, split over the node pairs it covers.

A route covers several links now that INSDC objects are split by archive:
``bioproject->run:ena_filereport`` reaches SRRs, ERRs and DRRs, and
``run->file:ena_submitted`` FASTQs, BAMs and CRAMs. How the route did on each
is already recorded -- one verdict per accession in ``data/surveys/`` -- so it
is read from there, never measured again and never guessed:

* the **source** of each verdict is its accession, placed on its node by
  :func:`fetch_series.accession.parse`, which reads the archive from the prefix
  letter;
* the **target** of each result is placed the same way, or, for a file, by its
  format (:func:`fetch_series.catalogue.nodes.read_format`).

This is filtered existing measurement. It adds no evidence, changes no
``RouteEvidence`` and no ranking: ``RouteRegistry.ranked()`` still sees one
route with one evidence block. It decides only which of a route's links its
survey actually measured -- a route surveyed on GEO series' experiments
queried no ERX, so says nothing about one -- and how each measured link is
drawn.

The six routes measured by the 2026-05 scripts left no per-accession verdicts,
only their result tables in ``data/query/results/``: one row per project and
run, with a success flag that lumped empty in with failed. Their splits keep
``resolved`` and ``empty`` unknown, as their evidence does.

The exports are Git LFS objects and CI builds the docs without them, so the
result is committed as ``route_splits.json`` by ``fetch catalogue splits
--write``, and a slow test recomputes it.
"""

from __future__ import annotations

import csv
import gzip
from collections import Counter
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from pathlib import Path

from fetch_series.accession import try_parse
from fetch_series.catalogue import Catalogue, NodeKind, Split
from fetch_series.catalogue.nodes import node_for, read_format
from fetch_series.graph import Route, RouteRegistry

SURVEYS = Path("data/surveys")
RESULTS_2026_05 = Path("data/query/results")

# The 2026-05 scripts' result tables, by the route each measured: the script
# that wrote each is named after the file (scripts/query_bioproject2sra_be.py
# writes bioproject2sra_be_summary.csv, and so on).
SUMMARY_EXPORTS: dict[str, tuple[str, str]] = {
    "bioproject->run:sra_be_direct_cgi": ("bioproject2sra_direct_cgi_summary.csv", "Run"),
    "bioproject->run:sra_be_elink": ("bioproject2sra_be_summary.csv", "Run"),
    "bioproject->run:efetch_elink": ("bioproject2sra_summary.csv", "Run"),
    "bioproject->run:efetch_direct": ("bioproject2sra_direct_summary.csv", "Run"),
    "bioproject->geo_series:elink": ("bioproject2geo_summary.csv", "accession"),
    "bioproject->geo_series:gds_direct": ("bioproject2geo_direct_summary.csv", "accession"),
}


def slug(name: str) -> str:
    """The filesystem form ``fetch survey export`` writes a route or corpus under."""
    return name.replace("->", "-to-").replace(":", "_").replace("/", "_")


@dataclass(slots=True)
class Verdict:
    """One accession's outcome over a route, as recorded."""

    accession: str
    outcome: str  # resolved / empty / failed, or "answered" where never split
    results: list[str] = field(default_factory=list)


def export_path(route: Route, root: Path = Path()) -> Path:
    """Where a route's verdicts are: its survey export, or its 2026-05 table."""
    if route.id in SUMMARY_EXPORTS:
        return root / RESULTS_2026_05 / SUMMARY_EXPORTS[route.id][0]
    assert route.evidence is not None
    return root / SURVEYS / slug(route.evidence.corpus) / f"{slug(route.id)}.csv.gz"


def _verdicts(route: Route, path: Path) -> Iterator[Verdict]:
    csv.field_size_limit(1 << 30)
    if route.id in SUMMARY_EXPORTS:
        # One row per (project, result); a project with no successful row got
        # no verdict at all. Answered and empty were never told apart.
        column = SUMMARY_EXPORTS[route.id][1]
        found: dict[str, list[str]] = {}
        with path.open(newline="") as handle:
            for row in csv.DictReader(handle):
                results = found.setdefault(row["bioproject_accession"], [])
                value = (row.get(column) or "").strip()
                if row["success"] == "True" and value:
                    results.append(value)
        for accession, results in found.items():
            yield Verdict(accession, "answered" if results else "failed", results)
        return
    with gzip.open(path, "rt", newline="") as handle:
        for row in csv.DictReader(handle):
            results = [r for r in row["results"].split(";") if r]
            yield Verdict(row["accession"], row["outcome"], results)


def place_target(value: str, files: bool) -> str | None:
    """The node one result lands on: an accession's node, or a file's format."""
    if files:
        return read_format(value)
    parsed = try_parse(value)
    return node_for(parsed) if parsed else None


class SplitError(ValueError):
    """A verdict landed somewhere the route's declarations do not cover."""


def route_splits(
    catalogue: Catalogue, route: Route, verdicts: Iterable[Verdict]
) -> tuple[list[Split], list[str], dict[str, int]]:
    """One :class:`Split` per node pair the route covers whose source was queried.

    A source accession or a result that lands on a node the route does not
    cover is an error, not a silent drop: it means the declarations no longer
    describe what the route returns.

    A verdict on something that is not an accession at all is returned
    alongside, never dropped. The 2026-05 scripts queried the corpus' ``-``
    placeholder as a BioProject, it failed, and their evidence counts it among
    the 12,756 queried; where a route has one source node the verdict is kept
    there, so the split agrees with the evidence and the placeholder is named.

    So is a verdict on a source the declarations rule out -- an ERR asked for
    its GSM, which GEO never brokers. Every one so far found nothing, which is
    the evidence for ruling it out; they are counted and returned. One that
    found something would contradict the rule, and is an error.
    """
    links = catalogue.route_links(route.id)
    sources = {link.source for link in links}
    targets = {link.target for link in links}
    files = all(catalogue.nodes[t].kind is NodeKind.READ_FORMAT for t in targets)

    outcomes: dict[str, Counter[str]] = {}
    reached: dict[tuple[str, str], int] = Counter()
    distinct: dict[tuple[str, str], set[str]] = {}
    stray: Counter[str] = Counter()
    unparsed: list[str] = []
    excluded: Counter[str] = Counter()
    for verdict in verdicts:
        parsed = try_parse(verdict.accession)
        source = node_for(parsed) if parsed else None
        if parsed is None and len(sources) == 1:
            unparsed.append(verdict.accession)
            source = next(iter(sources))
        ruled_out = source is not None and source not in sources and parsed is not None
        if (
            ruled_out
            and parsed is not None
            and parsed.entity is route.source
            and not any(place_target(r, files) for r in verdict.results)
        ):
            assert source is not None
            excluded[source] += 1
            continue
        if source is None or source not in sources:
            raise SplitError(f"{route.id}: {verdict.accession} lands on no source it covers")
        outcomes.setdefault(source, Counter())[verdict.outcome] += 1
        landed: dict[str, set[str]] = {}
        for result in verdict.results:
            target = place_target(result, files)
            if target is None:
                # An index, a file in no read format, or an accession of an
                # archive with no node: E-MTAB-4748's IDF names EGAS00001000593.
                continue
            if target not in targets:
                stray[f"{source}->{target}"] += 1
                continue
            landed.setdefault(target, set()).add(result)
        for target, values in landed.items():
            reached[(source, target)] += 1
            distinct.setdefault((source, target), set()).update(values)
    if stray:
        raise SplitError(f"{route.id}: results on nodes it does not cover: {dict(stray)}")

    splits = []
    for link in links:
        counts = outcomes.get(link.source)
        if not counts:
            continue
        queried = sum(counts.values())
        known = "answered" not in counts
        splits.append(
            Split(
                route=route.id,
                source=link.source,
                target=link.target,
                queried=queried,
                resolved=counts["resolved"] if known else None,
                empty=counts["empty"] if known else None,
                failed=counts["failed"],
                reached=reached[(link.source, link.target)],
                distinct=len(distinct.get((link.source, link.target), ())),
            )
        )
    return splits, sorted(unparsed), dict(sorted(excluded.items()))


def compute(
    catalogue: Catalogue, registry: RouteRegistry, root: Path = Path()
) -> tuple[list[Split], dict[str, dict[str, object]]]:
    """Every route's splits, and per route the export read and anything unparsed."""
    splits: list[Split] = []
    sources: dict[str, dict[str, object]] = {}
    for route in sorted(registry, key=lambda r: r.id):
        path = export_path(route, root)
        if not path.exists():
            raise FileNotFoundError(f"{route.id}: no recorded verdicts at {path}")
        found, unparsed, excluded = route_splits(catalogue, route, _verdicts(route, path))
        splits.extend(found)
        sources[route.id] = {
            "export": str(path.relative_to(root)),
            "unparsed": unparsed,
            "excluded": excluded,
        }
    return splits, sources


def cross_archive_pairs(root: Path = Path()) -> Counter[str]:
    """Every recorded verdict whose source and result are INSDC objects of two archives.

    The catalogue declares none: one submission's objects share an archive.
    This is the check that the assumption still holds.
    """
    found: Counter[str] = Counter()
    csv.field_size_limit(1 << 30)
    for path in sorted((root / SURVEYS).glob("*/*.csv.gz")):
        with gzip.open(path, "rt", newline="") as handle:
            reader = csv.DictReader(handle)
            if "accession" not in (reader.fieldnames or []):
                continue  # a file-verification table, not verdicts
            for row in reader:
                source = try_parse(row["accession"])
                if source is None or not source.is_insdc:
                    continue
                for value in filter(None, row["results"].split(";")):
                    result = try_parse(value)
                    if result and result.is_insdc and result.archive != source.archive:
                        found[f"{path.parent.name}/{path.name}"] += 1
    return found
