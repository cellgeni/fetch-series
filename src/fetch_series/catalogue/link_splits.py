"""What a link survey measured, link by link and corpus by corpus.

Read out of the committed exports (``data/surveys/*/link_*.csv.gz``), never out
of the uncommitted cache, so ``link_splits.json`` is reproducible on any
checkout that has fetched the Git LFS objects -- and a slow test checks that it
is.

A link split is measurement, and it is **not a ranking**. A draw of 300 puts a
95% interval of up to ±5.7 points on a yield: enough to say a link answers
always, sometimes or never, and far too coarse for the 1% band the route
ranking separates on. Nothing in ``graph.py`` or ``resolver.py`` reads this
module, and a test keeps it that way. Promoting a link to a route means
surveying it as a route, at a ranking-scale draw.

Three things are refused rather than counted:

* a **gate corpus** -- ``hard-cases``, ``probe-families``, anything drawn from
  them. Every accession in one is there because it broke something or because
  its link was known to answer, so a rate over it measures the selection;
* an **undeclared link** -- a split for a line the catalogue does not draw;
* a **route-measured link** -- its evidence is its route's split, and a second,
  smaller measurement beside it would read as an independent confirmation.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from fetch_series.results import Outcome, RouteResult
from fetch_series.survey.corpora import is_gate
from fetch_series.survey.export import SURVEYS_DIR, is_lfs_pointer, read_export, slug
from fetch_series.units import LINK_PREFIX, link_unit

LINK_SPLITS_PATH = Path(__file__).with_name("link_splits.json")


@dataclass(frozen=True, slots=True)
class CheckCounts:
    """How one check went across the accessions whose link found something."""

    passed: int = 0
    failed: int = 0
    undetermined: int = 0


@dataclass(frozen=True, slots=True)
class LinkSplit:
    """One link's verdicts over one corpus.

    Args:
        queried, resolved, empty, failed: the three outcomes, and their total.
        reached: accessions with at least one value on the target. A link's
            values are already the target's, so this equals ``resolved``; it is
            kept so a link split and a route split read alike.
        usable: reached, *and* every check made on what was found passed. For
            a link into a record there are no checks and this is ``reached``.
            For a link into a read file it is what matters: an ENA fastq that
            lists one file of a paired 10x library is reached and not usable.
        distinct: distinct values found across the corpus.
        checks: by check name, how often it passed, failed and could not be
            determined. Undetermined is not a pass.
        surveyed_on: the UTC date of the latest verdict.
    """

    link: str
    corpus: str
    queried: int
    resolved: int
    empty: int
    failed: int
    reached: int
    usable: int
    distinct: int
    surveyed_on: date
    checks: dict[str, CheckCounts] = field(default_factory=dict)

    @property
    def answered(self) -> int:
        """Accessions that got an answer. A split that only failed measured nothing."""
        return self.resolved + self.empty


class LinkSplitError(ValueError):
    """A split that must not be computed."""


def split_of(
    link_id: str,
    corpus: str,
    results: Iterable[RouteResult],
    *,
    declared: Mapping[str, object],
    measured: set[str],
) -> LinkSplit:
    """One link's split over one corpus, refusing what must not be counted."""
    if is_gate(corpus):
        raise LinkSplitError(f"{corpus} is a gate, never evidence ({link_id})")
    if link_id not in declared:
        raise LinkSplitError(f"{link_id} is not a declared link")
    if link_id in measured:
        raise LinkSplitError(
            f"{link_id} is measured by its route; a link split would sit beside it"
        )
    outcomes = {o: 0 for o in Outcome}
    reached = usable = 0
    distinct: set[str] = set()
    checks: dict[str, list[int]] = {}
    latest: date | None = None
    for result in results:
        outcomes[result.outcome] += 1
        day = result.fetched_at.date()
        latest = day if latest is None or day > latest else latest
        if result.outcome is not Outcome.RESOLVED:
            continue
        reached += 1
        distinct.update(result.results)
        made = [tuple(c) for c in (result.detail or {}).get("checks", ())]
        for name, passed in made:
            counts = checks.setdefault(str(name), [0, 0, 0])
            counts[0 if passed is True else 1 if passed is False else 2] += 1
        usable += all(passed is True for _, passed in made)
    if latest is None:
        raise LinkSplitError(f"no verdicts for {link_id} over {corpus}")
    return LinkSplit(
        link=link_id,
        corpus=corpus,
        queried=sum(outcomes.values()),
        resolved=outcomes[Outcome.RESOLVED],
        empty=outcomes[Outcome.EMPTY],
        failed=outcomes[Outcome.FAILED],
        reached=reached,
        usable=usable,
        distinct=len(distinct),
        surveyed_on=latest,
        checks={name: CheckCounts(*counts) for name, counts in sorted(checks.items())},
    )


def _link_exports(root: Path) -> Iterator[tuple[str, str, Path]]:
    """Every committed link export: (corpus, link id, path)."""
    for manifest in sorted(root.glob("*/manifest.txt")):
        if is_lfs_pointer(manifest):
            continue
        corpus = manifest.read_text().strip().removeprefix("corpus: ")
        for path in sorted(manifest.parent.glob(f"{slug(LINK_PREFIX)}*.csv.gz")):
            yield corpus, path.name.removesuffix(".csv.gz"), path


def compute(
    catalogue: Any, root: Path | None = None, *, only: set[str] | None = None
) -> list[LinkSplit]:
    """Every link split the committed exports support, gate corpora set aside.

    ``only`` restricts to those link ids -- a wave's, when results are reported
    a wave at a time.

    The link id is recovered by matching the export's file name against the
    slug of every declared link: a slug is not reversible on its own.
    """
    by_slug = {slug(link_unit(link.id)): link.id for link in catalogue.links}
    declared = {link.id: link for link in catalogue.links}
    measured = {link.id for link in catalogue.links if catalogue.split(link) is not None}
    splits = []
    for corpus, stem, path in _link_exports(root or SURVEYS_DIR):
        if is_gate(corpus):
            continue
        if stem not in by_slug:
            raise LinkSplitError(f"{path} names no declared link")
        link_id = by_slug[stem]
        if only is not None and link_id not in only:
            continue
        results = read_export(path, corpus, link_unit(link_id))
        splits.append(split_of(link_id, corpus, results, declared=declared, measured=measured))
    return sorted(splits, key=lambda s: (s.link, s.corpus))


def _as_json(split: LinkSplit) -> dict[str, Any]:
    row = asdict(split)
    del row["link"]
    row["surveyed_on"] = split.surveyed_on.isoformat()
    row["checks"] = {name: asdict(counts) for name, counts in split.checks.items()}
    return row


def save_link_splits(
    splits: Iterable[LinkSplit],
    no_population: Mapping[str, tuple[str, date]],
    path: Path = LINK_SPLITS_PATH,
) -> None:
    """Write splits by link, sorted, with the nodes that have no population and why."""
    by_link: dict[str, list[dict[str, Any]]] = {}
    for split in sorted(splits, key=lambda s: (s.link, s.corpus)):
        by_link.setdefault(split.link, []).append(_as_json(split))
    payload = {
        "note": (
            "Written by `fetch catalogue link-splits --write` from the committed link-survey "
            "exports. Measurement of whether a link answers over a seeded draw: never a "
            "ranking, never RouteEvidence."
        ),
        "no_population": {
            node: {"reason": reason, "recorded_on": when.isoformat()}
            for node, (reason, when) in sorted(no_population.items())
        },
        "links": by_link,
    }
    path.write_text(json.dumps(payload, indent=1) + "\n")


def load_link_splits(path: Path = LINK_SPLITS_PATH) -> list[LinkSplit]:
    """Recorded link splits; empty if none have been computed."""
    if not path.exists():
        return []
    payload = json.loads(path.read_text())
    return [
        LinkSplit(
            link=link_id,
            corpus=row["corpus"],
            queried=row["queried"],
            resolved=row["resolved"],
            empty=row["empty"],
            failed=row["failed"],
            reached=row["reached"],
            usable=row["usable"],
            distinct=row["distinct"],
            surveyed_on=date.fromisoformat(row["surveyed_on"]),
            checks={name: CheckCounts(**counts) for name, counts in row["checks"].items()},
        )
        for link_id, rows in payload["links"].items()
        for row in rows
    ]


__all__ = [
    "LINK_SPLITS_PATH",
    "CheckCounts",
    "LinkSplit",
    "LinkSplitError",
    "compute",
    "load_link_splits",
    "save_link_splits",
    "split_of",
]
