"""What each route in a direction adds that the direction's other routes do not.

Union mode asks every route in a direction, so a route that only repeats the
others costs a request on every resolve and returns nothing new. Whether a
second route earns its place, and whether a declared one still does, is measured
here: every route of the direction on one draw, accession by accession.

The rule was set before measuring, on 2026-10-27, and is argued in
docs/decisions/when-a-second-route-earns-its-place.md. A route earns its place
when, over the accessions every route of its direction answered, it adds what
none of the others returns for at least ``ADDS_SHARE`` of them:

- a value none of the others returns; or
- a resolution where every other route was empty or failed.

Nothing here changes ranking, union mode or ``--explain``. It measures.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

from fetch_series.accession import EntityType
from fetch_series.cache import SurveyCache
from fetch_series.results import Outcome, RouteResult
from fetch_series.survey.export import export_path, is_lfs_pointer, read_export

# The FAILURE_BAND scale, fixed on 2026-10-27 before anything was measured. Not
# imported from graph: retuning the ranking's band must not move this rule.
ADDS_SHARE = 0.001


def population_of(corpus: str) -> str:
    """``sample:3000@reprocessed-prj`` -> ``reprocessed-prj``; a whole population is its own."""
    if corpus.startswith("sample:") and "@" in corpus:
        return corpus.split("@", 1)[1]
    return corpus


def draw_verdicts(
    route_id: str,
    corpus: str,
    accessions: Iterable[str],
    cache: SurveyCache,
    *,
    root: Path | None = None,
) -> dict[str, RouteResult]:
    """A route's newest verdict on each of the draw's accessions, wherever it was recorded.

    Three places, newest verdict per accession winning: the cache under the
    draw's own name; the committed export of the draw; and the committed export
    of the population it was drawn from, which holds the draw's accessions
    wherever the route was surveyed over all of it. ``efetch_direct`` was
    surveyed over all 12,755 BioProjects, so its verdicts on
    ``sample:3000@reprocessed-prj`` are already recorded, under another name.
    """
    wanted = set(accessions)
    found: dict[str, RouteResult] = {}

    def offer(results: Iterable[RouteResult]) -> None:
        for result in results:
            if result.accession not in wanted:
                continue
            held = found.get(result.accession)
            if held is None or result.fetched_at > held.fetched_at:
                found[result.accession] = result

    offer(cache.results(corpus, route_id))
    for name in dict.fromkeys((corpus, population_of(corpus))):
        path = export_path(name, route_id, root)
        if path is not None and not is_lfs_pointer(path):
            offer(read_export(path, name, route_id))
    return found


def comparable(value: str, files: bool) -> str:
    """The form two routes' values are compared in.

    A file is compared by its name: ENA lists ``ftp.sra.ebi.ac.uk/...`` and
    BioStudies ``https://www.ebi.ac.uk/...`` for one file, and counting the
    second as something the first lacks would let every file route earn its
    place by spelling.
    """
    return value.rsplit("/", 1)[-1] if files else value


@dataclass(frozen=True, slots=True)
class RouteShare:
    """One route's verdicts over a direction's compared accessions, and what only it gave."""

    route_id: str
    declared: bool
    resolved: int
    empty: int
    failed: int
    distinct: int
    #: Accessions where it returned a value none of the others did.
    adds_values: int
    #: How many such values, over all of them.
    added_values: int
    #: Accessions it resolved where every other route was empty or failed.
    sole: int
    first: date | None
    last: date | None

    def earns(self, compared: int) -> bool:
        bar = ADDS_SHARE * compared
        return compared > 0 and (self.adds_values >= bar or self.sole >= bar)


@dataclass(frozen=True, slots=True)
class Direction:
    """Every route of one direction over one draw."""

    source: EntityType
    target: EntityType
    corpus: str
    drawn: int
    #: Accessions every route has a verdict for: the only ones compared.
    compared: int
    #: Accessions where every route answered the same outcome and values.
    agree: int
    routes: tuple[RouteShare, ...]
    #: Per route, how many of the draw's accessions it holds no verdict for.
    missing: dict[str, int] = field(default_factory=dict)

    @property
    def bar(self) -> float:
        return ADDS_SHARE * self.compared


def compare(
    source: EntityType,
    target: EntityType,
    corpus: str,
    accessions: Sequence[str],
    verdicts: Mapping[str, Mapping[str, RouteResult]],
    *,
    declared: Iterable[str] = (),
) -> Direction:
    """What each route of the direction adds over the accessions every route answered.

    Only those: an accession one route was never asked is not one it answered
    empty, and treating absence as emptiness would credit every other route with
    a sole resolution it may not have.
    """
    files = target is EntityType.FILE
    declared_ids = set(declared)
    route_ids = list(verdicts)
    missing = {r: sum(a not in verdicts[r] for a in accessions) for r in route_ids}
    compared = [a for a in accessions if all(a in verdicts[r] for r in route_ids)]

    values: dict[str, dict[str, set[str]]] = {
        r: {
            a: {comparable(v, files) for v in verdicts[r][a].results}
            for a in compared
            if verdicts[r][a].outcome is Outcome.RESOLVED
        }
        for r in route_ids
    }
    agree = sum(
        len({(verdicts[r][a].outcome, frozenset(values[r].get(a, frozenset()))) for r in route_ids})
        == 1
        for a in compared
    )

    shares = []
    for r in route_ids:
        others = [o for o in route_ids if o != r]
        adds_values = added_values = sole = 0
        for a, mine in values[r].items():
            theirs = set().union(*(values[o].get(a, set()) for o in others))
            extra = mine - theirs
            if extra:
                adds_values += 1
                added_values += len(extra)
            if others and not any(a in values[o] for o in others):
                sole += 1
        outcomes = [verdicts[r][a].outcome for a in compared]
        dates = [verdicts[r][a].fetched_at for a in compared]
        shares.append(
            RouteShare(
                route_id=r,
                declared=r in declared_ids,
                resolved=outcomes.count(Outcome.RESOLVED),
                empty=outcomes.count(Outcome.EMPTY),
                failed=outcomes.count(Outcome.FAILED),
                distinct=len({v for found in values[r].values() for v in found}),
                adds_values=adds_values,
                added_values=added_values,
                sole=sole,
                first=_day(min(dates)) if dates else None,
                last=_day(max(dates)) if dates else None,
            )
        )
    return Direction(
        source=source,
        target=target,
        corpus=corpus,
        drawn=len(accessions),
        compared=len(compared),
        agree=agree,
        routes=tuple(shares),
        missing=missing,
    )


def _day(when: datetime) -> date:
    return when.date()


def report(direction: Direction) -> list[str]:
    """A direction as the lines ``fetch survey promote --compare`` prints."""
    d = direction
    lines = [
        f"{d.source.value} -> {d.target.value}  over {d.corpus}: "
        f"{d.compared:,} of {d.drawn:,} compared, {d.agree:,} agree; bar {d.bar:.1f}",
    ]
    for share in sorted(d.routes, key=lambda s: (not s.declared, s.route_id)):
        span = f"{share.first}..{share.last}" if share.first != share.last else f"{share.first}"
        lines.append(
            f"  {'declared ' if share.declared else 'candidate'} {share.route_id:58s} "
            f"r={share.resolved:<5} e={share.empty:<5} f={share.failed:<4} "
            f"adds={share.adds_values:<4} (+{share.added_values:,}) sole={share.sole:<4} "
            f"{'EARNS' if share.earns(d.compared) else '-':5s} {span}"
        )
    for route_id, count in sorted(d.missing.items()):
        if count:
            lines.append(f"  no verdict: {route_id} for {count:,} of the draw")
    return lines


__all__ = [
    "ADDS_SHARE",
    "Direction",
    "RouteShare",
    "comparable",
    "compare",
    "draw_verdicts",
    "population_of",
    "report",
]
