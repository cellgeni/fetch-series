"""The link catalogue: every one-hop link the archives offer, measured or not.

The route registry in :mod:`fetch_series.graph` holds the links somebody has
measured. That is a small part of what the archives answer: one ENA
``filereport`` request returns seven INSDC levels from any one of them, for
all three archives' records. This package records the rest, so the gaps are
visible as gaps rather than as an absence nobody looked at.

It is kept apart from ``REGISTRY`` deliberately, and nothing here can change
the resolver, the ranking, or the parity of ``links.tsv``:

* a catalogue :class:`Link` is a statement that a link **exists**;
* a :class:`~fetch_series.graph.Route` is a statement about which link to
  **use**, and only a route carries evidence.

A route covers several links now that INSDC objects are split by archive:
``bioproject->run:ena_filereport`` is BioProject to SRR, to ERR and to DRR. How
each of those did is read out of the route's own recorded verdicts
(:mod:`fetch_series.catalogue.splits`) -- filtered existing measurement, never
new evidence. A link is ``measured`` where its route's survey queried at least
one accession of its source, and is otherwise confirmed by one live
:class:`Probe`. A probe proves existence and measures nothing, so it never
becomes ``RouteEvidence`` (``docs/decisions/what-counts-as-evidence.md``).
"""

from __future__ import annotations

import json
from collections import deque
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import asdict, dataclass
from datetime import date
from enum import StrEnum
from pathlib import Path

from fetch_series.accession import Archive
from fetch_series.catalogue.link_splits import LinkSplit, load_link_splits
from fetch_series.catalogue.mechanisms import MECHANISMS
from fetch_series.catalogue.model import (
    INSDC,
    PROBE_KEEP,
    Basis,
    Level,
    Link,
    Mechanism,
    Node,
    NodeKind,
    Origin,
    Probe,
)
from fetch_series.catalogue.nodes import (
    FAMILIES,
    FAMILY_OF,
    NODES,
    PROBE_ACCESSIONS,
    TEN_X_PROBES,
)
from fetch_series.graph import PROMOTED
from fetch_series.results import Check, Outcome

__all__ = [
    "CATALOGUE",
    "DRAWN",
    "INSDC",
    "PROBES_PATH",
    "PROBE_KEEP",
    "PROMOTED_LINKS",
    "SPLITS_PATH",
    "Basis",
    "Catalogue",
    "Check",
    "Level",
    "Link",
    "LinkSplit",
    "Mechanism",
    "Node",
    "NodeKind",
    "Origin",
    "Probe",
    "Split",
    "Status",
    "load_probes",
    "load_splits",
    "merge_probes",
    "save_probes",
    "save_splits",
    "status",
]

PROBES_PATH = Path(__file__).with_name("probes.json")
SPLITS_PATH = Path(__file__).with_name("route_splits.json")


class Status(StrEnum):
    """What is known about one link, from the strongest evidence it has.

    In order of precedence:

    1. a **route split** -- ``MEASURED``: a declared route's survey queried
       accessions of its source, and some reached its target.
       ``MEASURED_NONE``: queried, and none reached it.
    2. a **link split** -- ``SURVEYED``: a link survey's seeded draw found
       something usable (reached, and every check passed) for at least one
       accession. ``SURVEYED_NONE``: the draw answered, and nothing usable was
       found -- listed, never drawn. A split that only ever *failed* measured
       nothing, so it does not count here and the link falls through.
    3. a **probe** -- ``UNTESTED``: one probe found it answering.
       ``UNCONFIRMED``: the probe found nothing, found something that failed a
       check, or did not complete -- listed, never drawn.
    4. nothing -- ``UNPROBED``; a test keeps this from shipping.

    ``SURVEYED_NONE`` beside a probe that confirmed the link is a real finding,
    not a contradiction: the link exists, and over this project's records it
    never answered.
    """

    MEASURED = "measured"
    MEASURED_NONE = "measured-none"
    SURVEYED = "surveyed"
    SURVEYED_NONE = "surveyed-none"
    UNTESTED = "untested"
    UNCONFIRMED = "unconfirmed"
    UNPROBED = "unprobed"


DRAWN = (Status.MEASURED, Status.SURVEYED, Status.UNTESTED)


@dataclass(frozen=True, slots=True)
class Split:
    """One route's verdicts, restricted to one source node and one target node.

    Args:
        queried, resolved, empty, failed: the route's outcomes among the
            accessions of ``source`` -- SRRs alone, where the source is split by
            archive, or every accession, where it is not (a BioProject). These
            are the route's own outcomes, whatever the target: an SRR resolved
            when it returned anything at all. ``resolved`` and ``empty`` are
            ``None`` where the survey never recorded the split, as for the six
            2026-05 routes.
        reached: of those accessions, how many had at least one result on
            ``target`` -- an ERR among a BioProject's runs, a BAM among a run's
            files.
        distinct: distinct results on ``target``.
    """

    route: str
    source: str
    target: str
    queried: int
    resolved: int | None
    empty: int | None
    failed: int
    reached: int
    distinct: int

    @property
    def share(self) -> float | None:
        """Resolved among queried: a route line's ink."""
        if self.resolved is None or not self.queried:
            return None
        return self.resolved / self.queried


class Catalogue:
    """Nodes, the mechanisms that link them, and the links that result.

    Construction validates the declarations rather than trusting them: every
    node a mechanism names exists, every route it claims covers at least one of
    its own links, and no route is claimed by two mechanisms. A catalogue that
    assembled anyway would draw a line to nowhere or attribute one route to two
    requests.
    """

    def __init__(
        self,
        nodes: Iterable[Node],
        mechanisms: Iterable[Mechanism],
        probe_accessions: Mapping[str, str],
        *,
        families: Mapping[Archive, Mapping[str, str]] | None = None,
        ten_x: Mapping[str, str] | None = None,
        splits: Iterable[Split] = (),
        link_splits: Iterable[LinkSplit] = (),
        promoted: Iterable[tuple[str, str, str, str]] = (),
    ) -> None:
        self.nodes: dict[str, Node] = {}
        for node in nodes:
            if node.id in self.nodes:
                raise ValueError(f"Duplicate node id: {node.id}")
            self.nodes[node.id] = node
        self.mechanisms: dict[str, Mechanism] = {}
        for mechanism in mechanisms:
            if mechanism.id in self.mechanisms:
                raise ValueError(f"Duplicate mechanism id: {mechanism.id}")
            self.mechanisms[mechanism.id] = mechanism
        self.probe_accessions = dict(probe_accessions)
        self.families = {archive: dict(family) for archive, family in (families or {}).items()}
        self.ten_x = dict(ten_x or {})

        self.links: list[Link] = []
        claimed: dict[str, str] = {}
        for mechanism in self.mechanisms.values():
            unknown = (set(mechanism.inputs) | set(mechanism.outputs) | set(mechanism.via)) - set(
                self.nodes
            )
            if unknown:
                raise ValueError(f"{mechanism.id} names unknown nodes: {sorted(unknown)}")
            if (mechanism.basis is Basis.DOCUMENTED) != (mechanism.citation_url is not None):
                raise ValueError(
                    f"{mechanism.id}: a documented mechanism cites its page, and a "
                    "live-only one cites none"
                )
            pairs = {(s, t): f for s, t, f in mechanism.pairs(self.nodes)}
            covered: dict[tuple[str, str], str] = {}
            for (source_group, target_group), route_id in mechanism.routes.items():
                mine = [
                    pair
                    for pair in pairs
                    if self._in_group(pair[0], source_group)
                    and self._in_group(pair[1], target_group)
                ]
                if not mine:
                    raise ValueError(
                        f"{mechanism.id} claims {route_id} for "
                        f"{(source_group, target_group)}, not its link"
                    )
                if route_id in claimed:
                    raise ValueError(
                        f"{route_id} is claimed by {claimed[route_id]} and {mechanism.id}"
                    )
                claimed[route_id] = mechanism.id
                for pair in mine:
                    covered[pair] = route_id
            # A link promoted to a route by its own survey (graph.PROMOTED):
            # every link of the mechanism from a node of the route's source
            # entity type into a node of its target's.
            for mechanism_id, source_group, target_group, route_id in promoted:
                if mechanism_id != mechanism.id:
                    continue
                for pair in pairs:
                    if (
                        pair not in covered
                        and self._in_group(pair[0], source_group)
                        and self._in_group(pair[1], target_group)
                    ):
                        covered[pair] = route_id
            for (source, target), read_from in pairs.items():
                self.links.append(
                    Link(
                        source=source,
                        target=target,
                        mechanism=mechanism.id,
                        read_from=read_from,
                        route_id=covered.get((source, target)),
                    )
                )
        self._by_id = {link.id: link for link in self.links}
        if len(self._by_id) != len(self.links):
            raise ValueError("Two links share an id")

        self.splits: dict[tuple[str, str, str], Split] = {}
        for split in splits:
            self.splits[(split.route, split.source, split.target)] = split
        self._link_splits: dict[str, list[LinkSplit]] = {}
        for found in link_splits:
            self._link_splits.setdefault(found.link, []).append(found)

    def _in_group(self, node_id: str, group: str) -> bool:
        """Whether a node is ``group``: its own id, its entity type, or ``file``."""
        node = self.nodes[node_id]
        if node.id == group:
            return True
        if group == "file":
            return node.kind is NodeKind.READ_FORMAT
        return node.kind is NodeKind.RECORD and node.entity is not None and node.entity == group

    # ---------------------------------------------------------------- lookups

    def __iter__(self) -> Iterator[Link]:
        return iter(self.links)

    def link(self, link_id: str) -> Link:
        return self._by_id[link_id]

    def route_links(self, route_id: str) -> list[Link]:
        """Every link a declared route covers, one per node pair."""
        found = [link for link in self.links if link.route_id == route_id]
        if not found:
            raise KeyError(route_id)
        return found

    def between(self, source: str, target: str) -> list[Link]:
        return [link for link in self.links if link.source == source and link.target == target]

    def split(self, link: Link) -> Split | None:
        """The route's verdicts on this link, if its survey queried any of its source.

        ``None`` where no route covers the link, or where one does and its
        survey held no accession of this source -- a route surveyed on GEO
        series' experiments says nothing about an ERX, though the request would
        accept one. Such a link is probed like any other.
        """
        if link.route_id is None:
            return None
        found = self.splits.get((link.route_id, link.source, link.target))
        return found if found is not None and found.queried > 0 else None

    def link_splits(self, link: Link) -> list[LinkSplit]:
        """A link survey's splits for this link that measured something.

        One per corpus -- a BioProject link has one per issuing archive. A
        split that only ever failed is left out: it answered nothing.
        """
        return [s for s in self._link_splits.get(link.id, []) if s.answered > 0]

    def status(self, link: Link, probes: Mapping[str, Probe]) -> Status:
        if (split := self.split(link)) is not None:
            return Status.MEASURED if split.reached else Status.MEASURED_NONE
        if surveyed := self.link_splits(link):
            return Status.SURVEYED if any(s.usable for s in surveyed) else Status.SURVEYED_NONE
        probe = probes.get(link.id)
        if probe is None:
            return Status.UNPROBED
        return Status.UNTESTED if probe.confirmed else Status.UNCONFIRMED

    def probe_accession(self, link: Link) -> str:
        """The fixed accession that probes ``link``.

        In order: the mechanism's own override; for a link into a read format,
        the 10x family's member, so what it finds can be checked for 10x; for a
        link from a record shared across archives, the family of the archive
        the target belongs to (BioProject -> ERR is probed with PRJEB15401);
        and otherwise the source's own default.
        """
        mechanism = self.mechanisms[link.mechanism]
        source, target = self.nodes[link.source], self.nodes[link.target]
        override = mechanism.probe_with.get(f"{source.id}>{target.id}") or mechanism.probe_with.get(
            source.id
        )
        if override:
            return override
        if target.kind is NodeKind.READ_FORMAT and source.id in self.ten_x:
            return self.ten_x[source.id]
        if not source.insdc_object and target.archive in FAMILY_OF:
            family = self.families.get(FAMILY_OF[target.archive], {})
            if source.id in family:
                return family[source.id]
        return self.probe_accessions[source.id]

    @property
    def untested(self) -> list[Link]:
        """Links no route's survey measured: each needs a probe."""
        return [link for link in self.links if self.split(link) is None]

    # ------------------------------------------------------------------ paths

    def find_paths(
        self, source: str, target: str, links: Iterable[Link], max_hops: int = 4
    ) -> list[list[str]]:
        """Simple node paths from ``source`` to ``target`` over ``links``.

        The same search as :meth:`RouteRegistry.find_paths`, shortest first and
        neighbours in sorted order, over nodes rather than entity types -- so a
        path through SRRs and one through ERRs are different paths here, where
        the registry sees one ``run`` type.
        """
        adjacency: dict[str, set[str]] = {}
        for link in links:
            if link.source != link.target:
                adjacency.setdefault(link.source, set()).add(link.target)
        if source == target:
            return [[source]]
        paths: list[list[str]] = []
        queue: deque[list[str]] = deque([[source]])
        while queue:
            path = queue.popleft()
            if len(path) > max_hops:
                continue
            for nxt in sorted(adjacency.get(path[-1], ())):
                if nxt in path:
                    continue
                extended = [*path, nxt]
                if nxt == target:
                    paths.append(extended)
                else:
                    queue.append(extended)
        return sorted(paths, key=len)


# ------------------------------------------------------------- route_splits.json


def load_splits(path: Path = SPLITS_PATH) -> list[Split]:
    """Recorded route splits; empty if none have been computed."""
    if not path.exists():
        return []
    return [
        Split(route=route, **pair)
        for route, entry in json.loads(path.read_text())["routes"].items()
        for pair in entry["pairs"]
    ]


def save_splits(
    splits: Iterable[Split], sources: Mapping[str, Mapping[str, object]], path: Path = SPLITS_PATH
) -> None:
    """Write splits by route, sorted, each naming the export it was read from."""
    by_route: dict[str, list[Split]] = {}
    for split in splits:
        by_route.setdefault(split.route, []).append(split)
    routes = {
        route: {
            **sources[route],
            "pairs": [
                {k: v for k, v in asdict(s).items() if k != "route"}
                for s in sorted(found, key=lambda s: (s.source, s.target))
            ],
        }
        for route, found in sorted(by_route.items())
    }
    payload = {
        "note": (
            "Written by `fetch catalogue splits --write` from each route's recorded "
            "verdicts. Filtered existing measurement: never new evidence, never ranked."
        ),
        "routes": routes,
    }
    path.write_text(json.dumps(payload, indent=1) + "\n")


# Each graph.PROMOTED row as the (mechanism, source, target, route id) a
# Catalogue takes, so any catalogue built from the declared nodes agrees with it.
PROMOTED_LINKS = [(row.mechanism, row.source.value, row.target.value, row.id) for row in PROMOTED]

CATALOGUE = Catalogue(
    NODES,
    MECHANISMS,
    PROBE_ACCESSIONS,
    families=FAMILIES,
    ten_x=TEN_X_PROBES,
    splits=load_splits(),
    link_splits=load_link_splits(),
    promoted=PROMOTED_LINKS,
)


def status(link: Link, probes: Mapping[str, Probe], catalogue: Catalogue = CATALOGUE) -> Status:
    return catalogue.status(link, probes)


# ------------------------------------------------------------------ probes.json


def load_probes(path: Path = PROBES_PATH) -> dict[str, Probe]:
    """Recorded probes by link id; empty if none have been recorded."""
    if not path.exists():
        return {}
    probes: dict[str, Probe] = {}
    for row in json.loads(path.read_text())["probes"]:
        probes[row["link"]] = Probe(
            link=row["link"],
            accession=row["accession"],
            checked_on=date.fromisoformat(row["checked_on"]),
            outcome=Outcome(row["outcome"]),
            found=tuple(row["found"]),
            found_count=row["found_count"],
            error=row.get("error"),
            checks=tuple((name, passed) for name, passed in row.get("checks", [])),
            checked=row.get("checked"),
        )
    return probes


def merge_probes(
    recorded: Mapping[str, Probe], new: Iterable[Probe], catalogue: Catalogue = CATALOGUE
) -> list[Probe]:
    """The probes to keep: the new over the recorded, for links that need one.

    A probe of a link the catalogue no longer declares is dropped: it would
    describe a line that is not drawn. So is a probe of a link a route's survey
    measured, whenever it was made: a measured link's only evidence is its
    route's, and a probe beside it would read as a second measurement. That
    covers a probe recorded before a split made its link measured, too.
    """
    merged = {**recorded, **{probe.link: probe for probe in new}}
    needed = {link.id for link in catalogue.untested}
    return [probe for link_id, probe in merged.items() if link_id in needed]


def save_probes(probes: Iterable[Probe], path: Path = PROBES_PATH) -> None:
    """Write probes sorted by link id, so a re-run diffs line by line."""
    rows = []
    for probe in sorted(probes, key=lambda p: p.link):
        row: dict[str, object] = {
            "link": probe.link,
            "accession": probe.accession,
            "checked_on": probe.checked_on.isoformat(),
            "outcome": str(probe.outcome),
            "found_count": probe.found_count,
            "found": list(probe.found),
            "error": probe.error,
        }
        if probe.checks:
            row["checks"] = [list(check) for check in probe.checks]
            row["checked"] = probe.checked
        rows.append(row)
    path.write_text(json.dumps({"probes": rows}, indent=1, ensure_ascii=False) + "\n")
