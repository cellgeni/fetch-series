"""The link catalogue: every one-hop link the archives offer, measured or not.

The route registry in :mod:`fetch_series.graph` holds the links somebody has
measured. That is a small part of what the archives answer: one ENA
``filereport`` request returns seven INSDC levels from any one of them, and
twelve of those 42 pairs are routes. This package records the rest, so the gaps
are visible as gaps rather than as an absence nobody looked at.

It is kept apart from ``REGISTRY`` deliberately, and nothing here can change
the resolver, the ranking, or the parity of ``links.tsv``:

* a catalogue :class:`Link` is a statement that a link **exists**;
* a :class:`~fetch_series.graph.Route` is a statement about which link to
  **use**, and only a route carries evidence.

A link is ``measured`` when a declared route measures it, and is otherwise
confirmed by one live :class:`Probe`. A probe proves existence and measures
nothing, so it never becomes ``RouteEvidence``
(``docs/decisions/what-counts-as-evidence.md``).
"""

from __future__ import annotations

import json
from collections import deque
from collections.abc import Iterable, Iterator, Mapping
from datetime import date
from enum import StrEnum
from pathlib import Path

from fetch_series.accession import EntityType
from fetch_series.catalogue.mechanisms import MECHANISMS
from fetch_series.catalogue.model import PROBE_KEEP, Basis, Level, Link, Mechanism, Node, Probe
from fetch_series.catalogue.nodes import NODES, PROBE_ACCESSIONS
from fetch_series.results import Outcome

__all__ = [
    "CATALOGUE",
    "PROBES_PATH",
    "PROBE_KEEP",
    "Basis",
    "Catalogue",
    "Level",
    "Link",
    "Mechanism",
    "Node",
    "Probe",
    "Status",
    "load_probes",
    "save_probes",
    "status",
]

PROBES_PATH = Path(__file__).with_name("probes.json")


class Status(StrEnum):
    """What is known about one link.

    ``MEASURED``: a declared route measures it, so it has evidence and a rank.
    ``UNTESTED``: no survey has measured it, and one probe found it answering.
    ``UNCONFIRMED``: declared, and the probe found nothing or did not complete --
    listed, never drawn, because a line would claim a link nobody has seen.
    ``UNPROBED``: not probed yet; a test keeps this from shipping.
    """

    MEASURED = "measured"
    UNTESTED = "untested"
    UNCONFIRMED = "unconfirmed"
    UNPROBED = "unprobed"


def status(link: Link, probes: Mapping[str, Probe]) -> Status:
    if link.measured:
        return Status.MEASURED
    probe = probes.get(link.id)
    if probe is None:
        return Status.UNPROBED
    return Status.UNTESTED if probe.confirmed else Status.UNCONFIRMED


class Catalogue:
    """Nodes, the mechanisms that link them, and the links that result.

    Construction validates the declarations rather than trusting them: every
    node a mechanism names exists, every route it claims is one of its own
    links, and no route is claimed twice. A catalogue that assembled anyway
    would draw a line to nowhere or attribute one route to two links.
    """

    def __init__(
        self,
        nodes: Iterable[Node],
        mechanisms: Iterable[Mechanism],
        probe_accessions: dict[str, str],
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
        self.probe_accessions = probe_accessions

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
            pairs = {(s, t): f for s, t, f in mechanism.pairs()}
            for pair, route_id in mechanism.routes.items():
                if pair not in pairs:
                    raise ValueError(f"{mechanism.id} claims {route_id} for {pair}, not its link")
                if route_id in claimed:
                    raise ValueError(
                        f"{route_id} is claimed by {claimed[route_id]} and {mechanism.id}"
                    )
                claimed[route_id] = mechanism.id
            for (source, target), read_from in pairs.items():
                self.links.append(
                    Link(
                        source=source,
                        target=target,
                        mechanism=mechanism.id,
                        read_from=read_from,
                        route_id=mechanism.routes.get((source, target)),
                    )
                )
        self._by_id = {link.id: link for link in self.links}
        if len(self._by_id) != len(self.links):
            raise ValueError("Two links share an id")

    # ---------------------------------------------------------------- lookups

    def __iter__(self) -> Iterator[Link]:
        return iter(self.links)

    def link(self, link_id: str) -> Link:
        return self._by_id[link_id]

    def route_link(self, route_id: str) -> Link:
        """The link a declared route measures."""
        for link in self.links:
            if link.route_id == route_id:
                return link
        raise KeyError(route_id)

    def between(self, source: str, target: str) -> list[Link]:
        return [link for link in self.links if link.source == source and link.target == target]

    def node_of(self, entity: EntityType) -> Node | None:
        """The one node of an entity type, or ``None`` where several share it.

        Every file node is :attr:`EntityType.FILE`, so a file cannot be placed
        from its type alone; which archive's record lists it decides that.
        """
        found = [node for node in self.nodes.values() if node.entity is entity]
        return found[0] if len(found) == 1 else None

    def probe_accession(self, link: Link) -> str:
        """The fixed accession that probes ``link``."""
        mechanism = self.mechanisms[link.mechanism]
        return mechanism.probe_accession(
            link.source, link.target, self.probe_accessions[link.source]
        )

    @property
    def untested(self) -> list[Link]:
        """Links no declared route measures: each needs a probe."""
        return [link for link in self.links if not link.measured]

    # ------------------------------------------------------------------ paths

    def find_paths(
        self, source: str, target: str, links: Iterable[Link], max_hops: int = 4
    ) -> list[list[str]]:
        """Simple node paths from ``source`` to ``target`` over ``links``.

        The same search as :meth:`RouteRegistry.find_paths`, shortest first and
        neighbours in sorted order, over nodes rather than entity types -- so a
        route to ENA's files and one to NCBI's are different paths here, where
        the registry sees one ``file`` type.
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


CATALOGUE = Catalogue(NODES, MECHANISMS, PROBE_ACCESSIONS)


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
        )
    return probes


def save_probes(probes: Iterable[Probe], path: Path = PROBES_PATH) -> None:
    """Write probes sorted by link id, so a re-run diffs line by line."""
    rows = [
        {
            "link": probe.link,
            "accession": probe.accession,
            "checked_on": probe.checked_on.isoformat(),
            "outcome": str(probe.outcome),
            "found_count": probe.found_count,
            "found": list(probe.found),
            "error": probe.error,
        }
        for probe in sorted(probes, key=lambda p: p.link)
    ]
    path.write_text(json.dumps({"probes": rows}, indent=1, ensure_ascii=False) + "\n")
