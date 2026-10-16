"""The route graph and the link catalogue as plain data, for anything that draws them.

The knowledge base's route-graph page is built from this at docs build time, and
``fetch routes graph`` prints the same thing, so the picture and the terminal
cannot disagree with ``graph.py``, with the catalogue, or with each other.

Two kinds of edge, never mixed:

* ``measured`` -- one per declared route, carrying its ``RouteEvidence`` and
  its rank. Everything that involves a *judgement* -- which route ranks first,
  which paths exist -- is computed here by the registry's own
  :meth:`~fetch_series.graph.RouteRegistry.ranked` and by the same path search
  the registry uses, never re-derived in the browser. A second implementation
  of the ranking would be a second ranking.
* ``untested`` -- one per node pair the catalogue links and no route
  measures, listing the mechanisms that answer it and the probe that confirmed
  each. A probe proves the link exists; it carries no counts, because it
  measured nothing.

A catalogue link whose probe found nothing is ``unconfirmed``: listed, so the
page can say it is documented, and never drawn, because a line would claim a
link nobody has seen answer.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from typing import Any

from fetch_series.accession import RULES
from fetch_series.catalogue import (
    CATALOGUE,
    Catalogue,
    Level,
    Link,
    Probe,
    Status,
    load_probes,
    status,
)
from fetch_series.graph import PATHOLOGY_BACKLOG, REGISTRY, Route, RouteRegistry
from fetch_series.routes import IMPLEMENTATIONS

LAYER_LABELS: dict[Level, str] = {
    Level.DATASET: "Dataset",
    Level.SAMPLE: "Sample",
    Level.LIBRARY: "Library",
    Level.RUN: "Run",
    Level.FILE: "File",
    Level.OTHER: "Other",
}

# Paths through untested links are many more than through measured ones -- 143
# linked pairs against 40 routes -- so the page gets each pair's shortest paths
# and those one hop longer, at most this many, rather than every simple path.
MAX_PATHS_ALL = 12
MAX_HOPS = 4
FOUND_SHOWN = 4


def _evidence(route: Route) -> dict[str, Any] | None:
    evidence = route.evidence
    if evidence is None:
        return None
    return {
        "corpus": evidence.corpus,
        "surveyed_on": evidence.surveyed_on.isoformat(),
        "queried": evidence.accessions_queried,
        # resolved and empty are None, not 0, where the survey never recorded
        # the split: an unknown drawn as zero would be a fabricated measurement.
        "resolved": evidence.accessions_resolved,
        "empty": evidence.accessions_empty,
        "failed": evidence.accessions_failed,
        "unique_results": evidence.unique_results,
        "failure_rate": evidence.failure_rate,
        "notes": evidence.notes,
    }


def _mechanisms(catalogue: Catalogue) -> dict[str, dict[str, Any]]:
    """Each mechanism once; links refer to it by id rather than repeating it."""
    return {
        mechanism.id: {
            "name": mechanism.name,
            "api": mechanism.api,
            "endpoint": mechanism.endpoint,
            "basis": str(mechanism.basis),
            "citation_url": mechanism.citation_url,
            "via": list(mechanism.via),
            "note": mechanism.note,
        }
        for mechanism in catalogue.mechanisms.values()
    }


def _link(link: Link, probe: Probe | None) -> dict[str, Any]:
    """One link's mechanism, the field it is read from, and its probe."""
    return {
        "mechanism": link.mechanism,
        "field": link.read_from,
        "probe": None
        if probe is None
        else {
            "accession": probe.accession,
            "checked_on": probe.checked_on.isoformat(),
            "outcome": str(probe.outcome),
            # A few of what the field held, to show its shape; the full count
            # is beside it and the recorded sample is in probes.json.
            "found": list(probe.found[:FOUND_SHOWN]),
            "found_count": probe.found_count,
            "error": probe.error,
        },
    }


def _measured_edge(registry: RouteRegistry, route: Route, link: Link) -> dict[str, Any]:
    ranked = [r.id for r in registry.ranked(route.source, route.target)]
    return {
        "id": route.id,
        "status": str(Status.MEASURED),
        "source": link.source,
        "target": link.target,
        "provider": route.provider,
        "summary": route.summary,
        "kb_page": route.kb_page,
        # The resolver's order for this *entity-type* direction. Where file
        # nodes split one type, run -> ENA files shows "2 of 4": the resolver
        # ranks all four run -> file routes together.
        "rank": ranked.index(route.id) + 1,
        "of": len(ranked),
        "implemented": route.id in IMPLEMENTATIONS,
        "proves_data_exists": route.proves_data_exists,
        "source_archives": [str(a) for a in route.source_archives],
        "cost": {
            "requests": route.cost.requests,
            "rate_limit_rps": route.cost.rate_limit_rps,
            "paginates": route.cost.paginates,
        },
        "pathologies": [
            {"slug": slug, "documented": slug not in PATHOLOGY_BACKLOG}
            for slug in route.known_pathologies
        ],
        "evidence": _evidence(route),
        "link": _link(link, None),
    }


def _grouped(links: list[Link], probes: Mapping[str, Probe], kind: Status) -> list[dict[str, Any]]:
    """One entry per node pair, listing every mechanism that answers it."""
    by_pair: dict[tuple[str, str], list[Link]] = {}
    for link in links:
        by_pair.setdefault((link.source, link.target), []).append(link)
    return [
        {
            "id": f"{kind}:{source}>{target}",
            "status": str(kind),
            "source": source,
            "target": target,
            "links": [
                _link(link, probes.get(link.id))
                for link in sorted(pair, key=lambda link: link.mechanism)
            ],
        }
        for (source, target), pair in sorted(by_pair.items())
    ]


def _paths_all(adjacency: dict[str, set[str]], source: str, target: str) -> list[list[str]]:
    """Simple paths no more than one hop longer than the shortest, shortest first."""
    distance = {target: 0}
    reverse: dict[str, set[str]] = {}
    for a, outs in adjacency.items():
        for b in outs:
            reverse.setdefault(b, set()).add(a)
    queue = deque([target])
    while queue:
        node = queue.popleft()
        for previous in reverse.get(node, ()):
            if previous not in distance:
                distance[previous] = distance[node] + 1
                queue.append(previous)
    if source not in distance or distance[source] > MAX_HOPS:
        return []
    limit = min(distance[source] + 1, MAX_HOPS)

    found: list[list[str]] = []

    def walk(path: list[str]) -> None:
        here = path[-1]
        if here == target:
            found.append(path)
            return
        for nxt in sorted(adjacency.get(here, ())):
            # Prune any step that cannot reach the target within the limit.
            if nxt in path or nxt not in distance or len(path) + distance[nxt] > limit:
                continue
            walk([*path, nxt])

    walk([source])
    return sorted(found, key=lambda p: (len(p), p))[:MAX_PATHS_ALL]


def route_graph(
    registry: RouteRegistry = REGISTRY,
    catalogue: Catalogue = CATALOGUE,
    probes: Mapping[str, Probe] | None = None,
) -> dict[str, Any]:
    """Nodes, measured and untested edges, paths and gaps, as JSON-serialisable data."""
    probes = load_probes() if probes is None else probes
    statuses = {link.id: status(link, probes) for link in catalogue}

    edges: list[dict[str, Any]] = [
        _measured_edge(registry, route, catalogue.route_link(route.id))
        for route in sorted(registry, key=lambda r: r.id)
    ]
    untested = [link for link in catalogue if statuses[link.id] is Status.UNTESTED]
    unconfirmed = [
        link for link in catalogue if statuses[link.id] in (Status.UNCONFIRMED, Status.UNPROBED)
    ]
    edges += _grouped(untested, probes, Status.UNTESTED)

    nodes = []
    for node in catalogue.nodes.values():
        out = [e for e in edges if e["source"] == node.id]
        into = [e for e in edges if e["target"] == node.id]
        nodes.append(
            {
                "id": node.id,
                "label": node.label,
                "family": node.family,
                "layer": str(node.level),
                "database": node.database,
                "entity": str(node.entity) if node.entity else None,
                "note": node.note,
                # What an identifier of this node looks like, from the one
                # registry that parses them. A file has none: it is reached,
                # never parsed; an ArrayExpress sample has none of its own.
                "accessions": [
                    {"pattern": rule.pattern.pattern, "description": rule.description}
                    for rule in RULES
                    if node.entity is not None and rule.entity is node.entity
                ],
                "routes_out": sum(e["status"] == "measured" for e in out),
                "routes_in": sum(e["status"] == "measured" for e in into),
                "untested_out": sum(e["status"] == "untested" for e in out),
                "untested_in": sum(e["status"] == "untested" for e in into),
            }
        )

    measured_links = [link for link in catalogue if link.measured]
    drawn = [link for link in catalogue if statuses[link.id] in (Status.MEASURED, Status.UNTESTED)]
    adjacency: dict[str, set[str]] = {}
    for link in drawn:
        if link.source != link.target:
            adjacency.setdefault(link.source, set()).add(link.target)

    paths_measured: dict[str, dict[str, list[list[str]]]] = {}
    paths_all: dict[str, dict[str, list[list[str]]]] = {}
    unreachable: dict[str, list[str]] = {}
    for source in catalogue.nodes:
        for target in catalogue.nodes:
            if source == target:
                continue
            if found := catalogue.find_paths(source, target, measured_links, MAX_HOPS):
                paths_measured.setdefault(source, {})[target] = found
            if found := _paths_all(adjacency, source, target):
                paths_all.setdefault(source, {})[target] = found
        unreachable[source] = [
            target
            for target in catalogue.nodes
            if target != source and target not in adjacency.get(source, ())
        ]

    return {
        "layers": [{"id": str(level), "label": LAYER_LABELS[level]} for level in Level],
        "nodes": nodes,
        "edges": edges,
        "unconfirmed": _grouped(unconfirmed, probes, Status.UNCONFIRMED),
        "mechanisms": _mechanisms(catalogue),
        "paths_measured": paths_measured,
        "paths_all": paths_all,
        "unreachable": unreachable,
    }
