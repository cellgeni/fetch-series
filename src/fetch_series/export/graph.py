"""The route graph and the link catalogue as plain data, for anything that draws them.

The knowledge base's route-graph page is built from this at docs build time, and
``fetch routes graph`` prints the same thing, so the picture and the terminal
cannot disagree with ``graph.py``, with the catalogue, or with each other.

Two kinds of edge, never mixed:

* ``measured`` -- one per declared route *and node pair it covers*, carrying
  the route's ``RouteEvidence``, its rank, and its **split**: the route's own
  verdicts restricted to that source node and target node
  (:mod:`fetch_series.catalogue.splits`). ``bioproject->run:ena_filereport`` is
  three edges, to SRR, ERR and DRR. Everything that involves a *judgement* --
  which route ranks first, which paths exist -- is computed here by the
  registry's own :meth:`~fetch_series.graph.RouteRegistry.ranked` and by the
  same path search the registry uses, never re-derived in the browser. A
  second implementation of the ranking would be a second ranking.
* ``untested`` -- one per node pair the catalogue links and no route's survey
  measured, listing the mechanisms that answer it and the probe that confirmed
  each. A probe proves the link exists; it carries no counts, because it
  measured nothing.

* ``surveyed`` -- one per node pair where a *link survey's* seeded draw found
  something usable, listing each surveyed mechanism with its split per corpus.
  Counts, and **no rank**: a draw of 300 cannot separate two links whose
  yields differ by less than the ranking's 1% band, so ordering them would be
  noise presented as a measurement. Nothing here touches the route ranking.

Three kinds of link are listed and never drawn, because a line would claim a
link nobody has seen answer: ``unconfirmed`` (the probe found nothing, or found
a file or document that failed a check), ``measured_none`` (a route's survey
queried the source and no accession reached the target) and ``surveyed_none``
(a link survey answered and found nothing usable).
"""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from typing import Any

from fetch_series.accession import RULES
from fetch_series.catalogue import (
    CATALOGUE,
    DRAWN,
    Catalogue,
    Level,
    Link,
    LinkSplit,
    NodeKind,
    Probe,
    Split,
    Status,
    load_probes,
)
from fetch_series.catalogue.continuations import Continuation, continuations, status_of
from fetch_series.graph import PATHOLOGY_BACKLOG, REGISTRY, Route, RouteRegistry
from fetch_series.routes import IMPLEMENTATIONS

LAYER_LABELS: dict[Level, str] = {
    Level.DATASET: "Dataset",
    Level.SAMPLE: "Sample",
    Level.LIBRARY: "Library",
    Level.RUN: "Run",
    Level.OTHER: "Other",
    Level.FILE: "Read files",
    Level.SUPPLEMENTARY: "Supplementary",
    Level.METADATA: "Metadata",
}

# The graph's columns, left to right: which archive a node belongs to. The
# records all three INSDC archives share, and the read formats no archive owns,
# span the three INSDC columns.
COLUMNS = ("geo", "ncbi", "ebi", "ddbj", "arrayexpress")
COLUMN_OF_FAMILY = {
    "geo": "geo",
    "ncbi": "ncbi",
    "ebi": "ebi",
    "ddbj": "ddbj",
    "arrayexpress": "arrayexpress",
    "shared": "insdc",
    "file": "insdc",
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


def _split(split: Split) -> dict[str, Any]:
    return {
        "queried": split.queried,
        "resolved": split.resolved,
        "empty": split.empty,
        "failed": split.failed,
        "reached": split.reached,
        "distinct": split.distinct,
        "share": split.share,
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


def _link(catalogue: Catalogue, link: Link, probe: Probe | None) -> dict[str, Any]:
    """One link's mechanism, the field it is read from, who made its files, and its probe."""
    origin = catalogue.mechanisms[link.mechanism].origin_of(link.target)
    return {
        "mechanism": link.mechanism,
        "field": link.read_from,
        "origin": str(origin) if origin else None,
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
            # What a found file or document was checked for, and on what.
            "checks": [{"name": name, "passed": passed} for name, passed in probe.checks],
            "checked": probe.checked,
        },
    }


def edge_id(route_id: str, link: Link) -> str:
    """A measured edge: one route on one node pair."""
    return f"{route_id}|{link.source}>{link.target}"


def _measured_edge(
    registry: RouteRegistry, catalogue: Catalogue, route: Route, link: Link, split: Split
) -> dict[str, Any]:
    ranked = [r.id for r in registry.ranked(route.source, route.target)]
    return {
        "id": edge_id(route.id, link),
        "route": route.id,
        "status": str(Status.MEASURED),
        "source": link.source,
        "target": link.target,
        "provider": route.provider,
        "summary": route.summary,
        "kb_page": route.kb_page,
        # The resolver's order for this *entity-type* direction. Where nodes
        # split one type, BioProject -> ERR shows "2 of 5": the resolver ranks
        # every BioProject -> run route together, whichever archive answers.
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
        "split": _split(split),
        "link": _link(catalogue, link, None),
    }


def _link_split(split: LinkSplit) -> dict[str, Any]:
    return {
        "corpus": split.corpus,
        "queried": split.queried,
        "resolved": split.resolved,
        "empty": split.empty,
        "failed": split.failed,
        "usable": split.usable,
        "distinct": split.distinct,
        "surveyed_on": split.surveyed_on.isoformat(),
        "checks": {
            name: {"passed": c.passed, "failed": c.failed, "undetermined": c.undetermined}
            for name, c in split.checks.items()
        },
        # What the files held, where the content survey read this very corpus.
        "screened": None
        if split.screened is None
        else {
            "reads": split.screened.reads,
            "inspected": split.screened.inspected,
            "fit": split.screened.fit,
            "unfit": split.screened.unfit,
            "undetermined": split.screened.undetermined,
            "layouts": split.screened.layouts,
        },
    }


def _grouped(
    catalogue: Catalogue, links: list[Link], probes: Mapping[str, Probe], kind: Status
) -> list[dict[str, Any]]:
    """One entry per node pair, listing every mechanism that answers it.

    A surveyed link carries its splits, one per corpus, in corpus order -- and
    the mechanisms stay in name order, never in yield order: there is no rank.
    """

    def entry(link: Link) -> dict[str, Any]:
        found = _link(catalogue, link, probes.get(link.id))
        if kind in (Status.SURVEYED, Status.SURVEYED_NONE):
            found["splits"] = [
                _link_split(split)
                for split in sorted(catalogue.link_splits(link), key=lambda s: s.corpus)
            ]
        return found

    by_pair: dict[tuple[str, str], list[Link]] = {}
    for link in links:
        by_pair.setdefault((link.source, link.target), []).append(link)
    return [
        {
            "id": f"{kind}:{source}>{target}",
            "status": str(kind),
            "source": source,
            "target": target,
            "links": [entry(link) for link in sorted(pair, key=lambda link: link.mechanism)],
        }
        for (source, target), pair in sorted(by_pair.items())
    ]


def _continued(
    catalogue: Catalogue,
    found: list[Continuation],
    probes: Mapping[str, Probe],
    statuses: Mapping[str, Status],
) -> list[dict[str, Any]]:
    """Document -> record pairs: the second half of the request that fetched the document.

    Drawn as the record link they continue is drawn -- surveyed (or measured) as
    surveyed, untested as untested -- and listing that link's own evidence. Not a
    catalogue link, so no count of the catalogue's changes.
    """
    by_pair: dict[tuple[str, str], list[Continuation]] = {}
    for item in found:
        by_pair.setdefault((item.document, item.target), []).append(item)
    edges = []
    for (document, target), items in sorted(by_pair.items()):
        best = status_of(items, statuses)
        reads: list[dict[str, Any]] = []
        for item in items:
            entry = _link(catalogue, item.read_by, probes.get(item.read_by.id))
            entry["splits"] = [
                _link_split(split)
                for split in sorted(catalogue.link_splits(item.read_by), key=lambda s: s.corpus)
            ]
            reads.append(entry)
        edges.append(
            {
                "id": f"continues:{document}>{target}",
                "status": str(Status.UNTESTED if best is Status.UNTESTED else Status.SURVEYED),
                "source": document,
                "target": target,
                "links": reads,
                "continues": [
                    {
                        "fetched_by": item.fetched_by.id,
                        "read_by": item.read_by.id,
                        "route": item.read_by.route_id,
                        "status": str(statuses[item.read_by.id]),
                    }
                    for item in items
                ],
            }
        )
    return edges


def _measured_none(catalogue: Catalogue, links: list[Link]) -> list[dict[str, Any]]:
    """Pairs a route's survey measured and found nothing on, by pair."""
    by_pair: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for link in links:
        split = catalogue.split(link)
        assert split is not None and link.route_id is not None
        by_pair.setdefault((link.source, link.target), []).append(
            {
                "route": link.route_id,
                "mechanism": link.mechanism,
                "corpus": REGISTRY[link.route_id].evidence.corpus  # type: ignore[union-attr]
                if link.route_id in REGISTRY
                else None,
                "split": _split(split),
            }
        )
    return [
        {"source": source, "target": target, "routes": routes}
        for (source, target), routes in sorted(by_pair.items())
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
    statuses = {link.id: catalogue.status(link, probes) for link in catalogue}

    edges: list[dict[str, Any]] = []
    for route in sorted(registry, key=lambda r: r.id):
        for link in catalogue.route_links(route.id):
            split = catalogue.split(link)
            if split is not None and statuses[link.id] is Status.MEASURED:
                edges.append(_measured_edge(registry, catalogue, route, link, split))
    untested = [link for link in catalogue if statuses[link.id] is Status.UNTESTED]
    unconfirmed = [
        link for link in catalogue if statuses[link.id] in (Status.UNCONFIRMED, Status.UNPROBED)
    ]
    none = [link for link in catalogue if statuses[link.id] is Status.MEASURED_NONE]
    surveyed = [link for link in catalogue if statuses[link.id] is Status.SURVEYED]
    surveyed_none = [link for link in catalogue if statuses[link.id] is Status.SURVEYED_NONE]
    edges += _grouped(catalogue, surveyed, probes, Status.SURVEYED)
    edges += _grouped(catalogue, untested, probes, Status.UNTESTED)
    continued = continuations(catalogue, probes)
    edges += _continued(catalogue, continued, probes, statuses)
    # As links, for the path search: a document leads on to what it names.
    reads = [
        Link(c.document, c.target, c.read_by.mechanism, "the fetched document", None)
        for c in continued
    ]
    read_status = {
        (c.document, c.target, c.read_by.mechanism): statuses[c.read_by.id] for c in continued
    }

    nodes = []
    for node in catalogue.nodes.values():
        out = [e for e in edges if e["source"] == node.id]
        into = [e for e in edges if e["target"] == node.id]
        nodes.append(
            {
                "id": node.id,
                "label": node.label,
                "prefix": node.prefix,
                "family": node.family,
                "column": COLUMN_OF_FAMILY[node.family],
                "layer": str(node.level),
                "kind": str(node.kind),
                "database": node.database,
                "archive": str(node.archive) if node.archive else None,
                "entity": str(node.entity) if node.entity else None,
                "note": node.note,
                # What an identifier of this node looks like, from the one
                # registry that parses them -- narrowed to the node's archive
                # where it is one archive's object. A file has none: it is
                # reached, never parsed.
                "accessions": [
                    {"pattern": rule.pattern.pattern, "description": rule.description}
                    for rule in RULES
                    if node.kind is NodeKind.RECORD
                    and node.entity is not None
                    and rule.entity is node.entity
                ],
                "routes_out": sum(e["status"] == "measured" for e in out),
                "routes_in": sum(e["status"] == "measured" for e in into),
                "surveyed_out": sum(e["status"] == "surveyed" for e in out),
                "surveyed_in": sum(e["status"] == "surveyed" for e in into),
                "untested_out": sum(e["status"] == "untested" for e in out),
                "untested_in": sum(e["status"] == "untested" for e in into),
            }
        )

    measured_links = [link for link in catalogue if statuses[link.id] is Status.MEASURED]
    known_links = [
        link for link in catalogue if statuses[link.id] in (Status.MEASURED, Status.SURVEYED)
    ] + [
        r
        for r in reads
        if read_status[(r.source, r.target, r.mechanism)] in (Status.MEASURED, Status.SURVEYED)
    ]
    drawn = [link for link in catalogue if statuses[link.id] in DRAWN] + reads
    adjacency: dict[str, set[str]] = {}
    for link in drawn:
        if link.source != link.target:
            adjacency.setdefault(link.source, set()).add(link.target)

    paths_measured: dict[str, dict[str, list[list[str]]]] = {}
    paths_surveyed: dict[str, dict[str, list[list[str]]]] = {}
    paths_all: dict[str, dict[str, list[list[str]]]] = {}
    unreachable: dict[str, list[str]] = {}
    for source in catalogue.nodes:
        for target in catalogue.nodes:
            if source == target:
                continue
            if found := catalogue.find_paths(source, target, measured_links, MAX_HOPS):
                paths_measured.setdefault(source, {})[target] = found[:MAX_PATHS_ALL]
            if found := catalogue.find_paths(source, target, known_links, MAX_HOPS):
                paths_surveyed.setdefault(source, {})[target] = found[:MAX_PATHS_ALL]
            if found := _paths_all(adjacency, source, target):
                paths_all.setdefault(source, {})[target] = found
        unreachable[source] = [
            target
            for target in catalogue.nodes
            if target != source and target not in adjacency.get(source, ())
        ]

    return {
        "layers": [{"id": str(level), "label": LAYER_LABELS[level]} for level in Level],
        "columns": list(COLUMNS),
        "nodes": nodes,
        "edges": edges,
        "unconfirmed": _grouped(catalogue, unconfirmed, probes, Status.UNCONFIRMED),
        "measured_none": _measured_none(catalogue, none),
        "surveyed_none": _grouped(catalogue, surveyed_none, probes, Status.SURVEYED_NONE),
        "mechanisms": _mechanisms(catalogue),
        "paths_measured": paths_measured,
        "paths_surveyed": paths_surveyed,
        "paths_all": paths_all,
        "unreachable": unreachable,
    }
