"""The route graph as plain data, for anything that wants to draw it.

The knowledge base's route-graph page is built from this at docs build time, and
``fetch routes graph`` prints the same thing, so the picture and the terminal
cannot disagree with ``graph.py`` or with each other.

Everything the page shows that involves a *judgement* -- which route ranks first
for a direction, which type paths exist -- is computed here by the registry's
own :meth:`~fetch_series.graph.RouteRegistry.ranked` and
:meth:`~fetch_series.graph.RouteRegistry.find_paths`, never re-derived in the
browser. A second implementation of the ranking would be a second ranking.
"""

from __future__ import annotations

from typing import Any

from fetch_series.accession import RULES, EntityType
from fetch_series.graph import PATHOLOGY_BACKLOG, REGISTRY, Route, RouteRegistry
from fetch_series.routes import IMPLEMENTATIONS

# Which family of archive record a node is. INSDC types are one entity mirrored
# across NCBI, EBI and DDBJ; GEO and ArrayExpress records are curated records
# that *point at* INSDC ones, which is why they are drawn apart.
FAMILIES: dict[EntityType, str] = {
    EntityType.BIOPROJECT: "insdc",
    EntityType.BIOSAMPLE: "insdc",
    EntityType.STUDY: "insdc",
    EntityType.SAMPLE: "insdc",
    EntityType.EXPERIMENT: "insdc",
    EntityType.RUN: "insdc",
    EntityType.SUBMISSION: "insdc",
    EntityType.ANALYSIS: "insdc",
    EntityType.GEO_SERIES: "geo",
    EntityType.GEO_SAMPLE: "geo",
    EntityType.GEO_PLATFORM: "geo",
    EntityType.AE_EXPERIMENT: "arrayexpress",
    EntityType.BIOSTUDY: "arrayexpress",
    EntityType.FILE: "file",
}

# How fine-grained a record is, coarsest first. A dataset holds samples, a sample
# is sequenced as one or more libraries (INSDC "experiments"), a library is run
# one or more times, and a run carries the files. The page draws each layer as
# its own shape and row, which is why it is declared here with the families
# rather than guessed from names in the browser.
LAYERS: tuple[tuple[str, str], ...] = (
    ("dataset", "Dataset"),
    ("sample", "Sample"),
    ("library", "Library"),
    ("run", "Run"),
    ("file", "File"),
    ("other", "Other"),
)

LAYER_OF: dict[EntityType, str] = {
    EntityType.GEO_SERIES: "dataset",
    EntityType.BIOPROJECT: "dataset",
    EntityType.STUDY: "dataset",
    EntityType.AE_EXPERIMENT: "dataset",
    EntityType.BIOSTUDY: "dataset",
    EntityType.GEO_SAMPLE: "sample",
    EntityType.BIOSAMPLE: "sample",
    EntityType.SAMPLE: "sample",
    EntityType.EXPERIMENT: "library",
    EntityType.RUN: "run",
    EntityType.FILE: "file",
    # Administrative or descriptive records rather than a level of the data.
    EntityType.SUBMISSION: "other",
    EntityType.ANALYSIS: "other",
    EntityType.GEO_PLATFORM: "other",
}

LABELS: dict[EntityType, str] = {
    EntityType.BIOPROJECT: "BioProject",
    EntityType.BIOSAMPLE: "BioSample",
    EntityType.GEO_SERIES: "GEO series",
    EntityType.GEO_SAMPLE: "GEO sample",
    EntityType.GEO_PLATFORM: "GEO platform",
    EntityType.AE_EXPERIMENT: "ArrayExpress experiment",
    EntityType.BIOSTUDY: "BioStudy",
}


def _label(entity: EntityType) -> str:
    return LABELS.get(entity, str(entity).replace("_", " ").capitalize())


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


def route_graph(registry: RouteRegistry = REGISTRY) -> dict[str, Any]:
    """Nodes, edges and type paths of ``registry``, as JSON-serialisable data.

    Edges are routes, one each, so a direction answered by five routes has five
    parallel edges. ``rank`` / ``of`` place each route within its direction as
    :meth:`RouteRegistry.ranked` orders it -- the order the resolver tries them.
    """
    routes = list(registry)
    used = sorted({r.source for r in routes} | {r.target for r in routes})

    edges: list[dict[str, Any]] = []
    directions = sorted({(r.source, r.target) for r in routes})
    for source, target in directions:
        ranked = registry.ranked(source, target)
        for rank, route in enumerate(ranked, start=1):
            edges.append(
                {
                    "id": route.id,
                    "source": str(route.source),
                    "target": str(route.target),
                    "provider": route.provider,
                    "summary": route.summary,
                    "kb_page": route.kb_page,
                    "rank": rank,
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
                }
            )

    nodes = [
        {
            "id": str(entity),
            "label": _label(entity),
            "family": FAMILIES[entity],
            "layer": LAYER_OF[entity],
            # What an identifier of this type looks like, from the one registry
            # that parses them. A file has none: it is reached, never parsed.
            "accessions": [
                {"pattern": rule.pattern.pattern, "description": rule.description}
                for rule in RULES
                if rule.entity is entity
            ],
            "routes_out": sum(r.source is entity for r in routes),
            "routes_in": sum(r.target is entity for r in routes),
        }
        for entity in used
    ]

    paths: dict[str, dict[str, list[list[str]]]] = {}
    for source in used:
        for target in used:
            if source is target:
                continue
            found = registry.find_paths(source, target)
            if found:
                paths.setdefault(str(source), {})[str(target)] = [
                    [str(step) for step in path] for path in found
                ]

    layers = [{"id": layer, "label": label} for layer, label in LAYERS]
    return {"layers": layers, "nodes": nodes, "edges": edges, "paths": paths}
