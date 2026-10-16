"""MkDocs build hook: embed the route graph, and generate the catalogue's tables.

``routes/graph.md`` draws whatever :func:`fetch_series.export.graph.route_graph`
returns at build time, so it cannot drift from ``graph.py`` -- the hand-pasted
route matrix already has, which is the failure this avoids. ``graph/catalogue.md``
gets its counts, its mechanism table and its list of gaps the same way, from
the catalogue and its recorded probes.

Links to route and pathology pages are resolved here against the site being
built, and a missing page fails the build: the knowledge base runs ``--strict``
because a stale link is worse than no link, and links inside a JSON blob are
invisible to MkDocs' own validation.
"""

from __future__ import annotations

import json

from mkdocs.config.defaults import MkDocsConfig
from mkdocs.exceptions import PluginError
from mkdocs.structure.files import Files
from mkdocs.structure.pages import Page
from mkdocs.utils import get_relative_url

from fetch_series.catalogue import (
    CATALOGUE,
    DRAWN,
    Basis,
    NodeKind,
    Status,
    load_probes,
    status,
)
from fetch_series.export.graph import route_graph
from fetch_series.results import Outcome

PAGE = "routes/graph.md"
PLACEHOLDER = "<!-- route-graph-data -->"
CATALOGUE_PAGE = "graph/catalogue.md"
CATALOGUE_PLACEHOLDERS = (
    "<!-- catalogue-counts -->",
    "<!-- catalogue-mechanisms -->",
    "<!-- catalogue-gaps -->",
)


def _url(src: str, page: Page, files: Files) -> str:
    file = files.get_file_from_path(src)
    if file is None:
        raise PluginError(f"The route graph links to {src}, which is not in docs/.")
    return get_relative_url(file.url, page.url)


def _cell(text: str) -> str:
    """Table-safe: a pipe in a request template would split the row."""
    return text.replace("|", "/")


def _catalogue_sections() -> dict[str, str]:
    probes = load_probes()
    statuses = {link.id: status(link, probes) for link in CATALOGUE}
    counts = {s: sum(v is s for v in statuses.values()) for s in Status}
    pairs = {(link.source, link.target) for link in CATALOGUE}
    probed = sorted({p.checked_on.isoformat() for p in probes.values()})
    label = {node.id: node.label for node in CATALOGUE.nodes.values()}
    failed_checks = sum(
        1
        for link in CATALOGUE
        if statuses[link.id] is Status.UNCONFIRMED
        and (probe := probes.get(link.id)) is not None
        and probe.outcome is Outcome.RESOLVED
    )

    summary = (
        f"**{len(CATALOGUE.nodes)} nodes, {len(CATALOGUE.mechanisms)} mechanisms, "
        f"{len(CATALOGUE.links)} links** over {len(pairs)} node pairs: "
        f"{counts[Status.MEASURED]} measured by a declared route, "
        f"{counts[Status.MEASURED_NONE]} measured and never answering, "
        f"{counts[Status.UNTESTED]} untested and confirmed by a probe, and "
        f"{counts[Status.UNCONFIRMED]} documented and not confirmed, of which "
        f"{failed_checks} found a file or document that failed a check"
        + (f"; {counts[Status.UNPROBED]} not yet probed" if counts[Status.UNPROBED] else "")
        + (f". Probed {', '.join(probed)} (UTC)." if probed else ".")
    )

    rows = ["| Mechanism | API | Request | Links | Basis |", "|---|---|---|---|---|"]
    for mechanism in CATALOGUE.mechanisms.values():
        mine = [link for link in CATALOGUE if link.mechanism == mechanism.id]
        tally = ", ".join(
            f"{n} {s}"
            for s in (
                Status.MEASURED,
                Status.MEASURED_NONE,
                Status.UNTESTED,
                Status.UNCONFIRMED,
            )
            if (n := sum(statuses[link.id] is s for link in mine))
        )
        basis = (
            f"[documented]({mechanism.citation_url})"
            if mechanism.basis is Basis.DOCUMENTED
            else "live-only"
        )
        rows.append(
            f"| {mechanism.name} `{mechanism.id}` | {mechanism.api} | "
            f"`{_cell(mechanism.endpoint)}` | {tally} | {basis} |"
        )

    drawn = {(link.source, link.target) for link in CATALOGUE if statuses[link.id] in DRAWN}
    gaps = []
    for node in CATALOGUE.nodes.values():
        if node.kind is not NodeKind.RECORD:
            continue  # a file or a document is where links end
        missing = []
        for target in CATALOGUE.nodes:
            if target == node.id or (node.id, target) in drawn:
                continue
            declared = CATALOGUE.between(node.id, target)
            if not declared:
                continue
            names = sorted({CATALOGUE.mechanisms[link.mechanism].name for link in declared})
            missing.append(f"{label[target]} ({', '.join(names)})")
        if missing:
            gaps.append(f"- **{node.label}** → " + " · ".join(missing))
    gaps.append("")
    gaps.append(
        "Only declared links are listed: a pair nothing declares is a blank in the "
        "matrix under [the route graph](../routes/graph.md), and files and documents "
        "are left out, because nothing links onward from one."
    )
    return dict(
        zip(CATALOGUE_PLACEHOLDERS, (summary, "\n".join(rows), "\n".join(gaps)), strict=True)
    )


def on_page_markdown(markdown: str, *, page: Page, config: MkDocsConfig, files: Files) -> str:
    if page.file.src_uri == CATALOGUE_PAGE:
        for placeholder, text in _catalogue_sections().items():
            if placeholder not in markdown:
                raise PluginError(f"{CATALOGUE_PAGE} no longer carries the {placeholder} marker.")
            markdown = markdown.replace(placeholder, text)
        return markdown
    if page.file.src_uri != PAGE:
        return markdown
    if PLACEHOLDER not in markdown:
        raise PluginError(f"{PAGE} no longer carries the {PLACEHOLDER} marker.")

    graph = route_graph()
    # Only a measured edge is a route with a page. An untested one is a
    # catalogue link, and its documentation is the mechanism's own citation.
    for edge in (e for e in graph["edges"] if e["status"] == "measured"):
        edge["kb_url"] = _url(edge["kb_page"], page, files)
        for pathology in edge["pathologies"]:
            pathology["url"] = (
                _url(f"pathologies/{pathology['slug']}.md", page, files)
                if pathology["documented"]
                else None
            )

    # "</" would end the script element early if any note ever contained it.
    data = json.dumps(graph, separators=(",", ":")).replace("</", "<\\/")
    return markdown.replace(
        PLACEHOLDER, f'<script type="application/json" id="route-graph-data">{data}</script>'
    )
