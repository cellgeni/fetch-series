"""MkDocs build hook: embed the route graph in ``routes/graph.md``.

The page draws whatever :func:`fetch_series.export.graph.route_graph` returns at
build time, so it cannot drift from ``graph.py`` -- the hand-pasted route matrix
already has, which is the failure this avoids.

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

from fetch_series.export.graph import route_graph

PAGE = "routes/graph.md"
PLACEHOLDER = "<!-- route-graph-data -->"


def _url(src: str, page: Page, files: Files) -> str:
    file = files.get_file_from_path(src)
    if file is None:
        raise PluginError(f"The route graph links to {src}, which is not in docs/.")
    return get_relative_url(file.url, page.url)


def on_page_markdown(markdown: str, *, page: Page, config: MkDocsConfig, files: Files) -> str:
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
