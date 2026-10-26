"""MkDocs build hook: write the route graph's data, and generate the catalogue's tables.

``routes/graph.md`` draws whatever :func:`fetch_series.export.graph.route_graph`
returns at build time, so it cannot drift from ``graph.py`` -- the hand-pasted
route matrix already has, which is the failure this avoids. ``graph/catalogue.md``
gets its counts, its mechanism table and its list of gaps the same way, from
the catalogue and its recorded probes.

The data is a file beside the page, ``graph.json``, that the page fetches -- not
a ``<script type="application/json">`` inside it. Material's instant navigation
swaps a page in without reloading and re-creates every ``<script>`` in it to run
it, so an embedded JSON block was run as JavaScript, failed on its first ``:``,
and never reached the page: the graph drew when its URL was opened directly and
never when it was reached through the menu.

Links to route and pathology pages are resolved here against the site being
built, and a missing page fails the build: the knowledge base runs ``--strict``
because a stale link is worse than no link, and links inside a JSON blob are
invisible to MkDocs' own validation.
"""

from __future__ import annotations

import json
import re
from pathlib import Path, PurePosixPath

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
from fetch_series.graph import PROMOTED, REGISTRY, Route
from fetch_series.results import Outcome

PAGE = "routes/graph.md"
PLACEHOLDER = "<!-- route-graph-data -->"
DATA_FILE = "graph.json"

# The graph serialised by on_page_markdown, and where in the built site it goes;
# on_post_build writes it once the site directory exists.
_pending: dict[str, str] = {}
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
        f"{counts[Status.SURVEYED]} surveyed by a link survey and answering usably, "
        f"{counts[Status.SURVEYED_NONE]} surveyed and never answering usably, "
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
                Status.SURVEYED,
                Status.SURVEYED_NONE,
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
        "matrix under [the route graph](../routes/graph.md). Files are left out, because "
        "nothing links onward from one. A document links onward only as the second half "
        "of the request that fetched it, which is not a link of its own "
        "([the route graph](../routes/graph.md#a-line-out-of-a-metadata-document))."
    )
    return dict(
        zip(CATALOGUE_PLACEHOLDERS, (summary, "\n".join(rows), "\n".join(gaps)), strict=True)
    )


MATRIX_PAGE = "routes/index.md"
MATRIX_MARKERS = ("<!-- route-count -->", "<!-- route-matrix -->")
_FROM_MARKER = re.compile(r"<!-- promoted-routes:([a-z_]+) -->")


def _n(value: int | None) -> str:
    return "—" if value is None else f"{value:,}"


def _short(corpus: str, width: int = 58) -> str:
    """A long derived corpus name, cut where the matrix would wrap it."""
    return corpus if len(corpus) <= width else corpus[:width] + "…"


def _matrix_row(route: Route) -> str:
    evidence = route.evidence
    assert evidence is not None
    failed = evidence.accessions_failed
    return (
        f"| `{route.source.value}` → `{route.target.value}` | `{route.id.split(':', 1)[1]}` "
        f"| `{_cell(_short(evidence.corpus))}` | {_n(evidence.accessions_queried)} "
        f"| {_n(evidence.unique_results)} | {f'**{failed}**' if failed else 0} |"
    )


def _matrix_sections() -> dict[str, str]:
    """The route matrix, from REGISTRY: a hand-pasted one drifted from graph.py."""
    routes = sorted(REGISTRY, key=lambda r: (r.source.value, r.target.value, r.id))
    measured = [r for r in routes if r.evidence is not None]
    promoted = len(PROMOTED)
    count = (
        f"**All {len(routes)} declared routes carry measured evidence.**"
        if len(measured) == len(routes)
        else f"**{len(measured)} of {len(routes)} declared routes carry measured evidence.**"
    ) + (f" {promoted} of them are catalogue links surveyed as routes." if promoted else "")
    rows = [
        "| Direction | Route | Corpus | Queried | Results | Failed |",
        "|---|---|---|---:|---:|---:|",
        *(_matrix_row(r) for r in measured),
    ]
    return dict(zip(MATRIX_MARKERS, (count, "\n".join(rows)), strict=True))


def _promoted_page(source: str) -> str:
    """Every promoted route from one entity type, ranked per direction as the resolver tries them."""
    rows = [row for row in PROMOTED if row.source.value == source]
    if not rows:
        return "*No catalogue link from here has been surveyed as a route yet.*"
    out = []
    for target in sorted({row.target for row in rows}, key=lambda t: t.value):
        ranked = REGISTRY.ranked(rows[0].source, target)
        out += [
            f"### → {target.value}",
            "",
            "| Rank | Route | Corpus | Date | Queried | Resolved | Empty | Failed | Distinct "
            "| Requests | Pathologies |",
            "|---:|---|---|---|---:|---:|---:|---:|---:|---:|---|",
        ]
        for rank, route in enumerate(ranked, 1):
            e = route.evidence
            assert e is not None
            empty = e.accessions_empty
            resolved = (
                e.accessions_queried - e.accessions_failed - empty if empty is not None else None
            )
            pathologies = ", ".join(
                f"[{slug}](../pathologies/{slug}.md)" for slug in route.known_pathologies
            )
            out.append(
                f"| {rank} | `{route.id}` | `{_cell(e.corpus)}` | {e.surveyed_on.isoformat()} "
                f"| {_n(e.accessions_queried)} | {_n(resolved)} | {_n(empty)} "
                f"| {e.accessions_failed:,} | {_n(e.unique_results)} | {route.cost.requests} "
                f"| {pathologies or '—'} |"
            )
        out.append("")
    return "\n".join(out)


_WAVE_MARKER = re.compile(r"<!-- link-splits:wave=(\d+) -->")


def _wave_table(wave: int) -> str:
    """One wave's link splits, from ``link_splits.json``: counts, never a rank.

    Rows are in link order. A table sorted by yield would be a ranking by
    another name, and a draw of 300 cannot support one.
    """
    from fetch_series.catalogue.waves import wave_of

    probes = load_probes()
    rows = [
        "| Link | Corpus | Usable / queried | Resolved · empty · failed | Distinct "
        "| Fit · unfit · ? (read) | Status |",
        "|---|---|---|---|---|---|---|",
    ]
    for link in sorted(CATALOGUE, key=lambda link: link.id):
        if wave_of(link) != wave:
            continue
        for split in sorted(CATALOGUE.link_splits(link), key=lambda s: s.corpus):
            checks = "; ".join(
                f"{name} {c.passed}/{c.passed + c.failed + c.undetermined}"
                for name, c in split.checks.items()
            )
            rows.append(
                f"| `{_cell(link.id)}` | `{_cell(split.corpus)}` | "
                f"**{split.usable}** / {split.queried}"
                + (f" ({checks})" if checks else "")
                + f" | {split.resolved} · {split.empty} · {split.failed} | {split.distinct} | "
                + (
                    f"{split.screened.fit} · {split.screened.unfit} · {split.screened.undetermined}"
                    if split.screened
                    else "—"
                )
                + f" | {status(link, probes)} |"
            )
    if len(rows) == 2:
        return "*No link of this wave has been surveyed yet.*"
    return "\n".join(rows)


def on_page_markdown(markdown: str, *, page: Page, config: MkDocsConfig, files: Files) -> str:
    markdown = _WAVE_MARKER.sub(lambda m: _wave_table(int(m.group(1))), markdown)
    markdown = _FROM_MARKER.sub(lambda m: _promoted_page(m.group(1)), markdown)
    if page.file.src_uri == MATRIX_PAGE:
        for marker, text in _matrix_sections().items():
            if marker not in markdown:
                raise PluginError(f"{MATRIX_PAGE} no longer carries the {marker} marker.")
            markdown = markdown.replace(marker, text)
        return markdown
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

    # Beside the page, so the page names it relatively and the route and
    # pathology URLs above, relative to the page, hold in it too.
    _pending["dest"] = str(PurePosixPath(page.file.dest_uri).parent / DATA_FILE)
    _pending["data"] = json.dumps(graph, separators=(",", ":"))
    return markdown.replace(
        PLACEHOLDER, f'<div id="route-graph-data" data-src="{DATA_FILE}" hidden></div>'
    )


def on_post_build(*, config: MkDocsConfig) -> None:
    if not _pending:
        raise PluginError(f"{PAGE} was not built, so the route graph has no data.")
    target = Path(config.site_dir) / _pending["dest"]
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(_pending["data"], encoding="utf-8")
