---
title: Route graph
hide:
  - navigation
  - toc
---

# The route graph

Every link between record types the archives offer, and what is known about
each. A **measured** route is a solid line: its ink is the share of queried
accessions its survey resolved, its width how many it resolved. An
**untested** link is a thin dotted line with a hollow arrowhead: no survey has
measured it, and one live request found it answering. Colour says which
archive issues a record, shape says what level of the data it is.

<div id="route-graph" class="rg-root" aria-label="Interactive route graph"></div>

<!-- route-graph-data -->

<noscript>
The interactive graph needs JavaScript. The same numbers are in
<a href="../">the route matrix</a>, and every link is listed on
<a href="../../graph/catalogue/">the link catalogue</a>.
</noscript>

## How to read it

**Measured routes / All known links** switches between the 40 declared routes
alone and every link [the catalogue](../graph/catalogue.md) has confirmed. All
is the default, because the gaps between the routes are the point of drawing
it. Submissions, analyses and GEO platforms are hidden until asked for: they
are records *about* the data, not levels of it, and they cross every row.

### A measured route

Every declared route is one line, named along its length: a source type, a
target, and the provider that answers it. Where several routes answer the same
direction they run side by side, in the order the resolver tries them. That
order is `RouteRegistry.ranked()` over the evidence, not a hand-written
preference ([how routes are ranked](../decisions/route-ranking.md)).

- **Ink** is the share of queried accessions that resolved, in five steps from
  pale (under 25%) to full ink (95% or more).
- **Width** is the number of accessions resolved, on a log scale.
- **Hatched** marks the six routes measured by the 2026-05 scripts, which kept
  a single boolean per BioProject that lumped empty in with failed. Their share
  cannot be computed, and is drawn as unknown rather than guessed; their width
  is the accessions answered, which is the most that can be said.

The counts come from each route's `RouteEvidence`: one dated survey over one
named corpus, in which each queried accession ends **resolved** (at least one
result), **empty** (the archive answered and holds no such link) or **failed**
(no verdict at all), as defined in [how to read the matrix](index.md#how-to-read-it).
The adversarial `hard-cases` corpus is a gate, not evidence, and does not
appear here. Hover or click a route for the exact split.

### An untested link

One dotted line per node pair, however many mechanisms answer it: an ENA
`filereport`, NCBI's runinfo, DDBJ's `dblink` and five more all link a
BioSample to its INSDC sample. Their names appear along the line once you zoom
in, and in full on hover. Clicking it lists each mechanism's request, the field
the link is read from, whether the archive documents that field, and the probe
that confirmed it: which accession, on which day, and what came back.

A probe proves a link exists and **measures nothing**, so an untested line
carries no counts, no rank and no ink. It never becomes evidence and cannot
change the order the resolver tries routes in
([a probe is not evidence](../decisions/what-counts-as-evidence.md#a-probe-is-not-evidence)).
A documented link whose probe found nothing is not drawn at all; it is listed
in the panels and on [the catalogue page](../graph/catalogue.md#gaps).

### Panels, matrix and paths

Clicking a type lists everything it **reaches in one hop**, with what is known
about each, and every type it has **no one-hop link to**. The matrix under the
graph is the same information for every pair at once: filled where a route
measures the pair, outlined where only untested links do, dashed where a link
is documented and its probe found nothing, blank where nothing links them.
Clicking a cell selects that pair.

Paths are type paths of up to four hops. By default the path finder uses
measured routes only, from the same search as `RouteRegistry.find_paths()`,
and for each hop the best-ranked route the filters leave visible, which is what
the resolver would try first. With **include untested** it also offers paths
through untested links: each pair's shortest paths and those one hop longer.

The data behind this page is printed by `uv run fetch routes graph`, which
produces it the same way the docs build does. Node positions were searched, not
placed by hand, by `scripts/route_graph_layout.py`.
