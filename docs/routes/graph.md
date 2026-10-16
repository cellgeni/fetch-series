---
title: Route graph
hide:
  - navigation
  - toc
---

# The route graph

Every declared route between accession types, with what its survey found: how
many lookups resolved, came back empty, or failed.

<div id="route-graph" class="rg-root" aria-label="Interactive route graph"></div>

<!-- route-graph-data -->

<noscript>
The interactive graph needs JavaScript. The same numbers are in
<a href="../">the route matrix</a>.
</noscript>

## How to read it

Every declared route is one edge: a source accession type, a target, and the
provider that answers it. Where several routes answer the same direction they
run in parallel, numbered in the order the resolver tries them. That order is
`RouteRegistry.ranked()` over the evidence, not a hand-written preference
([how routes are ranked](../decisions/route-ranking.md)).

The counts on each route come from its `RouteEvidence`: one dated survey over
one named corpus. Each queried accession ends in one of three outcomes:
**resolved** (at least one result), **empty** (the archive answered, and holds
no such link) or **failed** (no verdict at all). These are defined in
[how to read the matrix](index.md#how-to-read-it). The adversarial
`hard-cases` corpus is a gate, not evidence, and does not appear here.

Six routes were measured by the 2026-05 scripts, which kept a single boolean
per BioProject that lumped empty in with failed. Their split is drawn as
**not recorded** (dashed edges, hatched bars) rather than guessed.

Paths are type paths of up to four hops, from `RouteRegistry.find_paths()`.
For each hop the path finder uses the best-ranked route the filters leave
visible, which is what the resolver would try first.

The data behind this page is printed by `uv run fetch routes graph`, which
produces it the same way the docs build does.
