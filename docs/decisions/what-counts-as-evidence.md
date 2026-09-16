---
title: What counts as evidence
---

# What counts as evidence

**Status:** in force · **Enforced by:** `tests/test_routes.py`,
`tests/test_graph.py`

[The ranking](route-ranking.md) is computed from evidence, so what qualifies as
evidence is not a methodological footnote — it is the input to every routing
decision the tool makes. Each rule below exists because breaking it produced a
wrong number that looked right.

## A verdict has three states, not two

`RESOLVED`, `EMPTY`, `FAILED` — and **absence is a fourth thing**, meaning the
accession was never queried.

Collapsing empty into failure made `bioproject->geo_series` look broken when
most of its empties are correct: a `PRJEB` project genuinely has no GEO series,
and saying so is the right answer. Collapsing failure into empty is worse — it
manufactures a true negative out of a request that never completed, which is how
[a stale experiment list](../pathologies/soft-names-experiments-with-no-runs.md)
becomes indistinguishable from a broken one.

`fetch survey compare` keys agreement on `(outcome, results)` for the same
reason. An earlier version reported a FAILED and an EMPTY route as *agreeing*,
because both returned nothing.

## Unique results, never rows

The 2026-05 predecessor to this project counted rows. **69,923 of its 824,216
"runs" were within-BioProject duplicates** — an 8.5% inflation that made the
`sra-db-be` routes look closer to ENA than they are.

Every `unique_results` figure in `graph.py` is a distinct-accession count, and
`sra_be.column()` deduplicates for this reason.

## The corpus name must identify the accession set exactly

The name is persisted with every verdict as provenance. If it does not pin down
which accessions were queried, two runs recorded under one name are not
comparable, and nothing in the data says so.

This is why:

- **`--limit` is never used to produce evidence.** A limited run records its
  verdicts against the *full* corpus name, so two differently-limited runs look
  comparable and are not. `sample:<n>@<corpus>` exists instead: seeded,
  reproducible, and named for its size.
- **`geo-sample-15` holding 10 accessions was a bug worth fixing**, not a
  rounding detail. `n // strata` silently returned 10 for any `n` below 20.
- **A sampled draw records its parent.** `sample:3000@reprocessed-srr` says both
  the size and where it came from; `results-of:<route>@<corpus>` says which
  survey produced the population.

## `hard-cases` is a gate, not evidence

Every route must produce a verdict for all 46 curated accessions before it earns
a larger run. None of those verdicts is recorded as `RouteEvidence`.

`hard-cases` is **adversarial by construction**: every accession in it is there
because it broke something. Its empty rate is high on purpose. Feeding that into
the ranking would order routes by how often they were deliberately pointed at
withdrawn projects and private series.

Tier one proves a route *runs*. Only a representative corpus measures how well.

## An undated number is not evidence

`RouteEvidence` requires `surveyed_on`, and a test asserts it is not in the
future. Archive contents drift — [0.77% of runs that reprocessed successfully no
longer exist in either archive](../routes/run-to-experiment/ena-filereport.md) —
so a coverage figure without a date makes a claim it cannot support.

Dates are **UTC**. A survey finishing at 00:12 BST is 23:12 UTC the previous
day; recording local time stamped two blocks a day ahead and CI, an hour behind,
rejected them as being in the future.

## Every measured route has a page, and that page cites a number

Two tests enforce it: a measured route with no knowledge-base page fails the
suite, and so does a page containing no digits. The rule is only worth having if
it cannot be quietly skipped, and a route whose evidence lives solely in a
dataclass is not reviewable by anyone who does not read the source.

## Recall and precision are not symmetric, and the asymmetry is stated

The [assay layer](../assays.md) reports **90.9% recall** against a corpus that
is 10x by construction, and reports **no precision figure at all** — there is no
labelled non-10x corpus at comparable scale to measure it against.

Claiming one would be exactly the invented default this project exists to avoid.
What can be said instead is narrower and checkable: the trigger phrases are
platform names, and `scRNA-seq` is deliberately **not** among them because it
appears in Smart-seq, Drop-seq and inDrop submissions too. Adding it would buy
recall by manufacturing false positives.

The same reasoning applies to
[chemistry](../assays.md#chemistry-the-one-place-metadata-beats-the-reads):
agreement with the empirical FASTQ inference is not claimed, because measuring
it needs the downloads the metadata layer exists to avoid.

## A census is not always worth its cost

Some routes were censused over the full corpus and some over a named seeded
draw. That is a judgement about cost, and it is recorded rather than hidden:

- **Censused** where the population definition was the point —
  `arrayexpress-ena` (20,693) because the ENA link is known to exist from
  outside the route being measured, so every miss is a countable recall failure.
- **Sampled** where the answer converges long before the corpus does. The run,
  sample and experiment columns hold 100k–263k accessions at one request each; a
  3,000-run draw answers to within a percentage point in a twentieth of the
  time.
- **Not re-censused** at all for the two `sra_be`/`efetch` BioProject→run
  routes: roughly five hours each, already 4–14% behind ENA on the 2026-05
  figures, and the ranking outcome cannot change. Recorded as a scoping decision
  rather than left looking like an oversight.

## What would change this

Any of these rules is reversible on evidence that it produces a worse answer.
The one that is not negotiable is the meta-rule: **a number in this project
carries its corpus and its date, or it is not used.**
