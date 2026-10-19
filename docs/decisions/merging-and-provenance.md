---
title: What happens when two archives disagree
---

# What happens when two archives disagree

**Status:** in force · **Implemented in:** `resolver.py`, `entities.py`,
`files.py`

[Ranking](route-ranking.md) decides what to try first. This decides what to do
with the answers — and the answer is almost always *keep both and say so*.

## Union by default, not a fallback chain

`Mode.UNION` is the default and `Mode.FIRST` is the opt-in.

A fallback chain stops at the first route that answers, which means it can only
ever be as complete as its best single route. The evidence says no single route
is complete: ENA holds 111,665 runs `efetch` never returns, and about 1,800 runs
are missing from ENA — both set differences over the same 12,755 projects in one survey. Taking the union of the two is strictly better than
picking either, and the cost is one extra request against a rate limit that is
not the bottleneck.

`Mode.FIRST` exists because sometimes latency matters more than completeness,
and the caller is the only one who knows which.

## Provenance is per value, not per record

Every resolved accession carries which route produced it, whether that route
proves data exists, and how it was reached. Not a per-record "source: ENA"
field.

This is what makes *"confidently"* in the project's goal statement a property
rather than a claim. A record-level source cannot express the common case: a run
whose experiment came from GEO, whose species came from ENA, and whose GEO sample
was confirmed by two routes that share no data source.

## First wins, and the loser is recorded

`RunRecord.set()` keeps the first value it is given and appends any different
later value to `conflicts`.

First-wins rather than last-wins because routes are offered in measured order:
the first answer comes from the route the evidence ranks highest. But the second
answer is **not discarded silently** — when two archives disagree about the same
field, surfacing the disagreement is the only honest option, and it is also the
more useful one. Those conflicts are where the pathologies came from.

`fetch relations` prints a warning naming the fields where any run has a
conflict, so a disagreement cannot pass unnoticed just because nobody thought to
inspect the object.

### The one place this was nearly wrong

The GEO sample is now attributed from two independent sources — GEO's own
`!Sample_relation = SRA:` and
[ENA's submitter alias](../routes/run-to-geo-sample/ena-alias.md). They agree
**8,604 times out of 8,604**.

It would have been easy to prefer GEO on the grounds that GEO owns GSMs. That
would have hidden the three runs the alias recovers and GEO names nothing for —
and, more importantly, it would have meant that if the two ever *did* disagree,
nobody would find out. They are both recorded; disagreement lands in
`conflicts`.

## Confirmed versus unconfirmed

A route sets `proves_data_exists` when its rows **are** the data. ENA's
`result=read_run` rows are runs, so an experiment returned from one is an
experiment that carries files.

GEO's SOFT file names experiments from the submitter's own record, which is a
weaker thing:
[1.23% of the experiments it names carry no runs at all](../pathologies/soft-names-experiments-with-no-runs.md)
(95% CI 0.84–1.63%, measured directly over 3,000 experiments).

So a resolution reports `confirmed` and `unconfirmed` separately, and
`--confirmed-only` exists. Merging them would make a complete-looking experiment
list that resolves to zero downloadable files indistinguishable from one that
works — which is precisely the failure the incumbent pipeline hit.

## Disagreement is compared only between routes that answered

`Resolution.disagreed` considers only routes that actually contributed a value.

An earlier version marked **every multi-hop resolution as disagreeing**, because
the routes used for earlier hops contribute no final values and therefore
trivially "disagree" with the ones that do. A conflict signal that fires on
every query is worse than none: it trains the reader to ignore it.

## The file layer keeps every offer

`FileSet` holds what every route offered; `recommend()` returns a choice plus
the alternatives it did not take.

Collapsing to a single pick before the caller sees it is what lost all 36 files
of E-MTAB-8060 in the incumbent — the SDRF's fastq URIs outranked the ENA BAM,
and [rule 1 never looked at what it was outranking](../pathologies/ae-sdrf-points-at-decommissioned-mirror.md).

A recommendation the caller cannot argue with is one they cannot check.

### Two kinds of judgement, deliberately kept apart

| Question | Answered by | Basis |
|---|---|---|
| Which **route** to try first | `RouteRegistry.ranked()` | measured evidence |
| Which **files** to use once several answered | `files.recommend()` | the downstream pipeline's requirements |

The second is not a preference between archives and must not be mistaken for
one. STARsolo reads fastq, so fastq outranks BAM; an `.sra` object must be
dumped before it is anything at all. Those are statements about the tool.

The ordering within that: a complete read pair beats everything; a set the
archive's own metadata **proves** incomplete ranks below every format that is
merely inconvenient; then format, then free-before-paid, then
checksummed-before-not.

That second clause is the
[39% finding](../pathologies/ena-paired-library-single-fastq.md) made
operational. It is a *fact* about the run — `library_layout=PAIRED` beside a
one-file list — not a preference, which is why it outranks format.

## Entity types are not merged across archives

A `GSE` is `GEO_SERIES`, not `STUDY`. A `GSM` is `GEO_SAMPLE`, not `SAMPLE`.

The INSDC triples genuinely are the same entity under three prefixes and share
one type. GEO and ArrayExpress records are not: a GEO series is a curated record
that usually *points at* an SRA study, and conflating the two is the root of
several pathologies in this knowledge base. `ranked_for()` can only filter by
issuing archive because the model keeps them apart.

`FILE` is a terminal node with no parse rule — a file is not an accession, and
nothing can resolve *from* one.

## `-` is a placeholder, never an identifier

It appears 18,623 times in the reprocessed corpus. `parse()` reports it as
unparsed rather than dropping it, because a corpus that silently discards rows
has an unknowable denominator — and because as a `grep -f` pattern it matches
every line of a metadata table.

## What would change this

- **Evidence that the union is worse than the best route.** It would have to be
  a route whose extra answers are *wrong* rather than additional; that is what
  `known_pathologies` records and `--explain` surfaces.
- **A field where first-wins demonstrably picks badly.** The conflict data is
  already recorded, so this is answerable from the surveys rather than by
  argument.
