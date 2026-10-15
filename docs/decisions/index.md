---
title: Decisions
---

# Decisions

Every choice this project made that a reader might reasonably have made
differently, with the evidence behind it and what would reverse it.

The [route pages](../routes/index.md) record *what the archives do*. These pages
record *what we decided to do about it* — which is the part that ages fastest
and is hardest to reconstruct from code.

## The log

| Decision | Basis | Status |
|---|---|---|
| [How routes are ranked](route-ranking.md) | census over 12,756 BioProjects and 13,045 GEO series | in force |
| [What counts as evidence](what-counts-as-evidence.md) | each rule exists because breaking it produced a wrong number | in force |
| [What counts as a metadata gold set](what-counts-as-a-metadata-gold-set.md) | 120 ArrayExpress studies: 119 carry curated labels, 0 carry an independent sample | in force |
| [How values are grounded](ontology-grounding.md) | three measured wrong answers, each from an obvious implementation | in force |
| [What happens when two archives disagree](merging-and-provenance.md) | union, per-value provenance, conflicts surfaced | in force |
| [Parity before improvement](parity-before-improvement.md) | 14 of 14 of the incumbent's own test cases | in force until the swap lands |
| [No DDBJ provider is needed](no-ddbj-provider.md) | ENA resolves 137/137 `PRJDB` projects | reverses on a corpus ENA misses |
| [The ENA browser XML API is not needed](ena-browser-xml-not-needed.md) | every field already in `filereport` | reverses for analysis or submission records |

## Decisions recorded elsewhere

Some belong with the thing they describe rather than here, and are cross-linked
so this page stays the index of all of them:

| Decision | Where |
|---|---|
| Ask every file route, not the first that answers | [the file layer](../files.md#why-every-route-is-asked-not-just-the-first) |
| Which *files* to use is a requirements question, not an evidence one | [the file layer](../files.md#two-kinds-of-judgement-kept-apart) |
| Precision is not claimed for the assay screen, and why | [the assay layer](../assays.md#what-is-not-measured-and-why) |
| Metadata sources are unranked until surveyed, so disagreement is reported | [the metadata layer](../metadata.md#no-source-is-ranked-and-the-code-says-so) |
| A key whose meaning depends on the record is resolved from the archive's own declaration | [the metadata layer](../metadata.md#what-it-recovers-that-was-being-thrown-away) |
| Library type and chemistry report ambiguity rather than picking | [the assay layer](../assays.md#library-type-is-study-level-and-says-so) |
| `hard-cases` gates a route; it never becomes evidence | [benchmarks](../benchmarks/index.md#tier-one-is-a-coverage-check-and-is-not-recorded-as-evidence) |
| A named draw, never `--limit`, for anything recorded | [benchmarks](../benchmarks/index.md#naming-a-draw-is-not-decoration) |
| Route by the accession's issuing archive | [routing by archive](../graph/routing-by-archive.md) |
| The two `sra_be`/`efetch` run routes were not re-censused | [what counts as evidence](what-counts-as-evidence.md#a-census-is-not-always-worth-its-cost) |

## Decisions taken outside the evidence

Two, and they are marked as such rather than dressed up:

- **No PyPI release for now.** The maintainer's call, 2026-09-16. Nothing in the
  measurements bears on it.
- **The upstream reports are drafted and not filed.** Sending them is
  outward-facing and belongs to the maintainer;
  [`upstream/`](../upstream/index.md) holds them ready to paste, each with the
  command that regenerates its evidence first.

## The form these take

Each page states the decision, the measurement it rests on, the argument, and
**what would reverse it**. That last section is the one that matters most: a
decision with no stated failure condition is a preference, and this project's
governing rule is that preferences do not ship.
