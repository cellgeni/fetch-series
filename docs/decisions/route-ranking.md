---
title: How routes are ranked, and why
---

# How routes are ranked, and why

**Status:** in force · **Implemented in:** `RouteRegistry.ranked()` in
[`graph.py`](https://github.com/cellgeni/fetch-series/blob/main/src/fetch_series/graph.py)

This is the single most consequential design decision in the project: what
happens when two routes answer the same question. Everything else follows from
it.

## The decision

**The order is computed from measured evidence. It is never written down.**

`graph.py` declares routes with a `RouteEvidence` block — corpus, date, counts —
and `ranked()` derives the ordering from those blocks at call time. There is no
preference list, no priority integer, and no `if provider == "ena"` anywhere in
the project.

A route nobody has measured sorts behind every route that has been, however
good it looks.

## Why not a hand-written order

Because every plausible-sounding preference this project started with turned
out to be wrong at least once, and the wrong ones were not obviously wrong.

The incumbent bash module tries NCBI's `efetch` before ENA. The evidence says
that is backwards by **111,665 runs** — the set difference, over the same 12,755
projects in the 2026-05 survey, of the runs ENA returns minus those `efetch` (via
ELink) returns ([the comparison](../routes/bioproject-to-run/index.md)):

| Route | Unique runs | Failed | Requests |
|---|---:|---:|---:|
| `ena_filereport` | **783,936** | **0** | 1 |
| `sra_be_direct_cgi` | 754,277 | 15 | 2 |
| `sra_be_elink` | 754,332 | 33 | 3 |
| `efetch_direct` | 673,157 | 14 | 2 |
| `efetch_elink` | 674,186 | 30 | 3 |

Nobody wrote that order down wrongly out of carelessness. It was a reasonable
guess, held for years, and only a census over all 12,755 BioProjects settled it.

## The precedence, in order

### 1. Measured before unmeasured

Even a poor measured route beats one nobody has tested. An unmeasured route
might be perfect; it might silently return half the data, which is what
[ELink does](../pathologies/elink-gds-sra-missing-links.md). The difference is
unknowable until someone counts, and until then the route is a liability rather
than an option.

### 2. Yield, **banded**

Not raw counts. Yields within `YIELD_BAND = 0.01` — one percent of the best
route in the group — are treated as indistinguishable, and the decision falls
through to reliability.

**This matters because ranking on raw yield put noise above substance twice.**

`sra_be_elink` returns **55 more runs** than `sra_be_direct_cgi` — out of
754,000, which is **0.007%** — while failing **33 times against 15** and costing
an extra request. On raw counts it wins. It should not: those 55 runs are noise,
and more than doubling the failure rate to get them is a bad trade the ranking would have
made silently.

The second instance was `bioproject->geo_series`, where `elink` beat
`gds_direct` on **one accession out of 12,114**.

Banding is therefore not a refinement. It is the difference between a ranking
that reflects the evidence and one that reflects its error bars.

### 3. Failure rate, banded

`FAILURE_BAND = 0.001` — a tenth of a percent — for exactly the same reason. A
route failing on 14 accessions and one failing on 15 are not meaningfully
different, and separating them on that basis would let sampling noise decide.

### 4. Cost in requests

Only once yield and reliability tie. The extra ELink hop buys nothing when
yields band together, and brings a failure mode of its own — MegaLink /
`pmquerysrv` backend flakiness accounted for 3,135 occurrences in the 2026-05
logs, the dominant failure signal in the whole corpus.

### 5. Route id

So the order is stable across runs. This is not cosmetic: **the route id is
recorded as provenance** on every resolved value, and an ordering that varies
between runs would make two identical queries produce differently-attributed
answers.

## Coverage is not correctness

The sharpest case against ranking on yield alone, and the reason
[rule 5](../index.md) exists.

`gse->bioproject:gds_summary` beats `soft_family` on **every headline number**:

| | `soft_family` | `gds_summary` |
|---|---:|---:|
| Resolved | 12,572 | **13,038** |
| Failed | 7 | **0** |
| Requests | 1 (a multi-MB gzip) | **1 API call** |
| Series only this route resolves | **0** | **466** |

And where both answer they never disagree — 12,572 for 12,572.

Then: of the 466 extra answers, **190 (41%) resolve to a species- or
consortium-level umbrella project.** `PRJNA30709` — "Production ENCODE
transcriptome data" — accounts for 138 of them and holds **7,591 runs**. A
two-sample GEO series routed through it returns 7,591 runs and reports success.

### What was *not* done about it

**The ranking was not hand-tuned against the evidence.** That would break the
rule this whole page exists to enforce, and the precedent is worse than the bug:
once one route is demoted by judgement, every ranking becomes negotiable.

Instead the defect is declared on the route as data, and printed beside the
value it produced:

```
GSE78298 -> bioproject (union)
  resolved gse->bioproject:gds_summary: 1 results
           known defect: docs/pathologies/series-under-shared-umbrella-bioproject.md
  empty    gse->bioproject:soft_family: the archive holds no such link
```

The route still ranks first, because on the evidence it should. The caller is
told what they are holding.

[The full comparison](../routes/gse-to-bioproject/gds-summary.md).

## Routes are filtered before they are ranked

`ranked_for()` drops routes whose provider does not index the accession's
issuing archive, before ordering the rest. That is not a preference either — it
is measured:

| Prefix | Issued by | NCBI's link table resolves |
|---|---|---:|
| `PRJNA` | NCBI | **99.3%** |
| `PRJEB` | EBI | **1.4%** |
| `PRJDB` | DDBJ | 32.8% |

Sending an EBI-native project to NCBI's ELink costs three requests and answers
nothing 98.6% of the time. The archives index their own accessions;
[routing by archive](../graph/routing-by-archive.md) has the figures.

## What would change this

A measurement. Specifically:

- **A new corpus that reorders the table.** The rankings cite dated surveys over
  named corpora; a different population may rank differently, and that is a
  finding rather than a problem.
- **Evidence that banding is hiding a real difference.** The bands are 1% and
  0.1%; if a route's genuine advantage ever falls inside one, the band is too
  wide and the argument for narrowing it will be a measurement, not an opinion.
- **A route whose answers are wrong rather than incomplete.** Yield cannot see
  that, which is what the `known_pathologies` mechanism is for. If declaring the
  defect ever proves insufficient — if callers demonstrably do not read
  `--explain` — the honest fix is a new evidence *axis*, not a thumb on the
  scale.
