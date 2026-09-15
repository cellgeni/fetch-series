---
title: Benchmarks
---

# Benchmarks

Every route ranking in this project is derived from a survey recorded here. This page is
the methodology; the per-route numbers live with
[the routes](../routes/gse-to-experiment/index.md).

## Corpora

Tiered, and the tiers answer different questions.

| Corpus | Size | What it is | What it can measure |
|---|---|---|---|
| `hard-cases` | 34 | Curated, one accession per known failure mode | *Can* this happen; does a route handle it |
| `reprocessed-gse` | 13,045 | Every GEO series that has been through reprocessing | Prevalence, for GEO entry points |
| `reprocessed-prj` | 12,755 | Every BioProject in the same table | Prevalence, for project entry points |
| `geo-sample-<n>` | n | Seeded stratified draw from all 275,238 GEO series | Prevalence across GEO as a whole |

A new route runs `hard-cases` first — 34 accessions, seconds, and a design error surfaces
immediately. Only then does it earn a full corpus.

## Three lessons that cost real time

### A pathology corpus cannot measure prevalence

On `hard-cases`, "GEO omits the SRA relation" looks like 2 series in 7. Across all 13,045
reprocessed series it is 16 — **0.12%**. Both numbers are correct and they answer different
questions. Quoting the curated rate as a prevalence overstates it by two orders of
magnitude.

The reverse trap is just as easy: a census says nothing about whether a route *handles* a
rare case, because the case may not be in it.

### A uniform sample of GEO is mostly not sequencing data

A 150-series stratified draw from all of GEO contained **24 series with any SRA data at
all**. The other 84% of the run confirmed that microarray series have no runs. For any
question about sequencing routes, `reprocessed-gse` is the right corpus: SRA-bearing by
construction.

### Samples miss the cases that break things

The HTTP 414 defect — ESummary rejecting a few hundred UIDs in a GET query string — did not
appear in the 150-series sample, because no series in it was large enough. It appeared 9
times in the census, and those 9 series held **12,849 experiments** that a sample-driven
implementation would have gone on losing silently.

## Outcomes

Three, not two. The distinction is load-bearing.

| Outcome | Meaning |
|---|---|
`resolved` | The route returned at least one target accession |
`empty` | The route worked and the archive holds no such link |
`failed` | The route could not find out |

Absence from the store means *never queried*. Conflating `empty` with `failed` is what made
BioProject → GEO look broken when most of its empties are correct answers; conflating it
the other way lets a silent failure pass as a true negative.

Comparisons are therefore keyed on `(outcome, results)`, never on results alone.

## Ranking

`RouteRegistry.ranked()` sorts measured routes ahead of unmeasured ones, then by yield
band, then failure-rate band, then cost. Both bands exist because raw numbers put noise
ahead of substance:

- Compared on raw counts, `sra_be_elink` outranked `sra_be_direct_cgi` on **55 extra runs
  out of 754,000** — 0.007% — while failing twice as often.
- The two BioProject → GEO routes differ by **one accession in 12,114** and return
  identical series sets, so without banding that one accession would permanently outrank
  the fact that one route costs an extra request.

## Reproducing a survey

```bash
fetch survey run --route "<id>" --corpus <name>
fetch survey show --route "<id>" --corpus <name>
fetch survey compare --route "<a>" --route "<b>" --corpus <name> --out disagreements.csv
fetch survey evidence --route "<id>" --corpus <name>   # the block to paste into graph.py
```

Runs are resumable: verdicts are recorded per accession as they land, so an interrupted
census continues rather than restarting. Failures are **not** retried on resume unless
`--retry-failed` is passed, because a survey that quietly re-attempted its own failures
would report a coverage figure no single run ever achieved.

`survey compare --intersection` restricts to accessions every route has answered, which is
what you want while a census is still in flight.

## A trap worth knowing

**Rotating an API key mid-survey is destructive.** NCBI answers an invalid key with a
**400**, which is not retryable, so every remaining accession is recorded as a permanent
failure. Stop the run, update `.env`, and resume.
