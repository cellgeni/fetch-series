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

### A limit is not a sample

Corpora arrive sorted, so taking the first N of one returns the N lowest accession numbers
— the oldest submissions. An early 150-series run sliced that way covered only GSE ≤ 59,184,
the oldest fifth of GEO. Every ELink failure in it was therefore an old accession, which
produced an age hypothesis that the full census disproved.

`fetch survey run --limit` now samples in the survey's own seeded visit order, and
`shuffled()` is exported so anything else that slices a corpus does the same. The lesson
generalises: **any prefix of an ordered corpus is a stratum, not a sample.**

### A route returning "resolved" does not mean it resolved everything

The sharpest lesson of all, and it invalidated a published figure.

`gse->experiment:soft_family` was censused over 13,045 series, and 16 came back empty. That
was reported as the prevalence of GEO omitting the SRA relation: 0.12%.

It is not. The census measures whether a route returned **anything**, so it sees only series
where *every* sample lacks a relation. A series where 37 of 50 samples lack it resolves 13
experiments and is recorded as a success. Measuring properly — comparing each series' sample
list against the samples that carry a relation — puts the affected-series rate about
**sevenfold** higher.

The general form: **a survey measures the question its route asks.** `resolved / empty /
failed` is a verdict about the *call*, not about completeness of the answer. Any question of
the form "how much of X is missing" needs a route that returns the missing thing, or a
comparison against an independent count. It cannot be read off a coverage rate.

## Tier one is a coverage check, and is not recorded as evidence

Every implemented route is exercised on `hard-cases` before anything larger. As of
2026-09-15 all 33 implemented routes have been, and the results are below. Two things to
read from it, and one thing not to.

| Route | Queried | Resolved | Empty | Failed |
|---|---|---|---|---|
| `ae_experiment->biosample:sdrf` | 5 | 4 | 1 | 0 |
| `bioproject->biosample:ena_filereport` | 16 | 13 | 3 | 0 |
| `bioproject->experiment:ena_filereport` | 16 | 13 | 3 | 0 |
| `bioproject->study:ena_filereport` | 16 | 13 | 3 | 0 |
| `biosample->run:ena_filereport` | 3 | 3 | 0 | 0 |
| `experiment->biosample:ena_filereport` | 3 | 2 | 1 | 0 |
| `experiment->run:ena_filereport` | 3 | 2 | 1 | 0 |
| `geo_sample->biosample:acc_cgi` | 3 | 3 | 0 | 0 |
| `geo_sample->geo_series:acc_cgi` | 3 | 3 | 0 | 4 series from 3 samples |
| `gse->bioproject:gds_summary` | 7 | 6 | 1 | 0 |
| `gse->bioproject:soft_family` | 7 | 5 | 1 | 1 |
| `gse->geo_sample:gds_summary` | 7 | 6 | 1 | 0 |
| `gse->geo_sample:soft_family` | 7 | 6 | 0 | 1 |
| `gse->run:elink_gds_sra` | 7 | 5 | 2 | 0 |
| `run->biosample:ena_filereport` | 2 | 2 | 0 | 0 |
| `run->experiment:ena_filereport` | 2 | 2 | 0 | 0 |
| `sample->run:ena_filereport` | 5 | 5 | 0 | 0 |
| `study->bioproject:ena_filereport` | 2 | 2 | 0 | 0 |
| `study->run:ena_filereport` | 2 | 2 | 0 | 0 |

Both failures are the same accession: `GSE207991` is private and its SOFT family file 404s.
That is the corpus doing its job, not a defect.

The most useful row is `experiment->run:ena_filereport`. It reports `SRX7571191` — the
experiment [GSE150508's SOFT file names, which has no data](../pathologies/soft-names-experiments-with-no-runs.md)
— as **empty**, while `SRX9670669` resolves two runs. The confirmation mechanism the
resolver depends on is doing the right thing at the level of a single route.

**These results are deliberately not recorded as `RouteEvidence`.** `hard-cases` is curated
for pathology, so its resolve rate is not a prevalence estimate, and `unique_results` over
43 hand-picked accessions is not comparable with a census over 13,045. Recording it would
let a route "measured" on 43 accessions outrank one measured on thousands, for no better
reason than that both had *some* number attached. A route stays unmeasured until something
with a real denominator has been run against it.

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
