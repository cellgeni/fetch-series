---
title: GEO sample → experiment, BioSample and series
---

# GEO sample → experiment, BioSample and series

**Needs:** a `GSM` accession **Feeds:** experiment → run → files
**Routes:** 3, all from GEO's own per-sample record

Until these existed a `GSM` was a dead end. The graph could reach one from a series but not
leave it, so half the GEO entry points resolved to nothing.

## Routes

All three read `acc.cgi?acc=<GSM>&targ=self&form=text&view=brief`, which returns GEO's SOFT
record for a single sample. One request each, and one request serves all three if the record
is parsed once.

| Route | Reads |
|---|---|
| `geo_sample->experiment:acc_cgi` | `!Sample_relation = SRA:` |
| `geo_sample->biosample:acc_cgi` | `!Sample_relation = BioSample:` |
| `geo_sample->geo_series:acc_cgi` | `!Sample_series_id` |

## Do not use these to enumerate a series

The per-sample record is the wrong tool for listing a series' samples. `acc.cgi` is a web
endpoint rather than an API, `view=brief` still returns the submitter's full protocol prose
— tens of kilobytes per sample — and a series of 2,396 samples would mean 2,396 requests.

[The SOFT family file](../gse-to-experiment/index.md) returns every sample and every
relation in **one** request. These routes are for when a `GSM` is the *input*, not for
walking down from a series.

## A sample can belong to several series

`GSM7518069` is in both `GSE236084` and `GSE236087`. Routing from a sample to "its" series
cannot assume there is one, which is why `geo_sample->geo_series` returns a list and the
tier-one corpus contains that accession specifically.

## The series-level pathologies show up here too

The same records back both views, so a sample inherits whatever its series omits. On the
tier-one corpus:

| Sample | From | experiment | biosample |
|---|---|---|---|
| GSM7518069 | GSE236084 (healthy) | resolves | resolves |
| GSM4005486 | [GSE135325](../../pathologies/geo-omits-sample-sra-relation.md) | **empty** | resolves |
| GSM4274734 | [GSE150508](../../pathologies/soft-names-experiments-with-no-runs.md) | resolves — but the experiment carries no runs | resolves |

GSM4005486 is the useful one: GEO records its BioSample and no SRA relation, which is
exactly what its series does, so the pathology is a property of the record rather than of
the family-file parser.

GSM4274734 is the more dangerous shape. The route resolves, the answer looks ordinary, and
the experiment it names has no data — which is only visible by
[confirming against a data-proving route](../../resolver.md).

## Measured

**Corpus:** `sample:1000@reprocessed-sample` — a seeded draw of 1,000 from the
104,515 samples in the reprocessed table. **825** of those 1,000 are GEO samples;
the other 175 are INSDC samples these routes cannot accept, and the runner filters
them out rather than counting them as failures. **Surveyed:** 2026-09-15.

All three directions come from the same `acc.cgi` record, and all three behave
identically on it:

| Route | Resolved | Empty | Failed | Unique results |
|---|---:|---:|---:|---:|
| `geo_sample->experiment:acc_cgi` | 822 | **0** | 3 | 822 |
| `geo_sample->biosample:acc_cgi` | 822 | **0** | 3 | 822 |
| `geo_sample->geo_series:acc_cgi` | 822 | **0** | 3 | **849** |

The same 3 failures in each case — one record, one fetch, one outcome.

**849 series for 822 samples.** 27 samples belong to more than one series, which
is the [multi-series membership](#a-sample-can-belong-to-several-series) case
above measured rather than asserted: 3.3% of samples, common enough that code
treating "its series" as singular is wrong for one sample in thirty.

### Why zero empties is not the good news it looks like

Every one of the 822 named an experiment. That is **not** evidence against
[GEO omitting the SRA relation](../../pathologies/geo-omits-sample-sra-relation.md):
the reprocessed table contains only samples the pipeline *succeeded* on, so a
sample GEO records no relation for is largely excluded from this corpus by
construction. The corpus can measure how reliable the route is for samples that
are known to have worked; it cannot measure how often GEO leaves a sample
unreachable, because those samples are not in it.

Measuring that needs a draw from all of GEO — `geo-sample-<n>` — which is what
the series-level census used.

Tier one (3 accessions) is described above, and is a **coverage** check rather
than a prevalence estimate — see [benchmarks](../../benchmarks/index.md) on why
the two cannot substitute for each other.
