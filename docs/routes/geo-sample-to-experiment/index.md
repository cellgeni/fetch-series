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

## Measured behaviour

A tier-two sample of 1,000 GEO samples drawn from the reprocessed table is in progress.

Tier one (3 accessions) is described above, and is a **coverage** check rather than a
prevalence estimate — see [benchmarks](../../benchmarks/index.md) on why the two cannot
substitute for each other.
