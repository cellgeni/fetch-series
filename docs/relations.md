---
title: The relation table
---

# The relation table

**Command:** `fetch relations <accession>` **Code:** `src/fetch_series/relations.py`
**Needs:** a series, project, study or sample accession **Feeds:** samplesheets

One row per run, with every cross-archive link on it.

```
$ fetch relations GSE236084
run          experiment    sample      biosample     study      bioproject   geo_sample  geo_series  ae_experiment  species       library_strategy
SRR25056225  SRX20810436   SRS18093899 SAMN36028297  SRP446371  PRJNA988806  GSM7518069  GSE236084   -              Mus musculus  RNA-Seq
...
```

```
$ fetch relations GSE236084 --sample-map
sample      runs
GSM7518065  SRR25056229
GSM7518066  SRR25056228
```

## Why this is a join and not a lookup

No single archive holds the whole chain, and for each part exactly one does:

| Part of the chain | Only known to |
|---|---|
| `GSM` → experiment | **GEO** — nothing in INSDC records which GEO sample an experiment belongs to |
| run, experiment, sample, BioSample, study, species | **ENA**, in one request, for almost any accession you key it on |
| the study's ArrayExpress accession | **ArrayExpress** |

The **experiment accession is the only key GEO and INSDC share**, so it is the join key. A
run whose experiment ENA did not report cannot be attributed to a GEO sample at all — which
is reported rather than guessed at.

## A GEO series goes through its BioProject

ENA's filereport does not accept GEO accessions; it answers a `GSE` with a 400. So a series
is resolved through the BioProject its SOFT file records — the same chain the
[`soft_bioproject_ena` route](routes/gse-to-experiment/soft-bioproject-ena.md) measures,
which resolved 12,551 of 13,045 series.

For the six series that record SRA relations but no BioProject — four of them because they
sit under [a shared umbrella project](pathologies/series-under-shared-umbrella-bioproject.md)
— ENA is queried per experiment instead.

## Two defects, reported differently

They look similar and are not the same thing, so the table says which one it hit.

**GEO names no SRA relation at all** (16 of 13,045 series). Rows carry the INSDC sample and
`geo_sample` is `-`:

```
$ fetch relations GSE135325
WARNING GSE135325: the SOFT file names no SRA relation for any sample, so no run
        could be attributed to a GEO sample; rows carry the INSDC sample only
```

**GEO names experiments that do not exist in the project's runs.** `GSE150508` names
`SRX7571191`, which has no runs; the project holds `SRX9670669`, which has two. The real
runs are kept:

```
$ fetch relations GSE150508
WARNING GSE150508: none of the 1 experiments its SOFT file names appears in the
        project's runs -- the GEO record is stale. Reporting the 2 runs the
        project actually holds
```

Discarding real data to honour a stale index would be the worst of both answers. See
[SOFT names experiments with no runs](pathologies/soft-names-experiments-with-no-runs.md).

## Missing values and conflicts

Absent fields are written `-`, never left blank. A blank would collapse in shell tooling
and shift every column after it — the same reason `fetch10xmeta` writes `-`, and the same
reason `-` must never be treated as an identifier.

Where two archives supply different values for one field, the **highest-ranked route's
value is used and the disagreement is recorded** on the record. The command reports how
many rows are affected and in which fields:

```
3 runs have fields where two archives disagree; the highest-ranked route's
value is used. Fields: species
```

Species is the field where this matters most: `fetch10xmeta` read one metadata table alone,
reported the runs it could not find as `UNKNOWN`, and the runs of a single sample then
disagreed about their species — which split the sample in two.

## Usability for reprocessing

A run is usable when it has an identity to group by and a species to pick a reference. The
sample identity may be a GEO sample, an INSDC sample or a BioSample; any one will do.

```
2 of 40 runs are not usable for reprocessing (missing: species x2)
```

`--no-incomplete` drops those rows; the default keeps them, because for counting what a
series contains they are still information.

## Not yet included

**File links.** That is milestone 2, and it is where "reliable files to use" gets decided:
ENA `fastq_ftp` versus `submitted_ftp` versus NCBI SDL, md5 and size verification, mate
completeness, and paywalled or rehydration-required objects. Until then this table stops at
the run.
