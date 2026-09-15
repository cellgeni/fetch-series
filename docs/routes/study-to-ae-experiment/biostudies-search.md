---
title: Study to ArrayExpress experiment, via BioStudies search
---

# Study to ArrayExpress experiment, via BioStudies search

**Route id:** `study->ae_experiment:biostudies_search`
**Provider:** EBI BioStudies
**Direction:** `SRP`/`ERP`/`DRP` study → `E-MTAB`/`E-GEOD`/`E-ERAD` experiment
**Status:** implemented; measurement pending

## Why this direction exists

ArrayExpress is an entry point, so it has to be reachable as a destination too.
A user arriving with `ERP126408` should be able to find `E-MTAB-10018`, the
ArrayExpress record describing the same submission — and thereby its SDRF, which
carries sample-level metadata no SRA-side route exposes.

## The request

```bash
curl -s 'https://www.ebi.ac.uk/biostudies/api/v1/arrayexpress/search?query=ERP126408&pageSize=100&page=1'
```

```json
{"totalHits": 1, "hits": [{"accession": "E-MTAB-10018", "type": "study", ...}]}
```

## What it actually matches

There is no field query for secondary accessions. This is a **free-text search**
of the ArrayExpress index, which happens to cover the IDF, so a study named there
is found. Two consequences follow, and neither is a bug in this route:

- A study accession that appears in some unrelated study's *description* matches
  it. Results are filtered to accessions beginning `E-`, but that filter cannot
  distinguish "describes this study" from "mentions it".
- More than one hit is a fact about ArrayExpress, not an error. The resolver
  records every hit with provenance rather than choosing one.

## Paging

The search endpoint pages, and returns the first 25 hits with no warning when no
explicit size is given. It also serves only the first 20,000 hits of any query and
answers the next page with HTTP 500 — see
[BioStudies serves 20,000 hits, then returns 500](../../pathologies/biostudies-20000-hit-window.md).
Neither limit binds on a single-study lookup, which returns one or two hits, but
both bind on any enumeration of the collection.

## Measured behaviour

Not yet surveyed against a representative corpus. Tier one
(`hard-cases`) produces a verdict for every study: `ERP126408` resolves to
`E-MTAB-10018`, and `SRP446371` — an NCBI-native study with no ArrayExpress
record — returns empty rather than failing, which is the correct answer.

Per [the governing rule](../../index.md), an unmeasured route sorts behind every
measured one and this page will not claim coverage it has not counted.
