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

## Measured

**Corpus:** a seeded 3,000-study draw from the 12,748 studies the
[BioProject to study](../bioproject-to-study/ena-filereport.md) census returned
— that is, the studies behind the reprocessed 10x corpus. **Surveyed:**
2026-09-15.

| | |
|---|---|
| Resolved | 100 (3.3%) |
| Empty | 2,900 |
| Failed | **0** |

**A 3.3% hit rate is the right answer, not a poor one.** The corpus is studies
reached from the BioProjects a 10x reprocessing pipeline has handled, and those
are overwhelmingly NCBI submissions with no ArrayExpress record to find. The
number measures the overlap between two archives' holdings, not the route's
reliability — for which the relevant figure is the zero failures.

The cost of a miss is one request that returns an empty result set, so this is a
cheap route to try and a useless one to rely on. Rank it accordingly: it is the
way *in* for someone holding a study accession who wants ArrayExpress's
sample-level metadata, not a step in any resolution path that already has one.
