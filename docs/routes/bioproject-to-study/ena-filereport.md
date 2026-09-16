---
title: BioProject to study, via the ENA portal
---

# BioProject to study, via the ENA portal

**Route id:** `bioproject->study:ena_filereport`
**Provider:** EBI ENA portal
**Direction:** `PRJNA`/`PRJEB`/`PRJDB` → `SRP`/`ERP`/`DRP`

```bash
curl -s 'https://www.ebi.ac.uk/ena/portal/api/filereport?accession=PRJNA988806&result=read_run&fields=secondary_study_accession&format=tsv&limit=0'
```

## Measured

**Corpus:** `reprocessed-prj`, all 12,755 BioProjects. **Surveyed:** 2026-09-15.

| | |
|---|---|
| Resolved | 12,732 |
| Empty | 23 |
| Failed | 0 |
| Distinct studies | 12,748 |

**12,748 studies for 12,732 projects.** Close to one-to-one, but not exactly:
a handful of projects carry more than one secondary study accession, so code
that treats the mapping as a function will drop data on those. The 23 empties
are the same projects every ENA route finds nothing for.

## Why this direction is asked at all

A study accession is what [ArrayExpress declares](../ae-experiment-to-study/index.md)
in its IDF and what several older tools take as their handle, so a BioProject
has to be convertible to one. It is also the cheapest way to confirm that a
project is EBI-visible at all: `result=read_run` rows *are* runs, so a study
returned here is a study with data behind it.
