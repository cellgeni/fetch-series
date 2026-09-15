---
title: Study to BioProject, via the ENA portal
---

# Study to BioProject, via the ENA portal

**Route id:** `study->bioproject:ena_filereport`
**Provider:** EBI ENA portal
**Direction:** `SRP`/`ERP`/`DRP` → `PRJNA`/`PRJEB`/`PRJDB`

```bash
curl -s 'https://www.ebi.ac.uk/ena/portal/api/filereport?accession=SRP446371&result=read_run&fields=study_accession&format=tsv&limit=0'
```

The inverse of [BioProject to study](../bioproject-to-study/ena-filereport.md),
and the reason a study accession is a usable entry point at all: almost
everything else in the graph is keyed on the project.

## Measured

**Corpus:** a seeded 3,000-study draw from
`results-of:bioproject->study:ena_filereport@reprocessed-prj` — that is, from
the 12,748 studies the BioProject census itself returned, so the population is
exactly the studies this project has to handle. **Surveyed:** 2026-09-15.

| | |
|---|---|
| Resolved | **3,000 (100%)** |
| Empty | 0 |
| Failed | 0 |
| Distinct projects | 2,999 |

A clean round trip: every study reached from a BioProject resolves back to one.
2,999 projects for 3,000 studies means one project appeared twice, which is the
same asymmetry the forward direction shows — a project may carry more than one
secondary study accession, so neither direction is a bijection even though both
are total here.

## What the round trip does and does not prove

It proves the two directions are consistent, not that either is complete. The
corpus was built from what the forward route returned, so a study ENA does not
know about could not appear in it by construction. That is the right population
for "can I go back the way I came", and the wrong one for "does ENA know about
every INSDC study" — a question this route has not been asked.
