---
title: Study to run, via the ENA portal
---

# Study to run, via the ENA portal

**Route id:** `study->run:ena_filereport`
**Provider:** EBI ENA portal
**Direction:** `SRP`/`ERP`/`DRP` → `SRR`/`ERR`/`DRR`

```bash
curl -s 'https://www.ebi.ac.uk/ena/portal/api/filereport?accession=SRP446371&result=read_run&fields=run_accession&format=tsv&limit=0'
```

The shortest path from a study to its data. `result=read_run` rows **are** runs,
so a run returned here is a run that exists — the `proves_data_exists` property
that separates this from the GEO-side routes, where
[1.23% of the experiments SOFT names carry no runs at all](../../pathologies/soft-names-experiments-with-no-runs.md).

## Measured

**Corpus:** a seeded 3,000-study draw from the 12,748 studies the
[BioProject to study](../bioproject-to-study/ena-filereport.md) census returned.
**Surveyed:** 2026-09-15.

| | |
|---|---|
| Resolved | **3,000 (100%)** |
| Empty | 0 |
| Failed | 0 |
| Unique runs | **205,175** |

**68.4 runs per study**, and not one study that resolved to nothing. The 23
BioProjects that every ENA route reports empty never produced a study accession
in the first place, so they cannot appear here — which is the right behaviour
and also the reason this route's 100% is not a stronger claim than the
BioProject route's 99.8%. They are measuring the same archive over populations
that differ by exactly those 23.

## Study or project?

Both directions from a BioProject are total over this corpus, and both are one
request. Prefer the project where you have one: it is the accession GEO records
and the one every cross-archive relation in this project is keyed on. The study
route matters when the study is what you were handed — from an ArrayExpress IDF,
or from a paper's data-availability statement, which quotes secondary study
accessions far more often than project accessions.
