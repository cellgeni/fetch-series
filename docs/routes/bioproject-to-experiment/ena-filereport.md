---
title: BioProject to experiment, via the ENA portal
---

# BioProject to experiment, via the ENA portal

**Route id:** `bioproject->experiment:ena_filereport`
**Provider:** EBI ENA portal
**Direction:** `PRJNA`/`PRJEB`/`PRJDB` → `SRX`/`ERX`/`DRX`

## The request

```bash
curl -s 'https://www.ebi.ac.uk/ena/portal/api/filereport?accession=PRJNA988806&result=read_run&fields=experiment_accession&format=tsv&limit=0'
```

`limit=0` means no limit. Omitting it returns a truncated report and does not
say so.

## Measured

**Corpus:** `reprocessed-prj` — all 12,755 BioProjects the reprocessing pipeline
has actually had to resolve. **Surveyed:** 2026-09-15.

| | |
|---|---|
| Resolved | 12,732 |
| Empty | 23 |
| Failed | **0** |
| Unique experiments | **481,315** |

Nothing failed. The 23 empties are projects ENA holds no runs for at all — the
same population the [BioProject to run](../bioproject-to-run/index.md) census
found empty, not a separate defect of this direction.

## Why it is the route to beat

One request per project, returning every experiment in the project, from the
same endpoint that answers the run, study, sample and BioSample directions. The
`result=read_run` rows **are** runs, so an experiment returned here is an
experiment that carries data — the `proves_data_exists` property that separates
this route from the GEO-side ones, where
[1.23% of the experiments SOFT names carry no runs at all](../../pathologies/soft-names-experiments-with-no-runs.md).

## Archive coverage

ENA indexes EBI- and DDBJ-registered projects as well as NCBI's, which is not
symmetric: NCBI's own routes resolve 99.3% of PRJNA and 1.4% of PRJEB. See
[routing by archive](../../graph/routing-by-archive.md).
