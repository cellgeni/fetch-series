---
title: Sample to run, via the ENA portal
---

# Sample to run, via the ENA portal

**Route id:** `sample->run:ena_filereport`
**Provider:** EBI ENA portal
**Direction:** `SRS`/`ERS`/`DRS` → `SRR`/`ERR`/`DRR`

```bash
curl -s 'https://www.ebi.ac.uk/ena/portal/api/filereport?accession=SRS18229461&result=read_run&fields=run_accession&format=tsv&limit=0'
```

The step a reprocessing pipeline actually needs: given a sample, which runs carry
its reads. One sample routinely spans several runs — sequenced across lanes, or
topped up in a second run — and a pipeline that takes only the first produces a
matrix from part of the library.

## Measured

**Corpus:** `sample:3000@reprocessed-srs`, a seeded draw from the 103,440 INSDC
samples in the reprocessed table. **Surveyed:** 2026-09-15.

| | |
|---|---|
| Resolved | 2,987 |
| Empty | 13 |
| Failed | 0 |
| Unique runs | **8,359** |

**2.8 runs per sample.** That is the number that makes this route matter: for
most samples in this corpus, taking one run takes roughly a third of the data.

The 13 empties are the same class as the
[23 missing runs](../run-to-experiment/ena-filereport.md#the-23-empties-are-runs-that-no-longer-exist) —
accessions that were in the reprocessed table and are no longer resolvable.

## Secondary or primary?

`SRS`-style accessions are *secondary* sample accessions; `SAMN`/`SAMEA`/`SAMD`
BioSamples are primary. ENA's `filereport` accepts both, and this project keeps
them as separate entity types (`sample` and `biosample`) with separate routes,
because the archives do not treat them as interchangeable: GEO records the
BioSample and not the SRS, and
[ENA under-reports NCBI-registered BioSamples relative to ELink](../bioproject-to-biosample/index.md)
in one direction while finding 125,376 more in the other.
