---
title: Run to BioSample, via the ENA portal
---

# Run to BioSample, via the ENA portal

**Route id:** `run->biosample:ena_filereport`
**Provider:** EBI ENA portal
**Direction:** `SRR`/`ERR`/`DRR` → `SAMN`/`SAMEA`/`SAMD`

```bash
curl -s 'https://www.ebi.ac.uk/ena/portal/api/filereport?accession=SRR25056225&result=read_run&fields=sample_accession&format=tsv'
```

The BioSample is the identity that crosses archives. GEO knows a sample as a
GSM, SRA knows it as an SRS, and the BioSample is the one both record — which is
what makes it the join key when GEO omits the SRA relation entirely
([the recovery](../../pathologies/geo-omits-sample-sra-relation.md) that
attributes 36 of 36 runs for GSE135325).

## Measured

**Corpus:** `sample:3000@reprocessed-srr`. **Surveyed:** 2026-09-15.

| | |
|---|---|
| Resolved | 2,977 |
| Empty | 23 |
| Failed | 0 |
| Unique BioSamples | 2,880 |

Identical resolution to [run → experiment](../run-to-experiment/ena-filereport.md),
and for the same reason: both come from one `filereport` row, so a run either has
a row or it does not. The 23 empties are exactly the same runs, and they are
[absent from both archives now](../run-to-experiment/ena-filereport.md#the-23-empties-are-runs-that-no-longer-exist).

2,880 BioSamples for 2,977 runs — slightly more sharing than at the experiment
level (2,888), which is what you would expect: one biological sample can carry
several libraries.

## One request, several answers

`run_accession`, `experiment_accession`, `sample_accession`,
`secondary_sample_accession`, `study_accession` and `secondary_study_accession`
all come from the same row. Declaring them as separate routes is about being
able to rank and measure each edge separately; it is not about issuing separate
requests. `fetch relations` fetches the row once and fills every field from it.
