---
title: BioSample to run, via the ENA portal
---

# BioSample to run, via the ENA portal

**Route id:** `biosample->run:ena_filereport`
**Provider:** EBI ENA portal
**Direction:** `SAMN`/`SAMEA`/`SAMD` → `SRR`/`ERR`/`DRR`

```bash
curl -s 'https://www.ebi.ac.uk/ena/portal/api/filereport?accession=SAMN36028297&result=read_run&fields=run_accession&format=tsv&limit=0'
```

The route that makes the BioSample a *usable* join key rather than only a shared
label. Two of this project's recoveries end here:

- [GEO omitting the SRA relation](../../pathologies/geo-omits-sample-sra-relation.md) —
  GSE135325 and GSE137444 record a BioSample per sample and nothing else, so the
  BioSample is the only way from a GSM to its reads.
- [ArrayExpress studies with no secondary accession](../ae-experiment-to-study/index.md) —
  E-MTAB-6505 is reachable only through the BioSamples its SDRF names.

## Measured

**Corpus:** a seeded 3,000-BioSample draw from the 363,885 that the
[BioProject → BioSample](../bioproject-to-biosample/index.md) census returned.
**Surveyed:** 2026-09-15.

| | |
|---|---|
| Resolved | **3,000 (100%)** |
| Empty | 0 |
| Failed | 0 |
| Unique runs | 4,827 |

**1.6 runs per BioSample**, and nothing empty. That total is not a surprise and
should not be read as a claim about BioSamples in general: the corpus was built
from what `result=read_run` returned, so every accession in it is one ENA
already associates with at least one run. It measures the round trip, not
coverage.

The honest coverage claim is the one on the
[BioProject → BioSample](../bioproject-to-biosample/index.md) page, where the
two routes disagree by 125,376 samples and the disagreement is explained by
which archive registered the project.
