---
title: ArrayExpress experiment to BioSample, via the SDRF
---

# ArrayExpress experiment to BioSample, via the SDRF

**Route id:** `ae_experiment->biosample:sdrf`
**Provider:** EBI BioStudies
**Direction:** `E-MTAB`/`E-GEOD` → `SAMEA`/`SAMN`/`SAMD`

```bash
curl -sL 'https://www.ebi.ac.uk/biostudies/files/E-MTAB-6505/E-MTAB-6505.sdrf.txt' \
  | head -1 | tr '\t' '\n' | grep -n 'BioSD_SAMPLE'
```

The `Comment[BioSD_SAMPLE]` column of the SDRF. It matters mostly as the
**fallback** for a study whose IDF declares no secondary accession —
E-MTAB-6505 is reachable only this way — and as the sample-level link for any
ArrayExpress entry point.

## Measured

**Corpus:** `sample:3000@arrayexpress-ena`, a seeded draw from the 20,693
studies BioStudies records an ENA link for. **Surveyed:** 2026-09-15; the nine
studies whose SDRF is [split across two files](../../pathologies/ae-sdrf-split-across-files.md)
re-asked on 2026-10-27, once the route read both. All nine are `E-GEOD` and now answer empty.

| | |
|---|---|
| Resolved | 1,221 (40.7%) |
| Empty | 1,777 |
| Failed | 2 |
| Unique BioSamples | 63,153 |

A 41% rate looks poor until it is split by where the study came from:

| Prefix | What it is | Studies | Names BioSamples | |
|---|---|---:|---:|---:|
| `E-MTAB` | native ArrayExpress submissions | 1,398 | 1,210 | **87%** |
| `E-GEOD` | ArrayExpress's imports of GEO series | 1,550 | **0** | **0%** |
| `E-ERAD` | the Sanger ERAD collection | 48 | 11 | 23% |

**Not one E-GEOD study carries the column.** Adding the 450 E-GEOD studies in
the [`arrayexpress-no-secondary`](../ae-experiment-to-study/index.md) corpus
makes that **0 of 2,000**. It is not a sampling artefact and not a coverage gap:
an ArrayExpress import of a GEO series has a GEO-derived SDRF, and GEO-derived
SDRFs have no BioSample column at all.

## What follows from that

**The route is for native ArrayExpress studies.** For an `E-GEOD` accession the
answer is not here and never will be — go to GEO, where `GSE63923` (the series
behind `E-GEOD-63923`) resolves through
[the SOFT family file](../gse-to-geo-sample/soft-family.md) at 99.9%.

**Where it applies, the chain it feeds is complete.** Over the 23 studies in
`arrayexpress-no-secondary` it found BioSamples for, the 756 BioSamples it named all resolved:
756 of 756 to 1,547 runs through
[BioSample → run](../biosample-to-run/ena-filereport.md), with nothing empty and
nothing failed. 21 of those 23 turned out to declare their study in the IDF after all, in a cell
the IDF reader did not read until 2026-10-28. As a fallback for an IDF that names no study, the
route adds 2 studies of 10 ([the route page](../ae-experiment-to-study/index.md#how-much-the-fallback-recovers-2-of-10)).

## Parsing

The SDRF is read as raw field lists, not row dicts. SDRF repeats column names by
design and `csv.DictReader` keeps only the last value for a repeated key — the
defect that halved E-MTAB-9221's file list. `Comment[BioSD_SAMPLE]` does not
usually repeat, but the file it sits in does, so the same reader is used
throughout.
