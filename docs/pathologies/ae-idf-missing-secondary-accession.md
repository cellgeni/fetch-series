---
title: An ArrayExpress IDF does not declare the ENA study it has
---

# An ArrayExpress IDF does not declare the ENA study it has

**Archive:** EBI ArrayExpress / BioStudies
**Affected route:** [`ae_experiment->study:idf_secondary`](../routes/ae-experiment-to-study/index.md)
**Status:** measured; partly worked around by a fallback; reproduced 2026-10-23
**Measured:** 2026-09-15, every ArrayExpress study BioStudies records an ENA link for

## The number

**551 of 20,693 studies (2.7%)**. That is the number of studies, among every ArrayExpress
study whose BioStudies record carries an ENA link, whose IDF declares no
`Comment[SecondaryAccession]`. The IDF is the field that names the ENA study. The link
exists, and the file that is supposed to state it does not.

## Reproducer

```bash
curl -s https://www.ebi.ac.uk/biostudies/files/E-MTAB-6505/E-MTAB-6505.idf.txt | grep -ci 'SecondaryAccession'
# 0
# while its SDRF's Comment[BioSD_SAMPLE] SAMEA5053920 resolves, through ENA, to PRJEB29431 / ERP111731
```

Checked on 2026-10-23.

## What it is not

- **Not a missing study.** The ENA study exists, and BioStudies links to it. Only the IDF
  is silent.
- **Not the 5 IDFs that answer 404.** Those studies register no files at all, and are
  counted as failures, not empties.

## Workaround

Fall back through the SDRF's BioSamples:

1. `ae_experiment->biosample:sdrf`;
2. then `biosample->run:ena_filereport`.

`fetch_series.relations` walks this composition for any ArrayExpress entry point. Measured
over exactly the 551 (corpus `arrayexpress-no-secondary`), the fallback recovers **4.2%**.
The route page has the full table.

## Affected accessions

The 551 are the committed corpus `data/corpora/arrayexpress-no-secondary/`, and the EMPTY
verdicts of `ae_experiment->study:idf_secondary` in `data/surveys/arrayexpress-ena/`.

## Regression test

`tests/pathologies/test_ae_idf_missing_secondary_accession.py` asserts that E-MTAB-6505's IDF
still declares no secondary accession.
