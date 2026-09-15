---
title: Experiment to BioSample, via the ENA portal
---

# Experiment to BioSample, via the ENA portal

**Route id:** `experiment->biosample:ena_filereport`
**Provider:** EBI ENA portal
**Direction:** `SRX`/`ERX`/`DRX` → `SAMN`/`SAMEA`/`SAMD`

```bash
curl -s 'https://www.ebi.ac.uk/ena/portal/api/filereport?accession=SRX21131063&result=read_run&fields=sample_accession&format=tsv&limit=0'
```

The experiment is the accession GEO names — `!Sample_relation = SRA:` points at
an `SRX` — so this is the step that turns GEO's own record into an identity ENA
and NCBI both recognise.

## Measured

**Corpus:** `sample:3000@reprocessed-srx`, a seeded draw from the 114,715
experiments in the reprocessed table. **Surveyed:** 2026-09-15.

| | |
|---|---|
| Resolved | 2,959 |
| Empty | **41** |
| Failed | 0 |
| Unique BioSamples | 2,893 |

**41 empty is 1.37%**, and it is the same population as
[the experiments SOFT names that carry no runs](../../pathologies/soft-names-experiments-with-no-runs.md),
measured directly at 1.23% (95% CI 0.84–1.63%) on a different draw. The two
agree inside that interval, which is the expected relationship: an experiment
with no runs has no `result=read_run` row, so it returns empty here for exactly
the same reason it returns no runs there.

That makes this route a **cheap detector for the pathology**: one request says
whether an experiment GEO names is one that carries data.

## Runs, experiments and samples do not line up one-to-one

2,959 experiments resolve to 2,893 BioSamples — a little sharing, where one
biological sample was prepared into two libraries (CITE-seq alongside gene
expression is the common case, and `SRS9161836` in the tier-one corpus is the
worked example). Going the other way, the
[sample → run](../sample-to-run/ena-filereport.md) census found 2.8 runs per
sample. Nothing in this hierarchy is a bijection, in either direction.
