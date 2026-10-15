---
title: SOFT files name experiment accessions that carry no data
---

# SOFT files name experiment accessions that carry no data

**Status:** open, unreported
**Found:** 2026-09-15 **Archives:** NCBI (GEO, SRA) **Severity:** resolves to zero files, silently

> The most dangerous pathology found so far, because it afflicts the route that is
> otherwise the best one. A caller that trusts the SOFT family file alone can come away
> with a complete-looking list of experiment accessions that have **no runs at all**.

## Symptom

A GEO series' SOFT family file names experiment accessions in
`!Sample_relation = SRA:`. The accessions exist in NCBI's `db=sra` — they are not typos
and they are not withdrawn — but they carry no runs, so nothing can be downloaded.
Meanwhile `elink gds→sra` names a *different, disjoint* set of experiments for the same
series, and those do carry runs.

Nothing in either answer signals which is which. Both look like ordinary successes.

## Affected accessions

Found by comparing the two routes across all 13,045 series in `reprocessed-gse`. Three
series return sets of equal size with **zero overlap**:

| Series | SOFT names | ELink names | Overlap |
|---|---|---|---|
| GSE114373 | 25 experiments | 25 experiments | 0 |
| GSE150508 | 1 experiment | 1 experiment | 0 |
| GSE164404 | 1 experiment | 1 experiment | 0 |

Verified for the first two:

| Series | Accession | Source | In NCBI `db=sra` | Runs in ENA |
|---|---|---|---|---|
| GSE114373 | SRX10584613 | SOFT | yes | **0** |
| GSE114373 | SRX10647230 | ELink | yes | 2 |
| GSE150508 | SRX7571191 | SOFT | yes | **0** |
| GSE150508 | SRX9670669 | ELink | yes | 2 |

In both cases the ELink experiments belong to the same BioProject the SOFT file itself
records — `PRJNA471193` and `PRJNA632602` respectively. So GEO knows the right project and
still points at the wrong experiments within it.

## Prevalence

Measured directly, 2026-09-15: 3,000 experiments drawn in visit order from **all 308,639**
that the SOFT census returned, each asked for its runs through ENA.

| | Value |
|---|---|
| Experiments with no runs | **37 / 3,000 = 1.23%** (95% CI 0.84–1.63%) |
| Extrapolated over all 308,639 | **~3,807** (CI 2,588–5,026) |
| Route failures | 0 |

An earlier figure of ~4,238 (1.37%) was reached indirectly, by sampling only the 23,546
experiments the BioProject→ENA route did not confirm and finding 18% of *those* dead. The
two agree within their confidence intervals, but **the direct measurement is the one to
quote**: it samples the whole population rather than a subset chosen for being suspicious,
so it needs no assumption about how the subset relates to the rest.

The corpus is reproducible:

```bash
fetch survey run --route "experiment->run:ena_filereport" \
  --corpus "results-of:gse->experiment:soft_family@reprocessed-gse" --limit 3000
```

### What "no runs" means here

It means ENA's `read_run` report returns nothing for the accession. A separate 40-experiment
check found none of the run-less accessions had runs at NCBI either, so these are dead
rather than un-mirrored — though that sub-question rests on the smaller sample.

## Diagnosis## Diagnosis

The submission was replaced. The original experiment records persist in SRA as metadata
shells with no runs attached, the replacements were registered under new accessions, and
Entrez's link table was updated while the GEO SOFT record was not.

Two further series, `GSE140021` and `GSE195844`, show a partial version of the same thing:
overlapping sets where each route holds accessions the other lacks (12 and 92 SOFT-only
respectively).

## Reproducer

```bash
# What GEO's own record says
curl -s "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE150nnn/GSE150508/soft/GSE150508_family.soft.gz" \
  | gunzip | grep -oE 'SRX[0-9]+' | sort -u
# -> SRX7571191

# It exists in SRA...
curl -s "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=sra&term=SRX7571191%5BACCN%5D&retmode=json" \
  | jq -r '.esearchresult.count'
# -> 1

# ...but has no runs.
curl -s "https://www.ebi.ac.uk/ena/portal/api/filereport?accession=SRX7571191&result=read_run&fields=run_accession&format=tsv&limit=0"
# -> header only

# ELink names a different experiment, which does have runs.
curl -s "https://www.ebi.ac.uk/ena/portal/api/filereport?accession=SRX9670669&result=read_run&fields=run_accession&format=tsv&limit=0"
# -> two runs
```

## Why this changes the recommendation

[The route comparison](../routes/gse-to-experiment/index.md) shows SOFT finding experiments
for 2,280 series that ELink cannot reach, against 10 the other way. On completeness alone
SOFT wins overwhelmingly, and that remains true.

But completeness is not correctness. **An experiment accession is not evidence that data
exists.** The resolver must verify that resolved experiments carry runs before reporting
them, and prefer the live set where two routes disagree.

This is cheap to do — one ENA filereport call already needed for the file layer — and it
converts a silent zero-file download into a diagnosable answer.

## Workaround

Resolve experiments by whichever route, then confirm each carries runs. Where SOFT and
ELink disagree and only one side has runs, take the side that does, and record the
divergence: a series whose SOFT record is stale is worth reporting to GEO.
