---
title: GEO omits !Sample_relation = SRA: for series that have SRA data
---

# GEO omits `!Sample_relation = SRA:` for series that have SRA data

**Status:** open, unreported
**Found:** documented in `fetch10xmeta` before 2026-09; re-measured 2026-09-14
**Archive:** NCBI (GEO) **Severity:** silent data loss

## Symptom

A SOFT family file lists samples that declare a BioSample relation and nothing else, even
though the samples have SRA experiments. Reading the SOFT file alone reports the series as
having no sequencing data.

## Prevalence

!!! warning "This section corrects an earlier figure"
    An earlier version of this page reported **16 in 13,045 series (0.12%)**, from a
    census of `gse->experiment:soft_family`. That number is real but it measures only
    *total* omission — series where **no** sample has an SRA relation, so the route
    returns empty. A series where 37 of 50 samples lack the relation still resolves 13
    experiments and looks like an ordinary success. **The census could not see partial
    omission at all, and partial omission is the common form.**

Measured properly on 500 series drawn in visit order from the reprocessed corpus,
2026-09-15, by comparing each series' sample list against the samples that carry a
relation:

| | Series | Share |
|---|---|---|
| Every sample linked | 493 | 98.6% |
| **Partially missing** | **6** | **1.2%** |
| No sample linked | 1 | 0.2% |
| **Affected at all** | **7** | **1.4%** |

The old figure corresponds to the 0.2% row. **Counting partial omission raises the
affected-series rate about sevenfold.**

### At the sample level, and why the headline number misleads

1,592 of 17,585 samples (9.05%) carry no SRA relation — but that figure is almost entirely
one series:

| | Samples affected | Rate |
|---|---|---|
| All 500 series | 1,592 / 17,585 | 9.05% |
| `GSE137870` alone | 1,534 | **96.4% of all affected samples** |
| Every other series | 58 / 14,494 | **0.40%** |

`GSE137870` has 3,091 samples and omits the relation for 1,534 of them. Quoting 9% as "the
rate" would attribute one series' problem to the archive as a whole.

**0.40% is the number to use** for "will a given sample resolve", and it is corroborated
independently: a separate 1,000-sample run of
[`geo_sample->experiment:acc_cgi`](../routes/geo-sample-to-experiment/index.md) found 4
samples with no relation — 0.4%.

### Worst partial cases found

| Series | Samples | Linked | Missing |
|---|---|---|---|
| GSE137870 | 3,091 | 1,557 | 1,534 (50%) |
| GSE197518 | 20 | 8 | 12 (60%) |
| GSE143151 | 39 | 27 | 12 (31%) |
| GSE216189 | 34 | 28 | 6 (18%) |
| GSE139388 | 7 | 3 | 4 (57%) |
| GSE165371 | 141 | 139 | 2 (1%) |

## Affected accessions, total omission

The 16 series where **no** sample carries a relation, from the full 13,045-series census.
Every one names a BioProject and has samples; none names a single SRA relation.

| Series | Samples | Samples with a BioSample relation | Recovered via BioProject → ENA |
|---|---|---|---|
| GSE135325 | 6 | 6 | 6 experiments |
| GSE137444 | 12 | 12 | 12 |
| GSE175516 | 2 | 2 | 2 |
| GSE175533 | 147 | 147 | 147 |
| GSE178485 | 4 | 4 | 4 |
| GSE203552 | 22 | 22 | 15 |
| GSE223155 | 5 | 5 | 5 |
| GSE270158 | 6 | 6 | 5 |
| GSE272976 | 16 | 16 | 4 |
| GSE208337 | 105 | 0 | 105 |
| GSE296731 | 10 | 0 | 10 |
| GSE156524 | 3 | 0 | none |
| GSE264624 | 26 | 0 | none |
| GSE275199 | 2 | 0 | none |
| GSE307587 | 20 | 0 | none |
| GSE308007 | 31 | 0 | none |

Two shapes. Nine series record a BioSample relation per sample and omit only the SRA half;
seven record no sample relations at all.

The five that recover nothing return nothing from ELink either, and their BioProjects have
no released runs — so they are most likely awaiting release rather than mis-linked. They
are listed because a resolver cannot tell the two cases apart from the outside, and "no
data yet" is a different answer from "I could not find it".

## Reproducer

```bash
curl -s "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE135nnn/GSE135325/soft/GSE135325_family.soft.gz" \
  | gunzip | grep '!Sample_relation'
# -> only BioSample: lines, no SRA: lines

fetch resolve GSE135325 --to experiment --explain
# -> gse->experiment:soft_family is empty; gse->experiment:elink_gds_sra returns 6
```

## Why it matters

This is the exact mirror image of
[ELink returning no links](elink-gds-sra-missing-links.md). The two routes fail on
**disjoint** populations:

| | SOFT family | ELink `gds`→`sra` |
|---|---|---|
| GSE135325, GSE137444 | empty | resolves |
| GSE27679 and 5 others | resolves | empty |
| GSE206528 | resolves (8) | empty |
| GSE203201 | resolves (15) | resolves (12) |

There is no ordering of these two routes that answers all of them, which is why the resolver
merges rather than falls through, and records per-accession provenance so a disagreement is
visible rather than arbitrated silently.

## Workaround

Query ELink `gds`→`sra` as well, and take the union.

`fetch10xmeta` instead reconstructs the relations downstream, by matching accession *shape*
rather than column position across the SRA and ENA metadata tables. That works, and it has
the advantage of needing no extra request — but it can only recover samples that are already
in a metadata table it fetched for another reason.
