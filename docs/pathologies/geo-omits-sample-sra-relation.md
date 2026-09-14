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

Measured by census, not by sample: every one of the 13,045 GEO series in the reprocessed
table, surveyed 2026-09-14.

| Outcome of `gse->experiment:soft_family` | Series | Share |
|---|---|---|
| Experiments found | 13,022 | 99.82% |
| **No SRA relation on any sample** | **16** | **0.12%** |
| Fetch failed (404 — private or withdrawn) | 7 | 0.05% |

**This is much rarer than the curated corpus suggests**, and that gap is the point. On
`hard-cases` this pathology looks like 2 series in 7; across the real population it is 16
in 13,045. A corpus selected for pathology cannot measure prevalence — only a census can.
Both numbers are true and they answer different questions: *can this happen* versus *how
often does it*.

## Affected accessions

All 16, with what the SOFT file does record. Every one names a BioProject and has samples;
none names a single SRA relation.

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
are listed here because a resolver cannot tell the two cases apart from the outside, and
saying "no data yet" is a different answer from "I could not find it".

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
