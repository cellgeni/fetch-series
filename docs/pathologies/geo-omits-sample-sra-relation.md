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

## Affected accessions

| GEO series | SOFT experiments | ELink `gds`→`sra` experiments |
|---|---|---|
| GSE135325 | 0 | 6 |
| GSE137444 | 0 | 12 |

Measured 2026-09-14 on `hard-cases`. Both series also mix library types — GSE135325 lists
four BD AbSeq samples next to its two 10x ones — so a resolver that drops them silently
loses part of a series rather than all of it.

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
