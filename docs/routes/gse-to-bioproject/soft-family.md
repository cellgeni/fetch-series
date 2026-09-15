---
title: GEO series to BioProject, via the SOFT family file
---

# GEO series to BioProject, via the SOFT family file

**Route id:** `gse->bioproject:soft_family`
**Provider:** GEO FTP
**Direction:** `GSE` → `PRJNA`

The hinge of the whole GEO side. ENA's `filereport` answers a GSE with a 400 —
GEO accessions are not INSDC accessions — so every route from a GEO series into
ENA goes through the BioProject the series names, and this is where that name
comes from.

## The request

```bash
curl -s 'https://ftp.ncbi.nlm.nih.gov/geo/series/GSE236nnn/GSE236084/soft/GSE236084_family.soft.gz' \
  | gzip -dc | grep '^!Series_relation'
```

```
!Series_relation = BioProject: https://www.ncbi.nlm.nih.gov/bioproject/PRJNA988806
!Series_relation = SRA: https://www.ncbi.nlm.nih.gov/sra?term=SRP446371
```

GEO buckets its FTP tree by masking the last three digits of the accession, so
`GSE236084` lives under `GSE236nnn`. Series with three digits or fewer sit in
`GSEnnn`.

## Measured

**Corpus:** `reprocessed-gse` — all 13,045 GEO series the reprocessing pipeline
has had to resolve. **Surveyed:** 2026-09-15.

| | |
|---|---|
| Resolved | 12,572 |
| Named no BioProject | 473 |
| Failed | 7 |

**Exactly one BioProject per series, every time.** Not one of the 12,572 named
two. That is worth stating because the opposite direction is emphatically not
one-to-one: a single BioProject can hold several GEO series — PRJNA1169288 holds
three — so the graph is a many-to-one, and code that assumes symmetry is wrong
in one direction only.

The 473 that name none are the population behind
[series under a shared umbrella BioProject](../../pathologies/series-under-shared-umbrella-bioproject.md):
3.6% of the corpus, for which the BioProject route cannot be the entry into ENA
and the experiment-level route has to be.

The 7 failures are series whose SOFT family file could not be fetched at all;
GSE207991 is among them and is private on GEO, which is the correct answer
rather than a transport problem.

## Compared with `esummary db=gds`

NCBI indexes the same relation through `esummary`, which is one request against
an API rather than a gzip download from an FTP mirror. That census is running;
until it lands this page will not claim which is better, and
[the governing rule](../../index.md) keeps the unmeasured route ranked behind
this one.
