---
title: Series under a shared umbrella BioProject record no BioProject of their own
---

# Series under a shared umbrella BioProject record no BioProject of their own

**Status:** open, not a defect — a modelling mismatch
**Found:** 2026-09-14 **Archives:** NCBI (GEO, BioProject) **Severity:** breaks project-based routing

## Symptom

A GEO series names SRA experiments per sample but carries no
`!Series_relation = BioProject:` line. Any route that goes GEO → BioProject → data
returns nothing, while the series plainly has sequencing data.

## Affected accessions

Four of the six such series found in the 2026-09-14 survey share one project:

| GEO series | Samples | BioProject in SOFT | BioProject reached via the experiment |
|---|---|---|---|
| GSE78298 | 2 | none | PRJNA30709 |
| GSE78299 | 2 | none | PRJNA30709 |
| GSE78395 | 2 | none | PRJNA30709 |
| GSE78416 | 2 | none | PRJNA30709 |
| GSE122357 | 3 | none | PRJNA495666 |
| GSE130731 | 4 | none | PRJNA541018 |

## Why it happens

`PRJNA30709` is a low-numbered, long-lived umbrella project. A GEO series whose samples
were deposited under someone else's umbrella has no project of its own to point at, so
GEO records nothing rather than recording a project the series does not own.

That is defensible on GEO's side, and it is why this page is filed as a modelling
mismatch rather than a bug. It is still worth documenting, because it silently breaks a
routing assumption that otherwise holds: *a series with sequencing data has a BioProject*.

## Reproducer

```bash
curl -s "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE78nnn/GSE78298/soft/GSE78298_family.soft.gz" \
  | gunzip | grep '!Series_relation'
# -> nothing

curl -s "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE78nnn/GSE78298/soft/GSE78298_family.soft.gz" \
  | gunzip | grep -oE 'SRX[0-9]+' | sort -u
# -> SRX1602977, SRX1602978

curl -s "https://www.ebi.ac.uk/ena/portal/api/filereport?accession=SRX1602977&result=read_run&fields=study_accession&format=tsv&limit=0"
# -> PRJNA30709
```

## Censused: 466 series, 41% of them under a true umbrella

Both routes have now been run over all 13,045 series of `reprocessed-gse`
(2026-09-15). The SOFT family file leaves **473** empty; `esummary db=gds`
resolves **466** of them, and the two never disagree where both answer.

So the BioProject is not missing from NCBI — only from the submitter's own
record. But recovering it does not make it useful:

| Project | Series it "resolves" | Runs it holds |
|---|---:|---:|
| `PRJNA30709` — Production ENCODE transcriptome data | 138 | **7,591** |
| `PRJNA66167` | 52 | **5,910** |
| 257 others | 1 each | specific to the series |

**190 of the 466 — 41% — resolve to a species- or consortium-level umbrella.**
Routing a two-sample series through `PRJNA30709` returns 7,591 runs and reports
success. For the other 276 the recovered project is genuine and specific, and
the SOFT file simply omits it.

This is why the pathology is worth a page even though GEO's behaviour is
defensible: the obvious fix — ask NCBI's index instead — makes the routing
failure *louder*, not quieter, for two fifths of the affected series.

## Workaround

Do not treat a missing BioProject as a missing link, and do not treat a
recovered one as an answer without checking its size. A project holding orders
of magnitude more runs than the series has samples is an umbrella.

The sample-level SRA relations in the SOFT file already answer the question, and
the project can be recovered from any one experiment through ENA if it is
genuinely needed.

The general rule this case argues for: **resolve upward from the smallest identifier you
already hold**, rather than insisting on the project as an intermediate hop.
