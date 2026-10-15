---
title: GEO series to BioProject, via esummary db=gds
---

# GEO series to BioProject, via `esummary db=gds`

**Route id:** `gse->bioproject:gds_summary`
**Provider:** NCBI E-utilities
**Direction:** `GSE` → `PRJNA`

```bash
curl -s 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=gds&id=200236084&retmode=json' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["result"]["200236084"]["bioproject"])'
```

NCBI's *index* of a GEO series, as against
[the SOFT family file](soft-family.md), which is the submitter's own record. The
two are not the same thing, and this census is the clearest demonstration in the
project of how they differ.

## Measured

**Corpus:** `reprocessed-gse`, all 13,045 series. **Surveyed:** 2026-09-15.

| | `soft_family` | `gds_summary` |
|---|---:|---:|
| Resolved | 12,572 | **13,038** |
| Empty | 466 | 7 |
| **Failed** | 7 | **0** |
| Unique projects | 12,572 | 12,830 |

Head to head over the same 13,045 series:

| | |
|---|---:|
| Agree exactly | 12,572 |
| Only `gds_summary` found anything | **466** |
| Only `soft_family` found anything | **0** |
| SOFT failed where gds returned empty | 7 |

**Where both answer they never disagree**, and `gds_summary` answers 466 times
more, for one API request instead of a multi-megabyte gzip download. On coverage
and cost it strictly dominates.

## And it still must not be ranked first

The 466 extra answers map to 259 distinct BioProjects. Two of those 259 are
species- or consortium-level umbrellas:

| Project | Series it "resolves" | Runs it actually holds |
|---|---:|---:|
| `PRJNA30709` — Production ENCODE transcriptome data | 138 | **7,591** |
| `PRJNA66167` | 52 | **5,910** |

**190 of the 466 — 41% — resolve to an umbrella.** Routing a series through one
does not recover its data; it returns thousands of unrelated runs and reports
success. A GEO series with six samples would resolve to 7,591 runs, and nothing
in the response says anything is wrong.

The remaining 276 map to projects holding exactly one of these series each, and
for those the extra coverage is genuine: the BioProject exists and is specific,
and only the submitter's SOFT file omits it.

## What to do with that

Take `gds_summary`'s answer, and **check the size of what it gives you** before
routing through it. A project holding orders of magnitude more runs than the
series has samples is an umbrella, not an answer. `fetch relations` already
filters project-level results to the experiments GEO attributes to the series,
which is the same defence arrived at from the other direction — see
[shared umbrella BioProject](../../pathologies/series-under-shared-umbrella-bioproject.md).

This is the sharpest case in the project against ranking routes on coverage
alone. `gds_summary` wins every headline number in the table above and is still
the more dangerous route to trust blindly.
