---
title: GEO series to GEO sample, via the SOFT family file
---

# GEO series to GEO sample, via the SOFT family file

**Route id:** `gse->geo_sample:soft_family`
**Provider:** GEO FTP
**Direction:** `GSE` → `GSM`

The membership question: which samples belong to this series. It is the one
thing the INSDC side cannot answer at all — a GSM is a GEO identity, and ENA has
never heard of it.

```bash
curl -s 'https://ftp.ncbi.nlm.nih.gov/geo/series/GSE236nnn/GSE236084/soft/GSE236084_family.soft.gz' \
  | gzip -dc | grep '^\^SAMPLE'
```

One request per series, returning every sample in it, from the same file that
carries the [BioProject relation](../gse-to-bioproject/soft-family.md) and the
per-sample SRA relations. The whole GEO half of the
[relation table](../../relations.md) comes out of this single download.

## Measured

**Corpus:** `reprocessed-gse`, all 13,045 GEO series the reprocessing pipeline
has had to resolve. **Surveyed:** 2026-09-15.

| | |
|---|---|
| Resolved | **13,038** |
| Empty | **0** |
| Failed | 7 |
| Unique GEO samples | **319,023** |

**Not one series that could be fetched named zero samples.** That is worth
stating because it is the exception in this project: every other census here has
an empty column with something in it. A series is defined by its samples, and
GEO's own record of them is complete.

The 7 failures are the same series whose SOFT family file cannot be fetched at
all — GSE207991 among them, which is private on GEO, so the failure is the
correct answer rather than a transport problem.

## Compared with `esummary db=gds`: identical

Both routes over all 13,045 series (2026-09-15):

| | `soft_family` | `gds_summary` |
|---|---:|---:|
| Resolved | 13,038 | 13,038 |
| Empty | 0 | 7 |
| Failed | 7 | **0** |
| Unique GEO samples | 319,023 | **319,023** |
| Agree exactly | 13,038 | 13,038 |
| Disagree on membership | **0** | **0** |

**Sample for sample, the same 319,023.** Not one series where the two surfaces
list different members. The only difference is the 7 series whose SOFT file
cannot be fetched at all, where `gds_summary` returns empty rather than failing.

That is worth stating because it is the exception. The same two surfaces, asked
[which BioProject a series belongs to](../gse-to-bioproject/gds-summary.md),
disagree on 466 series — and 41% of the extra answers are umbrella projects that
must not be used. Membership is one of the few things GEO's own record and
NCBI's index of it agree about completely.

**Prefer `gds_summary` when a single series is the question**: one API call
against a multi-megabyte gzip download, and it never fails. Prefer `soft_family`
when anything *else* about the samples is needed — the SRA relations, the
BioProject, the per-sample protocol text the [assay layer](../../assays.md)
reads — because all of it comes out of the same download, and 319,023 samples
across 13,038 series is **24.5 per series**, so the alternative is 24.5 further
requests.
