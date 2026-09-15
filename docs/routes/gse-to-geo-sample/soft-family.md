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

## Why this is the reference, not `esummary`

`esummary db=gds` indexes the same membership through an API rather than a gzip
download. The comparison census is running; until it lands this page will not
claim which is better, and [the governing rule](../../index.md) keeps the
unmeasured route ranked behind this one.

What is already clear is that 319,023 samples across 13,038 series is **24.5 per
series** on average, so the per-series cost of this route is one request for
what would otherwise be a paged query — and paging is where
[three of this project's five request-limit defects](../../pathologies/index.md)
came from.
