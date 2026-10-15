---
title: GEO series to GEO sample, via esummary db=gds
---

# GEO series to GEO sample, via `esummary db=gds`

**Route id:** `gse->geo_sample:gds_summary`
**Provider:** NCBI E-utilities
**Direction:** `GSE` → `GSM`

```bash
curl -s 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=gds&id=200236084&retmode=json' \
  | python3 -c 'import json,sys; print([s["accession"] for s in json.load(sys.stdin)["result"]["200236084"]["samples"]])'
```

NCBI's index of which samples belong to a series, as against
[the SOFT family file](soft-family.md), which is the submitter's own record.

## Measured

**Corpus:** `reprocessed-gse`, all 13,045 series. **Surveyed:** 2026-09-15.

| | |
|---|---|
| Resolved | 13,038 |
| Empty | 7 |
| Failed | **0** |
| Unique GEO samples | **319,023** |

## The two surfaces agree exactly

| | `soft_family` | `gds_summary` |
|---|---:|---:|
| Unique samples | 319,023 | **319,023** |
| Series where the membership lists differ | **0** | **0** |
| Failed | 7 | 0 |

**Sample for sample, the same 319,023.** Not one series where GEO's own record
and NCBI's index of it list different members. The only difference is the 7
series whose SOFT file cannot be fetched at all — GSE207991 is private — where
this route returns empty rather than failing.

That agreement is the exception, not the rule. The same two surfaces asked
[which BioProject a series belongs to](../gse-to-bioproject/gds-summary.md)
disagree on 466 series, and 41% of the extra answers there are umbrella projects
that must not be used for routing. Membership is one of the few facts the two
agree about completely — which is a reason to trust either, and a reason not to
generalise from it to anything else `esummary` reports.

## Which to use

**This one, for a single series where membership is the only question.** One API
request against a multi-megabyte gzip download, and it never fails.

**The SOFT file, for anything else about the samples** — the SRA relations, the
BioProject, the per-sample protocol text the [assay layer](../../assays.md)
reads. All of it comes out of the same download, and at 24.5 samples per series
the alternative is 24.5 further requests.
