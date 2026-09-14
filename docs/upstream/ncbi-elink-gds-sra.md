---
title: NCBI — ELink gds→sra returns no links for series with SRA data
---

# NCBI — ELink `gds`→`sra` returns no links for series with SRA data

**Status:** draft, not filed **Archive:** NCBI Entrez
**Evidence:** [pathologies/elink-gds-sra-missing-links.md](../pathologies/elink-gds-sra-missing-links.md)
**Where to file:** `info@ncbi.nlm.nih.gov`, or the NCBI Help Desk

Regenerate the affected list before filing:

```bash
fetch survey run --route "gse->experiment:soft_family"    --corpus geo-sample --limit 150
fetch survey run --route "gse->experiment:elink_gds_sra"  --corpus geo-sample --limit 150
fetch survey compare --route "gse->experiment:soft_family" \
                     --route "gse->experiment:elink_gds_sra" --corpus geo-sample-1000
```

---

## Draft report

Subject: ELink `gds`→`sra` returns no links for GEO series that have SRA data

We resolve GEO series to their sequencing data programmatically, and have found that
`elink.fcgi?dbfrom=gds&db=sra` reports no SRA neighbours for a substantial fraction of GEO
series that demonstrably have SRA experiments.

This is measured by census rather than by sample. We queried all 13,045 GEO series that
have been through our reprocessing pipeline — series that are known to carry sequencing
data — by two routes: ELink, and the `!Sample_relation = SRA:` lines in each series' own
SOFT family file.

| | ELink `gds`→`sra` | SOFT family file |
|---|---|---|
| Series resolved to experiments | 10,752 (82.4%) | 13,022 (99.8%) |
| Series reported as having none | 2,293 (17.6%) | 16 (0.1%) |
| Unique experiments returned | 178,879 | 308,639 |

For **2,280 series**, ELink returned nothing while the SOFT file named experiments. ELink
returns about 58% of the experiments GEO's own records point to.

Taking GSE27679 as a small example:

- `esearch db=gds` resolves it to UID 200027679.
- `elink.fcgi?dbfrom=gds&db=sra&id=200027679&cmd=neighbor` returns no link records.
  Passing `linkname=gds_sra` explicitly makes no difference, and `cmd=neighbor_history`
  behaves the same way.
- The SOFT family file at
  `ftp.ncbi.nlm.nih.gov/geo/series/GSE27nnn/GSE27679/soft/GSE27679_family.soft.gz`
  names nine SRA experiments, beginning SRX046652.
- `esearch db=sra&term=SRX046652[ACCN]` returns count 1, so the experiment is in SRA.
- ENA lists seven runs for SRX046652 (SRR121517–SRR121523).

The affected series are not confined to any era: GSE106544 (2,396 samples, all with SRX
accessions in its SOFT file) returns no ELink neighbours, and so do series from 2011
through 2022. We specifically tested and ruled out an age effect — ELink succeeds for
GSE21739, older than every affected series in our first sample, and for GSE56066, newer
than all of them.

In every case we checked, the records exist on both sides and GEO's own metadata states
the relationship; the Entrez link table does not express it.

The response is an ordinary HTTP 200 with no error field, so callers cannot distinguish
"this series has no SRA data" from "the link is missing". In our census ELink produced
**zero** outright failures — its incompleteness is entirely silent. That is why we are
asking rather than quietly working around it.

Three questions:

1. Is the `gds`↔`sra` link table expected to be complete, and is there a known reason
   these series are absent from it?
2. Is there an Entrez query that recovers the relationship? Searching `db=sra` for
   `GSE27679[All Fields]` also returns 0, so we have not found one.
3. Would a list of the 2,280 affected accessions be useful to you? We can supply it, and
   regenerate it on request.

---

## Secondary observation, possibly related

Going the other way, we found three series where ELink and the SOFT file return experiment
sets of the **same size with zero overlap** (GSE114373, GSE150508, GSE164404). In each case
the SOFT-named experiments exist in `db=sra` but carry no runs, while the ELink-named ones
do — consistent with a replaced submission where the link table was updated and the GEO
record was not. If the two are maintained by different processes, that may bear on question 1.
