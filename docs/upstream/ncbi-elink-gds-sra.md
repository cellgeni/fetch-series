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

We resolve GEO series to their sequencing data programmatically, and have found
that `elink.fcgi?dbfrom=gds&db=sra` reports no SRA neighbours for a substantial
fraction of GEO series that demonstrably have SRA experiments.

In a stratified random sample of 150 GEO series drawn from the full set of
275,238, 24 carry SRA experiments according to their own SOFT family file. For 6
of those 24 (25%), ELink returns a `linksets` array with no `linksetdbs` at all.

Affected series: GSE27679, GSE29158, GSE34359, GSE35884, GSE49732, GSE49780.

Taking GSE27679 as the example:

- `esearch db=gds` resolves it to UID 200027679.
- `elink.fcgi?dbfrom=gds&db=sra&id=200027679&cmd=neighbor` returns no link records.
  Passing `linkname=gds_sra` explicitly makes no difference, and `cmd=neighbor_history`
  behaves the same way.
- The SOFT family file at
  `ftp.ncbi.nlm.nih.gov/geo/series/GSE27nnn/GSE27679/soft/GSE27679_family.soft.gz`
  names nine SRA experiments, beginning SRX046652.
- `esearch db=sra&term=SRX046652[ACCN]` returns count 1, so the experiment is
  present in SRA.
- ENA lists seven runs for SRX046652 (SRR121517–SRR121523).

So the records exist on both sides and GEO's own metadata states the relationship,
but the Entrez link table does not express it.

We considered and ruled out an age effect: ELink succeeds for GSE21739, which is
older than every affected series, and for GSE56066, which is newer than all of
them. The accession ranges overlap almost entirely.

The response is an ordinary HTTP 200 with no error field, so callers cannot tell
the difference between "this series has no SRA data" and "the link is missing".
That makes it silent data loss for anyone traversing GEO → SRA through ELink, and
it is why we ask about it rather than working around it quietly.

Two questions:

1. Is the `gds`↔`sra` link table expected to be complete, and is there a known
   reason these series are absent from it?
2. Is there an Entrez query that recovers the relationship? Searching
   `db=sra` for `GSE27679[All Fields]` also returns 0, so we have not found one.
