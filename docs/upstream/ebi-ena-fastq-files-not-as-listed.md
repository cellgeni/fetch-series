---
title: EBI — listed fastqs that are not served, and fastqs derived from SRA Lite
---

# EBI — listed fastqs that are not served, and fastqs derived from SRA Lite

**Status:** draft, not filed **Archive:** EBI ENA
**Checked:** 2026-10-23. Both directories below are still served as directories, and
SRR11442250's fastq still carries `?` for every quality.
**Evidence:** [pathologies/ena-serves-a-listed-fastq-wrongly.md](../pathologies/ena-serves-a-listed-fastq-wrongly.md),
[pathologies/ena-fastq-is-lite-quality.md](../pathologies/ena-fastq-is-lite-quality.md),
[pathologies/ena-fastq-splits-the-barcode-read.md](../pathologies/ena-fastq-splits-the-barcode-read.md)
**Where to file:** the ENA support form at `https://www.ebi.ac.uk/ena/browser/support`

Regenerate the evidence before filing:

```bash
uv run python scripts/ena_listing_census.py \
  --census data/surveys/sample_3000@reprocessed-srr/run-to-file_ena_fastq.csv.gz \
  --out /tmp/ena-listing-recheck
uv run pytest -m pathology tests/pathologies/test_ena_serves_a_listed_fastq_wrongly.py \
  tests/pathologies/test_ena_fastq_is_lite_quality.py tests/pathologies/test_ena_fastq_splits_the_barcode_read.py
```

---

## Draft report

Subject: three `fastq_ftp` files are not served as listed; some derived fastqs carry SRA Lite qualities

We read ENA's run files for a seeded draw of 3,000 runs from a corpus of public 10x
single-cell data. We found three separate problems with the derived fastqs. In each case the
`filereport` listing looks complete.

### 1. Listed files that are not there

We sent a HEAD request for each of the 4,466 `fastq_ftp` links listed for 2,844 of those
runs. Two do not serve the file `filereport` describes:

| Run | File | `fastq_bytes` | What `ftp.sra.ebi.ac.uk` serves |
|---|---|---:|---|
| SRR11040464 | `SRR11040464_1.fastq.gz` | 583,713,371 | 301 to `…_1.fastq.gz/`, an empty directory |
| SRR27782870 | `SRR27782870_1.fastq.gz` | 606,656,529 | 301 to `…_1.fastq.gz/`, an empty directory |

```bash
curl -sI https://ftp.sra.ebi.ac.uk/vol1/fastq/SRR110/064/SRR11040464/SRR11040464_1.fastq.gz | head -1
```

The other file of each pair is served correctly.

We also noticed two transient behaviours, which we are not reporting as defects:

- The server answered 403 for 81 links under concurrent requests, and 200 for 80 of them
  when asked one at a time.
- For a while on 2026-10-23, it answered 4,096 bytes for ERR15320482's `_2.fastq.gz`. That file
  is listed at 7,197,985,953 bytes and has the same `Last-Modified`.

### 2. Fastqs derived from the SRA Lite object

For 122 of 452 NCBI-submitted runs we read, every quality in ENA's fastq is `?`, and the same
is true for 22 of 278 DDBJ-submitted runs. That is the signature of an SRA Lite object. NCBI's
Normalized object of the same run has the sequencer's qualities. Example:

```bash
curl -s -r 0-20000 https://ftp.sra.ebi.ac.uk/vol1/fastq/SRR114/050/SRR11442250/SRR11442250_1.fastq.gz | gzip -dc 2>/dev/null | head -4
```

### 3. A spot split through the barcode read

SRR14522447's spots are 8 + 28 + 91 bases in NCBI's Normalized object. ENA's two fastqs
split them 28 + 99, which joins the index read to the first 20 bases of the cell barcode. All
the bases are there, but no read starts with the barcode. Two files are listed for a `PAIRED`
library, so no listing check notices.

### What would resolve it

- **Listed files that are not there:** re-derive or remove the two files, and check
  `fastq_bytes` against what the server holds.
- **Lite-derived fastqs:** derive the fastqs from the Normalized object where NCBI has one.
- **The split spot:** split by the read boundaries NCBI records for the run.
