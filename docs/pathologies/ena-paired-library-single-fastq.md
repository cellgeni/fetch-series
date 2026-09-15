---
title: ENA publishes an unpaired fastq for a library it declares paired
---

# ENA publishes an unpaired fastq for a library it declares paired

**Archive:** EBI ENA
**Affected route:** [`run->file:ena_fastq`](../routes/run-to-file/index.md)
**Status:** detected and worked around; prevalence measurement in progress
**First observed:** 2026-09-15

## Symptom

ENA's `filereport` reports `library_layout=PAIRED` for a run and publishes a
single file in `fastq_ftp`. Nothing in the file listing marks the set as partial:
one URL, one checksum, one plausible size.

The consequence is the worst shape a data-loss bug can take. The download
succeeds. The checksum matches. The file is valid fastq. The reprocessing run
completes and produces a count matrix that is wrong, because half the reads —
for 10x, the half carrying the cell barcode and UMI — were never fetched. The
failure surfaces, if it surfaces at all, as a bad experiment rather than a bad
download.

## Reproducer

```bash
curl -s 'https://www.ebi.ac.uk/ena/portal/api/filereport?accession=ERP129702&result=read_run&fields=run_accession,library_layout,fastq_ftp,fastq_bytes,submitted_ftp,submitted_bytes&format=tsv&limit=0' \
  | head -2 | tr '\t' '\n'
```

```
run_accession    ERR6039559
library_layout   PAIRED
fastq_ftp        ftp.sra.ebi.ac.uk/vol1/fastq/ERR603/009/ERR6039559/ERR6039559.fastq.gz
fastq_bytes      25659911647
submitted_ftp    ftp.sra.ebi.ac.uk/vol1/run/ERR603/ERR6039559/Sample_1.bam;...Sample_1.bam.bai
submitted_bytes  50163915278;16
```

One fastq, 25.7 GB. The submitted BAM it was derived from is 50.2 GB — very
close to twice the size, which is what a second mate weighs.

All **15** runs of ERP129702 behave this way, and every one declares
`library_layout=PAIRED`.

## Expected

Either two fastq files, or `library_layout=SINGLE`, or a field saying the derived
fastq is a partial representation of the submission. Any of the three lets a
client notice. Publishing a paired declaration beside an unpaired file set means
the only way to notice is to compare two fields nothing tells you to compare.

## What it is not

**Not a broken submission.** The submitted BAM is present, complete, checksummed
and 50.2 GB. The data was deposited correctly; the derived representation of it
is the partial thing.

**Not the same defect as E-MTAB-9221**, which is the study usually named
alongside E-MTAB-8060. The two fail on different surfaces, which is why the
pair is worth keeping together:

| | ENA `fastq_ftp` | ENA `submitted_ftp` | SDRF-registered files |
|---|---|---|---|
| **E-MTAB-8060** (ERP129702) | 15/15 runs, **one file each, declared paired** | 15/15 BAM + BAI | none registered |
| **E-MTAB-9221** (ERP122394) | **0/20 runs** | 20/20 BAM + BAI | all 40 registered |

Both studies' SDRF URIs point at the decommissioned pre-BioStudies mirror. Taking
either study's SDRF at face value loses everything. Going to ENA recovers both —
but only if the fastq columns are not trusted alone, because for 8060 they are
truncated and for 9221 they are empty.

## Detection

Mechanical, and cheap: `library_layout == PAIRED` and `len(fastq_ftp) == 1`.
`fetch_series.files.Candidate.demonstrably_incomplete` implements it, and
`recommend()` ranks such a set below every format that is merely inconvenient —
so the BAM wins, and the reason is printed rather than assumed.

The check applies only to fastq sets. A single BAM for a paired library is
exactly right: a BAM carries both mates interleaved.

## Affected accessions

```
ERP129702 / E-MTAB-8060   15 runs, all affected: ERR6039559-ERR6039573
```

Prevalence across a random sample of the reprocessed corpus is being measured;
this page will carry the figure rather than an estimate.

## Regression test

`tests/test_files.py::TestDeclaredPairedButUnpaired` — asserts that the flag
fires on a paired declaration with one fastq, does **not** fire on a single
library, on an undeclared layout, or on a BAM, and that a genuine pair still
outranks everything.
