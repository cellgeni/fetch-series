---
title: ENA's fastq splits a spot at the wrong base and loses the barcode read
---

# ENA's fastq splits a spot at the wrong base and loses the barcode read

**Archive:** EBI ENA
**Affected route:** [`run->file:ena_fastq`](../routes/run-to-file/index.md)
**Status:** detected; one run known. How often it happens is not measured.
**Measured:** 2026-10-20, confirmed 2026-10-23

## The case

SRR14522447 is a 10x 3′ v3 run. NCBI's Normalized object holds each spot as three reads: an
8-base index read, the 28-base barcode read and a 91-base cDNA read, 127 bases in all. ENA
lists two fastqs, of 28 and 99 bases. They hold the same 127 bases, cut in the wrong places:

| | NCBI's Normalized object | ENA's fastq |
|---|---|---|
| read 1 | `TAATGACC` (index, 8) | `TAATGACC` + the first 20 barcode bases (28) |
| read 2 | `CATGCACTCCGTAGGCNAATAAGCGTCC` (barcode + UMI, 28) | the last 8 barcode bases + the cDNA read (99) |
| read 3 | the cDNA read (91) | — |

ENA's first read is 28 bases long, the length of a barcode read, and starts with an index
read. **No read of ENA's copy starts with a cell barcode.** Two files are listed for a
`PAIRED` library, so every listing check passes it. Only the reads catch it.

## Reproducer

```bash
for f in 1 2; do
  curl -sL -r 0-8000 https://ftp.sra.ebi.ac.uk/vol1/fastq/SRR145/047/SRR14522447/SRR14522447_$f.fastq.gz \
    | gzip -dc 2>/dev/null | sed -n 2p
done
# TAATGACCCATGCACTCCGTAGGCNAAT                                                                         (28)
# AAGCGTCCATTGGCCCTAAGAAGCCCCTGCCTGACNACGTGAGCATTGTGGAACCCAAAGATGAGATACTGCCCACCNCCCCCATCTCAGAACAGAAGG (99)

fastq-dump -X 1 --split-files https://sra-pub-run-odp.s3.amazonaws.com/sra/SRR14522447/SRR14522447
sed -n 2p SRR14522447_1.fastq SRR14522447_2.fastq SRR14522447_3.fastq
# TAATGACC
# CATGCACTCCGTAGGCNAATAAGCGTCC
# ATTGGCCCTAAGAAGCCCCTGCCTGACNACGTGAGCATTGTGGAACCCAAAGATGAGATACTGCCCACCNCCCCCATCTCAGAACAGAAGG
```

The Lite object carries ENA's split too. The Normalized object does not.

## What it is not

- **Not the single-fastq defect.** [That one](ena-paired-library-single-fastq.md) drops the
  barcode read and lists one file. Here both files are listed and every base is present.
- **Not a submitter's error.** NCBI's object of the same run is split correctly.

## Detection and workaround

`fetch inspect` refuses ENA's copy: no read starts with a listed barcode, under every fit rule
since `fit-1`. It calls NCBI's Normalized object 3′ v3, fit. `fetch files --inspect` would
choose that object.

## Affected accessions

One known: SRR14522447, found by the content survey's 500-run draw. Finding more needs reading,
not listing. The content survey found no other case in 1,400 runs.

## Regression test

`tests/pathologies/test_ena_fastq_splits_the_barcode_read.py` reads the first record of each
of ENA's fastqs and asserts they are still 28 and 99 bases, the first starting with the index
read.
