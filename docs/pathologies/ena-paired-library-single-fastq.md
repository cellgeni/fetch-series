---
title: ENA's derived fastq omits the read that carries the cell barcode
---

# ENA's derived fastq omits the read that carries the cell barcode

**Archive:** EBI ENA
**Affected route:** [`run->file:ena_fastq`](../routes/run-to-file/index.md)
**Status:** detected, measured, and worked around; ready to report upstream
**Measured:** 2026-09-15 on a random 3,000-run sample of the reprocessed corpus

## The number

**1,177 of 3,000 runs — 39.2%.**

For that many runs of a 10x corpus, ENA's `fastq_ftp` serves a single file for a
library ENA itself declares `PAIRED`, and the SRA archive holds **more bases than
ENA publishes**. Across the affected runs:

| | |
|---|---|
| Bases SRA holds | 19.1 Tb |
| Bases ENA does not publish | **5.93 Tb (31.1%)** |
| Median SRA/ENA base ratio | **1.40** |
| SRA `avgLength` for 83% of them | 120–139 bp |

An `avgLength` of 120–139 with a published read of ~91–98 bp is a spot holding a
~28 bp read alongside the long one. That short read is the 10x cell barcode and
UMI. It is not published.

## Why this is worse than a missing file

The download succeeds. The checksum matches — ENA publishes a correct `fastq_md5`
for the partial file. The file is valid fastq. The reprocessing run completes and
produces a count matrix in which **every read is unassignable to a cell**, because
the barcode was never fetched. The failure presents as a bad experiment rather
than a bad download.

Nothing in the file listing says so. The only signal is that `library_layout`
says `PAIRED` beside a one-file list — two fields nothing tells you to compare.

## Reproducer

```bash
# ENA: one fastq, library declared PAIRED
curl -s 'https://www.ebi.ac.uk/ena/portal/api/filereport?accession=SRR29534765&result=read_run&fields=library_layout,fastq_ftp,read_count,base_count&format=tsv'
# PAIRED  ftp.sra.ebi.ac.uk/vol1/fastq/SRR295/065/SRR29534765/SRR29534765.fastq.gz  89275969  9016872869

# SRA: the same run, 3.0 Gb more
curl -s 'https://trace.ncbi.nlm.nih.gov/Traces/sra-db-be/sra-db-be.cgi?rettype=runinfo&term=SRR29534765' \
  | python3 -c 'import sys,csv; r=next(csv.DictReader(sys.stdin)); print(r["spots"], r["bases"], r["avgLength"])'
# 89275969  12052255815  135
```

Same spot count. 135 bases per spot in SRA; 101 published by ENA.

And the published reads say which one survived — the FASTQ headers are tagged
`/3`, the third read of the spot:

```bash
curl -s -r 0-200000 'https://ftp.sra.ebi.ac.uk/vol1/fastq/SRR295/065/SRR29534765/SRR29534765.fastq.gz' | gzip -dc | head -1
# @SRR29534765.1 K00337:202:HWNYLBBXX:6:1101:2747:1297/3
```

## Confirmed by the reads, 2026-10-20

The number above was measured from metadata: a listing, a declared layout and a base
count. [The content survey](../benchmarks/content-survey-2026-10.md) read the files. It
read the first 100,000 reads of every file ENA lists for 500 runs drawn from the same
3,000, and applied the [fit rule](../decisions/what-a-read-sample-can-prove.md#refusing-a-source-needs-positive-evidence) to them.

| ENA's listing in the census | Fit | Unfit | Undetermined |
|---|---:|---:|---:|
| one fastq | 0 | **183** | 0 |
| two or more | 234 | 1 | 56 |

**Every one of the 183 single-fastq runs is proven unusable by its reads.** None holds a
read whose first 16 bases are a listed 10x barcode, or none holds a read long enough to be
the cDNA. For 142 runs with an unfit ENA fastq, NCBI's Normalized SRA object of the same run is
fit. (Read on 2026-10-22 under `fit-3`, judged under `fit-5`; the first reading, under `fit-2`,
found 182 and 141.)

The reads also found a defect the listing cannot see. SRR14522447 has two ENA fastqs, of
28 and 99 bases. NCBI's Normalized object holds the same 127 bases per spot split as 8, 28
and 91. ENA's split puts the index read and the first 20 barcode bases in one read, so no
read starts with a barcode. The SRA Lite object carries the same wrong split. Two files
are listed, so the check on this page passes the run. Only the reads catch it.

## What it is — and is not

**It is ENA's derivation, not a broken deposit.** Of the 1,201 single-fastq runs
checked, **1,177 (98.0%)** have more bases in SRA than ENA publishes. Only **11**
hold the same in both, and those are genuine submitter errors: a library declared
`PAIRED` where only one read was ever deposited (SRR30132199 is one — 28 bp
reads, 52.8 M spots, identical in both archives, and unrecoverable).

Separating the two matters. A report that conflates them is useless to either
archive: EBI can fix the first and only the submitter can fix the second.

**There is usually nothing else to fall back to.** Only **8.2%** of runs in the
same sample have any `submitted_ftp` file at all. For the rest the SRA object,
reached through NCBI SDL, is the sole complete source — which is why the file
layer asks every route rather than the first that answers.

## Expected

Any one of: publish the technical read; report `library_layout=SINGLE` for what
is being served; or add a field saying the derived fastq is a partial
representation of the run. Excluding technical reads is a defensible default for
bulk RNA-seq. For single-cell it removes the only thing that makes the reads
single-cell — and the metadata gives a client no way to notice.

## Detection and workaround

Mechanical and free: `library_layout == PAIRED` **and** `len(fastq_ftp) == 1`.
`fetch_series.files.Candidate.demonstrably_incomplete` implements it, and
`recommend()` ranks such a set below every format that is merely inconvenient,
so an affected run resolves to the SRA object instead:

```
SRR29534765: no route offered a complete read pair
  -> run->file:sdl: 1 sra file, md5 published
     run->file:ena_fastq: 1 fastq file, INCOMPLETE: archive declares the library paired
```

The check applies only to fastq sets. A single BAM for a paired library is
exactly right — a BAM carries both mates interleaved.

**The incumbent reaches the same verdict by a different route.**
`fetch10xmeta` requires ENA to offer both `_1.fastq.gz` and `_2.fastq.gz` before
it will use ENA fastq at all. That is a name test standing in for a completeness
test: it catches all 1,177 of these, and also rejects the 24 genuinely
single-end libraries in the same sample, which `library_layout` distinguishes.

## Affected accessions

The full list of 1,177 regenerates from the survey cache:

```bash
uv run fetch survey run --route run->file:ena_fastq --corpus sample:3000@reprocessed-srr
```

Worked examples on this page: `SRR29534765`, `SRR30132199` (the submitter-error
case), and all 15 runs of `ERP129702` / E-MTAB-8060, where the submitted BAM
makes the loss visible by comparison — 25.7 GB of fastq against a 50.2 GB BAM.

## Regression test

`tests/test_files.py::TestDeclaredPairedButUnpaired` — the flag fires on a
paired declaration with one fastq, and does not fire on a single library, an
undeclared layout, or a BAM; a genuine pair still outranks everything.

## Seen in the reads

[`fetch inspect`](../inspect.md) reads both copies and calls them. For SRR29534765, ENA's fastq
is one file of 101-base reads named `/3`, and is **unfit**: there is no barcode read in it.
The SRA object holds 8 + 26 + 101 bases and is called 3' v2. `fetch files --inspect` refuses
the ENA set on that evidence.

