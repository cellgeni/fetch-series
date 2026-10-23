---
title: ENA lists a fastq its file server does not serve
---

# ENA lists a fastq its file server does not serve

**Archive:** EBI ENA
**Affected route:** [`run->file:ena_fastq`](../routes/run-to-file/index.md)
**Status:** detected, measured, and caught by `fetch files --verify`; ready to report upstream
**Measured:** 2026-10-23, every ENA fastq link of a seeded 3,000-run draw of the reprocessed corpus

## The number

`filereport` lists each fastq with a size and an md5. The file server was asked about every
one of the 4,466 links ENA lists for the 2,844 runs of `sample:3000@reprocessed-srr` that
list any. It was asked as `fetch files --verify` asks: a HEAD with redirects followed.

| What the server did | Links |
|---|---:|
| served a file of the published size | 4,381 |
| answered 403, then 200 when asked again one at a time | 80 |
| answered 403 twice | 1 |
| timed out | 1 |
| **served an empty directory at the file's path** | **2** |
| **served a file of 4,096 bytes where 7,197,985,953 are published** | **1** |

Three listed files are not there to download, **3 of 4,466 links**. Each looks complete to a
listing: a name, a size and an md5.

- **SRR11040464 `_1.fastq.gz`** is listed at 583,713,371 bytes, and SRR27782870 `_1.fastq.gz`
  at 606,656,529. At each path the server answers 301 to the same path with a trailing slash,
  and serves an empty directory listing. The `_2` file beside each is real.
- **ERR15320482 `_2.fastq.gz`** is listed at 7,197,985,953 bytes. The server holds 4,096
  bytes: the gzip header of an upload that stopped.

## Reproducer

```bash
curl -sI https://ftp.sra.ebi.ac.uk/vol1/fastq/SRR110/064/SRR11040464/SRR11040464_1.fastq.gz | head -1
# HTTP/1.1 301 Moved Permanently   -> .../SRR11040464_1.fastq.gz/  (an empty directory)

curl -sI https://ftp.sra.ebi.ac.uk/vol1/fastq/ERR153/082/ERR15320482/ERR15320482_2.fastq.gz \
  | grep -i content-length
# Content-Length: 4096

curl -s 'https://www.ebi.ac.uk/ena/portal/api/filereport?accession=ERR15320482&result=read_run&fields=fastq_bytes&format=tsv'
# ERR15320482  4208835871;7197985953
```

Checked on 2026-10-23.

## What it is not

- **Not the 403s.** 81 links answered 403 under the census's concurrency. 80 answered 200
  when asked again one at a time at one a second, which makes them throttling, not missing
  files. `SurveyClient` now retries a 403 from `ftp.sra.ebi.ac.uk`
  (`survey.client.THROTTLES_WITH_403`), and only from there. The content survey's 403s on
  ERR4065012 and SRR34482399 (2026-10-22) were of this kind: both answered 200 the next day.
- **Not the single-fastq defect.** [That one](ena-paired-library-single-fastq.md) lists one
  real file where two reads exist. Here a listed file is not real.

## Why it matters

- **A directory answers 200.** A client that follows the redirect, as every client must for
  ENA's `ftp.sra.ebi.ac.uk` links, gets a 200 and an HTML index.
- **The truncated file decompresses** for its first 4 KB and then ends.
- **The fit rule is protected.** In the content survey SRR11040464's set was refused on its
  other file alone, until `fit-5` stopped any refusal while a listed file cannot be read.

## Detection and workaround

`fetch files --verify` flags all three.

- A redirect to a path ending `/` is `served_directory`, and never `resolves`
  (`filesets.Verdict`).
- The 4,096-byte file fails `size_matches`.

The other offers of the same run are unaffected: NCBI's SRA objects of SRR11040464 read fit.

## Affected accessions

```bash
uv run python scripts/ena_listing_census.py \
  --census data/surveys/sample_3000@reprocessed-srr/run-to-file_ena_fastq.csv.gz \
  --out data/benchmarks/ena-listing/<date>
```

The 2026-10-23 census is `data/benchmarks/ena-listing/2026-10-23/links.csv`.

## Regression test

`tests/pathologies/test_ena_serves_a_listed_fastq_wrongly.py` asserts that SRR11040464's
`_1.fastq.gz` is still served as a directory, and that ERR15320482's `_2.fastq.gz` still serves
4,096 bytes. The non-inverted half checks that `verify` flags both.
