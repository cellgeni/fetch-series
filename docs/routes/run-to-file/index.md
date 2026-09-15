---
title: Run to file
---

# Run to file

**Direction:** `SRR`/`ERR`/`DRR` run → download links
**Status:** implemented; census in progress over `sample:3000@reprocessed-srr`

The last hop, and the one the rest of the graph exists to reach. Four routes
answer it and none of them subsumes the others — see
[the file layer](../../files.md) for why all four are asked rather than the
first that replies.

## The routes

| Route | Endpoint | What it returns |
|---|---|---|
| `run->file:ena_fastq` | ENA `filereport`, `fastq_*` columns | ENA's own derived fastqs |
| `run->file:ena_submitted` | ENA `filereport`, `submitted_*` columns | the submitter's original deposit |
| `run->file:ena_sra` | ENA `filereport`, `sra_*` columns | the NCBI-format archive object at EBI |
| `run->file:sdl` | NCBI `locate.ncbi.nlm.nih.gov/sdl/2/retrieve` | NCBI's view, plus cloud mirrors and cost |

The three ENA routes are three column families of **one request**, so asking all
three costs what asking one costs. They are declared separately because they
have separate coverage and separate failure modes, and a route whose coverage is
not separately counted cannot be separately ranked.

## Requests

```bash
# all three ENA column families at once
curl -s 'https://www.ebi.ac.uk/ena/portal/api/filereport?accession=SRR25056225&result=read_run&fields=run_accession,library_layout,fastq_ftp,fastq_md5,fastq_bytes,submitted_ftp,submitted_md5,submitted_bytes,sra_ftp,sra_md5,sra_bytes&format=tsv&limit=0'

# SDL, which batches
curl -s -X POST 'https://locate.ncbi.nlm.nih.gov/sdl/2/retrieve' \
  -d 'acc=SRR25056225,ERR2861957' -d 'accept-charges=aws'
```

## What each route knows that the others do not

**SDL batches.** One POST carries up to 50 accessions, where every other file
route costs a request per run. At file-layer scale that is the difference
between a survey and an overnight job.

**SDL knows the price.** It is the only route that reports `payRequired` and
`rehydrationRequired`. A link that resolves and costs money to retrieve, or that
needs a cold-storage restore first, is not the same as a link that works, and no
ENA column says which is which.

**ENA knows the checksum for the file it is offering.** `fastq_md5` is published
per file, aligned positionally with `fastq_ftp`. This is the open issue in the
reprocessing pipeline — ENA publishes it and nothing plumbs it through — and it
is a first-class field on every `FileRecord` here.

**ENA's `submitted_*` is the only route to an original deposit at EBI.** For
ERR2861957 it is the only route to the data at all: `fastq_ftp` is empty, and
SDL's alternative is the `.sra` object rather than the submitter's BAM.

## Parsing

ENA packs a run's files into parallel `;`-joined lists — `fastq_ftp`,
`fastq_md5`, `fastq_bytes` — which are meaningful only positionally. A row whose
lists are different lengths is malformed, and pairing them by index anyway
attaches one file's checksum to another. That is worse than no checksum: it
fails a download that was fine. The mismatched field is dropped and the files
kept.

ENA publishes URLs with no scheme (`ftp.sra.ebi.ac.uk/vol1/...`); `https://` is
prepended.

## Known pathologies

- [ENA publishes an unpaired fastq for a paired library](../../pathologies/ena-paired-library-single-fastq.md)
  — all 15 runs of ERP129702, where the fastq route's answer is complete-looking
  and half the data.

## Measured behaviour

The census over `sample:3000@reprocessed-srr` — the same draw the ENA identity
routes were measured on, so the two are directly comparable — is running. Per
[the governing rule](../../index.md), this page will not claim coverage it has
not counted, and the routes carry no `RouteEvidence` until it does.
