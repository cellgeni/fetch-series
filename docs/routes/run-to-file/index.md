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

## Measured

**Corpus:** `sample:3000@reprocessed-srr` — a seeded draw from the 262,690 runs
the reprocessing pipeline has resolved, the same draw the ENA identity routes
were measured on. **Surveyed:** 2026-09-15. Nothing failed on any route.

| Route | Runs answered | Files returned | Files per answered run |
|---|---:|---:|---:|
| `run->file:sdl` | **2,996** (99.9%) | 3,085 | 1.03 |
| `run->file:ena_fastq` | 2,844 (94.8%) | 4,466 | 1.57 |
| `run->file:ena_submitted` | **246 (8.2%)** | 601 | 2.44 |
| `run->file:ena_sra` | **0** | 0 | — |

Four numbers, four separate conclusions.

**SDL answers for almost everything, and almost always with one file.** 1.03
files per run means the `.sra` object and nothing else. Near-total coverage of a
format that has to be dumped before it is usable.

**ENA's fastq columns answer for 94.8% and are frequently incomplete.** 1.57
files per run, for a corpus in which every run is 10x and therefore paired:
1,225 of the 2,844 answered runs offer exactly **one** fastq. That is not a
naming quirk — see
[ENA's derived fastq omits the read that carries the cell barcode](../../pathologies/ena-paired-library-single-fastq.md),
where 1,177 of those 1,225 are shown to have more bases in SRA than ENA
publishes.

**The submitter's original deposit exists for only 8.2% of runs.** This is the
number that makes the previous one serious. For the other 91.8% there is no
original to fall back on, so when ENA's fastq is partial the SRA object is the
only complete source there is.

**`sra_ftp` is empty. Every time.** Not a parsing failure and not an outage:
`sra_ftp`, `sra_md5`, `sra_bytes` and `sra_aspera` are documented, returnable
fields of `result=read_run`, and ENA populates them for none of the 3,000
sampled runs — nor for `SRR25056225`, `ERR2861957`, `DRR188691` or `SRR6639101`
checked by hand across all three archives. The route is kept, declared, and
measured at zero, because "we asked and the column is empty" is a different
fact from "we never asked", and only the first can be cited. It also means the
incumbent's step 6, *ENA's own SRA mirror*, never fires in practice.

## What this implies for the ranking

No single route is sufficient, and the union is not redundant:

- SDL alone: complete coverage, but `.sra` for nearly every run.
- ENA fastq alone: convenient format, missing the barcode read 41% of the time.
- ENA submitted alone: correct and original, available for 8% of runs.

Which is why [the file layer](../../files.md) asks all of them and ranks the
*answers* rather than the routes.
