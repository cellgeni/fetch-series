---
title: The file layer
---

# The file layer

A run accession is not a deliverable. The deliverable is a set of files that
**exist**, are **complete**, and are **what the submitter actually deposited**.
Those three properties fail independently, and the archives disagree about all
of them.

## Why every route is asked, not just the first

The resolver stops at the first route that answers, because identity routes
answer the same question and a good answer ends it. File routes do not answer
the same question:

| Route | What it holds |
|---|---|
| `run->file:ena_fastq` | ENA's *own derived* fastqs, generated from whatever was deposited |
| `run->file:ena_submitted` | The submitter's original deposit, in its original format |
| `run->file:ena_sra` | The NCBI-format archive object, mirrored at EBI |
| `run->file:sdl` | NCBI's view of all of the above, plus cloud mirrors and retrieval cost |

A run can have derived fastqs and no submitted deposit, a submitted deposit and
no derived fastqs, or both. All three occur in the first two runs the tier-one
corpus offers:

```
SRR25056225   ena_fastq: 2 files   ena_submitted: none     sdl: the .sra object
ERR2861957    ena_fastq: none      ena_submitted: 1 BAM    sdl: the BAM and the .sra object
```

Neither route is wrong about the other's run. Each returns empty and means it.

## Two kinds of judgement, kept apart

**Which route to try first** is an evidence question. It is answered by
`RouteRegistry.ranked()` from measured survey results, like every other route
ranking in this project, and no file route ships a hand-written preference.

**Which files to use**, once several routes have answered, is a requirements
question — and the requirements belong to the downstream pipeline, not to any
archive. In order:

1. **A complete read pair beats everything.** Half a pair is not half a
   download, it is an unusable one.
2. **A file set the archive's own metadata proves incomplete ranks below every
   format that is merely inconvenient.** See below.
3. **Format:** fastq, then BAM, then CRAM, then `.sra`. STARsolo reads fastq;
   an `.sra` object must be dumped before it is anything at all. This is a
   statement about the tool, not about the archive.
4. **Free before paid**, then **checksummed before not**, then more files, then
   route id for determinism.

Every alternative is carried alongside the recommendation. A recommendation the
caller cannot argue with is one they cannot check.

## The two traps that are not visible in a file listing

### An index file is not a data file

`x.bam.bai` contains `.bam`. Any classifier that tests for the data suffix first
calls every index in the archive by the name of the thing it indexes, and
ERR2861957's `.bai` is **16 bytes** — so counting it as data turns a one-file
submission into a pair without anything looking odd.

### A mate marker means nothing outside a fastq name

`_1` means "mate 1" by convention in fastq naming and by coincidence everywhere
else. E-MTAB-8060's runs deposit `Sample_1.bam`, where the `_1` is the
submitter's sample name. Reading it as a mate made a single BAM report as half a
pair. A BAM carries both mates interleaved by construction, so a mate number on
one is not unreliable — it is meaningless.

## Completeness the listing does not admit to

A fastq set can be incomplete in a way only the archive's *other* metadata
reveals, and this is not a rare corner: **39.2% of a random 3,000-run sample of
the reprocessed corpus**. ENA reports `library_layout=PAIRED` and publishes one
fastq, and SRA holds a median of **1.40x** the bases ENA serves — the missing
~28 bp read being the one that carries the 10x cell barcode.

A pipeline taking the convenient format downloads cleanly, checksums correctly,
and produces a matrix in which every read is unassignable to a cell. The file
layer refuses that set, recommends the complete source, and says which fact
drove the choice.

See [ENA publishes an unpaired fastq for a paired library](pathologies/ena-paired-library-single-fastq.md).

## Using it

```bash
fetch files SRR25056225                 # the recommended set, as TSV with md5 and size
fetch files GSE236084 --explain         # every route's offer and why one won
fetch files E-MTAB-8060 --all           # emit the alternatives too
fetch files GSE109816 --limit 5         # 880 runs at four requests each; cap it
```
