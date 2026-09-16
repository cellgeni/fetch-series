---
title: The assay layer
---

# The assay layer

**Screening before downloading.** GSE109816 is 880 runs of Smart-seq2 that the
reprocessing pipeline fetched in full — terabytes — before rejecting them at the
chemistry step. The metadata that would have said so was already in hand: the
ENA filereport this project fetches for every run carries
`library_construction_protocol`, and the difference between the two assays is
plain in it.

```bash
fetch screen GSE109816            # exits 2; nothing here is 10x
fetch screen GSE111360 --explain  # every run, with the phrases the call rests on
```

## What it is not

Not a replacement for inferring chemistry from the reads. `rename10xrun` in the
reprocessing pipeline reads the FASTQ files themselves and **fails closed**,
which is right for the thing that decides how reads are parsed. This layer is a
cheap prior computed before anything is downloaded. Where the two disagree, the
reads win, and the disagreement is a data-quality signal worth recording.

## Measured

**Recall: 90.9%**, on a random 2,000-run draw from `reprocessed-srr` — a corpus
in which every run is 10x *by construction*, since it is what the 10x pipeline
has already reprocessed successfully. Surveyed 2026-09-15.

| | Runs | Recall |
|---|---:|---:|
| ENA `library_construction_protocol` and friends | 1,757 / 1,986 | 88.5% |
| …plus GEO's own metadata for the 62 that are GEO submissions | **1,806 / 1,986** | **90.9%** |

Of the 229 runs ENA's metadata could not classify:

- **167 are not GEO submissions at all** — deposited directly to SRA or ENA, so
  there is no second description to consult.
- **49 of the remaining 62 were recovered from GEO** (79%), almost all from
  `!Sample_data_processing` naming CellRanger or 10x. GSM5659253 is typical: its
  ENA protocol describes the tissue dissociation in detail and never names a
  platform, while GEO says `Sequenced reads were mapped … with CellRanger
  (v6.0.0)`.
- **13 remain unrecognised** even with both archives' text.

Every run in the sample had *some* protocol text, so none of the misses is an
empty field. They are protocols that genuinely never name the platform.

### What is not measured, and why

**The false-positive rate.** There is no labelled non-10x corpus of comparable
scale to measure it against, so this page reports recall and does not report
precision. That asymmetry is stated rather than papered over — per
[the governing rule](index.md), an unmeasured number is not evidence, and
claiming one here would be exactly the kind of invented default this project
exists to avoid.

What can be said is narrower and checkable: the phrases are platform names
(`10x`, `Chromium`, `CellRanger`, `GemCode`, `Next GEM`), not descriptions of
single-cell work in general. `scRNA-seq` deliberately is **not** among them — it
appears in Smart-seq, Drop-seq and inDrop submissions too, and adding it would
buy recall by manufacturing false positives.

## Matching, and what a call means

Recognition is pattern matching over submitter free text, and cannot be anything
else. Two consequences are handled explicitly.

**A call carries its grounds.** Every match records the phrase and the field it
came from, so a classification can be argued with rather than only believed:

```
10x [GEX]: 'CellRanger' in sample_data_processing
```

**An absent field is unknown, not negative.** A run with no protocol text is
reported as such and is never screened out. Screening on absence would discard
every submission that left the field blank.

## Library type is study-level, and says so

ENA's `library_construction_protocol` is per-experiment in the schema and
per-**study** in practice: submitters paste the whole methods section into every
run of a study. GSE111360's protocol names both a Chromium 5' gene expression
kit and TCR V(D)J, so **every one of its runs matches two families**.

Choosing between them by declaration order would attach a confident label to a
coin flip. An ambiguous run therefore has no library type, and the candidates
stay visible:

```
run           assay  library_type
SRR6798781    10x    VDJ|GEX
2 of 2 runs recognised as 10x; 2 name more than one library type
```

Where the text names exactly one family, the type is asserted. The families are
checked specific-first — ATAC, VDJ, CRISPR, HTO, ADT, then GEX — because almost
every protocol says "gene expression" somewhere, so ordering decides
presentation while the ambiguity rule decides truth.

## It is an interface, and that is tested

An interface proven against a single implementation is not proven. Smart-seq is
implemented alongside 10x, and it has **no feature-barcode family at all** —
one library per cell — which exercises the parts of the interface 10x makes look
mandatory. Adding a third assay is a data change:

```python
PARSE = Assay(name="parse-seq", phrases=("Parse Biosciences", "Evercode"))
```

`tests/test_assays.py::TestTheInterfaceIsAnInterface` asserts that a new assay
needs no change to the core, and that an assay declaring no library types is
valid.
