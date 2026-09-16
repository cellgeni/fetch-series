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
fetch screen GSE111360 --explain  # every run, with the phrases each call rests on
```

```
run          assay  library_type  chemistry
SRR6798781   10x    VDJ|GEX       10x_5p_v1_v2
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

## Chemistry: the one place metadata beats the reads

`rename10xrun`'s `infer_10x_chemistry.py` decides chemistry from the FASTQ files
— barcode length, UMI length, which whitelist the barcodes hit. That is the
right way to decide how reads are parsed, it fails closed, and nothing here
replaces it.

But it cannot answer everything, and it says so itself, in a warning it emits:

> `737K-august-2016` cannot distinguish 3prime v2 from 5prime v1/v2 by FASTQ
> geometry alone

Both use a 16 bp barcode, a 10 bp UMI and the same whitelist. Geometry is
genuinely blind there. **Metadata is not**: a protocol naming a *"Chromium Next
GEM Single Cell 5' Library & Gel Bead Kit"* settles it, and settles it before
anything is downloaded.

### Measured

2,000 runs from `reprocessed-srr`, 2026-09-16:

| | Runs | |
|---|---:|---:|
| Metadata names exactly one chemistry | 648 | 32.4% |
| …of which **finer than FASTQ geometry can reach** | **292** | **14.6%** |
| Names more than one (a study that ran both kits) | 12 | 0.6% |
| Names none | 1,340 | 67.0% |

| Chemistry | Runs |
|---|---:|
| `10x_3p_v3_family` | 299 |
| `10x_5p_v1_v2` | **183** |
| `10x_3p_v2` | **109** |
| `10x_multiome_arc_gex` | 42 |
| `10x_atac_v1_v1.1_v2` | 11 |
| `10x_5p_v3_gemx`, `10x_3p_v4_gemx`, `10x_3p_v1` | 4 |

The two bolded rows are the payoff: **292 runs where the metadata distinguishes
3' v2 from 5' v1/v2** and the whitelist cannot. For the other 67% the metadata
is silent, and silence is the answer it gives.

### What is not measured

**Agreement with the empirical inference.** That needs the FASTQ files, which
needs exactly the downloads this layer exists to avoid. The honest claim is
"metadata settles it for 32.4% of runs"; comparing the two is a cross-check for
whenever the pipeline runs both, and a disagreement there is a finding about the
submission rather than a tie to break.

The ids are `rename10xrun`'s verbatim — `10x_3p_v3_family`, `10x_5p_v1_v2`,
`10x_multiome_arc_gex` — so the two answers compare without translation, and a
test asserts the vocabularies have not drifted apart.

### A study that ran two kits names both

The protocol field describes a study, not a library. GSE111360's names a
Chromium 3' kit *and* a 5' kit because the study used both, in one field shared
by every run, so those runs report `10x_3p_v3_family|10x_5p_v1_v2` and assert
nothing — the same rule the [library type](#library-type-is-study-level-and-says-so)
follows.

A GEM-X 3' kit is *not* ambiguous with the plain 3' family it implies, nor
Multiome with ATAC: that is one library described at two levels, and reporting
it as a conflict would manufacture an ambiguity that is not there.

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
