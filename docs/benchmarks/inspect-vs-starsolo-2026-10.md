---
title: fetch inspect against past STARsolo runs
---

# `fetch inspect` against the pipeline's own STARsolo runs (2026-10)

**What was measured:** whether the mapping block `fetch inspect` emits names the whitelist
and strand the reprocessing pipeline actually used, how its listed-barcode rate compares
with STARsolo's valid barcodes, and whether a sample the pipeline mapped is ever refused.
**Instrument:** classifier 0.3.0 and 0.3.1 (each row records which), fit rule `fit-4`, the
first 100,000 reads of each file.
**Data:** `data/benchmarks/inspect/starsolo-2026-10-22/` (`truth.csv`, `draw-heldout.csv`,
`calls-heldout-*.csv`, `summary-heldout.json`).
**Regenerate:** `fetch inspect truth import <bundle>`, `scripts/inspect_vs_starsolo.py`,
`scripts/inspect_vs_starsolo_report.py`.

## The record

[nf-reprocessing-public-10x](https://github.com/cellgeni/nf-reprocessing-public-10x) has
mapped thousands of public samples with STARsolo. Its `starsolo_10x_auto.sh` chooses the
whitelist by matching 200,000 barcodes against each list, and the strand by mapping a test
sample both ways and keeping the direction that assigns more reads to genes. The maintainer
exported the small files of every reprocessed dataset from iRODS: each dataset's `solo_qc`
table, which has one row per sample, and each sample's GeneFull `Summary.csv`. That bundle
holds 6,364 samples from 712 datasets.

- **Per sample.** STARsolo maps a sample: every run of it together. So the comparison is
  per sample, with one of its runs read, chosen by seed.
- **Public only.** 602 samples from EGA and HRA datasets are counted and set aside. Their
  reads cannot be read, and their names are not written to the committed record.
- **Halves by dataset.** The split is seeded. Studies the classifier's thresholds were set
  on (the [2026-10-19 benchmark](inspect-2026-10.md)'s design half) were left out of the draw.
- **The draw.** 300 held-out samples named by an accession (GSM or ERS), one per dataset. No
  threshold was set on this half or changed after it was read. The comparison was made
  once.

The pipeline used v2 for 236 of the drawn samples, v3 for 61, v1 for 1, the Multiome list
for 1, and named no list for 2. The pipeline's strand is Forward for 273 and Reverse for 27.

## Results

Three samples resolved to no run, so 297 were read. The copy compared for each sample is
its best-called source.

**The whitelist.** Where the reads named a list, it was the pipeline's every time:

| | Samples |
|---|---:|
| the same list | **287** |
| a different list | **0** |
| no call from the reads | 8 |
| the pipeline named no list (`Undef`) | 2 |

**The mapping block.** 357 blocks were emitted for 236 samples, one per copy called HIGH or
MEDIUM as 10x gene expression:

- **Every one of the 357 names the pipeline's whitelist.**
- **Every one of the 333 that names a `soloStrand` names the pipeline's strand.**
- 24 leave the strand out, because the cDNA read's strand was unstranded or not measured.
  None guesses it.

| Layout | Samples | Blocks | Same list | Same strand | Strand left out |
|---|---:|---:|---:|---:|---:|
| 3′ v2 | 175 | 261 | 261 | 256 | 5 |
| 3′ v3 family | 47 | 76 | 76 | 66 | 10 |
| 5′ v1/v2 | 12 | 17 | 17 | 8 | 9 |
| Multiome GEX | 2 | 3 | 3 | 3 | 0 |

This table is now in `inspect/readiness.py` (`VALIDATION`). A block for one of these layouts
says it has been set beside the pipeline's runs, with these counts. A layout no held-out
sample was called as, such as GEM-X v4 or 5′ v3, still says it is unvalidated.

**Where no block was emitted.** 32 gene-expression samples were called ambiguous between 3′ v2
and 5′ v1/v2. The two share a list. The barcode read was 26 to 28 bases, too short to show the
tail, and no two strand signals agreed. The pipeline mapped 25 of them Forward and 7 Reverse.
The rest:

- 18 were not gene expression (feature barcode or VDJ);
- 3 have a 10x 3′ v1 barcode read, or a 16-base one, that no registered list holds;
- for 5, nothing could be read.

**The listed-barcode rate against STARsolo's valid barcodes.** The two are close but not the
same measure. Ours is the share of the first reads' barcode windows that are listed within one
substitution. STARsolo's counts every read of the sample after its own correction. Over 287
samples:

- the median difference is 0.0005;
- 277 are within 0.05;
- the correlation is 0.62. Most samples sit at 0.93–0.99 on both, so the range is narrow.

Of the 10 that differ by more than 0.05:

- 6 read lower than STARsolo's valid barcodes;
- 4 read higher. The largest is SRR7788633: 0.98 listed, against 0.66 valid in STARsolo.
  Why is not known.

**Fit.**
- 233 samples had a fit copy.
- 38 had every copy undetermined, mostly as above.
- 24 had one copy refused and the others undetermined.
    - In 20, the refused copy is ENA's fastq, which lost the barcode read (the [ENA
      defect](../pathologies/ena-paired-library-single-fastq.md)).
    - In 4 (GSM3557970, ERS4547949, ERS4414948, ERS4414918), it is the SRA object, which holds
      the cDNA read alone, as ERR4265292's does. The submitted BAM of each carries the
      barcode read, so a usable copy exists.
- **2 had every copy refused: SRR8569915 (GSM3602372) and SRR6155060 (GSM2807820).** Both
  are 150 + 150 with no 10x barcode at any start. **The pipeline named no whitelist for
  either** (`Undef`), and mapped them anyway. They are in the reprocessed table but do not
  read as 10x. No sample the pipeline mapped with a whitelist was refused.

## Classifier 0.4.0: thresholds set on the design half

The held-out comparison above left 32 gene-expression samples ambiguous between 3′ v2 and
5′ v1/v2. Reading their recorded evidence showed why:

- **Undecided poly-A.** chrM and the housekeeping set agreed on a strand, but poly-A, under its
  0.80 bar, said "unstranded", and only an undecided *housekeeping* vote was tolerated.
- **The TSO tail.** A 150-base 5′ barcode read shows the template-switch oligo after the UMI in
  35–45% of reads, under the 0.5 bar a tail needed to decide anything.

Those samples have now been looked at, so they cannot test a fix. The fix was set on the
**design half** instead:

- **The draw.** 392 samples from 241 datasets (`draw-design.csv`), stratified so 5′ is not
  drowned out: 150 v2 Forward, 150 v2 Reverse (93 single, 57 paired), 60 v3, and every v1 and
  Multiome sample the half holds.
- **Reading.** Every copy was read once and its evidence recorded.
- **Scoring.** `classify.decide` makes a call from recorded evidence alone, so
  `scripts/tune_classifier.py` re-decides every copy under each of 240 settings without a
  read, and scores the sample's best copy against the pipeline's whitelist and strand
  (`tune-design.csv`).
- **The rule for choosing.** A setting is admissible only if no call contradicts the
  pipeline's list, layout or strand. Of those, the least ambiguous wins.

| Setting | Ambiguous 3′ v2 / 5′ | Layout right | Wrong |
|---|---:|---:|---:|
| 0.3.1 (undecided poly-A blocks; tail bar 0.5) | 114 | 253 | 0 |
| **0.4.0** (undecided poly-A tolerated; tail bar 0.25) | **67** | **300** | **0** |
| also tolerate an undecided chrM | 51 | 316 | 5 strands |
| a single strand vote decides | 57 | — | 1 layout, 1 strand |
| drop the human housekeeping vote on mouse libraries | 79 | — | 0 |

0.4.0 also makes a few-tag library whose cDNA reads start with 10x's TSO in 30% or more of
reads a GEX call, not a feature-barcode one. That cuts the design half's feature-barcode
calls on samples the pipeline mapped to genes from 4 to 2. The inspect gate's calls are
unchanged under 0.4.0.

0.4.0 is scored once, on a fresh held-out draw that excludes every sample and, where
possible, every dataset read before. That result is in the next section.

## What this does not show

- **A fraction of the record, by design.** 300 samples, one run each, from one half.
- **Only what the pipeline chose, not whether it chose right.** Agreement with the
  pipeline's whitelist and strand says the block reproduces what the pipeline did, which
  mapped. It is not independent ground truth about the kit.
- **Paired mode.** For a 5′ library whose barcode read is longer than 50 bases, the pipeline
  maps R1 and R2 as a pair (`--soloBarcodeMate 1 --clip5pNbases 39 0`). The block describes
  single-end mapping only, and this comparison scores its strand against the pipeline's test,
  not against how the pair was mapped.
- **GEM-X layouts** (3′ v4, 5′ v3) appear in no held-out sample: the bundle predates them.
