---
title: fetch inspect against past STARsolo runs
---

# `fetch inspect` against the pipeline's own STARsolo runs (2026-10)

**What was measured:** whether the mapping block `fetch inspect` emits names the whitelist
and strand the reprocessing pipeline actually used, how its listed-barcode rate compares
with STARsolo's valid barcodes, and whether a sample the pipeline mapped is ever refused.
**Instrument:** the first held-out draw, classifier 0.3.0 and 0.3.1 (each row records
which) and fit rule `fit-4`; the second, classifier 0.4.0 and `fit-5`. The first 100,000 reads
of each file.
**Data:** `data/benchmarks/inspect/starsolo-2026-10-22/` (`truth.csv`, `draw-heldout.csv`,
`calls-heldout-*.csv`, `summary-heldout.json`; `draw-design.csv`, `calls-design-*.csv`,
`tune-design.csv`; `draw-heldout2.csv`, `calls-heldout2-*.csv`, `summary-heldout2.json`).
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

## Classifier 0.4.0 on a fresh held-out draw (2026-10-23)

**The draw.** 300 held-out samples named by an accession, one per dataset, seeded
(`draw-heldout2.csv`). None was in the first draw. The held-out half holds 362 such datasets,
so 243 of the 300 datasets were also in the first draw, through a different sample. The
pipeline used v2 for 233, v3 for 62, v1 for 2, the Multiome list for 2, and named no list for
1. Its strand is Forward for 269 and Reverse for 31, and it mapped 15 as a pair.

**Read once, after the thresholds were frozen.** Every copy was read on 2026-10-23 under
0.4.0 and `fit-5`, and scored with `scripts/inspect_vs_starsolo_report.py --split heldout2`.
Nothing was changed after the read. Five samples resolved to no run, so 295 were read.

**The whitelist.** Where the reads named a list, it was the pipeline's every time:

| | Samples |
|---|---:|
| the same list | **285** |
| a different list | **0** |
| no call from the reads | 9 |
| the pipeline named no list (`Undef`) | 1 |

**The mapping block.** 404 blocks were emitted for 262 samples:

- **Every one of the 404 names the pipeline's whitelist.**
- **Every one of the 386 that names a `soloStrand` names the pipeline's strand.**
- 18 leave the strand out. None guesses it.

| Layout | Samples | Blocks | Same list | Same strand | Strand left out |
|---|---:|---:|---:|---:|---:|
| 3′ v1 | 1 | 1 | 1 | 1 | 0 |
| 3′ v2 | 182 | 272 | 272 | 270 | 2 |
| 3′ v3 family | 56 | 92 | 92 | 87 | 5 |
| 5′ v1/v2 | 21 | 34 | 34 | 23 | 11 |
| Multiome GEX | 2 | 5 | 5 | 5 | 0 |

This table is now `readiness.VALIDATION`, replacing the first draw's, because it scores the
classifier that makes the blocks.

**One layout contradicts the pipeline's strand.** ERS5577770 (E-MTAB-9964) is called 3′ v3 at
HIGH in all three copies, and the pipeline mapped it Reverse. Its block leaves `soloStrand`
out, because its strand signals disagree. The housekeeping genes read antisense, 1,697 to 630,
and poly-A reads sense, 2,126 to 79. Its cDNA reads start with the TSO in 64% of reads. The
pipeline's mapping assigned 16% of its reads uniquely to genes, against a median of 74%
on this draw. So neither instrument reads this library cleanly. The block is counted
as found: VALIDATION records its three blocks as `layout_other_strand`, and a v3 block's note
says so.

**Where no block was emitted.** 33 samples:

| Why | Samples | First draw |
|---|---:|---:|
| gene expression, ambiguous between 3′ v2 and 5′ v1/v2 | **7** | 32 |
| called VDJ | 8 | 6 |
| called feature barcode | 7 | 12 |
| no 10x layout | 10 | 10 |
| a LOW call | 1 | 1 |

**Ambiguity fell from 32 of 297 samples to 7 of 295**, with no list or `soloStrand`
disagreement. The pipeline mapped 6 of the 7 Forward and 1 Reverse.

**Library type is the new gap.** The pipeline mapped all 8 VDJ-called samples to genes.

- **Five read as B-cell libraries.** Their cDNA reads hit the immunoglobulin constant regions
  in 16–42% of reads.
    - Three are called 3′: ERS5345971 (3′ v3), GSM4200472 and GSM3516678 (3′ v2). A 10x V(D)J
      library is 5′, so a 3′ layout called VDJ is wrong.
    - These look like gene-expression libraries from plasma-cell-rich tissue.
- **Three read as T-cell libraries:** GSM3148580, GSM3440855 and ERS5208982. All are 5′ with
  150-base reads, and the pipeline reports saturation of 0.85 to 0.98. They look like
  TCR-enriched libraries the pipeline mapped as gene expression, but that is not settled.

The VDJ bar (`VDJ_SHARE`, 0.15) has never been set on a labelled set. It is not changed after
this read. A next classifier version would set it on the design half and be scored on samples
not yet read.

Of the 7 feature-barcode calls:

- three are samples the pipeline mapped to almost nothing: GeneFull unique 0.004 or less;
- one has a GeneFull value of 5.88 in the pipeline's table, which is not a fraction;
- three the pipeline mapped at 0.17, 0.42 and 0.70.

**The paired 5′ form.** The pipeline mapped 14 of the Reverse samples read as a pair.

- **8 of the 14 got `starsolo_paired`.**
- **5 of the 14 got no block at all**, because they were called VDJ.
- **ERS6306681 is read from a BAM.** Cell Ranger keeps only the barcode and UMI of R1 in its
  tags, so the rebuilt read is 26 bases and the paired form cannot apply.

Of the 16 mapped single-end, one was offered the paired form: ERS5142615 (E-MTAB-9455),
151 + 151. Why the pipeline did not pair it is not known. Its single-end block leaves
`soloStrand` out.

**Fit.**
- 261 samples had a fit copy.
- 18 had every copy undetermined.
- 13 had one copy refused and the others undetermined.
- **3 had every copy refused.**
    - GSM3602376: the pipeline named no list (`Undef`), as for both of the first draw's.
    - **GSM2928382 (SRR6459192): a reader bug.** Its submitted BAM's header rebuilds R1 from
      `CR`, `UR` and `TR`. Cell Ranger writes `TR` only on reads it trimmed. The BAM reader
      dropped every read that lacked a tag the header names, so it rebuilt no R1, and the
      BAM was refused for having no barcode. Every other copy of the run holds only the index
      and cDNA reads. The fix is a separate change, and this draw does not score it.
    - **GSM4013119 (SRR9917129): one run of eight.** It holds 17,067 spots, read to the end,
      and its 26-base read matches no list. The pipeline maps a sample's eight runs together,
      so the run read here does not stand for the sample.

**The listed-barcode rate against STARsolo's valid barcodes.** Over 285 samples:

- the median difference is 0.0009;
- 277 are within 0.05;
- the correlation is 0.71.

## What this does not show

- **A fraction of the record, by design.** Two draws of 300 samples, one run each, from one
  half. A sample of several runs is judged on the one read.
- **Only what the pipeline chose, not whether it chose right.** Agreement with the
  pipeline's whitelist and strand says the block reproduces what the pipeline did, which
  mapped. It is not independent ground truth about the kit.
- **Paired mode, offered rather than chosen.** For a 5′ library whose barcode read is longer
  than 50 bases, the pipeline maps R1 and R2 as a pair (`--soloBarcodeMate 1 --clip5pNbases 39
  0`). Since 0.4.0 the block offers that form as `starsolo_paired`, beside the single-end one.
  The second draw shows when it is offered, not that it maps better.
- **GEM-X layouts** (3′ v4, 5′ v3) appear in no held-out sample: the bundle predates them.
