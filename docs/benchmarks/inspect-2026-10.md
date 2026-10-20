---
title: Read classifier benchmark, 2026-10
---

# Read classifier benchmark, 2026-10

**Classifier:** `fetch_series.inspect.classify` version 0.2.0. **Run:** 2026-10-19 and
2026-10-20 (UTC). **Data:** `data/benchmarks/inspect/2026-10-19/`: `labels.csv`,
`calls-0.1.0.csv`, `calls-0.2.0.csv` and the summaries. It holds counts and calls and no
sequence. **Scripts:** `scripts/benchmark_inspect.py` and `scripts/benchmark_inspect_report.py`.

The question: from a run's first 100,000 reads, does the classifier name the 10x kit a
library was made with, and how sure should anyone be of that name?

## How it was measured

- **Labels that did not come from the reads.** Studies were drawn, seeded, from the
  reprocessed 10x table, with one run each. A run is labelled where its metadata (ENA's
  protocol fields and GEO's SOFT text, read by `assays.chemistry_of`) names exactly one kit.
  A study naming two is no label for either. 240 runs from 240 studies were labelled.
- **Gate runs excluded.** The studies of the runs the classifier was first looked at
  (SRR25056225, SRR11181954, SRR10340945, ERR4265292, SRR29534765 and two others) are not
  in the set. Those runs were chosen, not sampled.
- **Design and held-out halves, by study.** A seeded draw put 121 runs in the design half
  and 119 in the held-out half. Rules and thresholds were set on design. Held-out was read
  once, with the final version, and is reported as it fell.
- **Reads.** The first 200,000 spots of each run's SRA object, by `fastq-dump -X`. The SRA
  object holds the barcode read where ENA's fastq may not.
- **Reference instruments.** `tchem` v0.1.0 (Apache-2.0) and `rename10xrun` (GPL-3.0, at
  `a844b5f`) read the same first 100,000 spots. They are instruments here, never engines.

**A label is sometimes wrong.** Submitters write "3' v2" for a v3 kit. Where at least one
instrument agrees with the classifier's call and none contradicts it, the label is
*disputed* and counted apart, not as an error. That is the treatment `tchem`'s own
validation gives its disputed calls. An instrument that refuses or errors abstains.

## What design changed (0.1.0 → 0.2.0)

Three rules came out of the design half's errors, each named for the run that showed it:

- **A MEDIUM call needs a clear margin over the next list**, as HIGH already did.
  SRR31168852's 26-base barcode read matched v3 at 0.66 with no instrument able to call it.
- **3' v2 and 5' v1/v2 are told apart by strand only on two agreeing signals.** Poly-A
  alone, the only vote a library of an unreferenced species gets, called SRR19242687 3'
  v2 against a 5' label. `tchem` called it ambiguous.
- **A gene-expression layout needs room for its UMI**, less two bases. A 16-base read with a
  listed barcode and no UMI after it is an ATAC i5 read, not a GEX barcode read.

Then the thresholds were set on design. They are the lowest bars at which design's HIGH
calls carried no error and its MEDIUM calls one in twenty at most:

- `HIGH_NEAR` = **0.70**: 70% of first-16-base windows are a listed barcode within one
  substitution, plus a 0.20 margin over any other list, plus no contradiction;
- `MEDIUM_NEAR` = **0.70**: the same share, where a contradiction or a narrow margin denies
  HIGH.

## Held-out result

| Level | Runs | Same kit as the label | Label disputed | Wrong |
|---|---:|---:|---:|---:|
| HIGH | 72 | 66 | 6 | **0** |
| MEDIUM | 33 | 28 | 5 | **0** |
| LOW | 3 | 2 | 1 | 0 |
| ambiguous (no call) | 11 | | | |
| **all** | **119** | | | |

**0 errors in 66 HIGH calls** whose label was not disputed: the 95% upper bound on the
error rate is 4.5% (the rule of three). The same holds for MEDIUM, 0 in 28, with an upper
bound of 10.7%. That is the claim, at that precision, and no more.

On the design half the corresponding figures are 0 of 66 HIGH and 0 of 24 MEDIUM, with two
LOW calls wrong.

## What the other numbers say

- **100,000 reads are enough.** Every one of the 240 runs got the same call from its first
  100,000 spots as from its first 200,000. That answers the spike's question 3.
- **The instruments agree.** `tchem` named the same kit for 217 of the 221 runs both called.
  `rename10xrun` named the same barcode list for 206 of 221. Its other 15 are mostly
  refusals: it fails closed on variable read lengths, which is right for a renamer.
- **The labels are wrong about one time in nine.** 26 of the 240 labels were disputed by the
  reads and an instrument together:
  - 12 say 3' v2 where the reads say v3, the commonest single mistake;
  - 6 name an ATAC kit for what reads as a gene-expression library, which is a study-level
    label reaching the GEX runs of a multiome study;
  - 2 say Multiome GEX where the reads say plain v3;
  - the other 6 are singletons.
- **5' v1/v2 often goes uncalled.** All 19 ambiguous calls in both halves are v2-list runs
  undecided between 3' v2 and 5' v1/v2, and 18 are labelled 5'. A 5' barcode read stops at
  26 bases, so its TSO tail is not visible, and its cDNA read did not give two agreeing
  strand signals. That is a recall limit, reported rather than tuned away after held-out was
  read.
- **Library type:** 221 runs read as gene expression, 10 as feature barcode and 5 as VDJ.

## What this does not show

- **Precision, not prevalence.** The labelled set is the 10x runs whose metadata names a
  kit. Runs whose metadata is silent, and runs from vendors other than 10x, are not in it.
- **One copy of each run.** The calls were made on the SRA object. Whether ENA's fastqs or a
  submitted BAM give the same call is the content survey's question (`fetch inspect survey`).
- **The thresholds are 0.2.0's.** A change to any of them is a new version, and is
  benchmarked again before it is cited.
