---
title: Other technologies, detected and named
---

# Other technologies, detected and named (2026-10)

**What was measured:** whether `fetch inspect` names a run's single-cell technology when its
metadata names one that is not 10x, and whether it ever mistakes one for 10x.
**Instrument:** classifier 0.3.1, the first 4 MiB of each of a run's ENA fastqs.
**Data:** `data/benchmarks/inspect/technologies-2026-10-22/` (`labels.csv`, `calls-0.3.0.csv`,
`calls-0.3.1.csv`, `summary-0.3.1.json`).
**Regenerate:** `scripts/benchmark_technologies.py`, then `scripts/benchmark_technologies_report.py`.

This round only detects and names other technologies. A copy read as another technology gets
no 10x layout, no mapping block, and is never fit (rule `fit-4`). Full STARsolo parameters for
them follow this benchmark, not precede it.

## How it was done

- **Labels.** ENA runs whose `library_construction_protocol` names the technology. A protocol
  that also names 10x, Chromium or another technology is not a label for either. Thirty runs a
  label were drawn, seeded, one per study before a second from any. Ninety-five 10x controls
  come from the reprocessed corpus, which is 10x by construction.
- **Rows.** Each technology is a row of constants and the starts they may sit at
  (`inspect/classify/technologies.tsv`). The rows come from Teichlab/scg_lib_structs at the
  commit the barcode lists are pinned to, and every row cites its page. A read carries the
  technology when every constant is at one of its starts. A technology is named when more
  than half of one file's reads carry it.
- **Halves.** The draw is split by study, seeded. Studies whose runs were read by eye while
  the rows were written or changed are put in the design half, whichever half they hash to.
  Only the held-out half is reported below.

## What changed on the design half

Version 0.3.0 matched constants exactly, and named only 16 of 30 Parse runs. Reading the
misses showed two things:

- **A 30-base linker usually carries an error.** In SRR31555279 and SRR32654238 the first
  SPLiT-seq linker is plainly at base 18, but matched exactly in only 54–62% of reads. Constants
  now tolerate one substitution in ten bases. A constant shorter than ten must match exactly.
- **Parse's first linker varies, and its second does not.** In SRR32391187 the most common
  30-mer at 18 is the linker on only 148 reads of 5,000. The 22-base second linker sits at 56 in
  64% of reads exactly. Parse is now named by its second linker alone. It is microSPLiT's, so
  the row is named "microSPLiT or Parse Evercode": the two cannot be told apart this way.

That is version 0.3.1. Nothing else changed.

## Results (held-out half, 0.3.1)

| Label | Read | Named as labelled | Called 10x | What the reads named |
|---|---:|---:|---:|---|
| Parse Evercode | 11 | **9** | 0 | microSPLiT or Parse 9, unknown 2 |
| BD Rhapsody | 10 | **10** | 0 | original beads 6, enhanced beads 4 |
| inDrop | 17 | 6 | 0 | inDrop 6, unknown 11 |
| Microwell-seq | 15 | 2 | 0 | Microwell-seq 2, unknown 13 |
| HyDrop | 16 | 0 | 0 | unknown 16 |
| SPLiT-seq | 9 | 0 | 0 | unknown 9 |
| Seq-Well | 13 | 13 (unknown) | 0 | unknown 13 |
| Drop-seq | 17 | 17 (unknown) | 0 | unknown 17 |
| 10x controls | 54 | 37 | 37 | 10x 37, unknown 17 |

**No run labelled as another technology was called 10x, in either half or either version.**
That is the result this round needed: a non-10x copy is never handed a 10x layout or a
mapping block. **No 10x control was named as another technology.**

What the misses are:

- **The 17 unknown 10x controls** each have one ENA fastq and no barcode read. This is the
  ENA defect the [content survey](content-survey-2026-10.md) measured, not a detection failure.
- **Seq-Well and Drop-seq are unknown by design.** Their barcode reads are random bases and
  then poly-T, with no constant to find, so the table has no row for them. "Unknown" is the
  right answer, and it is what every one of them got.
- **The HyDrop and Microwell-seq labels are probably mostly other methods.** The held-out runs
  come from 16 and 15 different studies, one run each. A protocol that mentions microwells is
  not necessarily Microwell-seq. Of the design-half runs read by eye, two labelled HyDrop
  (SRR31699820, ERR15817930) carry no constant at all, and one labelled Microwell-seq
  (SRR25509311) carries a SPLiT-seq-like linker at base 9. That neither shows the rows wrong
  nor shows them right: only 2 of 15 Microwell-seq runs and none of 16 HyDrop runs carried the
  constants.
- **SPLiT-seq's held-out runs are all one study.** PRJDB9647's nine runs are single fastqs
  of 13 to 161 bases, none carrying a SPLiT-seq linker where the table looks. The label pool
  holds five studies in all, and the other four fell in the design half.
- **inDrop is named for 6 of 17.** inDrop v3 reads its first barcode as an index read, which
  ENA's fastqs do not carry. Ten of the 11 misses are a single fastq of 20 to 61 bases, which
  cannot hold the W1 adapter where the table looks for it.

## What this does not show

- **It is not a census.** Thirty runs a label is enough to say that detection never named a
  non-10x run 10x, and roughly how often it names each technology. It is not enough to rank
  anything, and nothing ranks on it.
- **The share threshold (50% of a file's reads) is provisional.** The 79 runs that were named
  carried their constants in 52–99% of reads. The share a run that was not named reached is
  not recorded, so how close any came is not known.
- **Parse cannot be told from microSPLiT** by these linkers, and is named together with it.
