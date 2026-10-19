---
title: Why fetch-series reads the reads itself
---

# Why fetch-series reads the reads itself

**Decision:** `fetch inspect` reads the first reads of each file a route found and
classifies them with fetch-series' own code. Two existing tools do the same job,
`tchem` and `rename10xrun`. Neither is called at runtime. Both are cited for method and
run beside ours, in the benchmark only, as reference instruments.

## The question metadata cannot answer

The [assay layer](../assays.md) reads what a submitter *wrote*. That answers a lot, and
leaves two things open that only the reads settle:

- **Whether a source holds the library at all.** ENA's derived fastq for SRR29534765 is one
  file of 101-base reads, every name ending `/3`. The SRA object of the same run holds
  three reads: 8 (index), 26 (barcode) and 101 (cDNA). The metadata is identical for both,
  because it describes the library, not the copy. ERR4265292 is the mirror image: its SRA
  object holds the 91-base cDNA read alone, while the submitted BAM carries the barcode
  read in its tags. Which copy is usable is a property of the copy.
- **Which kit, where the kits share a list.** 3' v2 and 5' v1/v2 draw barcodes from one
  list, with the same barcode and UMI lengths. Only the strand the cDNA read lies on, or
  what follows the UMI, separates them, and both are in the reads.

## Why not call `tchem` or `rename10xrun`

**`rename10xrun`** (in [nf-reprocessing-public-10x](https://github.com/cellgeni/nf-reprocessing-public-10x),
GPL-3.0) is the pipeline's own: it renames a run's fastqs for Cell Ranger and STARsolo, and
fails closed. It decides how reads are parsed, which is the right job for something that
stops when unsure. It is not built to say *why* a source is unusable, and it reports 3' v2
against 5' v1/v2 as one class (`10x_3p_v2_or_5p_v1_v2`), because its whitelist cannot
separate them.

**`tchem`** ([TusslChemistry](https://github.com/Tussl-Co/TusslChemistry), Apache-2.0) is a
library classifier with a strand measurement that separates those two. It is a Rust binary
with 25 layouts across vendors.

Calling either from here would:

- **put someone else's thresholds behind this project's evidence.** A call that feeds the
  file layer's refusal path has to be one whose thresholds were set on this project's
  benchmark, over this project's corpus, and reported on its held-out half. A dependency's
  thresholds move with its releases;
- **make the core need a compiled tool.** The FASTQ and BAM readers and the classifier use
  the standard library alone. Only CRAM and SRA need anything more: pysam and sra-tools,
  found at runtime, in an optional extra and a second image;
- **lose the provenance shape.** Every call here records its version, its evidence and its
  contradictions in the same `detail` a link survey records its checks in. That is what lets
  [`link_splits.json`](../graph/catalogue.md#link-surveys) gain a fit column for read-file
  links.

So the method is reimplemented and both are cited for it:

- from `rename10xrun`: assigning R1, R2, I1 and I2 by geometry and name, setting index
  reads aside, checking the mate set, and ATAC barcodes of 16 or 24 bases;
- from `tchem`: listed barcodes within one substitution with `N` as a wildcard, UMI
  entropy, poly-T against TSO tails, swapped-file detection, strand without a reference,
  library type from TCR/BCR constant regions, the sgRNA scaffold and tag concentration, and
  levels with evidence and contradictions.

Nothing is copied. The layout rows restate facts about the kits. The chrM genomes
(NC_012920.1, NC_005089.1) and the GENCODE 44 housekeeping and constant-region extracts
are fetched by this project at pinned versions and checked by sha256.

## Barcode lists are never shipped

10x's lists are distributed under 10x's terms. The repository carries a register,
`inspect/lists.tsv`, and nothing more. For each list it records the source at a pinned
commit of [Teichlab/scg_lib_structs](https://github.com/Teichlab/scg_lib_structs), plus its
sha256, count, length and terms. `fetch inspect lists --fetch` fills a local cache and
refuses any download that does not match. The same applies to `tchem`'s own lists.

Flex is absent: its list ships only inside Cell Ranger, and a layout whose list cannot be
fetched cannot be told from any other.

## What would change this

A classifier that published its benchmark against independently labelled runs, with a
held-out error bound, and a stable machine-readable output. `tchem` publishes the first. If
it were adopted as an engine, this project's benchmark would still have to be run over it,
because the refusal path cites this corpus, not any other.
