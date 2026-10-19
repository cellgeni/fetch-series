---
title: What a read sample can prove
---

# What a read sample can prove

**Decision:** `fetch inspect` classifies a run from the first 100,000 reads of each file. A
call from that sample may *refuse* a source only on positive evidence that the source
cannot be used. It never refuses on absence of evidence, and every call records the
classifier version that made it.

## The first reads are not a random sample

Files are written in the order a sequencer, an aligner or an archive produced them, and the
first reads are a stratum of that order:

- **A coordinate-sorted BAM starts at the start of chr1.** SRR10377488's Cell Ranger BAM
  has 680,000 records in its first 32 MB. All of them are in chr1's first megabase, and 97%
  are secondary alignments to repeats and to a mitochondrial insertion near 630 kb. Read for
  strand, they said the 3' library was mostly antisense. The barcodes are unaffected, since
  every read of every cell carries one. So on a coordinate-sorted BAM the classifier reads
  barcodes and refuses to measure strand or library type.
- **A fastq starts at the first tile of the first lane.** Edge tiles carry more `N`s and
  lower quality. That is why barcodes are matched within one substitution, with `N` as a
  wildcard.
- **A multi-lane run is one lane first.** A run whose lanes hold different libraries (a
  GEX lane and a feature-barcode lane in one SRA run) is called on the first lane's library.
  That is a limit of the method, and the call says which file it read.

What the first reads *are* a fair sample of is a library's **structure**: which reads
exist, how long they are, where the barcode starts, what follows the UMI. Those are the
same for every read of the file, which is why a few thousand reads already say them.

## Refusing a source needs positive evidence

The file layer's refusal path ([files](../files.md)) will drop a candidate only on a
*proven-unusable* verdict. The standard for one:

- **no read of 50 bases or more** (there is no cDNA read to align), or
- **no read of 26 bases or more whose first 16 bases are a listed barcode above
  background** (there is no barcode read). The background is what the list matches by
  chance: 0.016% exact for the v2 list, 0.16% for v3.

**Geometry alone never proves absence.** A file of 150-base reads may hold the barcode read
sequenced long. A 28-base-only run (SRR30132199) is unusable because it has no cDNA read,
not because 28 is an unusual length.

A library that is not gene expression gets `fit=None`. Whether a VDJ or feature-barcode
library is usable depends on a pipeline this project does not run.

Undetermined is not unfit. A CRAM read without pysam, an SRA object without sra-tools, or a
fetch that failed all leave a source *undetermined*, and undetermined has no effect on
recommendation.

## The classifier is a versioned instrument

Every call carries `version`. A threshold change is a new version, never an edit of an old
one, for the same reason a route's `RouteEvidence` carries a date: a call cited in the KB
must be reproducible from its version and its reads. The thresholds are set on the
benchmark's *design* half and reported on its *held-out* half, by study, seeded. Until that
has happened they are provisional, and `THRESHOLDS_SET_ON` says so.

The thresholds were first looked at on the gate runs: SRR25056225, SRR11181954,
SRR10340945, ERR4265292 and SRR29534765. Those runs are chosen, not sampled, so their
studies are excluded from the benchmark.

## What the spike settled (S0, 2026-10-19)

| Question | Answer |
|---|---|
| Does sra-tools read one specific object? | Yes. `fastq-dump -X N --split-files <url>` reads a given object's first spots. SRR25056225's Normalized object reads 28 + 91 bases with real qualities; its Lite object reads the same bases with `?` throughout. An object is attributed by its URL, so no quality-type fallback is needed |
| Does pysam read a range-fetched CRAM prefix? | Yes, for an unaligned CRAM. The first 32 MB of ERR13855116's `27865_1#3.cram` read 100,000 records in 0.1 s with `fetch(until_eof=True)`: paired 0x40/0x80, R1 26 bases, R2 96, `BC`/`QT` tags. An unaligned CRAM needs no reference. An aligned one needs EBI's registry (`REF_PATH`) |
| How stable is a call at 100,000 reads against 200,000? | Measured in the benchmark: each run is called on both |
| On what terms are the barcode lists available? | 10x's lists are under 10x's terms. They are fetched at runtime from `Teichlab/scg_lib_structs` at a pinned commit, sha256-checked, and never redistributed. Flex's list ships only inside Cell Ranger, so Flex is not a layout here |
| A 5' gate run | `SRR11181954` (GSE145926). Its metadata says exactly `10x_5p_v1_v2`, and its reads are R1 26 bases and R2 100 bases, the same geometry as 3' v2. It is classified 5' v1/v2 at HIGH, with the cDNA read 13% sense |

Two premises in the plan were wrong, and both are corrected here:

- **ERR4265292's BAM has no `10x_bam_to_fastq` header.** It was written by
  `fastq_pre_barcodes`. It keeps the cell barcode in `CR`/`CY` and the UMI in the SAM
  specification's `RX`/`QX`, not 10x's `UR`/`UY`. The barcode read is there all the same,
  16 + 12 bases, and the reader rebuilds it from whichever UMI tags are present. Its SRA
  object holds only the 91-base read.
- **`rename10xrun` is GPL-3.0, not MIT.** That is the licence of the repository it lives in.
  The method is reimplemented and cited, as before.
