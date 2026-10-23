---
title: Metadata against the reads and the pipeline
---

# What kit made a sample: metadata, reads and the pipeline (2026-10)

**What was measured:** how often a sample's metadata names its 10x kit, and how often the kit
it names is the one the reads show and the reprocessing pipeline used.
**Instruments:**

- **Metadata:** the assay layer's `chemistry_of` on ENA's protocol fields and GEO's SOFT
  text.
- **Reads:** `fetch inspect`'s call, decided under classifier 0.4.0 from recorded evidence.
- **Pipeline:** the whitelist the reprocessing pipeline chose and the strand its test mapping
  found. Neither was made from metadata or by this project's reading.

**Data:** `data/benchmarks/metadata/chemistry-2026-10-23/` (`calls.csv`, `summary.json`)
and `data/benchmarks/inspect/starsolo-2026-10-22/` (`truth.csv` and the read calls).
**Regenerate:** `scripts/metadata_vs_reads.py`, then `scripts/metadata_vs_reads_report.py`.

## How it was measured

- **The record.** `truth.csv` holds 5,762 public samples from 706 datasets that the pipeline
  mapped, split into a design and a held-out half by dataset.
- **The metadata.** It was harvested on 2026-10-23 with two requests a study:
    - ENA's `read_run` report for the study's BioProject, with the protocol fields;
    - GEO's SOFT family for a GSE, joined to ENA's rows by GSM.

  Per sample, the runs' calls are merged into the union of the candidates they name. Only
  ids and counts are recorded, never protocol text.
- **The reads.** 979 samples have been read, from three draws:
    - the design draw (391), on which classifier 0.4.0 was tuned;
    - the first held-out draw (297), since looked at;
    - the fresh held-out draw (295).

  Each sample's best-called copy is decided again under 0.4.0 from its recorded evidence.
- **The comparison.** A metadata kit is compared with the pipeline's whitelist by list
  (`layouts.list_class`), and with its strand by what the kit implies: 3′ is Forward, 5′
  Reverse.

## How often the metadata names a kit

| | Samples |
|---|---:|
| one kit | **964** |
| several kits (a study that ran more than one) | 7 |
| no kit | 4,494 |
| no metadata harvested | 297 |

No metadata was harvested for 297 samples: 149 because no BioProject was found for the
dataset, and 148 because ENA had no run. **Of the 5,465 samples whose metadata was harvested,
the metadata names exactly one kit for 964, or 17.6%.**

This differs from [the assay layer's](../assays.md#measured) 32.4%. That figure counts *runs*
of `reprocessed-srr` and includes datasets the pipeline never mapped. This one counts
*samples* the pipeline mapped.

## Metadata against the pipeline

Over the 964 samples whose metadata names one kit:

| | Design | Held-out | All |
|---|---:|---:|---:|
| the list the kit implies is the pipeline's | 321 | 579 | **900** |
| another list | 20 | 44 | **64** |
| the strand the kit implies is the pipeline's | 340 | 606 | **946** |
| the other strand | 0 | 2 | **2** |

**The list is wrong for 64 samples (6.6%), from 18 datasets.** Each mistake is a field
written once for a study and applied to every sample:

| Metadata names | Pipeline used | Samples |
|---|---|---:|
| 3′ v3 | v2 | 17 |
| 3′ v2 | v3 | 16 |
| Multiome GEX | v3 | 12 |
| ATAC | v2 | 8 |
| ATAC | v3 | 8 |
| GEM-X 3′ v4 | v2 | 2 |
| Multiome GEX | v2 | 1 |

**The strand is wrong for 2 of 948 samples, both in GSE121380.** That study's protocol
text names a 5′ kit for every sample. The pipeline mapped eleven of its samples as 5′
(Reverse, paired) and two as 3′ (Forward, single-end): GSM3433583 and GSM3433584. The reads
call GSM3433583 3′ v2 at HIGH. This is the trap
[CLAUDE.md](https://github.com/cellgeni/fetch-series/blob/main/CLAUDE.md) records as *a
study-scoped value cannot assign a sample-scoped field*.

## Metadata against the reads

Over the 979 samples read:

| | Samples |
|---|---:|
| the metadata names no kit, or several | 794 |
| **the same kit** | **147** |
| another list | 19 |
| the same list, another kit | 1 |
| the reads make no call, and the metadata's kit is one of their candidates | 9 |
| the reads make no call | 9 |

**Where the two disagree, the pipeline sides with the reads every time: 20 of 20.** In 19 the
pipeline's whitelist is the list the reads named, and in the other its strand is the one the
reads' kit implies. The disagreements are the same study-level mistakes as above:

- a v2 study labelled v3, or a v3 study labelled v2;
- ATAC or Multiome named for gene-expression samples;
- GEM-X v4 named for v2 samples;
- GSE121380's 5′ for a 3′ sample.

## Where the reads abstain

The one question reads can leave open is 3′ v2 against 5′ v1/v2. The two share a list and a
geometry, and a 26-base barcode read cannot show the TSO. Under 0.4.0, 94 of the samples read
are left ambiguous there.

| | Design | Held-out | All |
|---|---:|---:|---:|
| reads ambiguous 3′ v2 / 5′ | 67 | 27 | 94 |
| the metadata names neither, or both | 60 | 25 | 85 |
| **the metadata names one, as the pipeline mapped it** | **7** | **2** | **9** |
| the metadata names one, against the pipeline | 0 | 0 | 0 |

The metadata settles 9 of the 94, and settles all 9 the way the pipeline mapped them. What
that permits is argued on [its own page](../decisions/when-metadata-may-break-a-tie.md).

## What this does not show

- **Only what the pipeline chose.** Agreement with the pipeline's whitelist and strand is
  agreement with a mapping that worked, not independent ground truth about the kit.
- **The pipeline's record only.** Every sample here was mapped as 10x. Metadata that names a
  kit for a library that is not 10x, or no kit for one that is, is not in this population.
- **Two sources of metadata.** ENA's protocol fields and GEO's SOFT text. BioSample
  attributes and ArrayExpress SDRF are not read, so a study described only there counts as
  naming no kit.
- **The held-out reads include a draw that has been looked at.** The first held-out draw's
  evidence was read to design 0.4.0. Its samples are in the held-out column because their
  datasets are, not because they are unseen.
