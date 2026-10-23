---
title: When metadata may break a tie
---

# When metadata may break a tie

**Status:** decided 2026-10-23 and permitted by the rule set before measuring. It is not
built, because `fetch inspect` does not harvest metadata today.
**Evidence:** [metadata against the reads and the pipeline](../benchmarks/metadata-vs-reads-2026-10.md)

## The question

`fetch inspect` cannot always tell 3′ v2 from 5′ v1/v2.

- **What the two share.** The same whitelist, a 16-base barcode and a 10-base UMI.
- **What tells them apart.** The strand of the cDNA read, and the template-switch oligo
  after the UMI.
- **Where that fails.** A 26-base barcode read cannot show the TSO, and the strand signals do
  not always agree. Under classifier 0.4.0, 94 of 979 samples read are left ambiguous there,
  and get no mapping block.

A protocol that names *"Chromium Next GEM Single Cell 5′"* would settle such a sample before
anything is downloaded. The question is whether a block may rest on it.

## The rule, set before the numbers

The round's plan, approved on 2026-10-23, said: if the metadata's 3′/5′ call matches the
pipeline's strand with zero errors on the design half, a block for an ambiguous read call may
use it. It names the metadata as its source, and it stays unvalidated until scored on the
held-out half.

## What was measured

| | Design | Held-out |
|---|---:|---:|
| metadata kit's strand against the pipeline's, at scale | **340 of 340** | 606 of 608 |
| reads ambiguous 3′ v2 / 5′, metadata names one of the two | 7 | 2 |
| …settled as the pipeline mapped it | **7 of 7** | **2 of 2** |
| metadata against the reads, where both name a kit and disagree | the pipeline sides with the reads, 20 of 20 | |

**The design half meets the rule.** The held-out half then found the failure mode, though not
on a tied sample. GSE121380's protocol names a 5′ kit for all thirteen samples. The pipeline
mapped two of them as 3′, and the reads call one of those 3′ v2 at HIGH. Had the reads
abstained on those two samples, the metadata would have broken the tie wrongly.

## The decision

Metadata may break the 3′ v2 / 5′ v1/v2 tie, and only that tie, on these terms:

1. **Only where the reads abstain.** It never overrides a read call. In all 20
   disagreements, the pipeline sided with the reads.
2. **Only where the metadata names exactly one of the two kits.** A study that names both
   says nothing about any one of its samples.
3. **Offered beside the read-derived block, never merged into it.** The block's own rule is
   that `soloStrand` comes from the strand the reads were measured on, never from a kit's
   name (`inspect/readiness.py`). A tie broken by metadata is a different kind of claim, so
   it is offered separately, as `starsolo_paired` is:
    - it names its source, metadata, and the fields that matched;
    - it carries the measured agreement above: 9 of 9 ties, and 946 of 948 samples at scale
      with both errors in one study.
4. **Validated on 9 ties.** That is a small number, and it says so.

What it buys is 9 of 94 abstentions. The other 85 have metadata that names neither kit, or
both.

## What building it needs

`fetch inspect run` reads files and harvests no metadata. Building this means two requests a
study, as `scripts/metadata_vs_reads.py` makes them:

- ENA's `read_run` report for the project;
- GEO's SOFT family.

Their result would be passed to `readiness.mapping`. That is new network code on the inspect
path, behind `redact_exception` like every other call. It is not built in this round.

## A model's baseline

The deterministic tiers now have a measured baseline on a labelled set. A tier-3 (LLM)
metadata pass would have to beat it.

- **The labelled set.** `truth.csv`: 5,762 samples the pipeline mapped, labelled by the
  whitelist it chose and the strand its test mapping found. The design half has 2,695 samples
  and the held-out half 3,067. A model's prompt would be developed on design and scored once
  on held-out.
- **The baseline to beat:**
    - **Coverage.** The deterministic tiers name exactly one kit for 964 of 5,465 harvested
      samples (17.6%).
    - **List error.** The kit implies another list than the pipeline's for 64 of 964 (6.6%).
    - **Strand error.** It implies the other strand for 2 of 948 (0.2%).
- **What a model must also do.** Decline where a study ran two kits, and not turn a
  study-level protocol into a per-sample claim. Every error measured here is that one
  mistake.

Building tier 3 is out of this round.

## What would change this

- **A tie broken wrongly.** A held-out sample where the reads abstain and the metadata names
  the other kit from the pipeline's would end the exception, not adjust it.
- **A classifier that settles the tie from the reads.** It would make the exception
  unnecessary. The reads won all 20 disagreements, so they remain the instrument to improve.
