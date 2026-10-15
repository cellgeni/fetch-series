---
title: What counts as a metadata gold set
---

# What counts as a metadata gold set

**Status:** in force · **Implemented in:** `metadata/gold.py` ·
**Enforced by:** `tests/test_metadata_gold.py`

The metadata layer was planned around an evaluation corpus and the corpus does
not exist. This page records what was looked for, what was measured, and why the
layer therefore reports coverage and consistency and **does not report
accuracy**.

## What was expected

ArrayExpress SDRFs carry `Characteristics[organism part|disease|cell type|sex|
developmental stage|age]`, assigned by EBI curators against a controlled column
vocabulary, plus `Comment[BioSD_SAMPLE]` and `Comment[ENA_SAMPLE]` to say which
INSDC sample each row describes. This project already resolves
GSM to BioSample to ENA sample, so the join was free.

The curator's term would be the label, the submitter's prose the input, and
20,693 studies in the committed `arrayexpress-ena` corpus the scale. No other
tool is positioned to assemble that cheaply.

## What was measured

120 studies drawn from `arrayexpress-ena`, seed 20260916, on 2026-09-16.

| | |
|---|---:|
| SDRFs read | 119 / 120 |
| ...carrying characteristics that map to a declared field | **119** |
| ...carrying an NCBI- or DDBJ-registered sample | **0** |
| labelled samples in total | 1,737 |
| of those, EBI-registered | 1,179 |

Studies carrying each field, out of 119: organism 119, tissue 68,
development_stage 39, cell_type 39, sex 36, age 30, disease 30, donor_id 19,
timepoint 14. The labels are abundant. The independence is absent.

## Both halves are circular, for opposite reasons

The corpus splits cleanly by accession prefix, and neither half works.

**`E-MTAB-*` (9,802 studies) are EBI submissions.** Their ENA and BioSample
records are generated from the same submission as the SDRF. Scoring
`ena:filereport` against such a study measures whether EBI copied its own form
consistently. E-MTAB-10553 returns **100% agreement on all 16 (source, field)
pairs** that both sides carry, and ranks nothing.

**`E-GEOD-*` (10,597 studies) are ArrayExpress's imports of GEO series**, keyed
by GSM, and the sample genuinely is NCBI-registered. This was the promising
half. But the import is mechanical. For GSM677627:

| | GEO | ArrayExpress SDRF |
|---|---|---|
| stage | `developmental stage = late embryo 20dC 4.5 hrs post-early embryo` | identical |
| genotype | `genotype = wild type` | identical |
| sex | `Sex = Hermaphrodite` | `Sex = hermaphrodite` |
| strain | `strain = N2(genotype : wild type ...)` | `strain or line`, same value |

No ontology terms were added, no values normalised, no missing fields supplied.
The only substantive change is to a **key**.

Comparing a value against a copy of itself is not a measurement.

## So accuracy is not claimed

The layer reports coverage and consistency, and says which is which. This is
the same asymmetry
[the assay layer](../assays.md#what-is-not-measured-and-why) states when it
reports 90.9% recall and reports no precision figure at all, and it rests on the
same rule: [an unmeasured number is not evidence](what-counts-as-evidence.md).

Three things *are* measured:

1. **Key-mapping agreement.** The one thing the E-GEOD import genuinely
   contributes is EBI's judgement that `strain or line` names the same property
   as `strain`. Over 10,597 studies that is a large labelled set for the key
   synonym table, which is the part of this layer most likely to be wrong. The
   join is on the **value**, not the key -- keying on the key would beg the
   question being asked.
2. **Cross-archive agreement**, per field, per pair of sources. Consistency,
   not correctness: two archives can agree and both be wrong, and one
   disagreeing with three others is suspicious rather than refuted. It is
   exactly what found every pathology in this knowledge base.
3. **Grounding rate per step**, so the deterministic contribution and the
   approximate one can be reported apart.

## The consequence in the code

`Source.evidence` is empty for every declared source, and that is not a gap
waiting to be filled by assumption. It changes behaviour:

- `ranked()` puts unmeasured sources behind measured ones and otherwise orders
  by request cost, then by source id for stability. Today that means all six
  sort equally.
- `SampleMetadata.asserted()` **declines to pick** when two sources disagree.
  `RunRecord.set()` is first-wins and that is justified there, because routes
  arrive in measured order. Here there is no order, so first-wins would be
  declaration order presented as evidence.
- `fetch metadata --sources` prints the unmeasured state rather than leaving a
  reader to infer it from an absence.

## What would change this

Any of these, and the first is cheap:

- **A hand-adjudicated set.** A few hundred samples, adjudicated once, used as
  a **gate** rather than as evidence -- the `hard-cases` model, where a route
  must produce a verdict for every entry before it earns a bigger run and none
  of those verdicts becomes `RouteEvidence`. `metadata-hard-cases` is the eight
  accessions that shape exists for.
- **A study deposited to GEO and independently curated into ArrayExpress by a
  human.** The search above found none in 120 draws; a larger or differently
  stratified draw might. The corpus name would have to record the stratification.
- **Ontology terms in an SDRF whose samples are NCBI-registered.** That
  combination would be a genuine label. It was not found.
- **A submitter-facing correction loop.** Curator decisions recorded through
  `--review` accumulate into exactly the adjudicated set the first bullet
  describes, which is the cheapest route to accuracy that does not start by
  commissioning curation.
