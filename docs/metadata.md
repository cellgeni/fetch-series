---
title: The metadata layer
---

# The metadata layer

**Curation that scales, and data that can be searched.** A reprocessing corpus
of 104,442 rows cannot be curated by hand, and a tissue recorded as
`Brain, prefrontal cortex` cannot be searched for. This layer reads what the
four archives already state about each sample, normalises the keys and values,
and grounds each value in the ontology its field declares.

```bash
fetch metadata GSE111360                  # one row per sample, ontology CURIEs
fetch metadata PRJNA434002 --explain      # every claim, its source and its evidence
fetch metadata PRJNA434002 --review       # what a human should settle
fetch metadata GSE111360 --agreement      # how often the archives differ
fetch metadata GSE111360 --keys           # attribute keys no rule maps yet
```

```
sample          organism                        tissue                              sex
SAMN12039425    Mus musculus [NCBITaxon:10090]  suprachiasmatic nucleus [UBERON:0002034]   -
```

## Four tiers, kept separable

| tier | `method` | what it does | needs a key |
|---|---|---|---|
| 0 harvest | `archive` | read BioSample attributes, GEO characteristics, ENA columns, ArrayExpress SDRF | NCBI only |
| 1 normalise | `rule` | map the key to a field, the value to a vocabulary | no |
| 2 ground | `ontology` | OLS exact match, then ZOOMA, validated | no |
| 3 adjudicate | `llm` | *(not built)* the residual only | yes |

Every claim records which tier produced it. That is the point of building the
deterministic tiers first: the model's contribution has to be a figure, and it
is unanswerable if a stored value cannot say where it came from.

**Tiers 0 to 2 need no LLM key.** That is the layer, not a degraded mode.

## What it recovers that was being thrown away

Four sources were reachable through providers this project already had, and
none of them was read. The clearest single gain is structural:

> `biosample:attribute[isolate] = 5864`, in a sample titled `5864_BA9`.

NCBI's Human BioSample package defines `isolate` as the individual the sample
came from, so that is a **donor identifier** -- the rarest and most valuable
structural field there is, named by only 11% of datasets, and recovered here
from a source no previous tool in this project consulted.

The key is context-dependent -- `isolate` means a strain in a microbial record
-- so it is resolved from the archive's own package declaration and left
unmapped, with the ambiguity spelled out, when nothing says which.

## Disease is per sample, and only one archive says so

PRJNA434002 is 56 runs of post-mortem brain from an autism study. Its
**`study_title` names Autism Spectrum Disorder for every run, controls
included.** ENA's per-sample `disease` column is empty for all of them.
BioSample records `ASD` and `Control` per sample.

Two rules follow, and both are enforced rather than advised:

- A **study-scoped** value never assigns a sample-scoped field. Study titles,
  series summaries and library-construction protocols are held once, in
  `study_text`, and are visible under `--explain` without being attached to
  anything.
- A value shared by every sample is **not** thereby study-scoped. Twenty
  samples all reading `tissue: lung` are all lung; the submitter wrote it once
  per sample. Refusing invariant values would discard correct metadata from
  every single-tissue study, which is most of them.

Measured on that project (24 samples, 2026-09-16): BioSample and ENA agree
24/24 on tissue and 24/24 on sex; BioSample states a disease for 15 samples and
ENA for none.

## Grounding asserts only exact matches

Three steps, tried in this order, and the order is a finding.

1. **OLS exact match on the whole value.** Label or synonym. `PBMC` lands on
   `CL:2000001` here with no synonym table of this project's own.
2. **OLS exact match on the value's parts**, longest first.
3. **ZOOMA**, constrained to the field's ontologies.

`Brain, prefrontal cortex` has no exact match. Asked, ZOOMA returns
`UBERON:0001870` *frontal cortex* at confidence `GOOD`, with the correct
`UBERON:0000451` ranked second. Splitting the value first and matching
`prefrontal cortex` exactly gets the right term without an approximation.

Neither of OLS's filters does what its name suggests, and both were measured:

- **`exact=true` does not return only exact matches.** `q=male&ontology=pato`
  returns **40** documents, including *male with DSD* and *male sterile*. The
  first is right, which is a relevance rank and not a measurement, so every hit
  is re-checked locally against its own label and synonyms.
- **`ontology=cl` does not restrict results to CL.** The same request returns
  `GO:0032943`, because CL imports GO. The returned CURIE's prefix is checked
  against the field's declared set rather than assumed from the parameter.

## Two things are never asserted

Both are recorded, both are returned, and both carry a stated reason for a
reviewer. A claim with a reason is still searchable; it is simply never treated
as settled.

**An abbreviation matched by synonym.** `disease = ASD` on SAMN12011225, in an
autism study. MONDO holds `ASD` as a synonym of **atrial septal defect** and of
nothing else, so the lookup returns exactly one hit and looks unambiguous. It is
a different organ system. `PBMC` to `CL:2000001` is the same shape and correct,
and nothing internal to the lookup distinguishes them, so the study's own text
is consulted: where it spells the expansion out, the claim stands.

**Any ZOOMA match.** Asked about the real ENA value `CD45+ cells from a fresh
kidney tumor sample` (GSE111360), constrained to CL, ZOOMA returns
`CL:0001074` *CD34-positive, CD56-positive, CD117-positive common innate
lymphoid precursor*. CD45 is a pan-leukocyte marker. It is not a near miss.

A flagged claim renders as the submitter's own string with the CURIE marked:

```
cell_type
CD45+ cells from a fresh kidney tumor sample [CL:0001074?]
```

The `?` is the whole difference between a candidate and a finding.

## What is measured, and what is not

**Accuracy is not measured.** The evaluation corpus this layer was designed
around does not exist, and
[the reason is worth reading](decisions/what-counts-as-a-metadata-gold-set.md)
before anyone tries to build it again: sampling 120 studies from the 20,693 in
`arrayexpress-ena` found **119 with mappable curated characteristics and 0 with
an independently registered sample.** Both halves of the corpus are circular,
for opposite reasons.

What is measured instead is coverage and consistency:

- **Cross-archive agreement**, per field, per pair of sources. Consistency, not
  correctness -- two archives can agree and both be wrong -- and it is exactly
  what found every pathology in this knowledge base.
- **The unmapped-key backlog**, ordered by how many samples carry each key. A
  key on 800 samples earns a synonym entry; of 116 keys observed across 198
  datasets, **70 appeared exactly once** and are not worth declaring.
- **Grounding rate per step**, so "tier 2 grounds N% of tissue values" can be
  split into the deterministic part and the approximate part.

This is the same asymmetry [the assay layer](assays.md#what-is-not-measured-and-why)
states when it reports 90.9% recall and declines to report precision. An
unmeasured number is not evidence, and inventing one here would be the
behaviour this project exists to avoid.

## No source is ranked, and the code says so

Every declared source carries **no** `ExtractorEvidence`, because nobody has
surveyed them. That has a consequence the code enforces rather than documents:
`SampleMetadata.asserted()` **declines to pick** when two sources disagree, and
the disagreement is reported instead.

`RunRecord.set()` is first-wins, and that is justified there -- routes are
offered in the order `RouteRegistry.ranked()` computes from measured evidence.
No such measurement exists for metadata sources, so first-wins would be picking
by declaration order and calling it evidence.

```bash
fetch metadata GSE111360 --sources   # every source, and whether it is measured
```

## Two archive bugs found while building it

Both have a reproducer, an affected-accession list and an inverted regression
test, per the [pathology page contract](pathologies/index.md).

- [**`efetch db=biosample` ignores the accession prefix**](pathologies/biosample-efetch-ignores-accession-prefix.md).
  It strips the letters and reads the remainder as a UID. `id=SAMEA11294529`
  returns `SAMN11294529` -- a rumen bacterium where a human liver sample was
  asked for -- with HTTP 200 and 14 attributes. This affects **every EBI- and
  DDBJ-registered BioSample**, and ENA finds 125,271 BioSamples NCBI's own link
  table does not.
- [**ZOOMA's ontology filter forces a term**](pathologies/zooma-filter-forces-a-term.md).
  It does not prevent a wrong answer; it selects which one you get.

## It is an interface

Adding a field is a data change, and
`tests/test_metadata_fields.py::TestTheDeclarationIsData` asserts it:

```python
Field(name="ancestry", scope=Scope.SAMPLE, summary="...", ontologies=("hancestro",))
```

Adding a key synonym is a line in `vocab.CANONICAL_KEYS`. Adding a *source*
needs a provider and a declaration, and until it is surveyed it sorts behind
everything measured -- which today is everything, equally.
