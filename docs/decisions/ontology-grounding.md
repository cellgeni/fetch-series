---
title: How values are grounded in ontologies
---

# How values are grounded in ontologies

**Status:** in force · **Implemented in:** `metadata/ground.py`,
`providers/ols.py`, `providers/zooma.py` ·
**Enforced by:** `tests/test_metadata_ground.py`,
`tests/pathologies/test_zooma_filter_forces_a_term.py`

A tissue recorded as `Brain, prefrontal cortex` cannot be searched for. A tissue
recorded as `UBERON:0000451` can. Getting from one to the other is the whole
point of this layer, and it is also where it can do the most damage: **a wrong
CURIE is worse than no CURIE**, because the CURIE is the part a search index
trusts.

Every rule below exists because the obvious implementation produced a
confidently wrong answer.

## Two free services, and neither filter does what it says

Both findings were measured against the live APIs while the providers were
written, and both are load-bearing rather than defensive.

### OLS `exact=true` does not return only exact matches

`q=male&ontology=pato&exact=true` returns **40** documents. The first is
`PATO:0000384` *male*; the rest include `PATO:0040049` *male with DSD* and
`PATO:0000890` *male sterile*.

Taking the top hit works here and is trusting a relevance rank, which is not a
measurement. So every document is re-checked locally: it is accepted only if its
own label, or one of its own synonyms, equals the query after casefolding and
whitespace collapse.

The upside measured at the same time: **synonyms are indexed**, so `PBMC`
resolves to `CL:2000001` *peripheral blood mononuclear cell* with no synonym
table of this project's own.

### OLS `ontology=cl` does not restrict results to CL

The same request returns `GO:0032943` *mononuclear cell proliferation*, because
CL imports GO. The returned CURIE's prefix is therefore checked against the
field's declared set, and `is_defining_ontology` is required.

That second condition is not redundant. A term looked up by CURIE comes back
**once per ontology that imports it** -- `UBERON:0000451` returns 8 documents,
the first of them from `ado` -- and only one is the ontology that defines it.

### ZOOMA's filter selects which wrong answer you get

| value | constrained to | returns | correct |
|---|---|---|---|
| `male` | *nothing* | a MIxS **restroom-gender** term, `HIGH` | no |
| `male` | `uberon` | `UBERON:0003101` *male organism*, `GOOD` | no |
| `male` | `pato` | `PATO:0000384` *male*, `HIGH` | yes |
| `tumour adjacent normal lung` | `mondo` | `MONDO:0006049` *lung cancer*, `GOOD` | no, it is a control |

The filter forces a term out of whatever ontology it is given. Two things follow
and both are mandatory: the **field** declares the ontology that can express it
(`sex` declares PATO, not UBERON), and the returned CURIE is re-checked against
that declaration.

[The pathology page](../pathologies/zooma-filter-forces-a-term.md) carries the
reproducer and the inverted test.

## Exact on parts, before approximate on the whole

Three steps, in this order:

1. OLS exact match on the whole normalised value.
2. OLS exact match on the value's parts, longest first.
3. ZOOMA, constrained, with the submitter's own key as the property type.

Step 2 exists because of one measurement. `Brain, prefrontal cortex` -- a real
BioSample `tissue` value, SAMN12011225 -- has no exact match, so an
implementation that reached for ZOOMA at the first miss would ask it.
Constrained to UBERON, ZOOMA returns `UBERON:0001870` *frontal cortex* at
`GOOD`, **with the correct `UBERON:0000451` ranked second**. Splitting first and
matching `prefrontal cortex` exactly gets the right term deterministically.

Longest-first matters: the specific member of a hierarchical pair is usually the
longer string, so a caller taking the first part that grounds gets *prefrontal
cortex* and not *Brain*.

`+` is deliberately not a separator. Splitting on it turned
`CD4+ and CD8+ T cells` into a `CD4` no ontology has a term for, discarding the
one piece of information in the string.

## A value naming two things becomes two claims

`lung, lymph node` is two tissues; collapsing it to one discards half a mixed
sample's description. Grounding therefore returns a **list** of claims, one per
term found.

It never returns an empty list. A value nothing grounds comes back unchanged
with no term, because returning nothing would silently delete it.

## Two things are recorded and never asserted

Both carry a stated reason rather than a confidence score. A reason is something
a reviewer can act on; a number between 0 and 1 is not.

### An abbreviation matched by synonym

`disease = ASD` on SAMN12011225, in a study whose title reads *"post-mortem
brain tissue from Autism Spectrum Disorder patients"*. OLS exact-matches `ASD`
to **`MONDO:0006664` atrial septal defect**, because that is the only MONDO term
carrying `ASD` as a synonym -- autism spectrum disorder does not carry it.

There is exactly **one** hit. Nothing about the lookup looks ambiguous, so
ambiguity detection cannot catch it. And `PBMC` to `CL:2000001` is the same
shape -- a short all-caps token matched on a synonym -- and is correct.

So the study's own text is consulted: where it contains the term's label, the
abbreviation means what the term says and the claim stands. Where it does not,
the claim carries a review reason. The term is still attached, because the value
is usually right and dropping it would lose more than it protects.

This is the one place study-level text is read, and it is used only to
*corroborate*, never to assign -- which is a different operation from the one
[the study-scope rule](../metadata.md#disease-is-per-sample-and-only-one-archive-says-so)
forbids.

### Any ZOOMA match

Asked about `CD45+ cells from a fresh kidney tumor sample` (GSE111360, recorded
identically by ENA, BioSample *and* GEO), constrained to CL, ZOOMA returns
**`CL:0001074` CD34-positive, CD56-positive, CD117-positive common innate
lymphoid precursor**. CD45 is a pan-leukocyte marker. It is not a near miss.

Tier 2 therefore asserts only what an exact label or synonym match supports.
ZOOMA's contribution is a candidate with a reviewer attached.

The label is also re-resolved through OLS rather than taken from ZOOMA's
response, because ZOOMA echoes the *queried* string back as the annotated
property. Recording that printed `CL:0001074 (CD45+ cells from a fresh kidney
tumor sample)` -- a CURIE labelled with the prose it does not mean, which hid
exactly the error above. A flagged claim renders as the submitter's string with
the CURIE marked `?`.

## No confidence threshold ships

ZOOMA returns `HIGH`, `GOOD`, `MEDIUM`, `LOW`. Nothing in this project acts on
them.

A cutoff -- "accept HIGH and GOOD" -- is a default, and defaults need a survey.
`GOOD` is the band on two of the measured wrong answers above and `HIGH` on the
third, so the bands are not ordered by correctness in anything measured yet.
Once accuracy per band is measured, a threshold can be declared with a number
beside it.

## The memo is not an optimisation detail

A study of 800 samples from one tissue asks the same question 800 times, and a
corpus-scale run asks it hundreds of thousands of times. Without memoisation
this layer would be the slowest thing in the project and the heaviest user of
two free EBI services.

It also forced the statistics to be honest: an early summary read
*"5/20 grounded, 0 ungrounded"*, three mutually contradictory numbers, because
claim counts and lookup counts were being added together. They are now counted
and reported separately.

## What would change this

- **Measured accuracy per grounding step.** It would justify a ZOOMA threshold,
  and might justify asserting an approximate match outright. It needs the
  adjudicated set [the gold-set decision](what-counts-as-a-metadata-gold-set.md)
  describes.
- **An abbreviation-expansion service.** If one could rule out *atrial septal
  defect* for a brain study by anything other than reading the study text, the
  corroboration step becomes unnecessary rather than merely narrow.
- **OLS honouring its own filters.** Both local re-checks could then be dropped.
  They are cheap, so this is not urgent.
