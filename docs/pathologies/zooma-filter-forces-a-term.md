---
title: ZOOMA's ontology filter forces a term rather than declining
---

# ZOOMA's ontology filter forces a term

**Archive:** EBI ZOOMA
**Affected layer:** [ontology grounding](../decisions/ontology-grounding.md)
**Status:** detected, measured, and designed around; expected behaviour rather than a defect
**Measured:** 2026-09-16

## The symptom

ZOOMA's `filter=required:[none],ontologies:[...]` parameter does not restrict
ZOOMA to answers that are *correct* within those ontologies. It restricts the
answer space and then answers anyway.

```
propertyValue=male                    -> mixs:GenderRestroomEnum#male   HIGH
propertyValue=male  ontologies:[uberon] -> UBERON:0003101 male organism   GOOD
propertyValue=male  ontologies:[pato]   -> PATO:0000384   male           HIGH
```

Asked for an anatomy term for a phenotypic quality, it supplies one. Asked with
no constraint at all, it supplies a term from a minimum-information checklist
about restrooms.

## The reproducer

```bash
z() { curl -s -G -H 'Accept: application/json' \
  "https://www.ebi.ac.uk/spot/zooma/v2/api/services/annotate" \
  --data-urlencode "propertyValue=$1" --data-urlencode "filter=required:[none],ontologies:[$2]" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print([(a['confidence'], a['semanticTags']) for a in d[:2]])"; }

z male uberon    # -> GOOD, UBERON_0003101   (male organism: an anatomical entity)
z male pato      # -> HIGH, PATO_0000384     (male: the right answer)
```

## Two more, worse

```
propertyValue=tumour adjacent normal lung   ontologies:[mondo]
  -> MONDO:0006049 lung cancer   GOOD
```

A histologically normal control, labelled with the disease it is a control for.

```
propertyValue=CD45+ cells from a fresh kidney tumor sample   ontologies:[cl]
  -> CL:0001074 CD34-positive, CD56-positive, CD117-positive
     common innate lymphoid precursor   GOOD
```

CD45 is a pan-leukocyte marker; that term is a rare precursor population. This
value is real and is recorded identically by ENA, BioSample **and** GEO for
every sample of GSE111360.

## The confidence band is not ordered by correctness

| value | band | correct |
|---|---|---|
| `male` in UBERON | `GOOD` | no |
| `tumour adjacent normal lung` in MONDO | `GOOD` | no |
| `CD45+ cells from a fresh kidney tumor sample` in CL | `GOOD` | no |
| `male` unconstrained | `HIGH` | no |
| `male` in PATO | `HIGH` | yes |
| `PBMC` in CL | `HIGH` | yes |

`GOOD` appears on three wrong answers and `HIGH` on both a wrong and two right
ones. **No threshold ships** as a result: a cutoff would be a default without a
survey behind it, and it would let the third row through while looking
principled.

## What it is not

Not a bug. ZOOMA is a best-match service over curated annotations and it does
what it says. The defect is in the obvious way to *use* it -- constrain to the
ontology you want and trust the top hit -- which reads as a safeguard and is not
one.

Not a reason to drop it. ZOOMA answers values OLS cannot match exactly, which is
the tail this layer has to handle. It is simply never the last word.

## How it is designed around

Three measures, all in `metadata/ground.py` and `providers/zooma.py`:

1. **The field declares its ontology.** `sex` declares PATO, so the UBERON
   question is never asked. `metadata/fields.py` carries the declarations and a
   test asserts `"uberon" not in by_name("sex").permitted_ontologies()`.
2. **The returned CURIE is re-checked** against the field's declared set, and
   IRIs with no OBO or EFO shape -- SNOMED, MeSH, MIxS -- are dropped rather
   than transliterated into a prefix no ontology would recognise.
3. **ZOOMA is tried last and never asserted.** Exact OLS matching on the value's
   *parts* comes first, which is what recovers `UBERON:0000451` for
   `Brain, prefrontal cortex` where ZOOMA offers *frontal cortex* at `GOOD` and
   ranks the right answer second. A ZOOMA grounding is returned with a review
   reason attached, and renders with the CURIE marked `?`.

A combination annotation -- two `semanticTags` for one property value -- is
skipped rather than half-read: it says nothing unambiguous about a single field.

## Regression test

`tests/pathologies/test_zooma_filter_forces_a_term.py`, inverted: it asserts
`male` under an UBERON filter still returns `UBERON:0003101`, that the PATO
declaration still gets the right answer, and that the wrong answer is still in a
top confidence band.

A failure means ZOOMA has changed what it returns. Re-measure grounding before
relaxing anything, and update this page.
