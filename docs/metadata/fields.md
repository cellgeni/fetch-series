---
title: Metadata fields
---

# Metadata fields

Twelve declared fields in `metadata/fields.py`, as data rather than code. Two
halves, chosen for two different reasons.

## Grounded: what a search index needs

Seven fields, each naming the ontologies its values may come from. The set and
its ontologies follow the CELLxGENE schema, so output lands in the vocabulary
the single-cell community already indexes on rather than in one invented here.

| field | ontology | multivalued | notes |
|---|---|---|---|
| `organism` | NCBITaxon | no | usually needs no lookup: ENA returns `tax_id` |
| `tissue` | UBERON | yes | |
| `cell_type` | CL | yes | a sorted study can legitimately carry two |
| `disease` | MONDO | yes | `normal` is a value, not an absence |
| `development_stage` | HsapDv / MmusDv / UBERON | no | ontology depends on the organism |
| `sex` | PATO | no | **not** UBERON; see below |
| `assay` | EFO | no | deliberately overlaps the assay layer |

All nine ontology ids used here were checked against OLS4 before being
declared: `uberon` (26,624 terms), `cl` (19,167), `mondo` (63,460), `efo`
(94,312), `pato` (8,625), `ncbitaxon` (2,854,537), `hsapdv` (260), `mmusdv`
(178), `hancestro` (1,310). A wrong id makes grounding return nothing, silently.

### `sex` declares PATO, and that is load-bearing

Constrained to UBERON, ZOOMA answers the property value `male` with
`UBERON:0003101` *male organism* at confidence `GOOD` -- an anatomical entity
where a phenotypic quality was asked for. Constrained to PATO it answers
`PATO:0000384` *male* at `HIGH`.

The field declaring the ontology that can actually express it is what stops the
wrong question being asked.
[The pathology](../pathologies/zooma-filter-forces-a-term.md).

### `development_stage` declares one ontology per organism

HsapDv covers human and MmusDv covers mouse, and neither covers the other, so
grounding a mouse stage against HsapDv would either fail or find a false
friend. An unlisted taxon falls back to UBERON rather than to nothing: a generic
lifecycle term is a worse answer than a species-specific one and a better answer
than refusing to ground anything for every organism this project has not
enumerated.

## Structural: what a pipeline needs and no standard records

Five fields with no ontology. They carry a normalised string and no CURIE,
because there is nothing to ground an opaque submitter label against and
anything a lookup found would be a false friend.

| field | why it is here |
|---|---|
| `donor_id` | which libraries came from one individual. Named by only 11% of datasets, and the single most valuable structural field |
| `sample_grouping` | the key a grouping was computed on. A treatment or a timepoint is not a grouping key; a donor identifier is |
| `multiplexing` | the pooling scheme, where several donors share one library |
| `timepoint` | the submitter's own label, for time-course designs |
| `age` | as written, with units where given |

`age` is deliberately **not** normalised to a number. `E14.5`, `8 weeks`,
`60-65`, `adult` and `P0` are all real values, and collapsing them to a float
would invent precision. Where a comparable claim can be made it is made by
grounding `development_stage` instead.

## Scope: every field describes a sample

All twelve are sample-scoped, and a test asserts it. That is what makes the
study-scope rule checkable: a source declared study-scoped can never
legitimately assign any of them.

The rule exists because of PRJNA434002, whose `study_title` names Autism
Spectrum Disorder for all 56 runs, controls included, while the per-sample
`disease` column is empty for every one. Applying it would mislabel the
controls. See
[the metadata layer](../metadata.md#disease-is-per-sample-and-only-one-archive-says-so).

## Cardinality is not conflict

A field being multivalued means one *source* may legitimately name several
things: a submitter writing `lung, lymph node` is describing a mixture.

It does **not** mean two sources naming different things is acceptable. Conflict
is detected per source, so ENA saying `lung` where GEO says `brain` is reported
as a disagreement even on a multivalued field. Keying conflict detection on
cardinality instead reported the second case as a mixture and hid it.

## Adding a field

A frozen dataclass instance, and nothing else:

```python
Field(
    name="self_reported_ethnicity",
    scope=Scope.SAMPLE,
    summary="Self-reported ancestry of the donor.",
    ontologies=("hancestro",),
)
```

`tests/test_metadata_fields.py::TestTheDeclarationIsData` asserts that a new
field needs no change to the core, and that a field declaring no ontology is
valid.
