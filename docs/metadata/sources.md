---
title: Metadata sources
---

# Metadata sources

Six declared sources across four archives. **None carries a measurement**, for
[the reason recorded in the decision log](../decisions/what-counts-as-a-metadata-gold-set.md),
so `ranked()` orders them by request cost and then by id, and
`SampleMetadata.asserted()` declines to choose when two disagree.

```bash
fetch metadata GSE111360 --sources
```

| source | cost | what it is |
|---|---:|---|
| `ae:characteristics` | 1 | ArrayExpress SDRF `Characteristics[...]` and `Factor Value[...]`, curated by EBI against a controlled key vocabulary, sometimes carrying a `Term Accession Number` |
| `biosample:attribute` | 1 | BioSample attribute pairs, with NCBI's own `harmonized_name` |
| `biosample:description` | 0 | BioSample `Title`, `Organism`, free-text paragraph |
| `ena:filereport` | 1 | ENA's descriptive `filereport` columns |
| `geo:characteristics` | 1 | GEO `!Sample_characteristics_ch1`, already `key: value` |
| `geo:soft` | 0 | GEO per-sample prose: title, source name, organism |

Cost is requests for a whole accession, not per sample. `biosample:description`
and `geo:soft` are free because they arrive in a document already being fetched
for something else.

## What each one actually gives you

### `biosample:attribute` is the richest, and was being discarded

Until this layer, `bioproject_to_biosample_elink` fetched ESummary records
whose `sampledata` field holds the entire attribute block and kept only
`accession`. There was no `efetch db=biosample` call anywhere in the project.

Two things make it the best structured source:

- **NCBI supplies a `harmonized_name`** for keys it recognises, which is a
  curated key normalisation from the archive itself and strictly better than
  guessing at one. Where it is absent, the key is the submitter's own -- and
  `sampling enviroment`, complete with the typo, is real, in SAMN12039425.
- **It records per-sample disease where ENA does not.** On PRJNA434002 it
  carries `ASD` and `Control` per sample while ENA's `disease` column is empty
  for all 56 runs.

It also carries `Id[db="SRA"]` and `Id[db="GEO"]` cross-links, harvested as
aliases. Those are deliberately **not** declared as a route: a route needs a
survey, and nobody has measured this one.

The BioSample `Package` is captured too, because it is the only non-guessing way
to read a context-dependent attribute. See
[the metadata layer](../metadata.md#what-it-recovers-that-was-being-thrown-away).

!!! warning "`efetch db=biosample` does not accept accessions"
    It strips the alphabetic prefix and reads the remainder as a UID, which
    silently returns a different sample for every EBI- and DDBJ-registered
    BioSample. Every accession here is resolved through `esearch ... [accession]`
    first and every returned record is checked against what was asked for.
    [The pathology](../pathologies/biosample-efetch-ignores-accession-prefix.md).

### `ena:filereport` is structured and sparse

ENA *defines* `tissue_type`, `cell_type`, `cell_line`, `dev_stage`, `disease`,
`sex`, `age` and `strain`, and populates them far less often than the schema
suggests. PRJNA434002 fills `tissue_type` and `sex` for every run, leaves
`disease` empty for every run, and states the disease in `study_title` -- where
it applies to the cases and not to the controls.

Three of its columns are declared **study-scoped** and can never assign a
sample field: `study_title`, `experiment_title`,
`library_construction_protocol`. The last of those is per-experiment in the
schema and per-study in practice, which
[the assay layer already documents](../assays.md#library-type-is-study-level-and-says-so).

One request serves every sample, and it is keyed on the **resolved** project or
study accession rather than on the input: `filereport` answers a GEO series with
a 400, and it answers `E-MTAB-10553` with one too.

### `geo:characteristics` is unstandardised by design

GEO imposes no vocabulary on these keys. Across 198 datasets there were **116
distinct keys, of which 28 recur and 70 appear exactly once** -- a long tail
that is also flat, which is why a synonym table is the whole job and one-offs
are best left unmapped and counted.

Two parsing rules, both of which have a test:

- Characteristics are stored as a **list of pairs, not a dict**. The line
  repeats once per attribute and a submitter may repeat a *key* too --
  `tissue: lung` then `tissue: lymph node` for a mixed sample -- and a dict
  keeps only the last. It is the same trap `csv.DictReader` sets for SDRF
  columns, with the same silent result.
- The split is on the **first colon only**. `diagnosis: stage II, ER+: yes` is
  real, and a value containing a colon is far more common than a key containing
  one.

### `ae:characteristics` is the only curated one, and cannot be scored

EBI curators assigned these keys against a controlled column vocabulary, and
some rows carry the ontology term outright. That makes it the best source and
disqualifies it as a yardstick: it is both a declared source and the label, so
scoring it against itself returns 100%. `gold.score()` excludes it and a test
asserts that it does.

!!! warning "`Term Source REF` binds to the preceding *qualifiable* column"
    E-MTAB-10553's header runs
    `Characteristics[age]  Unit[time unit]  Term Source REF  Term Accession Number`
    and its first row runs `84`, `year`, `EFO`, `UO_0000036`. `UO_0000036` is
    the Units Ontology term for *year*: it qualifies the unit, not the age.
    Binding term columns to the nearest `Characteristics[...]` instead of the
    nearest qualifiable column records every such age as ontology-grounded, and
    grounded to a unit.

`E-GEOD-*` SDRFs have no `Comment[BioSD_SAMPLE]` column, so their key falls back
to `Source Name` -- which is the GEO sample plus a suffix, `GSM677627 1`. Left
as written it joins to nothing, so the accession is extracted.

## Adding a source

A provider module, a converter in `harvest.py`, and a `Source` declaration. The
provenance string must be `archive:surface[key]`: `evidence.source_of()`
recovers the declared source by splitting on the bracket, and an inconsistent
spelling makes a claim unattributable and therefore unmeasurable.

That is not hypothetical. ENA's values were first harvested as
`ena:tissue_type`, which `source_of` could not resolve, and the whole
cross-archive agreement report came back empty.
