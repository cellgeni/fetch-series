---
title: Route by the accession's issuing archive
---

# Route by the accession's issuing archive

**Needs:** a parsed accession **Feeds:** every route choice
**Rule:** an archive's index generally knows only its own accessions.

INSDC mirrors data between NCBI, EBI and DDBJ, so it is tempting to treat their APIs as
interchangeable views of one dataset. Their *indexes* are not interchangeable at all.

## The measurement

`bioproject->biosample:elink` — NCBI ESearch, then ELink `bioproject`→`biosample` — over the
BioProjects in the reprocessed table, split by the prefix that records which body issued
the project:

| Prefix | Issued by | Queried | Resolved | Resolve rate |
|---|---|---|---|---|
| `PRJNA` | NCBI | 12,114 | 12,028 | **99.3%** |
| `PRJDB` | DDBJ | 137 | 45 | 32.8% |
| `PRJEB` | EBI | 504 | 7 | **1.4%** |

Asking NCBI's link table about an EBI-native project answers nothing 98.6% of the time, at
three requests a go. In this corpus that is 497 projects × 3 requests spent to learn
nothing.

## How it is applied

A `Route` may declare `source_archives`, and `RouteRegistry.ranked_for()` drops any route
that cannot accept the accession in hand:

```python
REGISTRY.ranked_for(parse("PRJNA988806"), EntityType.BIOSAMPLE)
# -> ['bioproject->biosample:elink', 'bioproject->biosample:ena_filereport']

REGISTRY.ranked_for(parse("PRJEB42537"), EntityType.BIOSAMPLE)
# -> ['bioproject->biosample:ena_filereport']
```

`ranked()` answers "which routes exist between these two entity types"; `ranked_for()`
answers "which routes could actually resolve *this* identifier". The resolver and the CLI
use the latter.

## Why the accession registry records the issuing archive

[`fetch_series.accession`](accession-types.md) stores an `archive` on every parsed
accession, recovered from the prefix — and for the INSDC hierarchy, from the single leading
letter, where `S`, `E` and `D` mean NCBI, EBI and DDBJ for what is otherwise an identical
schema. `SRR1`, `ERR1` and `DRR1` are the same kind of thing issued by three different
bodies, and that difference decides which API can answer about them.

This rule is the first consumer of that field, and it is the reason the field exists.

## The inverse, which is also true — and it makes a DDBJ provider unnecessary

ENA's portal API is not an archive's own index in the same sense; it answers for all three
INSDC members. Measured on the same corpus, `bioproject->run:ena_filereport`:

| Prefix | Issued by | Queried | Resolved | Resolve rate |
|---|---|---|---|---|
| `PRJNA` | NCBI | 12,114 | 12,093 | 99.8% |
| `PRJEB` | EBI | 504 | 502 | 99.6% |
| `PRJDB` | DDBJ | 137 | **137** | **100.0%** |

Set against NCBI's 1.4% for EBI-issued projects, the contrast is the whole rule in one
table: **ask the archive that mirrors INSDC, not the archive that issued the accession** —
unless the accession's own issuer is the only one holding what you want, as GEO is for
`GSM → SRX`.

### DDBJ needs no provider of its own

The project plan carried "DDBJ — entirely unexplored" as outstanding work. It is covered:
ENA resolved every DDBJ-issued project in the corpus and returned 2,307 `DRR` runs for
them, and the tier-one corpus exercises `DRS188691` and `DRX730719` through the same ENA
routes.

A dedicated DDBJ provider would be a second way to fetch data that already arrives
complete. It is not being written, and this is the evidence for that decision rather than a
deferral.

The caveat worth keeping: this is measured for the **run-level** report. If a later
milestone needs something DDBJ holds that ENA does not mirror — submission-level metadata,
or DDBJ-specific file paths — that is a different question and needs its own measurement.

Do not declare a restriction without measuring it. A route wrongly marked NCBI-only becomes
invisible for two thirds of the graph.
