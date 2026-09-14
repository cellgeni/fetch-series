---
title: GEO series → INSDC experiment
---

# GEO series → INSDC experiment

**Needs:** a GSE accession **Feeds:** experiment → run → files
**Routes:** 2 measured, both incomplete

The direction that matters most for reprocessing, and the clearest example in this
knowledge base of why a fallback *order* is not enough.

## Routes

| Route | Provider | Requests | What it reads |
|---|---|---|---|
| `gse->experiment:soft_family` | GEO FTP | 1 | `!Sample_relation = SRA:` in the SOFT family file |
| `gse->experiment:elink_gds_sra` | E-utilities | 3 | ELink `gds`→`sra`, then ESummary |

## Measured behaviour

On `geo-sample-1000`, 150 series, of which 24 carry SRA experiments:

| | SOFT family | ELink `gds`→`sra` |
|---|---|---|
| Series resolved | 21 | 18 |
| Unique experiments found | 140 | 121 |
| Failures | 0 | 0 |
| Disagreements | \-- | 6, all ELink returning nothing |

On the `hard-cases` corpus the disagreements run the other way as often as not:

| Series | SOFT family | ELink |
|---|---|---|
| GSE135325 | empty | 6 experiments |
| GSE137444 | empty | 12 experiments |
| GSE206528 | 8 experiments | empty |
| GSE203201 | 15 experiments | 12 experiments |

## Recommendation

**Take the union, and record which route produced each accession.**

Neither route dominates. SOFT misses series where GEO never recorded the SRA
relation; ELink misses series absent from the Entrez link table. The two
populations are disjoint on everything measured so far, so any strict ordering
loses data for one of them.

Prefer SOFT as the first call — one request against FTP rather than three against
a rate-limited API, and it found more on the random sample — but do not stop there
when it comes back empty.

See [ELink returns no links](../../pathologies/elink-gds-sra-missing-links.md) and
[GEO omits the SRA relation](../../pathologies/geo-omits-sample-sra-relation.md).
