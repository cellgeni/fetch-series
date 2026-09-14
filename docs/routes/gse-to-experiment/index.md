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

A census, not a sample: every one of the 13,045 GEO series in the reprocessed table,
surveyed 2026-09-14/15.

| | SOFT family | ELink `gds`→`sra` |
|---|---|---|
| Series resolved | 13,022 (99.8%) | 10,752 (82.4%) |
| Series empty | 16 | 2,293 |
| Series failed | 7 (404, private) | 0 |
| **Unique experiments** | **308,639** | **178,879** |
| Requests per series | 1 | 3 |

ELink returns **58% of the experiments** SOFT does, and reports **2,293 series (17.6%) as
having no sequencing data** when they demonstrably do.

### Where they disagree

| Class | Series | Share |
|---|---|---|
| Agree | 10,695 | 82.0% |
| Only SOFT found anything | 2,280 | 17.5% |
| Only ELink found anything | 10 | 0.1% |
| Both found different sets | 53 | 0.4% |
| Same result, different outcome | 7 | 0.1% |

Counted as experiments rather than series, across the disagreeing set: **167,273 are
SOFT-only and 3,227 are ELink-only.**

The 53 divergent-set cases split further: ELink returned a strict superset in 29, SOFT in
19, and in 5 each route held accessions the other lacked. Those 5 are the serious ones —
see [SOFT names experiments with no runs](../../pathologies/soft-names-experiments-with-no-runs.md).

## Recommendation

**Call SOFT first, fall through to ELink when it is empty, and verify that the
experiments carry runs.**

The census changed this from what the sample suggested. SOFT is not merely the better
first call, it is overwhelmingly more complete: one request instead of three, 99.8% of
series resolved against 82.4%, and 130,000 more experiments. ELink earns its place as a
fallback for the 10 series SOFT cannot see, not as a peer.

The verification step is not optional, and it is the census's most uncomfortable result.
SOFT can name experiments that exist in SRA but carry no runs at all, so a complete-looking
answer can still resolve to zero files. Completeness and correctness are separate
properties and this direction fails them separately.

See [ELink returns no links](../../pathologies/elink-gds-sra-missing-links.md) and
[GEO omits the SRA relation](../../pathologies/geo-omits-sample-sra-relation.md).
