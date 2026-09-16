---
title: BioProject → GEO series
---

# BioProject → GEO series

**Needs:** an NCBI-issued BioProject **Feeds:** GEO series → samples
**Routes:** 2 measured, and they are the same route in practice.

## Routes

| Route | Requests | Reads |
|---|---|---|
| `gds_direct` | 2 | `esearch db=gds` for the accession → `esummary` |
| `elink` | 3 | `esearch db=bioproject` → ELink `bioproject`→`gds` → `esummary` |

## Measured behaviour

Over the 12,114 `PRJNA` projects in the reprocessed table, 2026-05-12:

| | `gds_direct` | `elink` |
|---|---|---|
| Series found | 15,185 | 15,186 |
| Failed accessions | 1,079 | 1,078 |

**Zero accessions where the two return different series sets.** They differ by one
accession in 12,114 — `PRJNA1244410`, found by ELink only — and by one series in 15,000.

## Recommendation

**Use `gds_direct`.** Identical answers for one fewer request. The extra ELink hop buys a
single accession across the whole corpus and adds a failure mode of its own: ELink is where
the MegaLink backend errors show up (1,712 occurrences in the 2026-05 logs).

Neither route applies to `PRJEB` or `PRJDB` projects. An EBI- or DDBJ-native project has no
GEO record, so the answer is legitimately empty rather than missing — see
[routing by archive](../../graph/routing-by-archive.md).

## About those 1,078 failures

They are not API failures. Manual triage of a sample found re-created, withdrawn and
private projects: `PRJNA644294` was re-created as `PRJNA644462`, `PRJNA735853` as
`PRJNA849641`, `PRJNA857927` is gone from BioProject entirely, and `GSE207991` is private.

One class *was* a client defect, now fixed: ELink can return a history response with no
usable `querykey`, and all 27 projects that hit that resolved through the `cmd=neighbor`
fallback. "No usable history link" is not "no GEO link" — see
[the troubleshooting notes](../../bioproject2geo_troubleshooting.md).
