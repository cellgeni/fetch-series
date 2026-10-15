---
title: BioProject → run
---

# BioProject → run

**Needs:** a BioProject accession **Feeds:** run → files
**Routes:** 5 measured. One of them is worth using.

The direction with the most competing implementations and the clearest winner.

## Routes

| Route | Provider | Requests | Reads |
|---|---|---|---|
| `ena_filereport` | ENA portal | 1 | `filereport?result=read_run` |
| `sra_be_direct_cgi` | SRA backend | 2 | `esearch db=sra [GPRJ]` → `sra-db-be.cgi` |
| `sra_be_elink` | SRA backend | 3 | `esearch db=bioproject` → ELink → `sra-db-be.cgi` |
| `efetch_direct` | E-utilities | 2 | `esearch db=sra [GPRJ]` → `efetch rettype=runinfo` |
| `efetch_elink` | E-utilities | 3 | `esearch db=bioproject` → ELink → `efetch` |

## Measured behaviour

Counted at the **unique-run** level over the BioProjects in the reprocessed table. That
qualifier matters: the `sra_be` result files hold 824,216 rows but only 754,277 distinct
runs — 69,923 rows repeat within a single project — so a row count overstates that route by
about 9%.

| Route | Unique runs | Failed | Surveyed |
|---|---|---|---|
| **`ena_filereport`** | **783,936** | **0** | 2026-09-15, harness census |
| `sra_be_elink` | 754,332 | 34 | 2026-05-12, script |
| `sra_be_direct_cgi` | 754,277 | 16 | 2026-05-12, script |
| `efetch_elink` | 674,186 | 31 | 2026-05-12, script |
| `efetch_direct` | 673,157 | 15 | 2026-05-12, script |

ENA holds **111,665 runs that `efetch` never returns**, against roughly 1,800 runs missing
from it. The 2026-09 census reproduced the 2026-05 ENA figure of 784,026 to within 0.011%
while eliminating all 15 of that run's failures — so the harness agrees with the scripts it
replaces, and improves on them.

### Why the NCBI routes were not re-censused

At roughly 11 seconds per accession the `sra_be` and `efetch` routes need five hours each
for a full corpus. Their 2026-05 figures already place them 4–14% behind ENA on
completeness, and nothing has been observed that would reverse a gap that size. Re-running
them is deliberately deferred rather than forgotten; the 2026-05 numbers above are labelled
with their date and their provenance.

## Recommendation

**Use `ena_filereport`.** One request, the most complete answer, zero failures across
12,755 projects, and it answers for every issuing archive — unlike the NCBI routes, which
[only reliably resolve NCBI-issued accessions](../../graph/routing-by-archive.md).

The 23 projects it reports as empty are withdrawn, private or unreleased. Five of them are
the BioProjects of the same GEO series that resolved to nothing in the
[GEO census](../gse-to-experiment/index.md), so two independent routes over two different
corpora agree about which data is not released.

Prefer `sra_be_direct_cgi` over either `efetch` route if an NCBI route is needed for
cross-checking: same yield band as its ELink sibling, one fewer request, and half the
failure rate.
