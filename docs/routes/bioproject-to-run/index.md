---
title: BioProject → run
---

# BioProject → run

**Needs:** a BioProject accession **Feeds:** run → files
**Routes:** 5 measured, all within 0.4% of each other once two archive caps are worked around.

The direction with the most competing implementations. Its "clear winner" was mostly two caps.

## Routes

| Route | Provider | Requests | Reads |
|---|---|---|---|
| `ena_filereport` | ENA portal | 1 | `filereport?result=read_run` |
| `sra_be_direct_cgi` | SRA backend | 2 | `esearch db=sra [GPRJ]` → `sra-db-be.cgi` |
| `sra_be_elink` | SRA backend | 3 | `esearch db=bioproject` → ELink → `sra-db-be.cgi` |
| `efetch_direct` | E-utilities | 2 | `esearch db=sra [GPRJ]` → `efetch rettype=runinfo` |
| `efetch_elink` | E-utilities | 3 | `esearch db=bioproject` → ELink → `efetch` |

## Measured behaviour

Counted at the **unique-run** level over the 12,755 BioProjects in the reprocessed table
(`reprocessed-prj`). That qualifier matters: the 2026-05 `sra_be` result files held 824,216
rows but only 754,277 distinct runs.

| Route | Unique runs | Empty | Failed | ENA's runs it lacks | Runs ENA lacks | Surveyed |
|---|---:|---:|---:|---:|---:|---|
| `efetch_elink` | 786,586 | 22 | 0 | 162 | 2,812 | 2026-10-24 to 26 |
| `sra_be_elink` | 785,656 | 22 | 0 | 851 | 2,571 | 2026-10-24 to 26 |
| `efetch_direct` | 784,976 | 23 | 0 | 1,270 | 2,310 | 2026-10-24 to 26 |
| `sra_be_direct_cgi` | 784,935 | 22 | 0 | 1,262 | 2,261 | 2026-10-24 to 26 |
| **`ena_filereport`** | **783,936** | 23 | **0** | — | — | 2026-09-15 |

- **Every NCBI route is within 0.4% of ENA.** The differences in both directions are
  fractions of a percent. Six weeks separate ENA's census from the NCBI re-survey, and runs
  released in between are on the NCBI side of that gap.
- **No failure is left.** Some projects failed a first pass and answered on retry: 2 on
  `sra_be_direct_cgi`, 4 on `sra_be_elink`, 2 on `efetch_direct` and 3 on `efetch_elink`.
  Every retry was a transport error: read timeouts, dropped connections and 502s.
  PRJNA772116, with 73,888 runs, answered `sra_be_elink` only on its fourth pass.
- **ELink and `[GPRJ]` find different experiments, and each is wrong once.** `efetch_elink`
  returns runs `efetch_direct` does not, and never the reverse:
    - **1,100 of them, in 2 projects, belong to the project.** [`[GPRJ]` misses
      them](../../pathologies/gprj-misses-experiments-of-the-project.md), as with PRJNA66167's
      411.
    - **607, in 22 projects, belong to a sibling under the same umbrella.** [ELink adds
      them](../../pathologies/elink-returns-runs-of-sibling-projects.md), as with PRJNA1033389's
      72. The resolver marks them UNCONFIRMED, because ENA does not hold them for the project.

### What the 2026-05 numbers were

| Route | Unique runs, 2026-05-12 | Failed | What capped it |
|---|---:|---:|---|
| `sra_be_elink` | 754,332 | 33 | [the history form stops at 5,000 runs](../../pathologies/sra-be-history-caps-at-5000-runs.md) |
| `sra_be_direct_cgi` | 754,277 | 15 | the same |
| `efetch_elink` | 674,186 | 30 | [one EFetch stops at 10,000 records](../../pathologies/efetch-returns-fewer-runs-than-esearch-counts.md) |
| `efetch_direct` | 673,157 | 14 | the same |

On those numbers this page said ENA holds **111,665 runs `efetch` never returns**. That was a
true set difference, over runs, from one survey. It described two archive caps, not the
routes. Measuring why is what found them.

### How the re-survey was protected

Large answers misbehave without an error, so the routes now refuse three things a client would
otherwise record as the answer:

- **A capped `sra-db-be` history.** The route asks for the project by accession instead.
- **An empty answer to a history ESearch counted.** Under load, `sra-db-be` answered
  PRJNA772116, with 73,888 runs, with an empty 200.
- **A body cut off mid-row.** PRJEB14362's 46,468 runs came back as 15,265 and as 23,748
  rows, each ending in a short row, with HTTP 200.

Each raises a retryable failure, and a project that still fails is recorded FAILED, never
EMPTY or short. In the re-survey every large project answered whole.

## Recommendation

**Use `ena_filereport`.** One request, as complete as any route, no failures across 12,755
projects, and it answers for every issuing archive — unlike the NCBI routes, which
[only reliably resolve NCBI-issued accessions](../../graph/routing-by-archive.md).

`ranked()` now puts it first on cost, not on yield. All five routes sit in one yield band and
fail nothing, so the order is ENA (one request), then `efetch_direct` and `sra_be_direct_cgi`
(two), then `efetch_elink` and `sra_be_elink` (three), each pair by route id. In the default
union mode every route is asked anyway, and the runs ENA lacks come from the NCBI routes.

The 23 projects ENA reports as empty are withdrawn, private or unreleased.
[BioProject → BioSample](../bioproject-to-biosample/index.md#why-this-route-has-25-empties-and-bioproject-run-has-23)
reports 25 through the same endpoint: two projects have runs in ENA but no BioSample on
any of them, so they resolve here and are empty there. Five of them are
the BioProjects of the same GEO series that resolved to nothing in the
[GEO census](../gse-to-experiment/index.md), so two independent routes over two different
corpora agree about which data is not released.
