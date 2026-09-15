---
title: BioProject → BioSample
---

# BioProject → BioSample

**Needs:** a BioProject accession **Feeds:** BioSample → runs
**Routes:** 2, and which one applies depends on who issued the accession.

## Routes

| Route | Provider | Requests | Applies to |
|---|---|---|---|
| `elink` | E-utilities | 3 | **NCBI-issued projects only** |
| `ena_filereport` | ENA portal | 1 | any |

## Measured behaviour

`elink` over all 12,755 BioProjects in the reprocessed table, 2026-09-15, split by issuing
archive.

| Prefix | Issued by | Queried | Resolved | Empty | Resolve rate |
|---|---|---|---|---|---|
| `PRJNA` | NCBI | 12,114 | 12,028 | 86 | **99.3%** |
| `PRJDB` | DDBJ | 137 | 45 | 92 | 32.8% |
| `PRJEB` | EBI | 504 | 7 | 497 | **1.4%** |
| **Total** | | **12,755** | **12,080** | **675** | 94.7% |

**Zero failures**, against the 2026-05 script's **330** over the same projects.

### The paging fix recovered 94,895 BioSamples

Those 330 were a client defect, not an archive one: the script's ESummary call was not
paged, so any project with more than 500 BioSamples hit the UID ceiling. E-utilities
reports that as an HTTP 200 whose JSON body has an `error` key and no `result`, which the
script surfaced as a failure.

Paging did not merely fix the 330. It recovered data the unpaged call had been silently
truncating everywhere else:

| | 2026-05 script | 2026-09 harness |
|---|---|---|
| Unique BioSamples | 149,429 | **244,324** |
| Failed projects | 330 | **0** |

**+94,895 BioSamples, a 64% increase.** The visible failures were the small part of the
problem; the silent truncation was the large part, and nothing in the output distinguished
a truncated project from a complete one.

## Recommendation

**Dispatch on the issuing archive.** `elink` for `PRJNA`, `ena_filereport` for everything
else. `RouteRegistry.ranked_for()` does this automatically from the accession's `archive`
field — see [routing by archive](../../graph/routing-by-archive.md).

Sending an EBI-native project to NCBI's link table costs three requests and answers nothing
98.6% of the time. In this corpus that was 497 projects.

`ena_filereport` is also the cheaper route where both apply, so the only reason to call
`elink` is to cross-check, or for the small number of NCBI BioSamples not mirrored into
ENA's run-level report.

## A subtlety in what each route means

The two routes do not answer quite the same question. `elink` walks the project's declared
BioSample links, so it returns samples whether or not they carry sequencing runs.
`ena_filereport` with `result=read_run` returns samples **that have runs**, because its rows
are runs.

For a reprocessing pipeline the ENA semantics are usually what you want — a sample with no
runs has nothing to download. When counting what a project *contains*, they are not
interchangeable.
