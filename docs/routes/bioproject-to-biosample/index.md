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

`elink` over the reprocessed BioProjects, split by issuing archive.

!!! note "Census in progress"
    3,569 of 12,755 projects at the time of writing. The `PRJEB` and `PRJDB` rows are
    effectively complete; the `PRJNA` row will grow. The contrast is already decisive.

| Prefix | Issued by | Queried | Resolved | Resolve rate |
|---|---|---|---|---|
| `PRJNA` | NCBI | 2,928 | 2,915 | **99.6%** |
| `PRJDB` | DDBJ | 137 | 45 | 32.8% |
| `PRJEB` | EBI | 504 | 7 | **1.4%** |

The 2026-05 script run over the same projects recorded **330 failures** — ten times any SRA
route. That was a client defect, not an archive one: its ESummary call was not paged, so
any project with more than 500 BioSamples hit the UID ceiling. The harness pages at 500 and
the census has recorded **zero** failures so far.

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
