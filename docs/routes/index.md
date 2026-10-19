---
title: The route matrix
---

# The route matrix

**All 40 declared routes carry measured evidence.** Every row below was produced
by a survey recorded in the SQLite cache and exported to
[`data/surveys/`](https://github.com/cellgeni/fetch-series/tree/main/data/surveys);
the counts here are generated from the `RouteEvidence` blocks in
`src/fetch_series/graph.py`, which is what `RouteRegistry.ranked()` reads. There
is no hand-written preference order anywhere in this project.

Regenerate this table's underlying numbers with:

```bash
uv run fetch routes list
```

The same routes are drawn as a graph, with each one's resolved, empty and
failed counts, on [the route graph](graph.md).

## How to read it

- **Queried** is how many accessions the route was actually asked about. It is
  below the corpus size where the corpus mixes entity types — `reprocessed-sample`
  holds GSMs and SRSs, and a GEO route is only asked the GSMs.
- **Results** counts *distinct* target accessions across the whole survey, not
  rows. The 2026-05 predecessor to this project counted rows, and 69,923 of its
  824,216 "runs" were within-project duplicates.
- **Failed** is accessions that produced no verdict at all. It is not the same
  as empty: "the archive holds no such link" and "I could not find out" are
  different answers, and conflating them made BioProject → GEO look broken when
  most of its empties were correct.
- **Corpus** identifies the accession set exactly, including the size of a
  seeded draw. `--limit` is deliberately not used for evidence, because it
  records verdicts against the full corpus name.

| Direction | Route | Corpus | Queried | Results | Failed |
|---|---|---|---:|---:|---:|
| `ae_experiment` → `biosample` | `sdrf` | `sample:3000@arrayexpress-ena` | 3,000 | 63,153 | **11** |
| `ae_experiment` → `file` | `sdrf` | `sample:3000@arrayexpress-ena` | 3,000 | 197,089 | **11** |
| `ae_experiment` → `study` | `idf_secondary` | `arrayexpress-ena` | 20,693 | 20,138 | **5** |
| `bioproject` → `biosample` | `elink` | `reprocessed-prj` | 12,755 | 244,324 | 0 |
| `bioproject` → `biosample` | `ena_filereport` | `reprocessed-prj` | 12,755 | 363,885 | 0 |
| `bioproject` → `experiment` | `ena_filereport` | `reprocessed-prj` | 12,755 | 481,315 | 0 |
| `bioproject` → `geo_series` | `elink` | `reprocessed` | 12,114 | 15,180 | **1078** |
| `bioproject` → `geo_series` | `gds_direct` | `reprocessed` | 12,114 | 15,179 | **1079** |
| `bioproject` → `run` | `efetch_direct` | `reprocessed` | 12,755 | 673,157 | **14** |
| `bioproject` → `run` | `efetch_elink` | `reprocessed` | 12,755 | 674,186 | **30** |
| `bioproject` → `run` | `ena_filereport` | `reprocessed-prj` | 12,755 | 783,936 | 0 |
| `bioproject` → `run` | `sra_be_direct_cgi` | `reprocessed` | 12,755 | 754,277 | **15** |
| `bioproject` → `run` | `sra_be_elink` | `reprocessed` | 12,755 | 754,332 | **33** |
| `bioproject` → `study` | `ena_filereport` | `reprocessed-prj` | 12,755 | 12,748 | 0 |
| `biosample` → `run` | `ena_filereport` | `sample:3000@results-of:bioproject->biosample:ena_filerepo…` | 3,000 | 4,827 | 0 |
| `experiment` → `biosample` | `ena_filereport` | `sample:3000@reprocessed-srx` | 3,000 | 2,893 | 0 |
| `experiment` → `run` | `ena_filereport` | `sample:3000@results-of:gse->experiment:soft_family@reprocessed-gse` | 3,000 | 4,811 | 0 |
| `geo_sample` → `biosample` | `acc_cgi` | `sample:1000@reprocessed-sample` | 825 | 822 | **3** |
| `geo_sample` → `experiment` | `acc_cgi` | `sample:1000@reprocessed-sample` | 825 | 822 | **3** |
| `geo_sample` → `geo_series` | `acc_cgi` | `sample:1000@reprocessed-sample` | 825 | 849 | **3** |
| `geo_series` → `bioproject` | `gds_summary` | `reprocessed-gse` | 13,045 | 12,830 | 0 |
| `geo_series` → `bioproject` | `soft_family` | `reprocessed-gse` | 13,045 | 12,572 | **7** |
| `geo_series` → `experiment` | `elink_gds_sra` | `reprocessed-gse` | 13,045 | 178,879 | 0 |
| `geo_series` → `experiment` | `soft_bioproject_ena` | `reprocessed-gse` | 13,045 | 290,462 | **7** |
| `geo_series` → `experiment` | `soft_family` | `reprocessed-gse` | 13,045 | 308,639 | **7** |
| `geo_series` → `geo_sample` | `gds_summary` | `reprocessed-gse` | 13,045 | 319,023 | 0 |
| `geo_series` → `geo_sample` | `soft_family` | `reprocessed-gse` | 13,045 | 319,023 | **7** |
| `geo_series` → `run` | `elink_gds_sra` | `reprocessed-gse` | 13,045 | 463,803 | 0 |
| `run` → `ae_experiment` | `ena_alias` | `results-of:biosample->run:ena_filereport@results-of:ae_ex…` | 1,547 | 6 | 0 |
| `run` → `biosample` | `ena_filereport` | `sample:3000@reprocessed-srr` | 3,000 | 2,880 | 0 |
| `run` → `experiment` | `ena_filereport` | `sample:3000@reprocessed-srr` | 3,000 | 2,888 | 0 |
| `run` → `file` | `ena_fastq` | `sample:3000@reprocessed-srr` | 3,000 | 4,466 | 0 |
| `run` → `file` | `ena_sra` | `sample:3000@reprocessed-srr` | 3,000 | 0 | 0 |
| `run` → `file` | `ena_submitted` | `sample:3000@reprocessed-srr` | 3,000 | 601 | 0 |
| `run` → `file` | `sdl` | `sample:3000@reprocessed-srr` | 3,000 | 3,085 | 0 |
| `run` → `geo_sample` | `ena_alias` | `sample:3000@reprocessed-srr` | 3,000 | 2,395 | 0 |
| `sample` → `run` | `ena_filereport` | `sample:3000@reprocessed-srs` | 3,000 | 8,359 | 0 |
| `study` → `ae_experiment` | `biostudies_search` | `sample:3000@results-of:bioproject->study:ena_filereport@r…` | 3,000 | 101 | 0 |
| `study` → `bioproject` | `ena_filereport` | `sample:3000@results-of:bioproject->study:ena_filereport@r…` | 3,000 | 2,999 | 0 |
| `study` → `run` | `ena_filereport` | `sample:3000@results-of:bioproject->study:ena_filereport@r…` | 3,000 | 205,175 | 0 |

## What the failures are

Every non-zero **Failed** figure in the table falls into one of two groups, and
only the first is an archive refusing an accession or a file that no longer
exists:

- **Accessions or files that are gone.** The 7 GEO series whose SOFT family file 404s,
  GSE207991 among them, which is private; the 5 ArrayExpress studies whose IDF
  is a genuine 404 while the study record and the search index still carry them;
  the 3 GEO samples whose `acc.cgi` record cannot be fetched; the 11 studies whose
  SDRF 404s on both SDRF routes, two of which register no files at all and nine
  `E-GEOD` imports whose IDF exists without one
  ([the SDRF census](ae-experiment-to-file/index.md)); and, by manual
  triage of a sample rather than of all of them, most of the 1,078–1,079
  BioProject → GEO series failures, which are re-created, withdrawn or private
  projects ([the triage](bioproject-to-geo-series/index.md#about-those-1078-failures)).
- **NCBI routes failing on the request.** The 14–33 failures of the four NCBI
  BioProject → run routes, whose declared pathologies include `sra-db-be` timing
  out on very large projects and ELink's backend flakiness. These are 2026-05
  script figures that were never re-censused through the harness
  ([why](bioproject-to-run/index.md#why-the-ncbi-routes-were-not-re-censused)), so
  they have not been separated into transient and permanent.

Client-side losses do not appear in the table, because each one found was fixed
and its route re-run:
[39 series lost to a UID list too long for a URL](gse-to-experiment/index.md#the-39-failures-were-ours-not-ncbis)
(that route now fails on nothing), and the
[330 BioProjects an unpaged ESummary call failed](bioproject-to-biosample/index.md#the-paging-fix-recovered-94895-biosamples)
in the 2026-05 script, which fail on nothing through the harness.
