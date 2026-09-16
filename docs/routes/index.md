---
title: The route matrix
---

# The route matrix

**All 38 declared routes carry measured evidence.** Every row below was produced
by a survey recorded in the SQLite cache and exported to
[`data/surveys/`](https://github.com/cellgeni/fetch-series/tree/main/data/surveys);
the counts here are generated from the `RouteEvidence` blocks in
`src/fetch_series/graph.py`, which is what `RouteRegistry.ranked()` reads. There
is no hand-written preference order anywhere in this project.

Regenerate this table's underlying numbers with:

```bash
uv run fetch routes list
```

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
| `bioproject` → `geo_series` | `elink` | `reprocessed` | 12,114 | 15,186 | **1078** |
| `bioproject` → `geo_series` | `gds_direct` | `reprocessed` | 12,114 | 15,185 | **1079** |
| `bioproject` → `run` | `efetch_direct` | `reprocessed` | 12,756 | 673,157 | **15** |
| `bioproject` → `run` | `efetch_elink` | `reprocessed` | 12,756 | 674,186 | **31** |
| `bioproject` → `run` | `ena_filereport` | `reprocessed-prj` | 12,755 | 783,936 | 0 |
| `bioproject` → `run` | `sra_be_direct_cgi` | `reprocessed` | 12,756 | 754,277 | **16** |
| `bioproject` → `run` | `sra_be_elink` | `reprocessed` | 12,756 | 754,332 | **34** |
| `bioproject` → `study` | `ena_filereport` | `reprocessed-prj` | 12,755 | 12,748 | 0 |
| `biosample` → `run` | `ena_filereport` | `sample:3000@results-of:bioproject->biosample:ena_filereport@reprocessed-prj` | 3,000 | 4,827 | 0 |
| `experiment` → `biosample` | `ena_filereport` | `sample:3000@reprocessed-srx` | 3,000 | 2,893 | 0 |
| `experiment` → `run` | `ena_filereport` | `results-of:gse->experiment:soft_family@reprocessed-gse` | 3,000 | 6,580 | 0 |
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
| `run` → `biosample` | `ena_filereport` | `sample:3000@reprocessed-srr` | 3,000 | 2,880 | 0 |
| `run` → `experiment` | `ena_filereport` | `sample:3000@reprocessed-srr` | 3,000 | 2,888 | 0 |
| `run` → `file` | `ena_fastq` | `sample:3000@reprocessed-srr` | 3,000 | 4,466 | 0 |
| `run` → `file` | `ena_sra` | `sample:3000@reprocessed-srr` | 3,000 | 0 | 0 |
| `run` → `file` | `ena_submitted` | `sample:3000@reprocessed-srr` | 3,000 | 601 | 0 |
| `run` → `file` | `sdl` | `sample:3000@reprocessed-srr` | 3,000 | 3,085 | 0 |
| `sample` → `run` | `ena_filereport` | `sample:3000@reprocessed-srs` | 3,000 | 8,359 | 0 |
| `study` → `ae_experiment` | `biostudies_search` | `sample:3000@results-of:bioproject->study:ena_filereport@reprocessed-prj` | 3,000 | 101 | 0 |
| `study` → `bioproject` | `ena_filereport` | `sample:3000@results-of:bioproject->study:ena_filereport@reprocessed-prj` | 3,000 | 2,999 | 0 |
| `study` → `run` | `ena_filereport` | `sample:3000@results-of:bioproject->study:ena_filereport@reprocessed-prj` | 3,000 | 205,175 | 0 |

## What the failures are

Almost every non-zero **Failed** figure in the table is an archive refusing an
accession that no longer exists, not a route defect:

- the 7 GEO series whose SOFT family file 404s, GSE207991 among them, which is
  private;
- the 5 ArrayExpress studies whose IDF is a genuine 404 while the study record
  and the search index still carry them;
- the 3 GEO samples whose `acc.cgi` record cannot be fetched.

The one genuine client-side loss —
[39 series lost to a UID list too long for a URL](gse-to-experiment/index.md#the-39-failures-were-ours-not-ncbis) —
was found by this census and fixed; that route now fails on nothing.
