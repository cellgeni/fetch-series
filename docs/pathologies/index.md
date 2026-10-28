---
title: Pathologies
---

# Pathologies

Archive behaviours that lose data silently. One page each: the symptom, a curl
reproducer, the affected accessions, what it is *not*, and the workaround.

Every page here has a matching test in `tests/pathologies/` that asserts the bug
**still reproduces**. Run them with `uv run pytest -m pathology`. A failure is
good news — the archive fixed something.

| Pathology | Archive | Effect |
|---|---|---|
| [ELink gds→sra returns no links](elink-gds-sra-missing-links.md) | NCBI | 25% of SRA-bearing series in a random sample report as having no sequencing data |
| [GEO omits the SRA relation](geo-omits-sample-sra-relation.md) | NCBI | Samples with SRA data look like they have none |
| [Shared umbrella BioProject](series-under-shared-umbrella-bioproject.md) | NCBI | Series with sequencing data record no BioProject, breaking project-based routing |
| [SOFT names experiments with no runs](soft-names-experiments-with-no-runs.md) | NCBI | A complete-looking experiment list resolves to zero downloadable files |
| [BioStudies 20,000-hit window](biostudies-20000-hit-window.md) | EBI | Enumerating a collection hangs at 96.6%, retrying a permanent 500 |
| [ENA fastq omits the barcode read](ena-paired-library-single-fastq.md) | EBI | **39.2% of runs.** Downloads cleanly, checksums correctly, and every read is unassignable to a cell |
| [SDRF URIs on a decommissioned mirror](ae-sdrf-points-at-decommissioned-mirror.md) | EBI | Trusting them discards the real submission; two studies look identical and are not |
| [BioSample efetch ignores the accession prefix](biosample-efetch-ignores-accession-prefix.md) | NCBI | `id=SAMEA11294529` returns a rumen bacterium where a human liver sample was asked for, with HTTP 200 |
| [ZOOMA's filter forces a term](zooma-filter-forces-a-term.md) | EBI | Constraining to an ontology selects which wrong answer you get, at confidence `GOOD` |
| [ELink's MegaLink backend fails in bursts](megalink-backend-flakiness.md) | NCBI | A backend message inside an HTTP 200 reads as "no links": 5,107 log lines on 2026-05-11, the dominant failure signal |
| [ENA's fastq carries Lite qualities](ena-fastq-is-lite-quality.md) | EBI | 122 of 452 NCBI runs' ENA fastqs read `?` for every quality, where NCBI serves real ones |
| [ENA's fastq splits through the barcode read](ena-fastq-splits-the-barcode-read.md) | EBI | Two files listed, every base present, and no read starts with a cell barcode (SRR14522447) |
| [ENA lists a fastq it does not serve](ena-serves-a-listed-fastq-wrongly.md) | EBI | 2 of 4,466 listed fastqs are empty directories behind a 301, listed with a size and an md5 |
| [ESummary refuses more than 500 UIDs](esummary-500-uid-limit.md) | NCBI | The refusal arrives inside an HTTP 200 with no `result`, which reads as "nothing found" |
| [SRA's ESummary buries its accessions](esummary-sra-buries-accessions-in-expxml.md) | NCBI | Every accession is inside an XML string in the JSON; reading fields finds none |
| [An IDF does not declare its ENA study](ae-idf-missing-secondary-accession.md) | EBI | 551 of 20,693 ArrayExpress studies with an ENA link name none in the IDF |
| [BioStudies lists 25 files by default](biostudies-silent-25-item-default.md) | EBI | A study with 43 files appears to register 25, unless `pagination.total` is read |
| [A series' BioProject holds more than the series](experiments-outside-the-series-bioproject.md) | NCBI | 77 series get 5,115 experiments their SOFT file does not name, by asking for their project |
| [One EFetch stops at 10,000 runs](efetch-returns-fewer-runs-than-esearch-counts.md) | NCBI | PRJEB14362's 46,468 runs come back as 5,387 from one request, with HTTP 200; the reason ENA once held "111,665 runs efetch never returns" |
| [sra-db-be stops at 5,000 runs, and is cut off under load](sra-be-history-caps-at-5000-runs.md) | NCBI | PRJEB82511's 7,263 runs come back as 10,000 rows of 5,000; under load an answer ends mid-row or is empty, with HTTP 200 |
| [`[GPRJ]` misses experiments its project holds](gprj-misses-experiments-of-the-project.md) | NCBI | PRJNA66167's SRX135150 names the project in every record and is not under `PRJNA66167[GPRJ]`: 1,100 runs in 2 projects |
| [ELink gives a BioProject its siblings' runs](elink-returns-runs-of-sibling-projects.md) | NCBI | PRJNA1033389's 8 runs come back as 80, with 72 from projects under the same umbrella: 607 runs in 22 projects |
| [BioStudies answers 404 for a file it holds](biostudies-files-404s-registered-files.md) | EBI | E-MTAB-13459's IDF answers 404 at its address and 200 on the mirror: 328 of 3,000 ArrayExpress IDFs on 2026-10-26 |
| [An SDRF split across two files](ae-sdrf-split-across-files.md) | EBI | E-GEOD-29264 has no `.sdrf.txt`; its IDF names a hyb and a seq SDRF. Every SDRF route failed 9 of 3,000 studies. Not a fault in the archive |
| [BioStudies serves a file cut off at 4,096 bytes](biostudies-serves-a-file-cut-at-4096-bytes.md) | EBI | E-MTAB-9791's SDRF, registered at 30,836 bytes, is served as its first 4,096 with HTTP 200 and a matching Content-Length: 4 of its 24 runs, read as all of them |

## Known, declared, and not yet written up

None, since 2026-10-26. A route can declare a behaviour it exhibits before that behaviour has
a page, and such a slug goes in `PATHOLOGY_BACKLOG` in `graph.py` rather than being left
implicit: `--explain` names it and says *"not yet written up"* instead of offering a path to a
file that does not exist. Three tests keep the backlog honest:

- a declared slug must be a page or a backlog entry;
- a written page must leave the backlog;
- a page no route declares is a warning the resolver can never issue.

## Reporting upstream

A pathology page is not finished until it can be handed to the archive. Drafts
live in [upstream/](../upstream/index.md), each with the command that regenerates
its affected-accession list, so a report is current on the day it is filed rather
than on the day it was written.

## Retired: declared once, not reproduced

A declared behaviour that no longer reproduces is retired here, with the date and how it was
checked, rather than left declared on a route as a claim nothing supports.

| Slug | Archive | Retired | Why |
|---|---|---|---|
| `gprj-falls-through-to-all-fields` | NCBI | 2026-10-26 | No `[GPRJ]` search fell through. The 24 projects where `[GPRJ]` and ELink disagree, the 22 `[GPRJ]` still finds empty, and an accession that does not exist all kept `[GPRJ]` in their query translation; the empty ones answer `phrasesnotfound`. What does go wrong is [narrower](gprj-misses-experiments-of-the-project.md), and ELink's [opposite](elink-returns-runs-of-sibling-projects.md). |
| `sra-be-timeout-on-large-projects` | NCBI | 2026-10-23 | It rested on one `ReadTimeout`, PRJEB9859's in the 2026-05 script survey. On 2026-10-23 all three named projects answered in full on both sra-db-be routes. What large projects meet is a [5,000-run cap and cut-off answers under load](sra-be-history-caps-at-5000-runs.md), now written up. |
| `superseries-carries-no-bioproject` | GEO | 2026-10-23 | Of 400 seeded series of the reprocessed corpus, 62 are SuperSeries, and all 62 carry their own BioProject relation. The SubSeries are the ones that often carry none (GSE287249's three siblings, GSE201855's), and the SuperSeries is where their project is found. Nothing measured the claim as written. |
| `elink-history-missing-querykey` | NCBI | 2026-10-23 | In 2026-05, 27 BioProjects got an ELink `cmd=neighbor_history` answer with no usable `querykey` (`docs/bioproject2geo_troubleshooting.md`). On 2026-10-23 all four named examples (PRJNA1214811, PRJNA1088541, PRJNA672686, PRJNA1169288) answered with a `querykey`, from UIDs and from an ESearch history alike. The routes use `cmd=neighbor`, which needs no history, so nothing depended on it. |
