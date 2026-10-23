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
| [ENA lists a fastq it does not serve](ena-serves-a-listed-fastq-wrongly.md) | EBI | 3 of 4,466 listed fastqs: two empty directories, one 4 KB stub of a 7.2 GB file |

## Known, declared, and not yet written up

A route can declare a behaviour it exhibits before that behaviour has a page.
Ten do, and they are listed in `PATHOLOGY_BACKLOG` in `graph.py` rather than
left implicit — `--explain` names them and says *"not yet written up"* instead
of offering a path to a file that does not exist.

| Slug | Archive | What it is |
|---|---|---|
| `esummary-500-uid-limit` | NCBI | over 500 UIDs is refused with an HTTP 200 and no `result` key |
| `elink-history-missing-querykey` | NCBI | `cmd=neighbor_history` returns no usable querykey; 27/27 recovered via `cmd=neighbor` |
| `gprj-falls-through-to-all-fields` | NCBI | `[GPRJ]` silently becomes `[All Fields]` for renamed or merged projects |
| `efetch-returns-fewer-runs-than-esearch-counts` | NCBI | PRJNA940674 is the worked example |
| `esummary-sra-buries-accessions-in-expxml` | NCBI | accessions live inside an XML blob in a JSON field |
| `sra-be-timeout-on-large-projects` | NCBI | PRJEB9859, PRJEB82511, PRJEB19038 |
| `superseries-carries-no-bioproject` | GEO | a SuperSeries records none of its own |
| `experiments-outside-the-series-bioproject` | GEO | the project holds runs belonging to other series |
| `ae-idf-missing-secondary-accession` | EBI | 551 of 20,693; [measured](../routes/ae-experiment-to-study/index.md) |
| `biostudies-silent-25-item-default` | EBI | the files endpoint returns 25 of however many, silently |

Every one is real and observed. What each lacks is the **page contract** above:
a curl reproducer, a machine-readable affected-accession list, and a matching
`@pytest.mark.pathology` test. Three tests keep the backlog honest — a declared
slug must be a page or a backlog entry, a written page must leave the backlog,
and a page no route declares is a warning the resolver can never issue.

## Reporting upstream

A pathology page is not finished until it can be handed to the archive. Drafts
live in [upstream/](../upstream/index.md), each with the command that regenerates
its affected-accession list, so a report is current on the day it is filed rather
than on the day it was written.
