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
| [Unpaired fastq for a paired library](ena-paired-library-single-fastq.md) | EBI | Half the reads download cleanly, checksum correctly, and produce a wrong matrix |

## Reporting upstream

A pathology page is not finished until it can be handed to the archive. Drafts
live in [upstream/](../upstream/index.md), each with the command that regenerates
its affected-accession list, so a report is current on the day it is filed rather
than on the day it was written.
