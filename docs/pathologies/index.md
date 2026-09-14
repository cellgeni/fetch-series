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

## Reporting upstream

A pathology page is not finished until it can be handed to the archive. Drafts
live in [upstream/](../upstream/index.md), each with the command that regenerates
its affected-accession list, so a report is current on the day it is filed rather
than on the day it was written.
