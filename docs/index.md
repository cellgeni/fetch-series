---
title: fetch-series knowledge base
---

# fetch-series knowledge base

How to get from one public sequencing accession to another, which routes can be
trusted for that, and where the archives lose data silently.

## Start here

- **What is this identifier, and who issued it?** → [Accession types](graph/accession-types.md)
- **Resolving an accession and want to know which route to use?** → [Routes](routes/gse-to-experiment/index.md)
- **A series came back empty and you suspect it shouldn't have?** → [Pathologies](pathologies/index.md)
- **Want to report something to an archive?** → [Upstream reports](upstream/index.md)
- **Want to know how a route gets chosen at runtime?** → [The resolver](resolver.md)
- **Wondering how any of this was measured?** → [Benchmarks](benchmarks/index.md)

## The rules

| | Rule | Why |
|---|---|---|
| 1 | **Evidence before defaults.** No route ranking ships without a survey behind it. | Every plausible-sounding preference measured so far has been wrong at least once. |
| 2 | **Empty is not failure.** "The archive holds no such link" and "I could not find out" are different answers. | Conflating them made BioProject → GEO look broken when most of its empties are correct. |
| 3 | **A disagreement is the evidence.** Agreement between routes is cheap and tells you little. | The 6 series where two GEO routes disagree taught more than the 144 where they agreed. |
| 4 | **Cite the accession.** Every claim names the accession that motivated it. | "Sometimes GEO omits the relation" is not actionable; GSE135325 is. |

## What has been measured

BioProject → runs, over 12,756 BioProjects (2026-05-12), counted at the unique-run level:

| Route | Unique runs | Failed |
|---|---|---|
| ENA portal `filereport` | 784,026 | 15 |
| SRA `sra-db-be` (direct CGI) | 754,277 | 16 |
| SRA `sra-db-be` (via ELink) | 754,332 | 34 |
| NCBI `efetch` runinfo (via ELink) | 674,186 | 31 |
| NCBI `efetch` runinfo (direct `[GPRJ]`) | 673,157 | 15 |

ENA holds 111,665 runs that `efetch` never returns; about 1,800 runs are missing
from ENA. GEO series → experiment is covered under [Routes](routes/gse-to-experiment/index.md).
