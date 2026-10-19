---
title: fetch-series knowledge base
---

# fetch-series knowledge base

How to get from one public sequencing accession to another, which routes can be
trusted for that, and where the archives lose data silently.

## Start here

- **What is this identifier, and who issued it?** → [Accession types](graph/accession-types.md)
- **Resolving an accession and want to know which route to use?** → [The route matrix](routes/index.md)
- **Want to see every route at once, with what each one resolved?** → [The route graph](routes/graph.md)
- **A series came back empty and you suspect it shouldn't have?** → [Pathologies](pathologies/index.md)
- **Want to report something to an archive?** → [Upstream reports](upstream/index.md)
- **Need sample-to-run links across archives?** → [The relation table](relations.md)
- **Which files should I actually download?** → [The file layer](files.md)
- **Is this even 10x?** → [The assay layer](assays.md)
- **What tissue, disease, donor is this sample?** → [The metadata layer](metadata.md)
- **Replacing `fetch10xmeta`?** → [The parity record](fetch10xmeta-parity.md)
- **Want to know how a route gets chosen at runtime?** → [The resolver](resolver.md)
- **Wondering how any of this was measured?** → [Benchmarks](benchmarks/index.md)
- **Want to know *why* a route is ranked where it is?** → [Decisions](decisions/index.md)

## The rules

| | Rule | Why |
|---|---|---|
| 1 | **Evidence before defaults.** No route ranking ships without a survey behind it. | Every plausible-sounding preference measured so far has been wrong at least once. |
| 2 | **Empty is not failure.** "The archive holds no such link" and "I could not find out" are different answers. | Conflating them made BioProject → GEO look broken when most of its empties are correct. |
| 3 | **A disagreement is the evidence.** Agreement between routes is cheap and tells you little. | The 6 series where two GEO routes disagree taught more than the 144 where they agreed. |
| 4 | **Cite the accession.** Every claim names the accession that motivated it. | "Sometimes GEO omits the relation" is not actionable; GSE135325 is. |
| 5 | **Coverage is not correctness.** A route that answers more often can still answer worse. | `gds_summary` beats `soft_family` on every headline number and sends 190 series to a project holding 7,591 unrelated runs. |
| 6 | **A confident identifier is worse than none.** A value a search index will trust must be asserted only where an exact match supports it. | `disease = ASD` exact-matches MONDO's synonym for *atrial septal defect*, in an autism study, with exactly one hit and nothing ambiguous about the lookup. |

Each rule is a decision with evidence behind it, written up in
[Decisions](decisions/index.md) — including what would reverse it. A decision
with no stated failure condition is a preference, and preferences do not ship
here.

## What has been measured

BioProject → runs, over 12,755 BioProjects (2026-05-12), counted at the unique-run level:

| Route | Unique runs | Failed |
|---|---|---|
| ENA portal `filereport` | 784,026 | 14 |
| SRA `sra-db-be` (direct CGI) | 754,277 | 15 |
| SRA `sra-db-be` (via ELink) | 754,332 | 33 |
| NCBI `efetch` runinfo (via ELink) | 674,186 | 30 |
| NCBI `efetch` runinfo (direct `[GPRJ]`) | 673,157 | 14 |

ENA holds 111,665 runs that `efetch` never returns; about 1,800 runs are missing
from ENA. GEO series → experiment is covered under [Routes](routes/gse-to-experiment/index.md).

### The five findings that changed what this tool does

| | Finding | Where |
|---|---|---|
| **39.2%** | of runs have an ENA fastq missing the read that carries the cell barcode — downloads cleanly, checksums correctly, and no read can be assigned to a cell | [pathology](pathologies/ena-paired-library-single-fastq.md) |
| **17.6%** | of GEO series with SRA data report none through NCBI's `elink gds→sra`, with zero errors | [pathology](pathologies/elink-gds-sra-missing-links.md) |
| **125,376** | BioSamples ENA finds that NCBI's link table does not, 93.5% of them in EBI-registered projects | [route](routes/bioproject-to-biosample/index.md) |
| **every `SAMEA`/`SAMD`** | BioSample that `efetch` is asked for by accession comes back as a *different sample* — prefix stripped, remainder read as a UID, HTTP 200 | [pathology](pathologies/biosample-efetch-ignores-accession-prefix.md) |
| **41%** | of the BioProjects `esummary db=gds` recovers for series GEO leaves empty are species-level umbrellas | [route](routes/gse-to-bioproject/gds-summary.md) |
| **0.77%** | of runs that were reprocessed successfully no longer exist in either archive | [route](routes/run-to-experiment/ena-filereport.md) |

Four of the five contradicted what this project's own code assumed before anyone
counted.
