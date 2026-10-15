---
title: GEO series → experiment, via the SOFT BioProject and ENA
---

# GEO series → experiment, via the SOFT BioProject and ENA

**Route id:** `gse->experiment:soft_bioproject_ena`
**Cost:** 2 requests — one to NCBI's FTP mirror, one to the ENA portal
**Needs:** a GSE accession **Feeds:** experiment → run → files

Read `!Series_relation = BioProject:` out of the SOFT family file, then ask the ENA
portal for that project's experiments.

## Why it exists

It is the recovery path for series that [ELink cannot
reach](../../pathologies/elink-gds-sra-missing-links.md), and the only route for this
direction that **crosses archives**: the GEO half comes from NCBI's FTP mirror and the
experiment half from EBI. It therefore shares no failure mode with either NCBI Entrez
route, which is what makes it useful as a cross-check rather than just another option.

## Measured recovery

Run against 85 series where `elink gds→sra` returned nothing but the SOFT file names
experiments (2026-09-14, drawn from `reprocessed-gse`):

| Outcome | Series | Share |
|---|---|---|
| Every SOFT experiment returned by ENA | 79 | 93% |
| Partial recovery | 0 | 0% |
| No BioProject recorded in SOFT | 6 | 7% |

The zero in the middle row matters as much as the 93%. Where the route works it is
complete — it never returned a subset of what the SOFT file names — so a caller does not
have to reason about partial answers.

## The 6 it cannot help

`GSE122357`, `GSE130731`, `GSE78298`, `GSE78299`, `GSE78395`, `GSE78416` declare SRA
relations per sample but no `!Series_relation = BioProject:` at all. Four of them
(`GSE78298`, `GSE78299`, `GSE78395`, `GSE78416`) sit under one shared umbrella project,
`PRJNA30709`, rather than having one of their own — see
[series under a shared umbrella BioProject](../../pathologies/series-under-shared-umbrella-bioproject.md).

For these the SOFT file has already answered the question directly: it names the SRX
accessions, and each resolves in ENA on its own. The BioProject hop is a cross-check that
is simply unavailable, not a dead end.

## When to prefer it

Not as a first call — it costs two requests where
[the SOFT route](index.md) costs one and answers the same question from the same file.

Use it to **confirm** a SOFT answer, or when a caller wants an experiment list that does
not depend on any NCBI index being complete. It is also the right route when the GEO
record is reachable but its samples are not, since it goes through the project rather than
through the sample relations.
