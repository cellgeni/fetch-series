---
title: ArrayExpress experiment → study
---

# ArrayExpress experiment → study

**Needs:** an `E-MTAB`-style accession **Feeds:** study → runs → files
**Routes:** 1 measured, with a fallback that is not yet a declared route.

## Route

`ae_experiment->study:idf_secondary` reads `Comment[SecondaryAccession]` from the study's
IDF file at `https://www.ebi.ac.uk/biostudies/files/<acc>/<acc>.idf.txt`. One request.

## Measured behaviour

All 7,179 ArrayExpress sequencing experiments, 2026-03-06:

| | Count |
|---|---|
| Resolved to exactly one secondary accession | 6,715 |
| Resolved to 2–4 | 7 |
| **No secondary accession declared** | **457** |
| Secondary accessions found | 6,732 (6,630 ERP, 20 EGA, 82 other) |

## The fallback

`E-MTAB-6505` declares no `Comment[SecondaryAccession]`, but its SDRF still carries
`Comment[BioSD_SAMPLE]` BioSample accessions, and querying ENA for one of them
(`SAMEA5053920`) recovers `ERR2861957` / `PRJEB29431` / `ERP111731`.

So the chain is: IDF secondary accession → else SDRF BioSamples → ENA. The second hop
exists as `ae_experiment->biosample:sdrf`; the composition is not yet declared as a route
of its own and the 457 have not been re-run through it. **Unverified:** how many of the 457
the fallback actually recovers.

## A trap in the file list

The BioStudies file API pages silently. With no explicit `limit` it returns the first 25 of
however many files a study registers, which for `E-MTAB-9221` would have declared 15 of its
40 registered FASTQs unregistered. Always pass a limit; `biostudies.registered_files` pages
at 500.

That matters because whether a study *registers* a file decides whether its declared URI
can be trusted — see the `E-MTAB-8060` versus `E-MTAB-9221` case in
[`fetch10xmeta`](https://github.com/cellgeni/nf-reprocessing-public-10x/tree/main/modules/cellgeni/fetch10xmeta),
which is the milestone-2 problem.
