---
title: Accession types
---

# Accession types

Every namespace `fetch_series.accession` recognises, what it identifies, and which body
issues it. Validated against all 104,442 rows of the reprocessed sample table: 792,610
tokens parse, and the only unrecognised value in the whole file is the `-` placeholder.

| Prefix | Identifies | Issued by |
|---|---|---|
| `GSE` | GEO series | GEO |
| `GSM` | GEO sample | GEO |
| `GPL` | GEO platform | GEO |
| `PRJNA` | BioProject | NCBI |
| `PRJEA` | BioProject (legacy EBI, mirrored by NCBI) | NCBI |
| `PRJDA` | BioProject (legacy DDBJ, mirrored by NCBI) | NCBI |
| `PRJEB` | BioProject | EBI |
| `PRJDB` | BioProject | DDBJ |
| `SAMN` | BioSample | NCBI |
| `SAMEA` | BioSample | EBI |
| `SAMD` | BioSample | DDBJ |
| `[SED]RP` | INSDC study (secondary study accession) | NCBI / EBI / DDBJ |
| `[SED]RS` | INSDC sample (secondary sample accession) | NCBI / EBI / DDBJ |
| `[SED]RX` | INSDC experiment (library) | NCBI / EBI / DDBJ |
| `[SED]RR` | INSDC run — the unit that carries files | NCBI / EBI / DDBJ |
| `[SED]RA` | INSDC submission | NCBI / EBI / DDBJ |
| `[SED]RZ` | INSDC analysis | NCBI / EBI / DDBJ |
| `E-[A-Z]{4}-` | ArrayExpress experiment | ArrayExpress |
| `S-[A-Z]+` | BioStudies study | BioStudies |

## Two things the registry gets right that inline regexes did not

**The INSDC prefix letter carries the issuing archive.** `SRR1`, `ERR1` and `DRR1` are the
same kind of entity under three namespaces, and which archive issued one decides which API
can answer about it — see [routing by archive](routing-by-archive.md).

**`-` is not an accession.** It appears 18,623 times in the sample table as a missing-value
placeholder. It must be reported as unparsed rather than silently dropped: as a `grep -f`
pattern it matches every line of a metadata table.

## A pattern that could never match

ArrayExpress accessions are hyphenated — `E-MTAB-6505`. This project previously carried
`E-MTAB\d+$`, which cannot match a real one.
