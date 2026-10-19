---
title: ArrayExpress experiment to file
---

# ArrayExpress experiment to file

**Route id:** `ae_experiment->file:sdrf`
**Provider:** EBI BioStudies
**Direction:** `E-MTAB`/`E-GEOD` experiment → submitter fastq URLs, keyed by run
**Status:** implemented; tier one passed, census pending

The only route to the **submitter's own fastq files** for an ArrayExpress study.
ENA's `fastq_ftp` holds ENA's derivations and `submitted_ftp` holds whatever
format was deposited; neither is the submitter's fastq as named in the study.

## Requests

Two, and the second is not optional:

```bash
curl -sL 'https://www.ebi.ac.uk/biostudies/files/E-MTAB-9221/E-MTAB-9221.sdrf.txt'
curl -sL 'https://www.ebi.ac.uk/biostudies/api/v1/studies/E-MTAB-9221/files?limit=500&offset=0'
```

The SDRF supplies the URIs. The file listing decides which of them to believe —
see [SDRF URIs point at a decommissioned mirror](../../pathologies/ae-sdrf-points-at-decommissioned-mirror.md),
which is the reason this route costs two requests instead of one.

## Parsing

URIs are found **by shape, not by column name**. SDRF column headers are not
standardised across studies, and the ones that matter repeat: E-MTAB-9221 has two
`Comment[FASTQ_URI]` columns, one per mate. A row is attributed to whatever run
accession it mentions.

## Tier one

| Study | Result | Why |
|---|---|---|
| E-MTAB-9221 | 40 files | registers all 40; URIs rewritten to BioStudies |
| E-MTAB-6505 | 8 files | registers 8 fastqs, all kept |
| E-MTAB-5448 | 36 files | URIs are already on `ftp.sra.ebi.ac.uk`, so untouched |
| E-MTAB-8060 | empty | registers no fastq; every mirror URI dropped, run falls through to the ENA BAM |
| E-MTAB-9216 | empty | declares no fastq URIs at all |

Five verdicts, no failures, and the two empties are the correct answer rather
than a gap.

## Measured

**Corpus:** `sample:3000@arrayexpress-ena`, a seeded draw from the 20,693
studies BioStudies records an ENA link for. **Surveyed:** 2026-09-15.

| | |
|---|---|
| Resolved | 2,316 (77.2%) |
| Empty | 673 |
| Failed | 11 |
| Unique file URIs | **197,089** |

Median **16** files per resolved study; mean 86, which is the distribution's own
warning that a handful of studies carry thousands.

Split by provenance, the same way every other ArrayExpress route splits:

| Prefix | Studies | Names fastq URIs | |
|---|---:|---:|---:|
| `E-MTAB` | 1,398 | 1,385 | **99%** |
| `E-GEOD` | 1,550 | 879 | 57% |
| `E-ERAD` | 48 | 48 | 100% |

Unlike the [BioSample column](../ae-experiment-to-biosample/sdrf.md), which is
absent from **every** `E-GEOD` SDRF, fastq URIs survive the import into
ArrayExpress a little over half the time. That is the difference between a
column GEO has no equivalent of and one it does.

**All 11 failures are a 404 on the SDRF itself, and none is transient.** Two
are `E-MTAB-14460` and `E-MTAB-15118`, the two of the
[five studies that register no files at all](../ae-experiment-to-study/index.md#five-studies-exist-without-existing)
that this draw happened to include. The other nine are `E-GEOD` imports
(`E-GEOD-15593` and eight more) whose IDF resolves — the IDF route finds their
SRA study — but whose SDRF is not served. The same 11 fail, for the same reason,
on [the SDRF's BioSample route](../ae-experiment-to-biosample/sdrf.md); both are
read from `data/surveys/sample_3000@arrayexpress-ena/`.
