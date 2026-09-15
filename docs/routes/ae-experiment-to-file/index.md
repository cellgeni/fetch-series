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

## Measured behaviour

Not yet surveyed against `arrayexpress-ena`. Per
[the governing rule](../../index.md) the route carries no `RouteEvidence` until
it has been, and sorts behind every measured route in the meantime.
