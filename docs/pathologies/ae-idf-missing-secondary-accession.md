---
title: An ArrayExpress IDF does not declare the ENA study it has
---

# An ArrayExpress IDF does not declare the ENA study it has

**Archive:** EBI ArrayExpress / BioStudies
**Affected route:** [`ae_experiment->study:idf_secondary`](../routes/ae-experiment-to-study/index.md)
**Status:** re-measured 2026-10-28 with this project's reader fixed: **10 of 20,693**, not 551;
partly worked around by a fallback; reproduced 2026-10-28
**Measured:** 2026-10-28, every ArrayExpress study BioStudies records an ENA link for
(`arrayexpress-ena`), and again over the 551 this page used to count (`arrayexpress-no-secondary`)

## The number

**10 of 20,693 studies (0.05%).** Each is an `E-MTAB` submission whose IDF has no
`Comment[SecondaryAccession]` row at all, among the ArrayExpress studies whose BioStudies record
carries an ENA link. `Comment[SecondaryAccession]` is the IDF field that names the ENA study.

The route's survey of the 20,693 records 10 empties, and they are these 10. It recorded 12 until
two studies were asked again. Each was an IDF served wrong, read by a survey whose reader checked
for neither fault, and each now fails the read instead:

- **E-MTAB-11257**'s IDF was served as
  [an empty directory listing](biostudies-serves-a-directory-listing-for-a-file.md): an Apache
  `Index of /biostudies/fire/E-MTAB-/257/E-MTAB-11257/Files/E-MTAB-11257.idf.txt` page, 837
  bytes, with HTTP 200. The legacy ArrayExpress copy is 4,480 bytes and names ERP133806. Asked
  again at 21:34 UTC, the IDF was whole and resolved to ERP133806.
- **E-GEOD-35315** resolved to SRP010570 over the 551 at 13:25 UTC and was recorded EMPTY at
  15:15. Its row ends at byte 5,121 of a 5,395-byte IDF, where
  [a copy cut off at 4,096 bytes](biostudies-serves-a-file-cut-at-4096-bytes.md) loses it. What
  was served at 15:15 was not kept. Asked again at 10:45 UTC on 2026-10-29, it was whole and
  resolved to SRP010570.

The 10 are exactly the 10 the reader left empty over the 551 that afternoon. Read again after the
survey, none of the 10 was served at 4,096 bytes.

## Until 2026-10-28 this page said 551, and 540 of those were this project's reader

From 2026-09-15 this page counted 551 of 20,693 studies (2.7%) as declaring no ENA study. Read again
on 2026-10-28, 541 of the 551 declare one. The reader missed them for two reasons:

| Why it was missed | Studies | Example |
|---|---:|---|
| The study is in a later cell of the row. The reader took the first cell, which on an E-GEOD import is the GEO series | 453 (449 E-GEOD, 4 E-MTAB) | E-GEOD-10571: `Comment[SecondaryAccession]	GSE10571	SRP003805` |
| The IDF quotes every cell and spaces the tag. The reader anchored on a bare `Comment[` | 87 (81 E-ERAD, 6 E-MTAB) | E-ERAD-129: `"Comment [SecondaryAccession]"	"ERP002094"` |
| The old reader reads it today: the IDF now names the study in a row of its own | 1 | E-GEOD-47226: `SRP023111` |
| No `Comment[SecondaryAccession]` row at all | **10** (all E-MTAB) | E-MTAB-13231 |

The first two were this project's, and are fixed: `biostudies.idf_rows` reads every cell of every
row, quoted or not, and every IDF reader goes through it. The quoted dialect cost the catalogue's
IDF links too. `idf_comments` read nothing at all from 16 of the 3,000 IDFs in
`sample:3000@arrayexpress-ena`.

The E-GEOD conclusion drawn from the 551 was wrong with them. The route page used to say an IDF
route "is not" right for a GEO import and that the answer is to go to GEO. Of the 450 E-GEOD
imports counted here, every one names its SRA study in its IDF.

## What the 10 declare instead

All 10 name an ENA study under `Comment[RelatedExperiment]`. It is the study BioStudies links as
the experiment's ENA record:

| Study | `RelatedExperiment` | SDRF runs | of those in that study |
|---|---|---:|---:|
| E-MTAB-13231 | ERP133516 | 345 | **345** |
| E-MTAB-13232 | ERP133516 | 199 | **199** |
| E-MTAB-15116 | ERP166146 | 16 | 2 |
| E-MTAB-15935, -16746, -16780, -16868, -16979, -7507, -9266 | one ERP each (E-MTAB-16868 two) | 0 | |

- **E-MTAB-13231 and E-MTAB-13232 are the pathology as stated.** Every run their SDRFs list is
  in ERP133516, and neither IDF calls it its secondary accession. The study is named, in the field
  for another experiment.
- **The other 8 may be right.** Seven list no ENA run in their SDRF at all. E-MTAB-15116's ERP
  holds 2 of its 16. An experiment that deposited nothing in a study has no reason to call it its
  own. That BioStudies links those studies as the experiment's ENA record says only that BioStudies
  reads `RelatedExperiment` as one.

**Reading `RelatedExperiment` as the study would not be a fix.** Of the IDFs of
`sample:3000@arrayexpress-ena` read on the morning of 2026-10-28, 5 name an ENA or EGA accession
there. All 4 that also declare a secondary accession name something else in each field:
E-MTAB-15999's secondary accession is ERP183804 and its related experiment ERP183055. The field
names a related study, which is what it says.

## Reproducer

```bash
curl -sL https://www.ebi.ac.uk/biostudies/files/E-MTAB-13231/E-MTAB-13231.idf.txt \
  | grep -E 'SecondaryAccession|RelatedExperiment'
# Comment[RelatedExperiment]	ERP133516
curl -s 'https://www.ebi.ac.uk/ena/portal/api/filereport?accession=ERP133516&result=read_run&fields=run_accession&limit=0' \
  | grep -c ERR
# every run of E-MTAB-13231's SDRF is among them
```

E-MTAB-6505, the case that motivated the fallback, is not in `arrayexpress-ena` at all: BioStudies
links no ENA study for it. Its IDF names none either, and its SDRF's `Comment[BioSD_SAMPLE]`
SAMEA5053920 resolves, through ENA, to PRJEB29431 / ERP111731.

Checked on 2026-10-28.

## What it is not

- **Not this project's reader, any more.** The 540 above are read since 2026-10-28.
- **Not a missing study.** The ENA study exists, and BioStudies links to it.
- **Not the IDFs that answer 404.** Those studies register no files at all, and are counted as
  failures, not empties.
- **Not an IDF served wrong.** A file cut at 4,096 bytes, or served as a directory listing, also
  reads as naming no study. E-GEOD-35315 and E-MTAB-11257 above were of that kind, and are not
  counted. A cut and a directory listing now fail the read rather than reading as empty.

## Workaround

Fall back through the SDRF's BioSamples:

1. `ae_experiment->biosample:sdrf`;
2. then `biosample->run:ena_filereport`.

`fetch_series.relations` walks this composition for any ArrayExpress entry point whose IDF names no
study. Of the 10, the SDRF names BioSamples for **2**: E-MTAB-13232 (199) and E-MTAB-15116 (8), in
the 2026-09-15 survey of `ae_experiment->biosample:sdrf` over the 551. The other 8 name none.

That survey once said the fallback recovers **4.2%** of the 551, 23 studies. 21 of the 23 declare
their study in the IDF, so the IDF route itself now finds them. What the fallback adds over the IDF
is the 2.

## Affected accessions

The EMPTY verdicts of `ae_experiment->study:idf_secondary` in `data/surveys/arrayexpress-ena/`
(2026-10-28). The committed corpus `data/corpora/arrayexpress-no-secondary/` is the 551 of
2026-09-15. It is kept as the record of what the old reader missed. Its own `idf_secondary`
verdicts, 541 resolved and 10 empty, are in `data/surveys/arrayexpress-no-secondary/`.

## Regression test

`tests/pathologies/test_ae_idf_missing_secondary_accession.py`, read through the fixed reader:
E-MTAB-6505's IDF still declares no secondary accession, and E-MTAB-13231's still names ERP133516
only as a related experiment.
