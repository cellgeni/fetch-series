---
title: ArrayExpress experiment → study
---

# ArrayExpress experiment → study

**Needs:** an `E-MTAB`-style accession **Feeds:** study → runs → files
**Routes:** 1 measured, with a fallback that is not yet a declared route.

## Route

`ae_experiment->study:idf_secondary` reads `Comment[SecondaryAccession]` from the study's
IDF file at `https://www.ebi.ac.uk/biostudies/files/<acc>/<acc>.idf.txt`, every cell of every
such row, quoted or not, and keeps the study accessions. One request.

## Measured

**Corpus:** `arrayexpress-ena` — every ArrayExpress study BioStudies records an
ENA link for, enumerated on 2026-09-15 and committed. **Surveyed:** 2026-10-28, one study asked
again on 2026-10-29; and before on 2026-09-15.

The population is the point. BioStudies independently says an ENA link exists
for each of these, so a study whose IDF declares no secondary accession is a
**countable recall failure** rather than an unknown.

| | 2026-09-15 | 2026-10-28 | |
|---|---:|---:|---|
| Resolved | 20,137 | **20,677** | 99.9% |
| No secondary accession declared | 551 | **10** | |
| Failed | 5 | 6 | |
| Distinct studies found | 20,138 | 20,688 | |

The 2026-09-15 survey read the first cell of the row, and nothing from an IDF that quotes its
cells. An E-GEOD import puts its GEO series in the first cell and its SRA study in the second
(`GSE10571	SRP003805`), and the Sanger ERAD collection quotes every cell
(`"Comment [SecondaryAccession]"	"ERP002094"`). Both were this project's reader. The 551 it
counted as declaring nothing are re-read on [the pathology page](../../pathologies/ae-idf-missing-secondary-accession.md):
541 declare a study.

### By where the study came from

| Prefix | What it is | Total | Resolved 2026-09-15 | Resolved 2026-10-28 |
|---|---|---:|---:|---:|
| `E-MTAB` | native ArrayExpress submissions | 9,802 | 9,777 | 9,786 |
| `E-GEOD` | ArrayExpress's imports of GEO series | 10,597 | 10,147 | **10,597** |
| `E-ERAD` | the Sanger ERAD collection | 274 | 193 | **274** |
| others | SYBR, GEUV, TABM | 20 | 20 | 20 |

The gap between the prefixes was the reader's. The 2026-09-15 split, 95.8% for `E-GEOD` and 70.4%
for `E-ERAD`, was read as two kinds of record, one derived from another archive and less to be
trusted. It was two spellings of one field, and the reader handled one of them.

Seven studies also gained a study from a later cell: E-MTAB-4045's row names SRP021098,
SRP041709 and SRP041710, where the first cell alone gave SRP041709.

### What the 10 empties are

All 10 name no `Comment[SecondaryAccession]`, and all are `E-MTAB`. Each names the study as a
`Comment[RelatedExperiment]` instead. That is [the pathology](../../pathologies/ae-idf-missing-secondary-accession.md).

**Two more were empty until they were asked again.** Each was an IDF served wrong, and the
survey's reader checked for neither fault: the checks for a cut and for a listing were added after
it ran. Both faults now fail the read instead of reading as an IDF that names no study.

- **E-MTAB-11257** was recorded EMPTY at 16:00 UTC, and at 17:32 UTC its IDF was served as
  [an empty directory listing](../../pathologies/biostudies-serves-a-directory-listing-for-a-file.md):
  HTTP 200, an Apache `Index of` page of 837 bytes. Re-asked at 21:34 UTC, the IDF was served whole
  and named ERP133806, which the route also found on 2026-09-15.
- **E-GEOD-35315** was recorded EMPTY at 15:15 UTC, and had resolved to SRP010570 over
  `arrayexpress-no-secondary` at 13:25. Its IDF is 5,395 bytes, and its
  `Comment[SecondaryAccession]` row ends at byte 5,121, so
  [a copy cut off at 4,096 bytes](../../pathologies/biostudies-serves-a-file-cut-at-4096-bytes.md)
  loses it, and so does a listing. What was served at 15:15 was not kept, so which it was is not
  known. The census served it whole at 21:52 UTC. Re-asked at 10:45 UTC on 2026-10-29, it was
  served whole and resolved to SRP010570. That is the one verdict in the export dated 2026-10-29;
  the other 20,692 are from 2026-10-28.

### Six studies exist without existing

Five failures are `E-MTAB` studies whose IDF is a genuine 404, as on 2026-09-15. The study
records themselves are live — `/api/v1/studies/E-MTAB-14460` returns a title and
a `ReleaseDate` of 2026-01-26 — and the search index returns them. They simply
register **no files at all**: no IDF, no SDRF, which are the two files an
ArrayExpress study is defined by.

```
E-MTAB-14460  E-MTAB-15098  E-MTAB-15118  E-MTAB-15165  E-MTAB-15306
```

The sixth, E-MTAB-17542, named ERP203773 on 2026-09-15. On 2026-10-28 BioStudies answers its
files and its record with a 403, `Study not accessible`.

### Throttling, and how it was kept out of the count

`ftp.ebi.ac.uk`, where BioStudies redirects every file, answered 520 of the 20,693 IDF requests
with a 403 while other jobs read the same host. A 403 there is not retried, so each was recorded as
a failure, and every failure was then asked again one at a time until none was a 403. Six DNS
failures on this machine were asked again the same way.

## The fallback

`E-MTAB-6505` declares no `Comment[SecondaryAccession]`, but its SDRF still carries
`Comment[BioSD_SAMPLE]` BioSample accessions, and querying ENA for one of them
(`SAMEA5053920`) recovers `ERR2861957` / `PRJEB29431` / `ERP111731`.

So the chain is: IDF secondary accession → else SDRF BioSamples → ENA. Both hops
are declared routes (`ae_experiment->biosample:sdrf`, `biosample->run:ena_filereport`)
and `fetch_series.relations` walks the composition for any ArrayExpress entry
point.

### How much the fallback recovers: 2 of 10

Until 2026-10-28 this section measured the fallback over the 551 studies of
`arrayexpress-no-secondary` and found BioSamples for 23 of them, **4.2%**. 21 of the 23 declare their
study in the IDF and are found by the route itself. What the fallback adds is measured over the
10 whose IDF has no `Comment[SecondaryAccession]` row, from the same 2026-09-15 survey of
`ae_experiment->biosample:sdrf`:

| Study | SDRF names BioSamples |
|---|---:|
| E-MTAB-13232 | 199 |
| E-MTAB-15116 | 8 |
| the other 8 | 0 |

Each of the 10 names an ENA study under `Comment[RelatedExperiment]` instead. That is not read as
the study, because in other IDFs the field names a different study from the experiment's own
([the pathology page](../../pathologies/ae-idf-missing-secondary-accession.md#what-the-10-declare-instead)).

Where it applies, the second hop is complete. In 2026-09-15's survey, the 756 BioSamples the
fallback found resolved 756 of 756 to 1,547 runs, nothing empty and nothing failed.

## A trap in the file list

The BioStudies file API pages silently. With no explicit `limit` it returns the first 25 of
however many files a study registers, which for `E-MTAB-9221` would have declared 15 of its
40 registered FASTQs unregistered. Always pass a limit; `biostudies.registered_files` pages
at 500.

That matters because whether a study *registers* a file decides whether its declared URI
can be trusted — see the `E-MTAB-8060` versus `E-MTAB-9221` case in
[`fetch10xmeta`](https://github.com/cellgeni/nf-reprocessing-public-10x/tree/main/modules/cellgeni/fetch10xmeta),
which is the milestone-2 problem.
