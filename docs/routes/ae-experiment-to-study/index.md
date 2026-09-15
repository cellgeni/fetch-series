---
title: ArrayExpress experiment → study
---

# ArrayExpress experiment → study

**Needs:** an `E-MTAB`-style accession **Feeds:** study → runs → files
**Routes:** 1 measured, with a fallback that is not yet a declared route.

## Route

`ae_experiment->study:idf_secondary` reads `Comment[SecondaryAccession]` from the study's
IDF file at `https://www.ebi.ac.uk/biostudies/files/<acc>/<acc>.idf.txt`. One request.

## Measured

**Corpus:** `arrayexpress-ena` — every ArrayExpress study BioStudies records an
ENA link for, enumerated on 2026-09-15 and committed. **Surveyed:** 2026-09-15.

The population is the point. BioStudies independently says an ENA link exists
for each of these, so a study whose IDF declares no secondary accession is a
**countable recall failure** rather than an unknown.

| | Count | |
|---|---:|---|
| Resolved | 20,137 | 97.3% |
| **No secondary accession declared** | **551** | 2.7% |
| Failed | 5 | 0.02% |
| Distinct studies found | 20,138 | |

### It is not one route, it is two populations

| Prefix | What it is | Total | Resolved | Rate |
|---|---|---:|---:|---:|
| `E-MTAB` | native ArrayExpress submissions | 9,802 | 9,777 | **99.7%** |
| `E-GEOD` | ArrayExpress's imports of GEO series | 10,597 | 10,086 | 95.2% |
| `E-ERAD` | the Sanger ERAD collection | 274 | 193 | **70.4%** |
| others | SYBR, GEUV, TABM | 20 | 20 | 100% |

For a study submitted *to* ArrayExpress the IDF route is essentially exact. For
a GEO series mirrored *into* ArrayExpress it is not, and the right answer for
those is to go to GEO — where [the SOFT family file resolves 99.8%](../gse-to-experiment/index.md)
— rather than to read a derived record of a derived record. `E-ERAD` is the
weakest at 70.4% and the smallest, at 274 studies.

This replaces a 2026-03 figure of 7,179 queried and 457 empty, whose population
was never recorded and so cannot be compared with anything.

### Five studies exist without existing

The 5 failures are all `E-MTAB`, and all are genuine 404s on the IDF. The study
records themselves are live — `/api/v1/studies/E-MTAB-14460` returns a title and
a `ReleaseDate` of 2026-01-26 — and the search index returns them. They simply
register **no files at all**: no IDF, no SDRF, which are the two files an
ArrayExpress study is defined by. Eight months past their stated release date.

```
E-MTAB-14460  E-MTAB-15098  E-MTAB-15118  E-MTAB-15165  E-MTAB-15306
```

## The fallback

`E-MTAB-6505` declares no `Comment[SecondaryAccession]`, but its SDRF still carries
`Comment[BioSD_SAMPLE]` BioSample accessions, and querying ENA for one of them
(`SAMEA5053920`) recovers `ERR2861957` / `PRJEB29431` / `ERP111731`.

So the chain is: IDF secondary accession → else SDRF BioSamples → ENA. Both hops
are declared routes (`ae_experiment->biosample:sdrf`, `biosample->run:ena_filereport`)
and `fetch_series.relations` walks the composition for any ArrayExpress entry
point.

### How much the fallback actually recovers: 4.2%

Measured over exactly the 551, on 2026-09-15
(corpus `arrayexpress-no-secondary`, committed):

| Prefix | Studies | SDRF names a BioSample | |
|---|---:|---:|---:|
| `E-GEOD` | 450 | **0** | 0% |
| `E-ERAD` | 81 | 16 | 20% |
| `E-MTAB` | 20 | 7 | 35% |
| **total** | **551** | **23** | **4.2%** |

**Not one of the 450 `E-GEOD` studies carries a `Comment[BioSD_SAMPLE]`.** That
is not a near miss, it is a categorical property: an ArrayExpress import of a
GEO series has a GEO-derived SDRF, and GEO-derived SDRFs do not carry BioSample
columns. 15 of the 450 could not be read at all.

The fallback is therefore real but narrow. It rescues E-MTAB-6505 and 22 other
studies, and it is the wrong tool for the group that dominates the failures.

**The right route for an `E-GEOD` study is GEO.** `E-GEOD-63923` is `GSE63923`;
the GEO series resolves through [the SOFT family file](../gse-to-experiment/index.md)
at 99.8%. Reading a derived record of a derived record, and then failing over to
a second derived field of the same derived record, is two ways of avoiding the
archive that actually holds the answer.

## A trap in the file list

The BioStudies file API pages silently. With no explicit `limit` it returns the first 25 of
however many files a study registers, which for `E-MTAB-9221` would have declared 15 of its
40 registered FASTQs unregistered. Always pass a limit; `biostudies.registered_files` pages
at 500.

That matters because whether a study *registers* a file decides whether its declared URI
can be trusted — see the `E-MTAB-8060` versus `E-MTAB-9221` case in
[`fetch10xmeta`](https://github.com/cellgeni/nf-reprocessing-public-10x/tree/main/modules/cellgeni/fetch10xmeta),
which is the milestone-2 problem.
