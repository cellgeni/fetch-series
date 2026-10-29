---
title: BioStudies serves a directory listing for a file
---

# BioStudies serves a directory listing for a file

**Archive:** EBI BioStudies (its FTP mirror, `ftp.ebi.ac.uk/biostudies/fire/`)
**Affected routes:** every route that reads an IDF or SDRF:
[`ae_experiment->study:idf_secondary`](../routes/ae-experiment-to-study/index.md),
[`ae_experiment->biosample:sdrf`](../routes/ae-experiment-to-biosample/sdrf.md),
[`ae_experiment->file:sdrf`](../routes/ae-experiment-to-file/index.md), and the promoted
`biostudies_idf` and `biostudies_sdrf_comments` routes
[from an ArrayExpress experiment](../routes/from-ae-experiment.md)
**Status:** intermittent, for an hour or more at a time; worked around by failing a read whose body
is a directory listing, without retrying it
**Measured:** 2026-10-28 and 29: every IDF and SDRF of the 20,693 ArrayExpress studies BioStudies
links to ENA (`arrayexpress-ena`), 41,506 files

## Symptom

E-MTAB-11257's IDF is asked for at `https://www.ebi.ac.uk/biostudies/files/E-MTAB-11257/E-MTAB-11257.idf.txt`,
which redirects to the mirror. At some hours the mirror serves the file. At others it answers the
file's path with a `301 Moved Permanently` to the same path with a slash on the end, and serves
HTTP 200 and an Apache listing of an empty directory there, with nothing in it but a link to its
parent:

```html
<title>Index of /biostudies/fire/E-MTAB-/257/E-MTAB-11257/Files/E-MTAB-11257.idf.txt</title>
```

The page is 837 bytes, `text/html`. `tests/test_biostudies.py::TestDirectoryListing` rebuilds it
byte for byte. The file is a 4,480-byte IDF that names ERP133806, served as `text/plain` with
ETag `"1180-60a5bfd864200"` when it is served at all. The legacy ArrayExpress copy, under
`ftp.ebi.ac.uk/pub/databases/arrayexpress/data/experiment/MTAB/E-MTAB-11257/`, is the same file.
The SDRF beside it is served normally throughout.

It flips, over hours:

| When (UTC) | What was served |
|---|---|
| 10-28 16:00 | `idf_secondary` recorded EMPTY; the body was not kept |
| 10-28 17:32, 17:33 | the listing |
| 10-28 21:21 | the IDF, whole, identical to the legacy copy |
| 10-28 21:34 | the IDF; `idf_secondary` asked again, and resolved to ERP133806 |
| 10-28 23:06 | the IDF, to the census |
| 10-29 00:33–01:25 | the listing, on every one of 96 requests: 95 followed a 301 to `…idf.txt/` to it, and one stopped at the 301 |

From 00:36 it was asked about every 40 seconds through the BioStudies address, as a reader asks,
with `curl -L`: 76 times, to 01:25, and the answer was the listing every time.
`data/benchmarks/biostudies-served-size/arrayexpress-ena-2026-10-28/e-mtab-11257-idf.csv` holds
each answer's status, size, `Content-Type` and the URL it ended at.

The status is 200, and the request named a file. Only the redirect and the body say otherwise,
and the reader checks the body: it is what was seen, and it is what a memoised response keeps.

## What a listing costs

A reader handed the page finds none of what it reads. An IDF reader finds no
`Comment[SecondaryAccession]`, no `Comment[SequenceDataURI]` and no `SDRF File` line. An SDRF
reader finds a table with none of the columns it reads. Each route records **EMPTY**: the study
names nothing. That is the answer a study that genuinely names nothing gives, so the two cannot be
told apart from the verdict. E-MTAB-11257 was counted among the empties of `idf_secondary` until
it was asked again.

## How often

Rarely, as far as a census can see. One file is known to be served this way, E-MTAB-11257's IDF.
A census of every IDF and SDRF of `arrayexpress-ena` that night found no listing among the 41,432
files it was served, E-MTAB-11257's IDF included.

| When (UTC) | What was read | Listings |
|---|---|---:|
| 10-28 13:23–14:17 | every IDF and SDRF of `sample:3000@arrayexpress-ena`, by [the cut's census](biostudies-serves-a-file-cut-at-4096-bytes.md#how-often): 5,796 answered 200 | 0 |
| 10-28 14:54–17:30 | every IDF of `arrayexpress-ena`, by the `idf_secondary` re-survey: 20,687 answered | 1, at most 2 |
| 10-28 21:28 – 10-29 00:28 | every IDF and SDRF of `arrayexpress-ena`: 41,506 asked, 39,647 answered 200 | **0** |
| 10-29 00:30–01:25 | the 1,812 files the census was refused, dropped, or served short or at exactly 4,096 bytes, asked again one at a time: 1,804 answered 200 | **0** |

- **The first census** kept every body served at a size other than the one registered. A listing
  would be one, and none of the three was: they were a cut and two stale sizes.
- **The re-survey kept no bodies**, so it is known only through what a listing does to its
  verdict. A listing reads as EMPTY, and 12 IDFs read so. Ten name no study, and read the same
  again afterwards. E-MTAB-11257's was the listing. E-GEOD-35315's resolved that afternoon,
  and what it was served at 15:15 was not kept: a listing or
  [a cut](biostudies-serves-a-file-cut-at-4096-bytes.md) would each lose its row. Asked again at
  10:45 UTC on 2026-10-29, it resolved.
- **The second census** flags a listing whatever its size (`directory_index`), and records each
  body's `Content-Type`. All 39,647 bodies were `text/plain`.
  `scripts/biostudies_served_size_census.py` wrote it to
  `data/benchmarks/biostudies-served-size/arrayexpress-ena-2026-10-28/files.csv`, and the second
  ask to `recheck.csv` beside it. 1,699 requests answered 403 between 23:15 and 00:01, all but
  two from `ftp.ebi.ac.uk`, the throttling the cut page describes, and 94 were disconnected or
  timed out. The second ask is for those. Every refused or dropped file answered it but six:
  E-MTAB-17542's two, which BioStudies answers `Study not accessible`, two that answered 404, and
  two SDRFs that timed out again. Of the 41,506 files, 41,432 were served at one ask or the
  other. The other 74 are 70 404s (60 SDRFs that studies split into other files, and the IDF and
  SDRF of 5 studies that register none), those two 403s and those two timeouts.

A census asks each file once. It counts the files that are a listing at the moment each is asked,
and E-MTAB-11257's IDF shows what that misses: the census was served the IDF at 23:06, and from
00:33 the same address was a listing for at least 53 minutes. So 0 of 41,432 says a listing is
rare at any moment. It does not say E-MTAB-11257's IDF is the only file it happens to.

## Reproducer

```bash
curl -sL https://www.ebi.ac.uk/biostudies/files/E-MTAB-11257/E-MTAB-11257.idf.txt | head -c 200
# <!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN"> ... <title>Index of /biostudies/fire/...
#   (2026-10-28 17:32-17:33 UTC, 2026-10-29 00:33-01:25 UTC)
# MAGE-TAB Version	1.1 ...
#   (2026-10-28 21:21-23:06 UTC)
curl -s -o /dev/null -w '%{http_code} %{redirect_url}\n' \
  https://ftp.ebi.ac.uk/biostudies/fire/E-MTAB-/257/E-MTAB-11257/Files/E-MTAB-11257.idf.txt
# 301 https://ftp.ebi.ac.uk/biostudies/fire/E-MTAB-/257/E-MTAB-11257/Files/E-MTAB-11257.idf.txt/
#   (while it is a listing; 200 and no redirect while it is the file)
curl -s https://ftp.ebi.ac.uk/pub/databases/arrayexpress/data/experiment/MTAB/E-MTAB-11257/E-MTAB-11257.idf.txt \
  | grep SecondaryAccession
# Comment [SecondaryAccession]	ERP133806
```

Which of the two it answers depends on the hour.

## What it is not

- **Not [a file cut off at 4,096 bytes](biostudies-serves-a-file-cut-at-4096-bytes.md).** A cut
  is the first 4,096 bytes of the file itself, and only a body of that length is checked against
  the registered size. A listing is not the file at all, and is 837 bytes.
- **Not the 404 BioStudies once gave for a file the study holds**,
  [retired on 2026-10-28](index.md#retired-declared-once-not-reproduced). That answered nothing,
  and the mirror was asked instead. Here the mirror is what answers wrongly.
- **Not [ENA's empty directories](ena-serves-a-listed-fastq-wrongly.md), though it is their
  shape exactly.** ENA's file server answers two listed fastqs with a 301 to the path with a
  slash, and an empty listing there. That is a different server, and those two were directories on
  every request on 2026-10-23. E-MTAB-11257's IDF is a directory at some hours and the file at
  others.
- **Not this project's reader.** The body is Apache's page, not the IDF read wrongly.

## Workaround

`providers.biostudies.study_file`, which every IDF and SDRF read goes through, checks every body
for Apache's listing title, `<title>Index of /`, in its first kilobyte, and raises
`survey.client.ServedDirectoryError` if it is there. The check costs no request, and it runs
before the check for [a cut](biostudies-serves-a-file-cut-at-4096-bytes.md). The read fails
rather than answering EMPTY.

It is **not retryable**, and so not a `MalformedResponseError`, which always is. A retry
is worth making when asking again soon can succeed. `SurveyClient` waits 15 seconds in all across
its four retries, and a listing lasts far longer: E-MTAB-11257's was the answer to every
one of 96 requests from 00:33 to 01:25 UTC. Four more requests would ask the same server the same question,
and the verdict would be FAILED either way. The file is served at other hours, so a survey's
`--retry-failed` is the retry that can succeed, and it asks every failure again whatever its type.
The verdict's `error_class` then names what went wrong.

The same evidence bears on the cut, and the cut is not retried either: E-MTAB-9791's SDRF was cut
on every request for at least 46 minutes. It raises `survey.client.ServedCutError`, for the reasons
[the cut's page](biostudies-serves-a-file-cut-at-4096-bytes.md#why-a-cut-is-not-retried) gives.

`core._study_file` does the same.

The legacy ArrayExpress copy is not read instead, for the reason the cut page gives: where the
two differ, the legacy copy is the older version.

The check matches Apache's title and nothing broader. An IDF may mention an index in its prose,
and a study may register an HTML file; neither is a directory listing.

## Regression test

None inverted. E-MTAB-11257's IDF is a listing at some hours and the file at others. A test pinned
to it would pass from 00:33 and fail from 21:21, and a failure reads as the archive fixing the
behaviour when it has not.
[The cut](biostudies-serves-a-file-cut-at-4096-bytes.md#regression-test) and
[MegaLink](megalink-backend-flakiness.md#regression-test) pages make the same call for the same
reason.

What is tested, offline, is the workaround: `tests/test_biostudies.py::TestDirectoryListing`. A
listing fails the read and is not retryable, an IDF is read at the cost of one request, prose that
mentions an index is not taken for one, and the synchronous helper checks too.

To measure it again, run the census, which flags a listing in its `directory_index` column:

```bash
uv run python scripts/biostudies_served_size_census.py census \
  --corpus arrayexpress-ena --out data/benchmarks/biostudies-served-size/<corpus>-<date>
uv run python scripts/biostudies_served_size_census.py recheck --out data/benchmarks/biostudies-served-size/<corpus>-<date>
uv run python scripts/biostudies_served_size_census.py report --out data/benchmarks/biostudies-served-size/<corpus>-<date>
```
