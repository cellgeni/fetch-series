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
**Status:** intermittent; worked around by failing a read whose body is a directory listing
**Measured:** CENSUS-TBD

## Symptom

E-MTAB-11257's IDF is asked for at `https://www.ebi.ac.uk/biostudies/files/E-MTAB-11257/E-MTAB-11257.idf.txt`,
which redirects to the mirror. On 2026-10-28 the mirror answered with HTTP 200 and an Apache
listing of the file's own path, with nothing in it but a link to its parent:

```html
<title>Index of /biostudies/fire/E-MTAB-/257/E-MTAB-11257/Files/E-MTAB-11257.idf.txt</title>
```

The page was 837 bytes. Apache's listing of an empty directory at that path is 837 bytes, byte
for byte, and `tests/test_biostudies.py::TestDirectoryListing` rebuilds it. The file itself is a
4,480-byte IDF that names ERP133806. The legacy ArrayExpress copy, under
`ftp.ebi.ac.uk/pub/databases/arrayexpress/data/experiment/MTAB/E-MTAB-11257/`, is that file, and
by 21:21 UTC the BioStudies address served it too, identical to the legacy copy.

| When (2026-10-28, UTC) | What was served |
|---|---|
| 16:00 | `idf_secondary` recorded EMPTY; the body was not kept |
| 17:32 | 837 bytes, read beside the legacy copy's 4,480 |
| 17:33 | the listing above, read by hand |
| 21:21 | 4,480 bytes, `text/plain`: the IDF, whole |
| 21:34 | whole; `idf_secondary` asked again, and resolved to ERP133806 |

The status is 200, and the request named a file. The body is what says otherwise: its first lines
are Apache's. Its headers were not kept. The mirror serves its listings as `text/html` and the IDF
as `text/plain`, but the body is what was seen, and the body is what the reader checks.

## What a listing costs

A reader handed the page finds none of what it reads. An IDF reader finds no
`Comment[SecondaryAccession]`, no `Comment[SequenceDataURI]` and no `SDRF File` line. An SDRF
reader finds a table with none of the columns it reads. Each route records **EMPTY**: the study
names nothing. That is the answer a study that genuinely names nothing gives, so the two cannot be
told apart from the verdict. E-MTAB-11257 was counted among the empties of `idf_secondary` until
it was asked again.

## How often

CENSUS-TBD

## Reproducer

```bash
curl -sL https://www.ebi.ac.uk/biostudies/files/E-MTAB-11257/E-MTAB-11257.idf.txt | head -c 200
# <!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN"> ... <title>Index of /biostudies/fire/...
#   (2026-10-28, 17:32-17:33 UTC)
# MAGE-TAB Version	1.1 ...
#   (from 21:21 UTC the same day)
curl -s https://ftp.ebi.ac.uk/pub/databases/arrayexpress/data/experiment/MTAB/E-MTAB-11257/E-MTAB-11257.idf.txt \
  | grep SecondaryAccession
# Comment [SecondaryAccession]	ERP133806
```

It is intermittent, and E-MTAB-11257 already reads whole. The census below finds whichever file
is being served this way when it runs.

## What it is not

- **Not [a file cut off at 4,096 bytes](biostudies-serves-a-file-cut-at-4096-bytes.md).** A cut
  is the first 4,096 bytes of the file itself, and only a body of that length is checked against
  the registered size. A listing is not the file at all, and is 837 bytes.
- **Not [the 404 for a file the study holds](biostudies-files-404s-registered-files.md).** That
  answers nothing, and the mirror is asked instead. Here the mirror is what answers wrongly.
- **Not [ENA's empty directories](ena-serves-a-listed-fastq-wrongly.md), though it has their
  shape.** ENA's file server answers two listed fastqs with a 301 to a directory, on every request
  on 2026-10-23. That is a different server, and those two did not heal. E-MTAB-11257's IDF did,
  within four hours.
- **Not this project's reader.** The body is Apache's page, not the IDF read wrongly.

## Workaround

`providers.biostudies.study_file`, which every IDF and SDRF read goes through, checks every body
for Apache's listing title, `<title>Index of /`, in its first kilobyte, and raises
`MalformedResponseError` if it is there. The check costs no request, and it runs before the check
for [a cut](biostudies-serves-a-file-cut-at-4096-bytes.md). The read fails rather than answering
EMPTY. RETRY-TBD

`core._study_file` does the same.

The legacy ArrayExpress copy is not read instead, for the reason the cut page gives: where the
two differ, the legacy copy is the older version.

The check matches Apache's title and nothing broader. An IDF may mention an index in its prose,
and a study may register an HTML file; neither is a directory listing.

## Regression test

None inverted. A listing comes and goes, and E-MTAB-11257 read whole within four hours. A test
pinned to it would already fail, which reads as the archive fixing the behaviour when it has not.
[The cut](biostudies-serves-a-file-cut-at-4096-bytes.md#regression-test) and
[MegaLink](megalink-backend-flakiness.md#regression-test) pages make the same call for the same
reason.

What is tested, offline, is the workaround: `tests/test_biostudies.py::TestDirectoryListing`. A
listing fails the read, an IDF is read at the cost of one request, prose that mentions an index is
not taken for one, and the synchronous helper checks too.

To measure it again, run the census, which flags a listing in its `directory_index` column:

```bash
uv run python scripts/biostudies_served_size_census.py census \
  --corpus arrayexpress-ena --out data/benchmarks/biostudies-served-size/<corpus>-<date>
uv run python scripts/biostudies_served_size_census.py recheck --out data/benchmarks/biostudies-served-size/<corpus>-<date>
uv run python scripts/biostudies_served_size_census.py report --out data/benchmarks/biostudies-served-size/<corpus>-<date>
```
