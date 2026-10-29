---
title: BioStudies serves a file cut off at 4,096 bytes
---

# BioStudies serves a file cut off at 4,096 bytes

**Archive:** EBI BioStudies (its FTP mirror, `ftp.ebi.ac.uk/biostudies/fire/`)
**Affected routes:** every route that reads an IDF or SDRF:
[`ae_experiment->study:idf_secondary`](../routes/ae-experiment-to-study/index.md),
[`ae_experiment->biosample:sdrf`](../routes/ae-experiment-to-biosample/sdrf.md),
[`ae_experiment->file:sdrf`](../routes/ae-experiment-to-file/index.md), and the promoted
`biostudies_idf` and `biostudies_sdrf_comments` routes
[from an ArrayExpress experiment](../routes/from-ae-experiment.md)
**Status:** intermittent; worked around by asking for the registered size of any body exactly
4,096 bytes long, and failing a read that was cut
**Measured:** 2026-10-28

## Symptom

The mirror answers a request for an IDF or SDRF with HTTP 200 and the first 4,096 bytes of the
file. The body stops mid-line, and the headers agree with it: `Content-Length: 4096`, and an
Apache ETag whose size field is `1000`, which is 4,096 in hex. The file listing registers the
file at its full size.

- **E-MTAB-9791's SDRF** is registered at 30,836 bytes. It was served at 4,096 at 13:43 UTC on
  2026-10-28, and on every request one at a time until at least 14:29, each with ETag
  `"1000-5e532425bef00"`. The legacy ArrayExpress copy is 30,836 bytes, and the served body is
  its first 4,096, byte for byte. The cut falls partway through the sixth row. The copy names
  4 of the study's 24 runs, and an SDRF route reading it would answer with those 4 as RESOLVED.
  On 2026-10-26 at 20:34 UTC, the same routes read all 24.
- **E-GEOD-43366's SDRF** is registered at 50,461 bytes, and was served at 4,096 at 14:26 and
  14:29 UTC, with ETag `"1000-6044d1e715280"`. It is the legacy copy's first 4,096 bytes and names
  4 runs. On 2026-10-26 at 20:57 UTC the SDRF routes read 42.
- **Four IDFs that morning.** E-GEOD-58432 (registered 4,171 bytes), E-GEOD-67915 (5,031),
  E-MTAB-13170 (9,700) and E-MTAB-15453 (9,387) were served cut at 4,096 between 08:18 and 09:01
  UTC. E-GEOD-58432's `Comment[SequenceDataURI]` stopped at `http://www.ebi.ac.uk/`; the legacy
  copy holds `SRR1382298-SRR1382313`. All four were served whole by 13:17 UTC.

Nothing in a cut body says it was cut. The status is 200, the length matches the
Content-Length, and the ETag is the server's own for a 4,096-byte file. Only the file listing,
a second request, says otherwise.

## How often

| When (2026-10-28, UTC) | What was read | Cut |
|---|---|---:|
| 08:17–09:02 | the IDFs of `sample:3000@arrayexpress-ena`, by the `ae_experiment->run:biostudies_idf` re-survey | 4 IDFs |
| 13:23–14:17 | every IDF and SDRF of the same 3,000 studies: 2,894 IDFs and 2,902 SDRFs answered 200 | **1 SDRF** |
| 14:17–14:28 | the 217 files the census found refused, disconnected, short or at exactly 4,096 bytes, asked again one at a time | **1 SDRF** more |

The morning's four are known from what they cost: each resolved on 2026-10-26 between 20:13
and 20:26 UTC, in the export the route's evidence of 2026-10-27 cites, and came back empty on
2026-10-28. The afternoon's figure is a census: what `biostudies.served_file` is served for
every file, against the size the study's file listing registers.
`scripts/biostudies_served_size_census.py` wrote it to
`data/benchmarks/biostudies-served-size/2026-10-28/files.csv`, and the second ask to
`recheck.csv` beside it. E-GEOD-43366's SDRF was one of the census's 403s, and was cut when it
was asked again.

Every cut seen was at exactly 4,096 bytes, and no file was served short at any other length.
Two IDFs were served at a size other than the registered one, and neither is a cut
([below](#what-it-is-not)).

## What a cut costs

A cut at 4,096 bytes can only reach a file longer than that. 1,824 of the 2,894 IDFs are.
What each would lose, by the line it would cut into or past:

| Line | In | Ends past byte 4,096 |
|---|---:|---:|
| `Comment[SecondaryAccession]` | 2,893 | 1,634 |
| `Comment[SequenceDataURI]` | 2,671 | 1,692 |
| `Comment[RelatedExperiment]` | 288 | 190 |
| `SDRF File` | 2,894 | 1,692 |

A cut at 4,096 bytes would take a field a route reads from 1,783 of those 1,824. What the route
then records:

- **`idf_secondary` and the promoted IDF routes record EMPTY.** The field is missing, and a
  missing field is how an IDF says "none". The four studies of the morning did exactly this.
- **An SDRF route records RESOLVED, with part of the study.** A cut SDRF is a header and
  however many rows fit, and the reader cannot tell it from a short study. E-MTAB-9791 would have
  answered 4 runs of 24.
- **The split-SDRF fallback records FAILED, and blames the wrong file.** It reads the IDF's
  `SDRF File` line only when `<study>.sdrf.txt` answers 404
  ([the split SDRF](ae-sdrf-split-across-files.md)). 9 of the 3,000 studies split their
  SDRF, and each one's `SDRF File` line is the last line of an IDF of 4.8 to 17 KB. A cut at 4,096
  removes it from every one, and the study is recorded as having no SDRF: a permanent 404.

## Reproducer

```bash
curl -s "https://www.ebi.ac.uk/biostudies/api/v1/studies/E-MTAB-9791/files?search=E-MTAB-9791.sdrf.txt" \
  | python3 -c 'import json,sys;print([f["Size"] for f in json.load(sys.stdin)["items"]])'
# [30836]
curl -sL -D - -o /tmp/cut.sdrf https://www.ebi.ac.uk/biostudies/files/E-MTAB-9791/E-MTAB-9791.sdrf.txt \
  | grep -i '^content-length\|^etag'; wc -c < /tmp/cut.sdrf
# Content-Length: 4096
# ETag: "1000-5e532425bef00"
# 4096          (on 2026-10-28, 13:43-14:29 UTC)
curl -s https://ftp.ebi.ac.uk/pub/databases/arrayexpress/data/experiment/MTAB/E-MTAB-9791/E-MTAB-9791.sdrf.txt | wc -c
# 30836
```

It is intermittent. The morning's four files were whole within hours, and the reproducer
answers 30,836 once E-MTAB-9791 is too.

## What it is not

- **Not a file that is 4,096 bytes long.** Four IDFs are, registered, served and in the legacy
  copy: E-MTAB-7492, E-MTAB-11383, E-MTAB-14730 and E-GEOD-73453. E-GEOD-73453 and E-MTAB-14730
  end on a whole `SDRF File` line. E-MTAB-7492 ends on `Comment [ExperimentType]` and E-MTAB-11383
  on `Comment [RelatedExperiment]`, which is where their sibling studies' IDFs end, E-MTAB-7491's
  and E-MTAB-11384's; and BioStudies' record of E-MTAB-11383 links no experiment but E-MTAB-11384.
- **Not a stale registered size.** E-MTAB-11506's IDF is registered at 6,747 bytes and served at
  6,326, and E-MTAB-13475's at 4,513 and 4,449. Each served copy ends on a whole `SDRF File`
  line, and has an ETag that matches its length. E-MTAB-11506's is a later version than its legacy
  copy, and names different runs: `ERR10453043-ERR10453060` where the legacy copy names
  `ERR9077610-ERR9077614`. That is a disagreement between two copies of one study, not a cut.
- **Not a dropped connection.** 55 requests in the census ended with httpx's
  `RemoteProtocolError`. 54 were served whole asked again; the one that recurred, E-SYBR-3's IDF,
  says `Server disconnected without sending a response`. That raises, and is retried as any
  transport error is. A cut does not raise: the server declares 4,096 bytes and sends 4,096.
- **Not load.** Both SDRFs were cut on every request made one at a time, two seconds apart. Load
  shows up as [a 403](#the-403s-around-it), and every one of the census's 155 was answered in full
  when asked again, except E-GEOD-43366's, which was cut then too.
- **Not [the 404 for a file the study holds](biostudies-files-404s-registered-files.md).** That
  answers nothing. This answers part of the file.

## The 403s around it

`ftp.ebi.ac.uk` answers 403 under concurrent load, as `ftp.sra.ebi.ac.uk` does. The census asked
at concurrency 4 while two other surveys on the same machine read the same mirror, and 155 of
its 6,018 requests were refused, between 13:31 and 14:07 UTC. Asked again one at a time, 150
answered: 148 whole, E-GEOD-43366's cut, and one genuine 404. The other 5 answered 403 again, and
were served whole when asked once more at 14:28.

That morning, the re-survey of `ae_experiment->run:biostudies_idf` saw 244 of its 3,000 IDF
requests refused at concurrency 10, all served when asked again one at a time; those requests were
not recorded. The concurrency-10 test was not repeated here. A re-survey of the same IDFs by
another session was reading the mirror at the time, and the combined load had already cost it 124
verdicts recorded FAILED from a 403.

A 403 was not retryable for this host, so each was a permanent FAILED. `ftp.ebi.ac.uk` is now in
`survey.client.THROTTLES_WITH_403`.

## Should a reader compare every body with its registered size?

No. Three checks were weighed, against the census:

| Check | Extra requests | Cuts caught of the 6 seen | Whole files it would fail |
|---|---|---:|---:|
| Every body against its registered size | one per read: 5,796 | 6 | 2: E-MTAB-11506, E-MTAB-13475 |
| A body of exactly 4,096 bytes against its registered size | one per such body: 5 | 6 | 0 |
| An IDF with no `SDRF File` line | none | the 4 IDFs, neither SDRF | 0 of 2,894 |

- **Every body** doubles what an IDF route costs, and fails two whole IDFs whose registered size
  is stale. That is a FAILED where the route had a complete answer, to catch a cut at a length
  nobody has seen.
- **Exactly 4,096** costs a request for 5 of 5,796 bodies, and catches every cut seen.
  What it would miss is a cut at another length. None of the 5,796 files, nor the 217 asked
  again, was cut at another length, and every cut file's ETag says the server held 4,096 bytes.
- **No `SDRF File` line** is free and fails no whole IDF, but it cannot see a cut SDRF, which
  is where both of the afternoon's cuts were. And it misses a cut IDF whose `SDRF File` line
  comes before byte 4,096 and a field a route reads after it: 113 of the 1,824.

So the reader takes the second.

## Workaround

`providers.biostudies.study_file`, which every IDF and SDRF read goes through:

1. Read the file as before (`served_file`: the front door, or the mirror where it 404s).
2. If the body is exactly 4,096 bytes, ask the study's file listing for that one file
   (`files?search=<name>`).
3. If the listing registers more than 4,096 bytes, raise `MalformedResponseError`. It is
   retryable: E-MTAB-15453's IDF was cut at 08:27 UTC and whole by 09:00. A cut that outlasts the
   retries fails the read. It is not answered from part of the file.

`core._study_file` does the same.

The legacy ArrayExpress copy is not used instead. It is whole for both cut SDRFs and for
E-GEOD-58432, but it is an older version where the two differ: E-MTAB-11506's names other runs.
And 31 of the 217 files asked again have no legacy copy at all.

## Regression test

None inverted. A cut comes and goes: the morning's four were whole by the afternoon, and both of
the afternoon's SDRFs were read whole two days earlier. A test pinned to one cut file would fail as soon as
that file healed, which reads as the archive fixing the behaviour when it has not. The
[MegaLink page](megalink-backend-flakiness.md#regression-test) makes the same call for the same
reason.

What is tested, offline, is the workaround: `tests/test_biostudies.py::TestCutAt4096`. A 4,096-byte
body registered larger fails the read and is retryable; a file that is 4,096 bytes is read; a body
of any other length costs no request; the synchronous helper checks too.

To measure it again, run the census:

```bash
uv run python scripts/biostudies_served_size_census.py census \
  --corpus sample:3000@arrayexpress-ena --out data/benchmarks/biostudies-served-size/<date>
uv run python scripts/biostudies_served_size_census.py recheck --out data/benchmarks/biostudies-served-size/<date>
uv run python scripts/biostudies_served_size_census.py report --out data/benchmarks/biostudies-served-size/<date>
```
