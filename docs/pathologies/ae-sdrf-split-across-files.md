---
title: An ArrayExpress SDRF split across two files
---

# An ArrayExpress SDRF split across two files

**Archive:** EBI ArrayExpress, served by BioStudies
**Affected routes:** [`ae_experiment->biosample:sdrf`](../routes/ae-experiment-to-biosample/sdrf.md),
[`ae_experiment->file:sdrf`](../routes/ae-experiment-to-file/index.md), and the four promoted
`biostudies_sdrf_comments` routes [from an ArrayExpress experiment](../routes/from-ae-experiment.md)
**Status:** not a fault in the archive; worked around by reading the files the IDF names
**Measured:** 2026-10-27

## Symptom

Every route that reads an ArrayExpress SDRF asked for `<accession>.sdrf.txt`. For 55 `E-GEOD`
studies there is no such file. Each study's IDF names two SDRFs in its place, and the study
registers both:

```
SDRF File	E-GEOD-29264.hyb.sdrf.txt	E-GEOD-29264.seq.sdrf.txt
```

A request for the usual name answers 404, at BioStudies' file address and on the mirror. A
404 is not retryable, so each of those routes recorded the study as FAILED.

In E-GEOD-29264 the two files hold different halves of one study:

- the **hyb** file has 16 rows, one per array hybridisation, with an `Array Data File`;
- the **seq** file has 21 rows, one per sequencing run, with `Comment[ENA_RUN]` and
  `Comment[FASTQ_URI]`.

The headers share their first 14 columns, `Source Name` to the second protocol's
`Term Source REF`, and then diverge.

## How often

| Survey | Split | Failures |
|---|---:|---:|
| `sample:3000@arrayexpress-ena`, the six routes above | **9 of 3,000** | 11 |
| `arrayexpress-ena`, `biosample:sdrf`, 2026-09-15 (a partial run, 4,157 studies) | 55 | 55 |
| `arrayexpress-ena`, `file:sdrf`, 2026-09-15 (a partial run, 1,794 studies) | 54 | 54 |
| `arrayexpress-no-secondary`, `biosample:sdrf`, 2026-09-15 (551 studies) | 15 | 15 |

On 2026-10-27, every study that had failed an SDRF read in any committed export was asked
again: 57 studies.

- **55** split their SDRF. Every one is an `E-GEOD` import, from `E-GEOD-12646` to
  `E-GEOD-34626`. Every one names exactly two files, `.hyb.sdrf.txt` and `.seq.sdrf.txt`.
- **2**, `E-MTAB-14460` and `E-MTAB-15118`, register no files at all
  ([the other 404](../routes/ae-experiment-to-study/index.md#six-studies-exist-without-existing)).

So no SDRF read recorded in this project has failed for any other reason.

## Reproducer

```bash
curl -sL -o /dev/null -w '%{http_code}\n' \
  https://www.ebi.ac.uk/biostudies/files/E-GEOD-29264/E-GEOD-29264.sdrf.txt
# 404   (on 2026-10-27, after the redirect to the mirror)
curl -sL https://www.ebi.ac.uk/biostudies/files/E-GEOD-29264/E-GEOD-29264.idf.txt | grep '^SDRF File'
# SDRF File	E-GEOD-29264.hyb.sdrf.txt	E-GEOD-29264.seq.sdrf.txt
curl -sL https://www.ebi.ac.uk/biostudies/files/E-GEOD-29264/E-GEOD-29264.seq.sdrf.txt | head -c 200
```

## What it is not

- **Not a defect in the archive, and nothing to report upstream.** BioStudies agrees with itself
  here. The IDF names two SDRFs, the study's file list registers those two, and the mirror
  serves them. The assumption of one SDRF named after the study was this project's. It held
  for 2,989 of the draw's 3,000 studies, which is why nothing questioned it.
- **Not the 404 BioStudies once gave for a file the study holds**,
  [retired on 2026-10-28](index.md#retired-declared-once-not-reproduced). There, the mirror had
  the file. Here nobody has it, because it does not exist.
- **Not lossy to merge.** The two files describe one study, so the routes read them as one
  table. Columns are aligned by name. A repeated name is matched by its order of occurrence, so
  both `Comment[FASTQ_URI]` columns of a paired study survive. A `Term Source REF` or `Unit` is
  matched within the column it qualifies, so it stays beside it. Counted on its own it drifts:
  the sixth `Term Source REF` of E-GEOD-29264's hyb file qualifies its array design, and the
  sixth of its seq file a protocol.

## Workaround

`providers.biostudies.sdrf_raw_rows` and `sdrf_rows`, which every SDRF route reads through:

1. Ask for `<accession>.sdrf.txt`, as before, through the mirror fallback.
2. On a 404, read the IDF and its `SDRF File` row (`sdrf_files`).
3. Fetch each file it names and merge them (`merge_sdrf_tables`). Where the headers are the
   same, that appends their rows.

The 404 stands if the IDF names nothing else or is itself missing. If one of the named files is
missing, the study fails: half a study read as all of it would be a silent loss.
`core.ae2biosamples` does the same. The link survey's memory already admits any `*.sdrf.txt`,
so the three mechanisms reading one study still fetch each file once.

Over the draw, the nine studies now answer every SDRF route:

- 8 resolve to fastq files, and `E-GEOD-30583` names its 8 runs but no fastq;
- all 9 resolve to runs, experiments and GEO samples;
- none names a BioSample, as no `E-GEOD` SDRF does.

Each of the six routes went from 11 failures to 2.

## Regression test

`tests/pathologies/test_ae_sdrf_split_across_files.py` has two tests:

- E-GEOD-29264 still has no `.sdrf.txt`, and its IDF still names the hyb and seq files;
- the fallback still reads both halves: an `Array Data File` from one and a
  `Comment[ENA_RUN]` from the other.
