---
title: BioStudies' file address answers 404 for a file the study registers
---

# BioStudies' file address answers 404 for a file the study registers

**Archive:** EBI BioStudies (`www.ebi.ac.uk/biostudies/files/`)
**Affected routes:** [`ae_experiment->study:idf_secondary`](../routes/ae-experiment-to-study/index.md),
[`ae_experiment->biosample:sdrf`](../routes/ae-experiment-to-biosample/sdrf.md),
[`ae_experiment->file:sdrf`](../routes/ae-experiment-to-file/index.md), and every promoted
route that reads an IDF or SDRF
**Status:** intermittent, worked around by reading the file from the FTP mirror the study's
`info` names
**Measured:** reproduced 2026-10-26

## Symptom

`/biostudies/files/<study>/<name>` is the address every ArrayExpress route reads an IDF or SDRF
from. For a file it holds, it answers with a redirect to the FTP mirror. Sometimes it answers
404 instead, for a file that is there.

- **E-MTAB-13459.** Six requests in a row for its IDF answered 404 on 2026-10-26. Meanwhile:
    - the study's own file listing names `E-MTAB-13459.idf.txt`;
    - the mirror serves it, at
      `ftp.ebi.ac.uk/biostudies/fire/E-MTAB-/459/E-MTAB-13459/Files/`;
    - its SDRF, at the same address, redirected to the mirror.
- **E-MTAB-15472.** Its IDF answered 404 twice, then redirected.

A 404 is not retryable, so a client records the study as having no IDF, and every link the
IDF would have given is lost with it.

## How often

| Survey | IDF 404s | SDRF 404s |
|---|---:|---:|
| `sample:300@node_ae_experiment@arrayexpress-ena`, 2026-10-19 | 8 of 300 | 10 of 300 |
| `sample:3000@arrayexpress-ena`, 2026-10-26 | **328 of 3,000** | **203 of 3,000** |
| `arrayexpress-ena`, 2026-09-15 (`idf_secondary`) | 5 of 20,693 | |

Every one of the 328 IDFs was asked for again that evening, at the address and on the mirror:

| At the address | On the mirror | Studies |
|---|---|---:|
| 404 again | 200 | 186 |
| redirected this time | 200 | 140 |
| 404 | 404 | 2 |

So 326 of the 328 were files the study holds. The other two, E-MTAB-14460 and E-MTAB-15118,
register no files at all.

The rate has grown from 0.02% to 11% in six weeks. The 5 of September were studies that
register no files at all, so they are a different case; see below.

## Reproducer

```bash
for i in 1 2 3; do
  curl -s -o /dev/null -w '%{http_code}\n' \
    https://www.ebi.ac.uk/biostudies/files/E-MTAB-13459/E-MTAB-13459.idf.txt
done
# 404 404 404   (on 2026-10-26)
curl -s https://www.ebi.ac.uk/biostudies/api/v1/studies/E-MTAB-13459/info \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["httpLink"])'
# https://ftp.ebi.ac.uk/biostudies/fire/E-MTAB-/459/E-MTAB-13459
curl -s -o /dev/null -w '%{http_code}\n' \
  https://ftp.ebi.ac.uk/biostudies/fire/E-MTAB-/459/E-MTAB-13459/Files/E-MTAB-13459.idf.txt
# 200
```

## What it is not

- **Not a study without the file.** The September survey's 5 IDF failures, and E-MTAB-14460
  and E-MTAB-15118 today, are studies that register no files at all. The mirror has nothing
  for them either, and the fallback keeps their 404 as the verdict.
- **Not the [decommissioned mirror](ae-sdrf-points-at-decommissioned-mirror.md).** That page
  is about fastq URIs an SDRF *names*. This one is about reading the SDRF itself.
- **Not known to affect data files.** Supplementary and fastq links built on the same address
  have not been checked for it, and links are published as the address, unchanged.

## Workaround

`providers.biostudies.study_file`, used for every IDF and SDRF read:

1. Ask `/biostudies/files/<study>/<name>`, as before.
2. On a 404, and only a 404, ask `api/v1/studies/<study>/info`. It names the study's
   mirror directory as `httpLink`.
3. Read `<httpLink>/Files/<name>`. Its answer, 404 or not, is the verdict.

If `info` names no mirror, the front door's 404 stands. `core.py`'s `ae2secondary` and
`ae2biosamples` do the same. The link survey's memory admits the mirror's IDF and SDRF and the
`info` call, so the three mechanisms reading one SDRF still cost one fetch.

## Regression test

`tests/pathologies/test_biostudies_files_404s_registered_files.py` has two tests:

- at least one of five requests for E-MTAB-13459's IDF still answers 404;
- the IDF still comes back through the mirror.
