---
title: ArrayExpress SDRF URIs point at a decommissioned mirror
---

# ArrayExpress SDRF URIs point at a decommissioned mirror

**Archive:** EBI ArrayExpress / BioStudies
**Affected route:** [`ae_experiment->file:sdrf`](../routes/ae-experiment-to-file/index.md)
**Status:** worked around; the workaround is the route's defining feature
**Provenance:** the rule was worked out first in `fetch10xmeta`'s `parse_metadata.sh`; this page restates it with the measurement that confirms it.

## Symptom

`Comment[FASTQ_URI]` columns in an ArrayExpress SDRF routinely point under

```
ftp://ftp.ebi.ac.uk/pub/databases/{microarray,arrayexpress}/data/experiment/...
```

the pre-BioStudies mirror, which is no longer maintained. The spelling means two
entirely different things, and **nothing in the SDRF distinguishes them**:

- the file still exists under BioStudies and the URI is merely a stale spelling; or
- the file does not exist anywhere, and the study's real data is elsewhere.

## The pair that proves it

E-MTAB-8060 and E-MTAB-9221 are indistinguishable at the SDRF level. Both point
every `Comment[FASTQ_URI]` at the mirror. Both also declare a `Comment[BAM_URI]`.

| | Files the study registers | of which fastq | SDRF URIs after the guard |
|---|---|---|---|
| **E-MTAB-9221** | 43 | 40 | 40 kept, rewritten to BioStudies |
| **E-MTAB-8060** | 2 (the IDF and the SDRF) | 0 | none — every URI dropped |

E-MTAB-8060's reads exist solely on the unmaintained mirror. Its real submission
is the ENA BAM. Taking the SDRF at face value discarded that BAM for all 15 runs,
and left no trace that it had done so.

## Reproducer

```bash
# what each study actually registers
for AE in E-MTAB-8060 E-MTAB-9221; do
  printf '%s: ' "$AE"
  curl -sL "https://www.ebi.ac.uk/biostudies/api/v1/studies/$AE/files?limit=500" \
    | python3 -c 'import json,sys; i=json.load(sys.stdin)["items"]; print(len(i),"files,",sum(1 for f in i if ".fastq" in f.get("path","")),"fastq")'
done
```

```
E-MTAB-8060: 2 files, 0 fastq
E-MTAB-9221: 43 files, 40 fastq
```

## Workaround

A URI under the mirror is believed only if the study still registers a file of
that name, and is rewritten to `https://www.ebi.ac.uk/biostudies/files/<acc>/<name>`
when it is. A URI anywhere else is left exactly as the SDRF wrote it.

When the file list **cannot be fetched**, every URI is kept as written. This
matters: treating an unreachable API as "registers nothing" would reroute every
ArrayExpress study to its BAMs on a transient EBI outage. The route distinguishes
"registers nothing" from "could not ask", and only the first triggers the guard.

## Two parsing traps in the same route

**The SDRF repeats column names by design.** E-MTAB-9221 has two
`Comment[FASTQ_URI]` columns, one per mate, and `csv.DictReader` keeps only the
last value for a repeated key. Read that way the study yielded 20 URIs for its 20
runs instead of 40 — one mate each, exactly half the data, with no error. The
route reads raw field lists.

**The BioStudies file listing is under `items`, not `files`.** A
`payload.get("files", [])` returns an empty list and no error, which reads as
"this study registers nothing" — and since registering nothing *is* the finding
for E-MTAB-8060, the bug was invisible: it turned every study into E-MTAB-8060
and dropped every SDRF URI in the archive. It was caught by checking the counts
against the two studies above, which is why the counts are on this page.

The listing also pages silently: with no explicit limit it returns the first 25
of however many there are.

## Regression tests

`tests/test_biostudies.py` — the guard keeps a registered file, drops an
unregistered one, keeps everything when the list could not be fetched, never
second-guesses a URI off the mirror, and preserves both mates from repeated
columns.
