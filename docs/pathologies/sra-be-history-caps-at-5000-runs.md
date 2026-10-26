---
title: sra-db-be's history form stops at 5,000 runs, and its answers are cut off under load
---

# sra-db-be's history form stops at 5,000 runs, and its answers are cut off under load

**Archive:** NCBI SRA backend (`trace.ncbi.nlm.nih.gov/Traces/sra-db-be`)
**Affected routes:** [`bioproject->run:sra_be_direct_cgi`](../routes/bioproject-to-run/index.md),
[`bioproject->run:sra_be_elink`](../routes/bioproject-to-run/index.md)
**Status:** deterministic cap, worked around by asking for the project by accession. Cut-off
and empty answers are intermittent and are now failures, not results.
**Measured:** reproduced 2026-10-23

## Symptom

Two behaviours, and both answer HTTP 200 with no error.

- **The history form stops at 5,000 runs.** `sra-db-be.cgi?rettype=runinfo` is driven from
  an ESearch history. PRJEB82511 holds 7,263 runs, and its history is answered with exactly
  10,000 rows of 5,000 distinct runs: past 5,000 the backend repeats itself.
    - `retstart=5000` gives the same 10,000 rows.
    - `retmax=20000` gives the same 10,000 rows.

  The by-accession form, `runinfo?acc=PRJEB82511`, answers all 7,263.
- **Under load, an answer is cut off or empty.** On the evening of 2026-10-23,
  `runinfo?acc=PRJEB14362` (46,468 runs) was asked three times. It answered:

  | Try | Time | What came back |
  |---|---:|---|
  | 1 | 1,005 s | 0 bytes |
  | 2 | 1,104 s | 23,748 rows, the last one 6 fields of 47 |
  | 3 | 503 s | 15,265 rows, the last one 27 fields of 47 |

    - The same evening, PRJEB82511 answered 0 bytes in 2 s, then all 7,263 runs in 15 s.
    - An ELink-route survey running then recorded PRJNA772116 (73,888 runs) as having
      none, and PRJEB14362 as 7,533.

A client that reads an empty body as "no runs", and a cut-off one as the list, records a
fabricated answer, and nothing in the response says so.

## Reproducer

```bash
E=https://eutils.ncbi.nlm.nih.gov/entrez/eutils
h=$(curl -s "$E/esearch.fcgi?db=sra&term=PRJEB82511%5BGPRJ%5D&usehistory=y&retmode=json&retmax=0")
W=$(echo "$h" | python3 -c 'import json,sys;print(json.load(sys.stdin)["esearchresult"]["webenv"])')
Q=$(echo "$h" | python3 -c 'import json,sys;print(json.load(sys.stdin)["esearchresult"]["querykey"])')
curl -s "https://trace.ncbi.nlm.nih.gov/Traces/sra-db-be/sra-db-be.cgi?rettype=runinfo&WebEnv=$W&query_key=$Q" \
  | python3 -c 'import csv,sys;r=[x["Run"] for x in csv.DictReader(sys.stdin)];print(len(r),len(set(r)))'
# 10000 5000
curl -s "https://trace.ncbi.nlm.nih.gov/Traces/sra-db-be/runinfo?acc=PRJEB82511" \
  | python3 -c 'import csv,sys;print(len({x["Run"] for x in csv.DictReader(sys.stdin)}))'
# 7263   (on a bad evening: 0)
```

Checked on 2026-10-23.

## What it is not

- **Not EFetch's cap.** [That one](efetch-returns-fewer-runs-than-esearch-counts.md) stops at
  10,000 records and pages by `retstart`. This one ignores `retstart`, so paging cannot help.
- **Not a timeout reported as one.** A cut-off answer closes cleanly with HTTP 200. Only a
  short last row gives it away.
- **Not `sra-be-timeout-on-large-projects`**, which this page replaces. That slug rested on one
  `ReadTimeout`, for PRJEB9859 in the 2026-05 script survey. On 2026-10-23 all three of its
  named projects (PRJEB9859, PRJEB82511, PRJEB19038) answered on both routes in full. What
  large projects do run into is the cap above, and the cut-off answers.

## Workaround

- **A capped history falls back to the project by accession.**
    - `sra_be.history_was_capped` recognises 10,000 rows, or any repeated run.
    - The route then asks `runinfo_by_accession` for the project.
    - If that answer holds fewer runs than the capped history already showed, it is a
      retryable failure.
- **A cut-off body is a failure.** `sra_be.parse_runinfo` raises when the last row is short of
  the header. That parser reads EFetch's runinfo too.
- **An empty answer to a counted history is a failure.** ESearch counted records, so an
  empty body is not that project's answer.

All three raise `MalformedResponseError`, which the survey client retries with backoff. A
project that still fails is recorded FAILED, never EMPTY.

## Affected accessions

Every project of more than 5,000 runs meets the cap: 19 of `reprocessed-prj`. Any project can
be met by a cut-off answer under load. The re-survey's failures are in its export:

```bash
uv run fetch survey export --route 'bioproject->run:sra_be_direct_cgi' --corpus reprocessed-prj
```

## Regression test

`tests/pathologies/test_sra_be_history_caps_at_5000_runs.py` has two tests:

- PRJEB82511's history still answers 5,000 distinct runs;
- the workaround still wins: by accession, the project answers all 7,263. It skips on an
  empty 200, which is the backend under load and not a verdict.
