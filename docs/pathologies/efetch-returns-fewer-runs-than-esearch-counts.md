---
title: One EFetch answers at most 10,000 runs, whatever ESearch counted
---

# One EFetch answers at most 10,000 runs, whatever ESearch counted

**Archive:** NCBI E-utilities
**Affected routes:** [`bioproject->run:efetch_direct`](../routes/bioproject-to-run/index.md),
[`bioproject->run:efetch_elink`](../routes/bioproject-to-run/index.md)
**Status:** deterministic; worked around by paging the history (`eutils.efetch_pages`)
**Measured:** reproduced 2026-10-23; both routes re-surveyed over 12,755 BioProjects, 2026-10-24 to 26

## Symptom

ESearch over `db=sra` counts **experiments**. An EFetch of that history with
`rettype=runinfo` lists **runs**. The two numbers never match exactly, and for two reasons
they can differ a great deal. One is a defect and one is not.

- **One request stops at 10,000 records.** PRJEB14362 counts 46,085 experiments. One EFetch of
  its history answers 9,999 runs with `retmax=10000`, and **5,387** with `retmax=100000`. No
  error is given, and the answer is an HTTP 200 with a well-formed CSV. Paged by `retstart` in
  steps of 5,000, the same history answers all 46,468 runs.
- **An experiment can hold no run.** PRJNA940674 counts 4 experiments and lists 3 runs. The
  fourth, SRX19556101, has an empty `runs` field in its ESummary. That is the record, not a
  defect, and nothing recovers a run that does not exist.

The 2026-05 surveys of both EFetch routes made one request a project. That single request is
why the knowledge base said, until 2026-10-23, that ENA holds **111,665 runs `efetch` never
returns**.

## The number

The same 12,755 BioProjects (`reprocessed-prj`). These are the runs ENA lists that each EFetch
route does not, set against ENA's census of 2026-09-15:

| | One request (2026-05-12) | Paged (2026-10-26) |
|---|---:|---:|
| ENA runs `efetch_elink` does not return | 111,665 | **162** |
| ENA runs `efetch_direct` does not return | 112,314 | **1,270** |
| distinct runs, `efetch_elink` | 674,186 | 786,586 |
| distinct runs, `efetch_direct` | 673,157 | 784,976 |

- **The scale.** Seven projects of the corpus hold 10,000 runs or more, and 19 hold more
  than 5,000. The largest is PRJNA772116, at 73,888 runs.
- **What remains is not this defect.** For `efetch_direct`, most of what is left is `[GPRJ]`
  matching different experiments from ELink. PRJNA66167's `[GPRJ]` search finds 5,499 runs,
  where ELink and ENA find 5,910.
- **Answers that stop partway.** On 2026-10-23, before the cut-off check existed, a survey
  recorded 51,726 of PRJNA772116's runs from a response that raised no error. That is the
  same failure [sra-db-be shows under load](sra-be-history-caps-at-5000-runs.md), and the
  runinfo parser now refuses a body whose last row is short. The re-survey answered all
  73,888.

## Reproducer

```bash
E=https://eutils.ncbi.nlm.nih.gov/entrez/eutils
h=$(curl -s "$E/esearch.fcgi?db=sra&term=PRJEB14362%5BGPRJ%5D&usehistory=y&retmode=json&retmax=0")
W=$(echo "$h" | python3 -c 'import json,sys;print(json.load(sys.stdin)["esearchresult"]["webenv"])')
Q=$(echo "$h" | python3 -c 'import json,sys;print(json.load(sys.stdin)["esearchresult"]["querykey"])')
echo "$h" | python3 -c 'import json,sys;print(json.load(sys.stdin)["esearchresult"]["count"])'
# 46085
curl -s "$E/efetch.fcgi?db=sra&WebEnv=$W&query_key=$Q&retmax=100000&rettype=runinfo&retmode=text" | grep -c '^[SDE]RR'
# 5387   (9999 with retmax=10000)
curl -s "$E/efetch.fcgi?db=sra&WebEnv=$W&query_key=$Q&retstart=10000&retmax=5000&rettype=runinfo&retmode=text" | grep -c '^[SDE]RR'
# 5000   -- the records past the cap are there, a page at a time
```

Checked on 2026-10-23.

## What it is not

- **Not the URL.** The history carries the set. No UID list is in the query string.
- **Not the 500-UID ESummary limit.** [That one](esummary-500-uid-limit.md) refuses with an
  error inside its JSON. This one answers part of the set and says nothing.
- **Not [`sra-db-be`'s cap](sra-be-history-caps-at-5000-runs.md).** That backend's history form
  stops at 5,000 distinct runs and ignores `retstart`, so paging does not help it.

## Workaround

- **Page the history.** `eutils.efetch_pages` asks for `retstart` 0, 5,000, 10,000, … up to
  ESearch's count (`EFETCH_PAGE`). It pages a UID list in slices of the same size.
- **Count runs, not experiments.** Both routes deduplicate runs across pages. Nothing compares
  the run count with ESearch's experiment count, because an experiment may hold none.
- **A cut-off page is a failure.** `sra_be.parse_runinfo`, which reads every EFetch page,
  raises when the last row is short of the header, and the survey client retries it.

## Affected accessions

Any project of more than 5,000 experiments asked for in one request. The corpus's projects of
5,000 runs or more, from the paged survey:

```bash
uv run fetch survey export --route 'bioproject->run:efetch_elink' --corpus reprocessed-prj
python3 -c "
import csv, gzip
rows = csv.DictReader(gzip.open('data/surveys/reprocessed-prj/bioproject-to-run_efetch_elink.csv.gz', 'rt'))
print(sorted((int(r['n_results']), r['accession']) for r in rows if int(r['n_results'] or 0) > 5000))"
```

## Regression test

`tests/pathologies/test_efetch_returns_fewer_runs_than_esearch_counts.py` has three tests:

- PRJEB14362's single EFetch still stops at or below 10,000 runs;
- PRJNA940674 still counts 4 and lists 3;
- the workaround still wins: paged, the same history answers more than 40,000 runs.
