---
title: "[GPRJ] misses experiments its project holds"
---

# `[GPRJ]` misses experiments its project holds

**Archive:** NCBI E-utilities (`db=sra`)
**Affected routes:** [`bioproject->run:efetch_direct`](../routes/bioproject-to-run/index.md),
[`bioproject->run:sra_be_direct_cgi`](../routes/bioproject-to-run/index.md)
**Status:** deterministic; nothing in the route recovers it. The union with the ELink and ENA
routes does.
**Measured:** 2026-10-26, over the 12,755 BioProjects of `reprocessed-prj`; reproduced the
same day

## Symptom

An ESearch of `db=sra` for `PRJNA66167[GPRJ]` finds 2,902 experiments, and their runs number
5,499. ENA, ELink and NCBI's own runinfo all put 5,910 runs in the project. The 411 missing
runs are in experiments that are not indexed under the project's `[GPRJ]`, although every
record of them names it:

- `SRX135150[ACCN]` finds the experiment.
- `SRX135150[ACCN] AND PRJNA66167[GPRJ]` finds nothing.
- `runinfo?acc=SRR453077`, a run of that experiment, gives `BioProject` as `PRJNA66167`.

The same holds for all 30 experiments of its study, SRP012040.

No error is given and the search is not rewritten. The query translation is
`PRJNA66167[GPRJ]` as asked, and the answer is a smaller project.

## The number

Over the 12,755 projects, the direct routes miss **1,100 runs in 2 projects** that ELink
returns and ENA lists under the same project:

| Project | `[GPRJ]` finds | ELink finds | Missed by `[GPRJ]`, listed by ENA |
|---|---:|---:|---:|
| PRJNA30709 | 7,665 runs | 8,574 | 689 |
| PRJNA66167 | 5,499 runs | 5,910 | 411 |

A third difference, PRJNA1035929 (0 against 8), was timing: the project was empty to `[GPRJ]`
when surveyed and answers 8 now.

## Reproducer

```bash
E=https://eutils.ncbi.nlm.nih.gov/entrez/eutils
curl -s "$E/esearch.fcgi?db=sra&term=SRX135150%5BACCN%5D&retmode=json&retmax=0" | grep -o '"count":"[0-9]*"'
# "count":"1"
curl -s "$E/esearch.fcgi?db=sra&term=SRX135150%5BACCN%5D+AND+PRJNA66167%5BGPRJ%5D&retmode=json&retmax=0" | grep -o '"count":"[0-9]*"'
# "count":"0"
curl -s "https://trace.ncbi.nlm.nih.gov/Traces/sra-db-be/runinfo?acc=SRR453077" | cut -d, -f1,22 | head -2
# the run's BioProject column: PRJNA66167
```

Checked on 2026-10-26.

## What it is not

- **Not a fall-through to `[All Fields]`.** That was this slug's predecessor,
  `gprj-falls-through-to-all-fields`, now retired:
    - every `[GPRJ]` search checked kept its field;
    - the 22 projects `[GPRJ]` still finds empty answer `phrasesnotfound`, not a broader search;
    - so does a project accession that does not exist.
- **Not ELink's error.** ELink [returns too much](elink-returns-runs-of-sibling-projects.md),
  in other projects. Here `[GPRJ]` returns too little, in this one.

## Workaround

None inside the route. The resolver's union mode asks the ELink and ENA routes too, and both
return these runs, so the default output holds them. A caller using `Mode.FIRST` gets ENA
first, which holds them.

## Affected accessions

```bash
python3 -c "
import csv, gzip, json
read = lambda r: {x['accession']: set(json.loads(x['results'] or '[]')) for x in csv.DictReader(gzip.open(f'data/surveys/reprocessed-prj/bioproject-to-run_{r}.csv.gz', 'rt'))}
d, e, n = read('efetch_direct'), read('efetch_elink'), read('ena_filereport')
print({a: len((e[a] - d[a]) & n.get(a, set())) for a in d if (e[a] - d[a]) & n.get(a, set())})"
```

## Regression test

`tests/pathologies/test_gprj_misses_experiments_of_the_project.py` asserts that SRX135150 is
still outside `PRJNA66167[GPRJ]` while its run's runinfo still names PRJNA66167.
