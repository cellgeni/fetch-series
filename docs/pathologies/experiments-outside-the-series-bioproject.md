---
title: A series' BioProject holds experiments that are not the series'
---

# A series' BioProject holds experiments that are not the series'

**Archive:** NCBI GEO / SRA, read through ENA
**Affected route:** [`gse->experiment:soft_bioproject_ena`](../routes/gse-to-experiment/index.md)
**Status:** measured; worked around in `relations`; declared on the route
**Measured:** from the 2026-09-15 census exports; re-checked live 2026-10-23

## The number

The route takes the BioProject a series' SOFT file names and asks ENA for every experiment
in it. A project is not a series. It can also hold runs submitted to SRA directly under the
same project, or other series' runs. Over the census of 12,540 series that both this route
and `soft_family` resolve, **77 series** get experiments the SOFT file does not name:
**5,115 in all**.

| Series | Extra experiments |
|---|---:|
| GSE290975 | 2,362 |
| GSE175521 | 1,726 |
| GSE234261 | 232 |
| GSE98600 | 76 |

## Reproducer

```bash
curl -s 'https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE234261&targ=self&form=text&view=brief' \
  | grep -E 'BioProject|Series_sample_id' | sort | uniq -c | sort -rn | head -2
# 100 !Series_sample_id ...      one BioProject: PRJNA980643

curl -s 'https://www.ebi.ac.uk/ena/portal/api/filereport?accession=PRJNA980643&result=read_run&fields=sample_alias&format=tsv' \
  | tail -n +2 | cut -c1-3 | sort | uniq -c
# 100 GSM, and 232 runs whose aliases are T1-..., T6-...: submitted to SRA under the project, not in the series
```

Checked on 2026-10-23.

## What it is not

- **Not the umbrella project.** [That defect](series-under-shared-umbrella-bioproject.md) is a
  species-wide project holding thousands of unrelated series. This is a study's own project
  holding a little more than its series.
- **Not wrong data.** The extra experiments are real. They are not the series'.

## Workaround

`relations.py` keeps only the project's runs whose experiment the series' SOFT file names. It
falls back to the whole project only when that filter would leave nothing: GSE150508's SOFT
names a stale experiment with no runs. The route itself answers by project, and `--explain` names this page
beside its values.

## Affected accessions

```bash
python3 - <<'PY'
import csv, gzip, sys; csv.field_size_limit(sys.maxsize)
load = lambda n: {r["accession"]: set(filter(None, r["results"].split(";"))) for r in
    csv.DictReader(gzip.open(f"data/surveys/reprocessed-gse/{n}.csv.gz", "rt")) if r["outcome"] == "resolved"}
a, b = load("gse-to-experiment_soft_bioproject_ena"), load("gse-to-experiment_soft_family")
print(sorted(g for g in set(a) & set(b) if a[g] - b[g]))
PY
```

## Regression test

`tests/pathologies/test_experiments_outside_the_series_bioproject.py` asserts that
PRJNA980643 still holds runs whose sample alias is not one of GSE234261's GSMs.
