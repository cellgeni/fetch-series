---
title: ELink gives a BioProject its siblings' runs
---

# ELink gives a BioProject its siblings' runs

**Archive:** NCBI E-utilities (ELink `bioproject` → `sra`)
**Affected routes:** [`bioproject->run:efetch_elink`](../routes/bioproject-to-run/index.md),
[`bioproject->run:sra_be_elink`](../routes/bioproject-to-run/index.md)
**Status:** deterministic; declared, not filtered (see Workaround)
**Measured:** 2026-10-26, over the 12,755 BioProjects of `reprocessed-prj`; reproduced the
same day

## Symptom

ELink from a BioProject to `db=sra` returns runs that belong to other BioProjects: the
project's siblings under a shared umbrella. Every route that follows the link attributes
those runs to the project it was asked about.

PRJNA1033389 holds 8 runs, by `[GPRJ]`, by ENA, and by each run's own runinfo. Through ELink
it holds 80. The other 72 belong to its siblings:

- PRJNA1033389 links up to the umbrella PRJNA1121229 (`bioproject_bioproject_d2u`).
- That umbrella links down to PRJNA1121326, PRJNA1121327 and PRJNA1033389.
- SRR29287414, one of the 72, has PRJNA1121326 as its own `BioProject` in NCBI's runinfo,
  and as its study in ENA.

## The number

Over the 12,755 projects, ELink returns **607 runs in 22 projects** that ENA does not list
under the project asked about. Most come in small numbers, from projects sharing an umbrella:

| Project | Runs by `[GPRJ]` | Runs ENA does not list under it |
|---|---:|---:|
| PRJNA30709 | 7,665 | 220 |
| PRJNA1240655 | 150 | 104 |
| PRJNA1033389 | 8 | 72 |
| PRJNA1273890 | 44 | 37 |
| PRJNA648991 | 18 | 36 |
| PRJNA486013 | 2 | 34 |
| …16 more | | 104 |

Every NCBI route returns some runs ENA's census of 2026-09-15 lacks: `efetch_direct` returns
2,310, mostly released since. `efetch_elink` returns 2,812, and these runs are most of the
difference.

## Reproducer

```bash
E=https://eutils.ncbi.nlm.nih.gov/entrez/eutils
curl -s "$E/elink.fcgi?dbfrom=bioproject&db=bioproject&id=1121229&retmode=json&cmd=neighbor" | grep -o '"links":\[[^]]*\]'
# the umbrella's children: 1121327, 1121326, 1033389
curl -s "https://trace.ncbi.nlm.nih.gov/Traces/sra-db-be/runinfo?acc=SRR29287414" | cut -d, -f1,22 | head -2
# SRR29287414's own BioProject: PRJNA1121326
uv run fetch resolve PRJNA1033389 --to run --explain | grep SRR29287414
# SRR29287414	UNCONFIRMED	efetch_elink,sra_be_elink
```

Checked on 2026-10-26.

## What it is not

- **Not wrong for the umbrella.** Asked about an umbrella project, its children's runs are the
  answer. The defect is a child being given its siblings' runs.
- **Not the `[GPRJ]` defect.** [That one](gprj-misses-experiments-of-the-project.md) misses
  runs of the project. This one adds runs of others.

## Workaround

None applied. The routes declare this pathology, and `--explain` prints it beside each one's
answer.

- **A filter exists.** NCBI's runinfo carries each run's own `BioProject`.
- **It is not applied.** Kept to the project asked about, it would empty the umbrella case
  above, and nothing has measured how often the corpus asks about an umbrella.
- **The resolver already marks these runs.** Each run's provenance names its routes, and a run
  ENA does not hold is `UNCONFIRMED`: `fetch resolve PRJNA1033389 --to run --explain` lists
  SRR29287414 as `UNCONFIRMED` from `efetch_elink,sra_be_elink` alone, and every run of the
  project's own as `confirmed`.

## Affected accessions

```bash
python3 -c "
import csv, gzip, json
read = lambda r: {x['accession']: set(json.loads(x['results'] or '[]')) for x in csv.DictReader(gzip.open(f'data/surveys/reprocessed-prj/bioproject-to-run_{r}.csv.gz', 'rt'))}
d, e, n = read('efetch_direct'), read('efetch_elink'), read('ena_filereport')
print({a: len(e[a] - d[a] - n.get(a, set())) for a in e if e[a] - d[a] - n.get(a, set())})"
```

## Regression test

`tests/pathologies/test_elink_returns_runs_of_sibling_projects.py` asserts that ELink from
PRJNA1033389 still returns SRR29287414, whose own runinfo names PRJNA1121326.
