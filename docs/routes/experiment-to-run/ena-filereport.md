---
title: Experiment to run, via the ENA portal
---

# Experiment to run, via the ENA portal

**Route id:** `experiment->run:ena_filereport`
**Provider:** EBI ENA portal
**Direction:** `SRX`/`ERX`/`DRX` → `SRR`/`ERR`/`DRR`

```bash
curl -s 'https://www.ebi.ac.uk/ena/portal/api/filereport?accession=SRX20810434&result=read_run&fields=run_accession&format=tsv&limit=0'
```

The second tier of the GEO path. GEO's SOFT file names *experiments* (`SRX`), taken
from the submitter's record; it never names a run. This route asks ENA which runs each
of those experiments holds. `result=read_run` rows **are** runs, so an answer here
proves the data exists (`proves_data_exists`). That property is why the route matters.
The empty answers are not noise; they are the measurement:
[some experiments GEO names carry no runs at all](../../pathologies/soft-names-experiments-with-no-runs.md).

## Measured

**Corpus:** `sample:3000@results-of:gse->experiment:soft_family@reprocessed-gse`, a seeded
draw of 3,000 of the 308,639 experiments the
[GSE to experiment](../gse-to-experiment/index.md) `soft_family` census returned.
**Surveyed:** 2026-10-19.

| | |
|---|---|
| Resolved | 2,963 |
| Empty | **37 (1.23%)** |
| Failed | 0 |
| Unique runs | 4,811 |

### Why this replaced an earlier survey

Until 2026-10-19 this route cited a 2026-09-15 run made with `--limit 3000`. `--limit`
records its verdicts under the **full** corpus name, so that evidence read as a census of
308,639 experiments when it covered 3,000 of them: the first 3,000 in the survey's seeded
visit order. That was a fair random sample, but its name did not say so, and a second run
with a different limit would have recorded itself under the same name and looked
comparable. `survey run --limit` now writes to a throwaway cache for exactly this reason.
The old export stays under `data/surveys/results-of_gse-to-experiment_soft_family@reprocessed-gse/`
and is cited by nothing.

The two draws share **no** accession, and that is not a coincidence. `sample_of` and the
runner's shuffle use the same seed, and `random.sample` picks first the elements that
`shuffle` moves to the end, so a seeded sample and a shuffle prefix are disjoint by
construction. Each is still a uniform draw. The re-survey is therefore an independent
replicate, and it found the same empty count, 37 of 3,000.

Unique runs differ (6,580 against 4,811) because runs per experiment are heavy-tailed: a
few experiments with dozens of lane runs move the total a lot. Read run yield from the
[BioProject to run](../bioproject-to-run/index.md) census, not from here.
