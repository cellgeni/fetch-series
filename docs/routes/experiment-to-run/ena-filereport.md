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

**Corpus:** the experiments the
[GSE to experiment](../gse-to-experiment/index.md) `soft_family` census returned over
`reprocessed-gse`: 308,639 of them.

| | |
|---|---|
| Resolved | 2,963 |
| Empty | **37 (1.23%)** |
| Failed | 0 |
| Unique runs | 6,580 |

### The corpus label on this evidence is wrong, and a re-survey is pending

These numbers come from a `--limit 3000` run. `--limit` records its verdicts under the
**full** corpus name, so the evidence reads as a census of 308,639 experiments when it
covered 3,000 of them: the first 3,000 in the survey's seeded visit order. That is a
fair random sample, but its name does not say so, and a second run with a different
limit would have recorded itself under the same name and looked comparable.
`survey run --limit` now writes to a throwaway cache for exactly this reason.

On 2026-10-19 the route was re-surveyed over the seeded draw
`sample:3000@results-of:gse->experiment:soft_family@reprocessed-gse`, giving 2,963
resolved, **37 empty**, 0 failed and 4,811 unique runs. That result is in the survey
cache but not yet exported, so `RouteEvidence` still cites the run above until it is.

The two draws share **no** accession. The disjointness is not a coincidence:
`sample_of` and the runner's shuffle use the same seed, and `random.sample` picks first
the elements that `shuffle` moves to the end. So a seeded sample and a shuffle prefix
are disjoint by construction, and each is still a uniform draw. The re-survey is
therefore an independent replicate, and it found the same empty rate exactly.

Unique runs differ (6,580 against 4,811) because runs per experiment are heavy-tailed:
a few experiments with dozens of lane runs move the total a lot. Read run yield from
the [BioProject to run](../bioproject-to-run/index.md) census, not from here.
