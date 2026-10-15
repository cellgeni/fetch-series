---
title: Run to experiment, via the ENA portal
---

# Run to experiment, via the ENA portal

**Route id:** `run->experiment:ena_filereport`
**Provider:** EBI ENA portal
**Direction:** `SRR`/`ERR`/`DRR` → `SRX`/`ERX`/`DRX`

```bash
curl -s 'https://www.ebi.ac.uk/ena/portal/api/filereport?accession=SRR25056225&result=read_run&fields=experiment_accession&format=tsv'
```

Walking *up* the hierarchy, so that a bare run accession — the thing a
collaborator actually pastes into an email — is a valid entry point to the whole
graph.

## Measured

**Corpus:** `sample:3000@reprocessed-srr`, a seeded draw from the 262,690 runs
the pipeline has reprocessed. **Surveyed:** 2026-09-15.

| | |
|---|---|
| Resolved | 2,977 |
| Empty | **23** |
| Failed | 0 |
| Unique experiments | 2,888 |

2,977 runs to 2,888 experiments — runs share experiments, as they should: a 10x
library sequenced across lanes is one experiment and several runs.

## The 23 empties are runs that no longer exist

They are not a defect in this route. Every one of the 23 was checked against
NCBI directly, and **all 23 are absent from `esearch db=sra` as well**:

```bash
curl -s 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=sra&term=SRR28779487&retmode=json'
# "count": "0"
```

Every one of these runs was reprocessed successfully at some point — that is how
it got into the corpus — and is now unresolvable in either archive. Withdrawn,
suppressed, or moved to controlled access; the archives do not distinguish, and
nothing announces any of them.

**0.77% of a random sample of already-processed runs.** That is the attrition
rate of public sequencing data over the corpus's lifetime, and it is the reason
[`fetch cache diff`](../../index.md) and the archive-state monitor exist: a
resolution that worked last year is not evidence that it works today, and the
only way to know is to keep asking and record the dates.
