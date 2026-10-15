---
title: GEO series → INSDC experiment
---

# GEO series → INSDC experiment

**Needs:** a GSE accession **Feeds:** experiment → run → files
**Routes:** 2 measured, both incomplete

The direction that matters most for reprocessing, and the clearest example in this
knowledge base of why a fallback *order* is not enough.

## Routes

| Route | Provider | Requests | What it reads |
|---|---|---|---|
| `gse->experiment:soft_family` | GEO FTP | 1 | `!Sample_relation = SRA:` in the SOFT family file |
| `gse->experiment:elink_gds_sra` | E-utilities | 3 | ELink `gds`→`sra`, then ESummary |

## Measured behaviour

A census, not a sample: every one of the 13,045 GEO series in the reprocessed table,
surveyed 2026-09-14/15.

| | SOFT family | SOFT BioProject → ENA | ELink `gds`→`sra` |
|---|---|---|---|
| Requests per series | 1 | 2 | 3 |
| Series resolved | **13,022 (99.8%)** | 12,551 (96.2%) | 10,752 (82.4%) |
| Series empty | 16 | 487 | **2,293** |
| Series failed | 7 (404, private) | 7 | 0 |
| **Unique experiments** | **308,639** | 290,462 | 178,879 |
| Share of the three-route union | 98.3% | 92.5% | 57.0% |

The union of all three is 314,009 experiments, and at least one route resolves
**13,033 of 13,045 series (99.91%)**.

### ELink is redundant

| Pair | A-only | B-only | Shared |
|---|---|---|---|
| SOFT vs ELink | 132,987 | 3,227 | 175,652 |
| SOFT vs BioProject→ENA | 23,546 | 5,369 | 285,093 |
| ELink vs BioProject→ENA | **43** | 111,626 | 178,836 |

SOFT and the ENA route together account for 314,008 of the union's 314,009 experiments.
**Adding ELink contributes exactly one experiment — SRX8090149.** Three requests per
series, for one accession across the entire corpus.

### Completeness is not correctness

Measured directly over 3,000 experiments drawn from all 308,639 the census returned:
**1.23% carry no runs at all** (95% CI 0.84–1.63%), roughly **3,807 experiments** pointing at
nothing downloadable. See
[SOFT names experiments with no runs](../../pathologies/soft-names-experiments-with-no-runs.md).

The other 82% of that gap is a limitation of the ENA route rather than of SOFT: a series'
experiments do not all sit under the BioProject its SOFT file records, so keying the
filereport on the project misses them. Keyed on the experiment accession instead, ENA
returns them.

## Recommendation

**Call SOFT for the experiment list. Resolve those experiments to runs through ENA. Treat
"no runs" as the answer, not as an error.**

The verification is free, because resolving experiments to runs is the next step anyway.
An experiment that returns no runs is a stale SOFT entry, and saying so is more useful than
reporting a file list that downloads nothing.

ELink is not worth calling. It costs three requests, misses 17.6% of series silently, and
adds one experiment in 314,009 over the two cheaper routes.

See [ELink returns no links](../../pathologies/elink-gds-sra-missing-links.md) and
[GEO omits the SRA relation](../../pathologies/geo-omits-sample-sra-relation.md).

## The run-level twin

`gse->run:elink_gds_sra` takes the same ELink hop and follows it with EFetch
runinfo instead of ESummary, so it returns runs rather than experiments. Over
the same 13,045 series (2026-09-16):

| | `gse->experiment:elink_gds_sra` | `gse->run:elink_gds_sra` |
|---|---:|---:|
| Resolved | 10,752 | 10,751 |
| Empty | 2,293 | **2,294** |
| Failed | 0 | 0 |
| Unique results | 178,879 experiments | 463,803 runs |

**2,293 of the 2,294 empties are the same series**, and 2,281 of those are
series the SOFT family file resolves without difficulty. Confirming the defect
from the other side: it is in the ELink hop the two share, not in the ESummary
or EFetch step that follows it. Changing what you ask ELink to give you does not
change what ELink cannot find.

### The 39 failures were ours, not NCBI's

The first run of this census recorded 39 permanent failures. Every one was a
request this project never sent: GSE241770 links to **7,905** runs, and the
joined UID list overflowed httpx's own URI limit, which refuses as `InvalidURL`
rather than letting the server answer 414.

Retrying with the UID list posted instead resolved **all 39** and recovered
**72,777 runs**. It is the fifth time a UID list in a query string has cost this
project data, and the first time the loss happened client-side — which is worse,
because nothing in the failure named the archive or the size. The regressions
for all five now live in one file, `tests/test_request_limits.py`.
