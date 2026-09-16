---
title: Run to GEO sample, via ENA's submitter aliases
---

# Run to GEO sample, via ENA's submitter aliases

**Route ids:** `run->geo_sample:ena_alias`, `run->ae_experiment:ena_alias`
**Provider:** EBI ENA portal
**Direction:** `SRR`/`ERR`/`DRR` → `GSM`, or → `E-MTAB`

ENA returns the submitter's own name for every run, experiment and sample. For a
GEO-brokered submission that name is **the GEO accession itself**:

```bash
curl -s 'https://www.ebi.ac.uk/ena/portal/api/filereport?accession=SRR9895473&result=read_run&fields=run_alias,experiment_alias,sample_alias&format=tsv'
```

```
SRR9895473    GSM4005486_r1    GSM4005486    GSM4005486
```

## Why this matters more than it looks

`SRR9895473` is a run of **GSE135325** — the series in this knowledge base's
[GEO omits the SRA relation](../../pathologies/geo-omits-sample-sra-relation.md)
pathology, where GEO records a BioSample for every sample and an SRA relation
for none. Every route that goes *through* GEO cannot attribute that run to a
GSM. ENA has been carrying the answer the whole time, in a field nothing was
reading.

The same holds for `SRR10229932` (GSE137444), and ArrayExpress submissions carry
their own accession the same way — `ERR2861957`'s alias is
`E-MTAB-6505:Day2_r1`.

**One request**, against an endpoint the file layer already calls, with no SOFT
family download and no BioSample join.

## Measured against the archive that has authority

An alias is submitter-chosen free text, so a route reading an accession out of it
is only worth having if it agrees with GEO's own record. GEO's SOFT family file
states, per sample, which SRA experiment that sample is; that is the authority.

**200 GEO series, 8,607 runs, 2026-09-16:**

| | Runs | |
|---|---:|---|
| Both name a GSM, and it is **the same one** | **8,604** | |
| Both name a GSM and they **differ** | **0** | disqualifying, had there been any |
| SOFT names one, the alias does not | **0** | |
| The alias names one, SOFT does not | 3 | the recovery |

**Agreement where both answer: 8,604 of 8,604 — 100.000%.** Not one
contradiction. `SRR24033304`, `SRR22853603` and `SRR22853604` are the three the
alias recovers.

## Coverage, and what it tracks

**Corpus:** `sample:3000@reprocessed-srr`. **Surveyed:** 2026-09-16.

| | |
|---|---|
| Resolved | 2,477 (82.6%) |
| Empty | 523 |
| Failed | **0** |
| Unique GEO samples | 2,395 |

The 82.6% is not a shortfall, it is the population. **82.7% of the reprocessed
sample column is GEO samples**; the rest were deposited straight to SRA or ENA
and have no GSM to find. The route resolves almost exactly the share of runs
that came through GEO.

Where the submitter named their samples something else, it returns nothing and
means it: `SRR27190799`'s `sample_alias` is `E12`, and `DRR188691`'s is
`SAMD00142600` — a BioSample, which an unanchored pattern would have been happy
to mistake for an accession.

## Parsing, and why it is strict

`^GSM\d+` anchored, required to end at a non-alphanumeric, and `sample_alias`
read before the other two because it is the bare accession where the others
carry an `_r1` suffix.

Guessing would be worse than answering nothing. An unanchored `GSM\d+` matches
`GSM123` inside `GSM123ABC` and attributes the run to a sample that may well
exist — and for a tool whose whole claim is provenance, attributing data to the
wrong sample is the failure that matters most.

## How to use it

As a **cross-check**, not a replacement. It agrees with SOFT on everything SOFT
answers, so where the two disagree in future that is a finding about the
archives worth recording rather than a tie to break. Where SOFT answers nothing
— the GSE135325 case — it is the cheapest route there is.
