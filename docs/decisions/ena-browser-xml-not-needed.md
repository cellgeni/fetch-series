---
title: The ENA browser XML API is not needed
---

# The ENA browser XML API is not needed

**Decided:** 2026-09-16 · **Reverses:** a plan item, on evidence

The [plan](https://github.com/cellgeni/fetch-series) listed
`providers/ena_browser.py` as untested and *"likely the cleanest single-record
route"*, to be compared against `filereport`. It was probed and the comparison
came out the other way. No provider was written.

## What the XML offers

```bash
curl -s 'https://www.ebi.ac.uk/ena/browser/api/xml/SRR25056225'
```

A run record with `EXPERIMENT_REF`, `IDENTIFIERS`, and a `RUN_LINKS` block
naming `ENA-SUBMISSION`, `ENA-STUDY`, `ENA-SAMPLE` and `ENA-FASTQ-FILES`. Also
an `EXTERNAL_ID namespace="GEO"` — which looked, at first, like the find of the
day.

## Every one of those is already in `filereport`

| From the XML | `filereport` field |
|---|---|
| `EXPERIMENT_REF` | `experiment_accession` |
| `ENA-STUDY` | `secondary_study_accession` |
| `ENA-SAMPLE` | `secondary_sample_accession` |
| `ENA-SUBMISSION` | `submission_accession` |
| `ENA-FASTQ-FILES` | `fastq_ftp`, `fastq_md5`, `fastq_bytes` |
| — | `first_public`, `last_updated` |

`filereport` returns them as parsed TSV, from the endpoint this project already
calls for every run, and accepts a project or study accession so one request
covers a whole series. The XML is one record per request and needs an XML
parser this project does not otherwise have.

## And the one thing that looked unique is unreliable

The GEO `EXTERNAL_ID` is present for **1 of 5** runs checked — `SRR25056225`
has it; `SRR9895473`, `ERR2861957`, `SRR6639101` and `SRR8363194` do not. It is
absent precisely where it would have mattered most: `SRR9895473` is a run of
GSE135325, the series GEO records no SRA relation for.

`filereport`'s `sample_alias` carries the GEO accession for **all** of those
runs that have one, GSE135325 included. That became
[`run->geo_sample:ena_alias`](../routes/run-to-geo-sample/ena-alias.md), which
agrees with GEO's own record 8,604 times out of 8,604.

So the probe was worth doing and produced a better route than the one it was
looking for — just not through the API the plan expected.

## What would reverse this

A record type `filereport` does not cover. `result=read_run` is run-centric; if
this project ever needs analysis objects (`ERZ`) or submission-level metadata
beyond the accession, the browser API is where they live and this decision
should be revisited.

## Also noted

`last_updated` is a per-run date from the archive itself. The
[archive-state monitor](../benchmarks/index.md) currently detects churn by
re-resolving and diffing verdicts; comparing `last_updated` would spot a changed
record in one request, without resolving anything. Unimplemented, and the
cheaper design of the two.

## Since: the catalogue probes it

`ena_portal.browser_xml` and `browser_cross_references` exist for
[the link catalogue](../graph/catalogue.md), which records that a project's
XML names its GEO series (`PRJNA258286` → `GSE60450`) or its ArrayExpress
experiment as a URL (`PRJEB15401` → `E-MTAB-5061`). No route reads them, and
the reliability finding above is why that link is drawn as untested rather
than surveyed into a route.

One clause above no longer holds: analysis objects do not need the browser
API. `filereport` with `result=analysis` reports an analysis' study, samples,
experiment, runs, submission and files, and accepts a project, study or sample
accession as well as the `ERZ` itself (`ERZ10183054` names four runs).
