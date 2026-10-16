---
title: The link catalogue
---

# The link catalogue

**Needs:** nothing **Feeds:** [the route graph](../routes/graph.md)'s untested
links, and the next round of surveys
**Rule:** a catalogue link says a link *exists*. Only a route, with evidence,
says which link to *use*.

The [route registry](../routes/index.md) holds the links somebody has
measured. The archives answer far more than that. One ENA `filereport` request
returns seven INSDC levels from any one of them, and only twelve of those 42
pairs are routes. Before this catalogue nothing led to an INSDC sample at all,
no route joined an INSDC sample to its BioSample though ENA returns both in one
row, and an ArrayExpress sample was not modelled.

The catalogue lists every one-hop link an archive API offers between the
nodes below, says what each is read from, and confirms each one with a live
request. It lives in `src/fetch_series/catalogue/`, apart from `REGISTRY`, so
nothing in it can change the resolver, the ranking, or the parity of
`links.tsv`.

<!-- catalogue-counts -->

## Nodes: one per level and database

INSDC is merged. An `SRS`, `ERS` and `DRS` are one entity under three
prefixes, and one node; so are the experiment, run, study, submission and
analysis triples. GEO, ArrayExpress and BioStudies records stay apart from
them, because they are curated records that *point at* INSDC ones rather than
mirrors of them, which is the distinction
[`EntityType`](accession-types.md) already draws.

| Level | Nodes |
|---|---|
| Dataset | BioProject · INSDC study · GEO series · ArrayExpress experiment · BioStudies study |
| Sample | BioSample · INSDC sample · GEO sample · ArrayExpress sample |
| Library | INSDC experiment |
| Run | INSDC run |
| File | ENA files · NCBI SRA files · ArrayExpress files · GEO supplementary files |
| Other | INSDC submission · INSDC analysis · GEO platform |

### Files are grouped by the archive whose record lists them

Not by the host that serves the bytes. An SDRF's `Comment[FASTQ_URI]` usually
points at ENA's FTP server, and it is still ArrayExpress's statement about
which files belong to the run. That statement can be wrong where ENA's is
right, as
[E-MTAB-8060](../pathologies/ae-sdrf-points-at-decommissioned-mirror.md)
shows, so the two have to stay distinguishable. The four existing file routes
land accordingly: `run->file:ena_fastq`, `ena_submitted` and `ena_sra` on ENA
files, `run->file:sdl` on NCBI SRA files, `ae_experiment->file:sdrf` on
ArrayExpress files.

### An ArrayExpress sample has no accession

It is a row of an SDRF. Nothing issues it an identifier, so it is addressed as
the experiment plus the row's `Source Name` (`E-MTAB-5061:AZ_A1`), the same
spelling ENA uses for the sample's alias. Every link from it therefore starts
with fetching the experiment's SDRF. One row is one *assay*, so several rows
can describe one specimen. It is modelled because the SDRF row is where
ArrayExpress joins its own sample to a BioSample, an INSDC sample, a run and
its files, and those joins are otherwise invisible.

## Mechanisms

A mechanism is one request shape and the fields of its answer. Links are
generated from its inputs × outputs, so a response that fans out is declared
once. A route-specific chain (an ELink followed by EFetch, a SOFT file followed
by an ENA lookup) declares only the link its route measures; the general
fan-out of the response it ends in is declared once, on its plainest
mechanism.

**Basis** says what the documentation says. *Documented* means the archive's
own documentation names the field or parameter the link is read from, and was
checked against the page on 2026-10-16. *Live-only* means it does not. Those
links cite nothing, and their probe is the whole of the evidence for them.

NCBI serves its documentation to scripts as a browser check, so its pages were
read through the Internet Archive's copies. GEO's SOFT page documents
`!Series_sample_id`, `!Sample_platform_id` and `!Sample_supplementary_file`,
and **none of the `_relation` lines** (BioProject, SRA, BioSample, SuperSeries
or SubSeries) that every GEO route reads. GEO adds those itself. The E-utilities
book documents `linkname` and `cmd=neighbor`, and not `rettype=runinfo`. The
SRA backend's runinfo and NCBI's Storage Data Locator have no public
documentation; SDL's own root answers *"Documentation is missed"*.

<!-- catalogue-mechanisms -->

## Probes

Each untested link is confirmed by one live request, with a fixed accession
from one of two linked families the knowledge base already cites:

| | Dataset | Sample | Library | Run | Other |
|---|---|---|---|---|---|
| NCBI / GEO | `GSE60450`, `PRJNA258286`, `SRP045534` | `GSM1480291`, `SAMN02991530`, `SRS685538` | `SRX681985` | `SRR1552444` | `SRA179382`, `GPL13112` |
| EBI / ArrayExpress | `E-MTAB-5061`, `PRJEB15401`, `ERP017126` | `SAMEA4437023`, `ERS1348472`, `E-MTAB-5061:AZ_A1` | `ERX1700348` | `ERR1630015` | `ERA705448` |

The EBI half is used where a mechanism only answers for an EBI-issued or
ArrayExpress-brokered record: an alias names an ArrayExpress experiment only on
a run ArrayExpress brokered. A few links need a record of their own: a
SuperSeries (`GSE236087`), an umbrella BioProject (`PRJNA988804`), an analysis
(`ERZ10183054`, in `PRJEB44468`), a Europe PMC record (`S-EPMC9302581`), and
the GEO import `E-GEOD-60450`.

One request answers every link a mechanism offers from one accession. The
values are then normalised by the target, not trusted: an accession field keeps
only tokens that `fetch_series.accession.parse` places at the target's type. A
self link drops its own input; E-MTAB-5061's JSON links E-MTAB-5061 (its
Expression Atlas page), which is no link.

```bash
uv run fetch catalogue probe --write   # one request per mechanism and accession
uv run fetch catalogue list            # nodes, links by status, and the gaps
```

Results are in `src/fetch_series/catalogue/probes.json`, dated in UTC. A probe
that found nothing leaves its link **documented, not confirmed**. Such a link
is listed here and in the graph's panels and never drawn, because a line would
claim a link nobody has seen answer. A probe that failed is kept apart from one
that found nothing, as everywhere else in this project.

### A probe is not evidence

A probe proves that a link exists, for one record, on one day. It measures
nothing: one accession out of one is not a yield, and a link that answered for
GSE60450 says nothing about how often it answers. So a probe never becomes
`RouteEvidence`, never changes a ranking, and carries no counts on the graph.
The argument is in
[what counts as evidence](../decisions/what-counts-as-evidence.md#a-probe-is-not-evidence).
Measuring an untested link is a survey, and the next step.

## What the probes found on the way

- **ENA's `links` endpoint reads `limit=0` as zero rows.** `filereport` and
  `search` read the same parameter as *no limit*. PRJEB44468's 106 analyses
  came back as a header and nothing else until the parameter was dropped.
- **ENA's `links` takes only primary accessions.** `ERP017126` and
  `ERS1348472` return an empty table where `PRJEB15401` and `SAMEA4437023`
  return their records: an empty answer indistinguishable from *no links*.
- **NCBI's `bioproject_bioproject` link answers nothing.** The umbrella
  relation is carried by the directed `bioproject_bioproject_u2d` /
  `_d2u` pair. NCBI records no umbrella for PRJNA258286 at all, where ENA's
  `parent_study_accession` names PRJNA9559.
- **ENA answers a burst of portal searches with an HTML 500**, not a 429.
  The probes pace EBI at two requests a second and retry.
- **The BioSamples API's `extd:ArrayExpress` filter returns a sample group**
  (`SAMEG318185` for E-MTAB-5061), not samples, so it is not an
  ArrayExpress → BioSample link and is not declared.
- **`gds` ESummary needs the entry type.** `GSM1480291[ACCN]` alone matches the
  sample, its series and its platform, and the first UID is the series'.

## Gaps

Every node a node cannot reach in one request: no mechanism in the catalogue
links them, or one does and its probe found nothing. Generated from the
catalogue at build time.

<!-- catalogue-gaps -->
