# Where fetch-series stands

**2026-10-21** · [v0.1.0 released](https://github.com/cellgeni/fetch-series/releases/tag/v0.1.0),
then the link surveys and `fetch inspect` on `main` · knowledge base published at
[cellgeni.github.io/fetch-series](https://cellgeni.github.io/fetch-series/)

A point-in-time overview. The evidence behind every number here lives in
[`docs/`](docs/index.md) and in the `RouteEvidence` blocks in
[`src/fetch_series/graph.py`](src/fetch_series/graph.py); this file is the map,
not the territory.

---

## What exists now

A CLI that takes any public accession and hands back verified download links,
with every routing decision traceable to a measurement:

```bash
fetch links GSE111360                      # links.tsv, in the schema the pipeline reads today
fetch files SRR25056225 --verify --explain # links + checksums + why this set was chosen
fetch screen GSE109816                     # exits 2 — Smart-seq2, do not download 880 runs
fetch relations GSE236084 --sample-map     # GEO ↔ SRA ↔ ENA ↔ ArrayExpress, per run
fetch metadata PRJNA434002 --explain       # tissue/disease/donor per sample, as ontology CURIEs
fetch resolve GSM7518069 --to run --explain
fetch inspect run SRR29534765              # read every copy: ENA's fastq is UNFIT, the SRA object is 3' v2
fetch files SRR29534765 --inspect          # recommend, refusing what the reads prove unusable
```

Behind it:

| | |
|---|---|
| Routes declared, implemented and **measured** | **40 / 40 / 40** |
| Archives covered | GEO, SRA, ENA, ArrayExpress/BioStudies, DDBJ |
| Link catalogue | **1,127** one-hop links between 39 nodes: 96 measured by a route's survey, **288 surveyed** by a link survey of their own (268 answering usably, 20 never), 480 confirmed by one live probe, 263 unconfirmed; none ranked ([the catalogue](docs/graph/catalogue.md), [link survey results](docs/graph/link-survey-results.md)) |
| Read classifier | 10x kits from the first 100,000 reads: **0 errors in 66 held-out HIGH calls**, 0 in 28 MEDIUM ([benchmark](docs/benchmarks/inspect-2026-10.md)) |
| Content survey | every run-to-reads link's files read for 1,400 runs ([results](docs/benchmarks/content-survey-2026-10.md)) |
| Mapping block | classifier 0.4.0, set beside the reprocessing pipeline's own STARsolo runs on a fresh draw of 300 held-out samples: the pipeline's whitelist in **404 of 404** blocks, its strand in **386 of 386** that name one; 7 left ambiguous, down from 32 ([comparison](docs/benchmarks/inspect-vs-starsolo-2026-10.md)) |
| Metadata kit call | names one kit for **964 of 5,465** samples the pipeline mapped; its list is the pipeline's for 900, its strand for 946 of 948; where it and the reads disagree, **the pipeline sides with the reads 20 of 20** ([comparison](docs/benchmarks/metadata-vs-reads-2026-10.md)) |
| Other technologies | Parse, BD Rhapsody, inDrop and others named from their constants; **no non-10x run called 10x** on the held-out half ([benchmark](docs/benchmarks/technologies-2026-10.md)) |
| Survey verdicts recorded and committed | **681,144** across 3,364 files in `data/surveys/` |
| Offline tests | **893** collected by `uv run pytest` (plus integration, pathology and slow suites) |
| Knowledge-base pages | every measured route, 9 pathologies, 6 upstream drafts |

The governing rule held throughout: **no fallback order, ranking or heuristic
ships without a survey behind it and a KB page citing that survey.**
`RouteRegistry.ranked()` derives the order from the evidence. There is no
hand-written preference anywhere in the project.

---

## The findings that changed the code

| | Finding | Where |
|---|---|---|
| **39.2%** | of runs have an ENA fastq missing the read carrying the cell barcode — downloads cleanly, checksums correctly, and every read is unassignable to a cell | [pathology](docs/pathologies/ena-paired-library-single-fastq.md) |
| **17.6%** | of GEO series with SRA data report none through NCBI's `elink gds→sra`, with zero errors | [pathology](docs/pathologies/elink-gds-sra-missing-links.md) |
| **125,271** | BioSamples ENA finds that NCBI's link table does not; 93.6% of them in EBI-registered projects | [route](docs/routes/bioproject-to-biosample/index.md) |
| **every `SAMEA`/`SAMD`** | BioSample asked of `efetch` by accession returns a *different sample* — prefix stripped, remainder read as a UID, HTTP 200, full attribute block | [pathology](docs/pathologies/biosample-efetch-ignores-accession-prefix.md) |
| **41%** | of the BioProjects `esummary db=gds` recovers for series GEO leaves empty are species-level umbrellas — `PRJNA30709` holds 7,591 runs | [route](docs/routes/gse-to-bioproject/gds-summary.md) |
| **0.77%** | of runs already reprocessed successfully no longer exist in *either* archive, and nothing announces it | [route](docs/routes/run-to-experiment/ena-filereport.md) |
| **183/183** | runs whose ENA listing is one fastq are proven unusable by their reads; NCBI's Normalized object of the same run is fit for 142 runs with an unfit ENA fastq | [content survey](docs/benchmarks/content-survey-2026-10.md) |
| **248/452** | objects DDBJ Search links for NCBI runs do not exist at DDBJ | [content survey](docs/benchmarks/content-survey-2026-10.md) |
| **8,604/8,604** | agreement between GEO's own SRA relation and ENA's submitter alias — a route that works where GEO records no relation at all | [route](docs/routes/run-to-geo-sample/ena-alias.md) |

Four of the first five contradicted what this project's own code assumed before
anyone counted. The umbrella finding forced a rule onto the front page —
**coverage is not correctness** — because `gds_summary` wins every headline
number against `soft_family` and is the more dangerous route to trust blindly.

### Two claims that must not be confused

**1,566 of 1,566** recommended file links resolve and serve exactly the byte
count the archive published. **And ENA's fastq is incomplete for 39.2% of runs
anyway.** Nothing in a link check could catch that: the files resolve, the
published md5 is correct, the served size matches — because the file really is
that size. It is half the run. What keeps those files out of the recommended
set is the file layer's completeness rule, which reads `library_layout`: in the
verified 1,000-run draw, 442 runs were sent to the complete `.sra` object and
only 6 recommended sets are a lone ENA fastq
([the file layer](docs/files.md#the-two-claims-that-must-not-be-confused)).

---

## Milestones

| | | Status |
|---|---|---|
| **M1** | accession graph, 40 routes, knowledge base, resolver | **complete** |
| **M2** | file layer, link verification, ranked recommendation | **complete** |
| **M3** | `fetch10xmeta` parity, container, Nextflow module | **complete** |
| **M4** | assay layer, chemistry, pre-download screening | **complete** |
| **M5** | upstream reports, archive monitor, docs site, contribution guide | **drafted** |
| **M6** | metadata layer: harvest, rule normalisation, ontology grounding | **tiers 0–2 complete** |
| **M7** | link surveys: every unmeasured catalogue link over seeded draws | **complete** (six waves) |
| **M8** | `fetch inspect`: reads, classifier, content survey, refusal path | **complete** |

**M3 parity:** 14 of 14 of the incumbent's own nf-test cases, 422 snapshot rows (400 distinct runs), no
differences ([the record](docs/fetch10xmeta-parity.md)). Getting there required
fixing three cases where this project resolved *fewer* runs than the bash module
it replaces.

**M4:** 90.9% assay recall on a corpus that is 10x by construction; chemistry
named for 32.4% of runs, of which **14.6% are finer than FASTQ geometry can
reach** ([the assay layer](docs/assays.md)).

**M6:** descriptive metadata per sample, grounded in UBERON/CL/MONDO/PATO/EFO/
NCBITaxon, from four archives that were previously read for identity only
([the metadata layer](docs/metadata.md)). It works with no LLM key. Two archive
bugs found building it, both with reproducers and inverted tests. **Accuracy is
deliberately not claimed** — the ArrayExpress gold set the layer was designed
around turned out to be circular in both of its halves, and
[that negative result](docs/decisions/what-counts-as-a-metadata-gold-set.md) is
the most useful thing the measurement produced. Tier 3, model adjudication, is
designed and not built: it has to beat this baseline on an adjudicated set to
earn a place.

---

## Where the reasoning is written down

The route pages record *what the archives do*.
[`docs/decisions/`](docs/decisions/index.md) records *what we decided to do about
it*, which is the part that ages fastest and is hardest to reconstruct from code:
how routes are ranked and why yields are banded, what qualifies as evidence,
what happens when two archives disagree, why parity precedes improvement, and
the two planned providers dropped on measurement.

Each page ends with **what would reverse it**. A decision with no stated failure
condition is a preference, and the governing rule is that preferences do not
ship.

## What is left

### Needs your decision or your credentials

1. **File the seven upstream reports.** Written and ready to paste, each carrying
   the command that regenerates its evidence first — four to EBI, two to NCBI,
   one to DDBJ. [`docs/upstream/`](docs/upstream/index.md). Left drafted
   deliberately: sending them is outward-facing and yours. The most
   consequential is
   [BioSample `efetch` ignoring the accession prefix](docs/upstream/ncbi-biosample-efetch-accession-prefix.md):
   it silently attributes one organism's metadata to another, with HTTP 200 and
   no warning. The newest, checked 2026-10-19, is
   [DDBJ Search linking mirrored runs' SRA objects that do not exist](docs/upstream/ddbj-search-mirrored-sra-links.md):
   30 of 59 answered 404. The content survey has since measured it over a draw:
   248 of 452 NCBI runs' objects are missing. A seventh, to EBI, was drafted on
   2026-10-23: ENA lists fastqs its server does not serve (2 of 4,466 links), derives
   some fastqs from SRA Lite (122 of 452 NCBI runs) and split SRR14522447 through
   its barcode read ([the draft](docs/upstream/ebi-ena-fastq-files-not-as-listed.md)).
### Deferred until the `fetch inspect` validations land

2. **Publish the container** to `quay.io/cellgeni/fetch-series`. Deferred by
   the maintainer on 2026-10-21 until the inspect checks and their validation
   against past STARsolo runs are done, so the image ships with the report it
   will be used for. v0.1.0 shipped without one: the image is built and
   smoke-tested by CI on every pull request and push to `main`, and measured
   253 MB when built by hand on 2026-09-15. [`docker/README.md`](docker/README.md).
3. **A PR on `nf-reprocessing-public-10x`** swapping the bash module for the
   container. The drop-in is written and parity-tested
   ([`nextflow/fetch10xmeta/`](nextflow/fetch10xmeta/README.md)); it needs an
   image digest to pin, so it waits on (2).

### Decided on 2026-10-21

- **What `fetch inspect` is for.** For each downloadable copy of a run: are its
  files readable; will they do for mapping (STARsolo) and for conversion
  (BAM/CRAM/SRA to FASTQ); basic QC, chiefly barcode and UMI metrics; and its
  features — strand, whitelist, geometry, trimming, library type and
  technology. A fit verdict is one part of that report, not the whole of it.
- **A 47-base cDNA read is usable.** The cDNA floor becomes any non-barcode,
  non-index read of 30 bases or more (`fit-3`), found by SRR34778689.
- **The CLAUDE.md wording drafted in PR #11** ("A link survey is measurement,
  not a ranking") is approved and applied.

### Honest gaps

4. **Three known archive behaviours have no pathology page.** They are declared
   on the routes that exhibit them and listed in `PATHOLOGY_BACKLOG`, so
   `--explain` names them and says *"not yet written up"* rather than offering a
   path to a file that does not exist. Each is real and observed; what they lack
   is the page contract — a curl reproducer, an affected-accession list, and a
   regression test. The first of the eleven,
   [`megalink-backend-flakiness`](docs/pathologies/megalink-backend-flakiness.md),
   was written up on 2026-10-23, and writing it found that a backend message
   inside an ELink link set still read as "no links". Five more were written up the
   same day, and `elink-history-missing-querykey` and
   `superseries-carries-no-bioproject` were retired: neither reproduces.
   [The list](docs/pathologies/index.md#known-declared-and-not-yet-written-up).

5. **The read classifier's VDJ call has never been set on labelled data.** On
   the second held-out draw (2026-10-23), 8 samples the pipeline mapped to genes
   were called VDJ. Five read as B-cell libraries, and three of those on a 3′
   layout, which no 10x V(D)J kit is: gene-expression libraries from
   plasma-cell-rich tissue, by the look of them. `VDJ_SHARE` stays as it is
   until it is set on the design half and scored on samples not yet read
   ([the comparison](docs/benchmarks/inspect-vs-starsolo-2026-10.md#classifier-040-on-a-fresh-held-out-draw-2026-10-23)).

6. **Metadata may break the 3′ v2 / 5′ tie, but nothing does yet.** Decided on
   2026-10-23 under the rule set before measuring
   ([the decision](docs/decisions/when-metadata-may-break-a-tie.md)): offered
   beside the read-derived block, only where the reads abstain and the metadata
   names exactly one of the two. It would settle 9 of 94 abstentions. Building it
   needs `fetch inspect run` to harvest metadata, which it does not.

### Deliberately dropped, with the reasons recorded

- **PyPI release** — off the list on your call. Recorded in
  [the decision log](docs/decisions/index.md#decisions-taken-outside-the-evidence)
  as a maintainer decision rather than an evidence-based one, because it is.
- **A DDBJ provider** — ENA resolves 137/137 PRJDB projects and 2,307 DRR runs,
  so a third implementation of a route two archives already answer earns
  nothing. Reverses on evidence if a corpus ever finds PRJDB projects ENA misses.
- **The ENA browser XML API** — every field it offers is already in
  `filereport`, parsed, from an endpoint that also accepts a project accession.
  [The decision](docs/decisions/ena-browser-xml-not-needed.md), and the better
  route the probe found instead.

---

## Standing machinery

- **`scripts/archive_state_monitor.sh`** re-runs the tier-one corpus, diffs
  against the last snapshot, and exits non-zero when archive state moved. The
  0.77% attrition rate is why it exists.
- **A weekly CI job** runs the inverted pathology suite against live archives.
  A failure there is *good news*: an archive fixed something. Transient failures
  skip rather than fail, so the signal stays clean.
- **`fetch cache diff`** compares two snapshots by hand.
- **`fetch survey export`** writes every verdict to gzipped CSV, so a comparison
  or an affected-accession list can be regenerated months from now.
