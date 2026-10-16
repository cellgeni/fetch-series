# Where fetch-series stands

**2026-10-15** · M1–M6 on `main` ([PR #2](https://github.com/cellgeni/fetch-series/pull/2),
[PR #3](https://github.com/cellgeni/fetch-series/pull/3)) · knowledge base published at
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
```

Behind it:

| | |
|---|---|
| Routes declared, implemented and **measured** | **40 / 40 / 40** |
| Archives covered | GEO, SRA, ENA, ArrayExpress/BioStudies, DDBJ |
| Survey verdicts recorded and committed | **245,117** across 76 files |
| Offline tests | **493** (plus integration, pathology and slow suites) |
| Knowledge-base pages | every measured route, 9 pathologies, 5 upstream drafts |

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
| **125,376** | BioSamples ENA finds that NCBI's link table does not; 93.5% of them in EBI-registered projects | [route](docs/routes/bioproject-to-biosample/index.md) |
| **every `SAMEA`/`SAMD`** | BioSample asked of `efetch` by accession returns a *different sample* — prefix stripped, remainder read as a UID, HTTP 200, full attribute block | [pathology](docs/pathologies/biosample-efetch-ignores-accession-prefix.md) |
| **41%** | of the BioProjects `esummary db=gds` recovers for series GEO leaves empty are species-level umbrellas — `PRJNA30709` holds 7,591 runs | [route](docs/routes/gse-to-bioproject/gds-summary.md) |
| **0.77%** | of runs already reprocessed successfully no longer exist in *either* archive, and nothing announces it | [route](docs/routes/run-to-experiment/ena-filereport.md) |
| **8,604/8,604** | agreement between GEO's own SRA relation and ENA's submitter alias — a route that works where GEO records no relation at all | [route](docs/routes/run-to-geo-sample/ena-alias.md) |

Four of the first five contradicted what this project's own code assumed before
anyone counted. The umbrella finding forced a rule onto the front page —
**coverage is not correctness** — because `gds_summary` wins every headline
number against `soft_family` and is the more dangerous route to trust blindly.

### Two claims that must not be confused

**1,566 of 1,566** recommended file links resolve and serve exactly the byte
count the archive published. **And the data behind 39% of the fastq ones is
incomplete anyway.** Nothing in a link check could catch that: the files
resolve, the published md5 is correct, the served size matches — because the
file really is that size. It is half the run. A verification layer that only
checked links would have called the corpus perfectly healthy.

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

**M3 parity:** 14 of 14 of the incumbent's own nf-test cases, 447 runs, no
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

0. **A fifth upstream report is drafted**, on
   [BioSample `efetch` ignoring the accession prefix](docs/upstream/ncbi-biosample-efetch-accession-prefix.md).
   Arguably the most consequential of the five: it silently attributes one
   organism's metadata to another, with HTTP 200 and no warning. Like the rest
   it is **not filed** — sending it is outward-facing and yours.

1. **File the five upstream reports.** Written and ready to paste, each carrying
   the command that regenerates its evidence first — three to EBI, two to NCBI.
   [`docs/upstream/`](docs/upstream/index.md). Left drafted deliberately:
   sending them is outward-facing.
2. **Publish the container** to `quay.io/cellgeni/fetch-series`. Verified
   working locally and in CI at 253 MB; cannot be pushed to your registry from
   here. [`docker/README.md`](docker/README.md).

### Blocked on the above

3. **A PR on `nf-reprocessing-public-10x`** swapping the bash module for the
   container. The drop-in is written and parity-tested
   ([`nextflow/fetch10xmeta/`](nextflow/fetch10xmeta/README.md)); it needs an
   image digest to pin, so it waits on (2).

### Two honest gaps

4. **Eleven known archive behaviours have no pathology page.** They are declared
   on the routes that exhibit them and listed in `PATHOLOGY_BACKLOG`, so
   `--explain` names them and says *"not yet written up"* rather than offering a
   path to a file that does not exist. Each is real and observed; what they lack
   is the page contract — a curl reproducer, an affected-accession list, and a
   regression test. `megalink-backend-flakiness` is the one worth writing first:
   3,135 occurrences in the 2026-05 logs, the dominant failure signal in the
   whole corpus, and an upstream report in its own right.
   [The list](docs/pathologies/index.md#known-declared-and-not-yet-written-up).

5. **Chemistry agreement with `rename10xrun` is unmeasured.** Measuring it needs
   the FASTQ files, which needs exactly the downloads the metadata layer exists
   to avoid. The right place to close it is the first real pipeline run with
   both enabled — the ids are `rename10xrun`'s verbatim so the two diff without
   translation, and a test asserts the vocabularies have not drifted.

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
