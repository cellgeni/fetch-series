---
title: The link catalogue
---

# The link catalogue

**Needs:** nothing **Feeds:** [the route graph](../routes/graph.md)'s untested
links, and the next round of surveys
**Rule:** a catalogue link says a link *exists*. Only a route, with evidence,
says which link to *use*.

The [route registry](../routes/index.md) holds the links somebody has
measured. The archives answer far more than that: one ENA `filereport` request
returns seven INSDC levels from any one of them, for all three archives'
records. The catalogue lists every one-hop link an archive API offers between
the nodes below, says what each is read from, and confirms each one, either by
a route's survey or by a live request. It lives in `src/fetch_series/catalogue/`,
apart from `REGISTRY`, so nothing in it can change the resolver, the ranking, or
the parity of `links.tsv`.

<!-- catalogue-counts -->

## Nodes

| Level | NCBI | EBI | DDBJ | Shared, and the other archives |
|---|---|---|---|---|
| Dataset | SRA study `SRP` | ENA study `ERP` | DRA study `DRP` | BioProject `PRJ*` · GEO series `GSE` · ArrayExpress experiment `E-XXXX-n` · BioStudies study `S-…` |
| Sample | SRA sample `SRS` | ENA sample `ERS` | DRA sample `DRS` | BioSample `SAM*` · GEO sample `GSM` · ArrayExpress sample (SDRF row) |
| Library | SRA experiment `SRX` | ENA experiment `ERX` | DRA experiment `DRX` | |
| Run | SRA run `SRR` | ENA run `ERR` | DRA run `DRR` | |
| Other | SRA submission `SRA` · analysis `SRZ` | ENA submission `ERA` · analysis `ERZ` | DRA submission `DRA` · analysis `DRZ` | GEO platform `GPL` |
| Read files | | | | FASTQ · BAM · CRAM · SRA Normalized · SRA Lite |
| Supplementary | | | | GEO supplementary (`suppl/`) · ArrayExpress supplementary (processed files) |
| Metadata | SRA metadata (runinfo, SRA XML) | ENA metadata (filereport, ENA XML) | DDBJ metadata (DRA XML, Search JSON) | GEO metadata (SOFT, MINiML, matrix) · ArrayExpress metadata (IDF, SDRF) · BioSample metadata |

### INSDC objects are split by archive

An `SRR`, an `ERR` and a `DRR` share one schema and are mirrored between the
three archives, but they are not reached the same way. NCBI's ELink resolved
BioProject → BioSample for 99.3% of NCBI-registered projects and 1.4% of
EBI's; ENA holds submitted files for every EBI run it was asked about and for
none of 2,737 NCBI runs; GEO links to NCBI's records only. Merging the three
into one node hid exactly the split the routing depends on.

A parsed accession lands on its node by its prefix letter, which
`fetch_series.accession.parse` already reads into `Accession.archive`.
BioProject and BioSample stay **one node each**: all three archives hold the
same record under the same accession, and each sits on a row of its own
within the three INSDC columns.

### Which archives a record can link to

Two rules decide which pairs are declared at all, and neither is assumed:

- **One submission's objects share an archive.** No SRP is linked to an ERR.
  Every recorded verdict in `data/surveys/` was scanned for a pair that crosses
  archives and none does; a slow test repeats the scan, so the first one that
  appears fails the suite rather than going undrawn.
- **A curated record links only the archives it points at.** GEO brokers to
  NCBI's SRA alone, and ArrayExpress brokers to ENA and imports GEO's series.
  The verdicts agree without exception: `gse->experiment:soft_bioproject_ena`
  reached no ERX or DRX from 13,045 series, `run->geo_sample:ena_alias` no GSM
  from 263 EBI and DDBJ runs, `ae_experiment->study:idf_secondary` no DRP from
  20,693 experiments, and `study->ae_experiment:biostudies_search` no
  ArrayExpress experiment from 38 DRPs.

### Read files are nodes by format

What a pipeline can consume is a FASTQ, a BAM, a CRAM or an SRA object,
whoever lists it, so read files are grouped by **format**. Who made the file is
drawn on the line instead, because the same format arrives three ways that are
not the same offer:

- **submitted** — the files as deposited: ENA's `submitted_ftp`, SDL's original
  files;
- **archive-generated** — derived by the archive: ENA's `fastq_ftp` and
  `bam_ftp`, DDBJ's fastqs;
- **archive format** — the archive's own container: an SRA object.

An SDRF's `Comment[FASTQ_URI]` is **mixed**: it names whichever copy the curator
chose. Of the 198,423 URIs its route's survey returned, 182,988 (in 2,164 of
3,000 studies) are ENA's generated fastqs and 13,930 (in 115) ENA's copy of the
submitted files; E-MTAB-9221 names the submitter's own R1/R2 on the
decommissioned ArrayExpress mirror.

**SRA Lite and SRA Normalized can be told apart**, so they are two nodes. SDL
answers a plain request with the Normalized object, named for its run;
`filetype=sralite` (or the capability `ZQA:Z` sra-tools sends) swaps in SRA
Lite, named `SRR25056225.lite`; runinfo's `download_path` is always the Lite
object; and SRA XML names each file outright, `semantic_name="SRA Normalized"`
or `"SRA Lite"`. ENA's `sra_ftp` says neither, and has held nothing for any run
ever asked.

### Supplementary files and metadata documents

A series' or sample's `suppl/` files, and an ArrayExpress study's processed
data, are where the submitter's processed results live: nodes of their own,
per archive. So are the metadata documents that describe a record: one node per
archive, reached from the records they describe.

### An ArrayExpress sample has no accession

It is a row of an SDRF, addressed as the experiment plus the row's `Source
Name` (`E-MTAB-5061:AZ_A1`), the spelling ENA uses for the sample's alias. It
is modelled because the SDRF row is where ArrayExpress joins its own sample to
a BioSample, an INSDC sample, a run and its files.

## What confirms a link

| Into | Confirmed when |
|---|---|
| a record | an accession of exactly that node is found: its type **and** its archive |
| a read format | what was found is a **valid file** |
| a supplementary file | it downloads, and its size can be checked |
| a metadata document | it downloads, and parses with the parser the project already uses for it |

### A valid file: downloadable, verifiable, complete for 10x

Checked on one run's file set — the probe's own run where it is among those
found:

- **Downloadable.** Every file answers a HEAD with 2xx, serving the size the
  archive published (`filesets.verify`). A file listed without a link fails.
- **Verifiable.** The archive publishes an md5 and a size for every file.
- **Complete for 10x.** Both the barcode read and the cDNA read are there.
  For FASTQ, both reads of the pair are files of the set, by the archives'
  `_1`/`_2` or by the sequencer's `_R1_001`/`_R2_001`; a paired library the
  archive publishes one file for has lost one. A BAM or CRAM holds every read
  of a spot in one file, the barcode in its tags, so one file per run is
  complete. For an SRA object it is read from SRA XML's run `Statistics`: two
  reads per spot of at least 26 bases (a 10x barcode is 16 bases plus a 10- or
  12-base UMI; an index read is 8 or 10). Where that cannot be read, completeness
  is **undetermined, and never assumed**.

A link whose probe found a file that failed any check is listed with the check
named, and not drawn. A probe proves nothing about how often a link answers;
the checks prove only that what it found on that day was usable.

## Mechanisms

A mechanism is one request shape and the fields of its answer. Links are
generated from its inputs × outputs, within the two rules above, so a response
that fans out is declared once. A route-specific chain (an ELink followed by
EFetch, a SOFT file followed by an ENA lookup) declares only the link its route
measures; the general fan-out of the response it ends in is declared once, on
its plainest mechanism.

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

## Measured routes on split nodes

A route now covers several links: `bioproject->run:ena_filereport` is
BioProject → SRR, → ERR and → DRR, and `run->file:ena_submitted` reaches FASTQs,
BAMs and CRAMs. How it did on each is read from **its own recorded verdicts**
(`data/surveys/*.csv.gz`, and for the six 2026-05 routes the result tables in
`data/query/results/`): each verdict's source placed by its accession's prefix,
each result by its accession or its file's format. That is filtered
measurement, not new evidence, and it changes no ranking
([a route's split is not new evidence](../decisions/what-counts-as-evidence.md#a-routes-split-is-not-new-evidence)).

- **Ink** is the share of the source's queried accessions that resolved: among
  SRRs alone where the source is split by archive, the route's own share where
  it is not (a BioProject). The 2026-05 routes never recorded the split, and
  stay unknown.
- **Width** is how many of those accessions had a result on *this* target.
- A pair whose source the survey never queried is **not measured**:
  `experiment->run:ena_filereport` was surveyed on GEO series' experiments, all
  SRXs, so ERX → ERR is probed like any untested link.
- A pair whose source was queried and never reached the target is a
  **measured absence**, listed and not drawn: `run->file:ena_sra` answered for
  none of its 3,000 runs.

CI builds the docs without the LFS objects, so the splits are computed by
`fetch catalogue splits --write` into `catalogue/route_splits.json`, and a slow
test recomputes them from the exports. Two kinds of verdict are set aside
rather than dropped, and named there: the 2026-05 scripts queried the corpus's
`-` placeholder as a BioProject, which is not one, so it is reported as
unparsed and counted on no link; and two routes queried sources the rules above
rule out (263 EBI and DDBJ runs asked for a GSM, 38 DRPs asked for an
ArrayExpress experiment), all of which came back empty.

## Probes

Every link no route's survey measured is confirmed by one live request, with a
fixed accession from a linked family:

| | Dataset | Sample | Library | Run | Other |
|---|---|---|---|---|---|
| NCBI / GEO | `GSE60450`, `PRJNA258286`, `SRP045534` | `GSM1480291`, `SAMN02991530`, `SRS685538` | `SRX681985` | `SRR1552444` | `SRA179382`, `SRZ4627089`, `GPL13112` |
| EBI / ArrayExpress | `E-MTAB-5061`, `PRJEB15401`, `ERP017126` | `SAMEA4437023`, `ERS1348472`, `E-MTAB-5061:AZ_A1` | `ERX1700348` | `ERR1630015` | `ERA705448`, `ERZ10183054` |
| DDBJ | `PRJDB9974`, `DRP006494` | `SAMD00229074`, `DRS151759` | `DRX221998` | `DRR231743` | `DRA010287`, `DRZ010000` |

A link from a record all archives share is probed with the family of the archive
its target belongs to: BioProject → ERR with `PRJEB15401`, BioProject → DRR with
`PRJDB9974`. A link into a read format is probed on a **10x run**, one family
per archive, so what it finds can be checked for 10x:

| | Run | Why |
|---|---|---|
| NCBI | `SRR25056225` (GSE236084) | both reads in its SRA object (28 + 91 bases); submitted as R1/R2 fastqs |
| EBI | `ERR4265292` (E-MTAB-9221) | submitted as a 10x BAM; its SRA object holds the 91-base read alone |
| DDBJ | `DRR345778` (PRJDB12979) | both reads (28 + 91 bases) |

A format no main family offers is probed on one that has it, each a 10x run
checked to carry both reads: `ERR11262882` (E-MTAB-12893) for ENA's and SDL's
fastqs, `ERR13855116` (the Foetal Cell Atlas) for a submitted CRAM, and
`SRR8787106` (GSE120926) for the submitter's files in a public bucket. A few
record links need a record of their own: a SuperSeries (`GSE236087`), an
umbrella BioProject (`PRJNA988804`), an EBI analysis (`ERZ10183054`, in
`PRJEB44468`), a Europe PMC record (`S-EPMC9302581`), and the GEO import
`E-GEOD-60450`.

One request answers every link a mechanism offers from one accession. The
values are then normalised by the target, not trusted: an accession field keeps
only tokens that `fetch_series.accession.parse` places on the target's node,
at its archive as well as its type.

```bash
uv run fetch catalogue probe --write          # every link no route measured
uv run fetch catalogue probe --stale --write  # only those whose probe accession changed
uv run fetch catalogue splits --write         # each route's verdicts, per node pair
uv run fetch catalogue list                   # nodes, links by status, and the gaps
```

Results are in `src/fetch_series/catalogue/probes.json`, dated in UTC, with
every check made on a file or document. A probe that failed is kept apart from
one that found nothing, as everywhere else in this project.

### A probe is not evidence

A probe proves that a link exists, for one record, on one day. It measures
nothing: one accession out of one is not a yield. So a probe never becomes
`RouteEvidence`, never changes a ranking, and carries no counts on the graph.
The argument is in
[what counts as evidence](../decisions/what-counts-as-evidence.md#a-probe-is-not-evidence).
Measuring an untested link is a survey, and the next step.

## What the probes found on the way

**About files:**

- **E-MTAB-9221's runs cannot be rebuilt from any SRA object.** It was
  submitted as 10x BAMs, whose barcodes live in tags; the SRA objects NCBI made
  from them hold the 91-base cDNA read alone (`avgLength 91`), and ENA dumps no
  fastq for them at all. Every SRA link for `ERR4265292` fails *complete*; the
  submitted BAM is the only valid file.
- **SDL knows the submitter's files and cannot hand them over.** Asked with
  `accept-alternate-locations=yes`, it lists `SRR25056225`'s
  `TSF2_S35_L002_R1_001.fastq.gz` and `R2` — barcode read and all — with an
  empty link: they sit in cold storage behind Cloud Data Delivery. Asked
  plainly, it offers originals only where they sit in a public bucket: 33 of the
  3,000 runs its route surveyed.
- **runinfo's file is never verifiable.** Its one file column points at the SRA
  Lite object, with no size and no checksum.
- **DDBJ publishes no checksum anywhere.** Its entries name its own runs' fastqs
  (bzip2, named for the run) and SRA objects, and an SRA object for each run it
  mirrors from NCBI or EBI; none carries an md5.
- **DDBJ links mirrored runs' objects without checking they exist, by design.**
  Its converter checks its own runs' files and builds a mirrored run's link from
  a template, and documents that a 404 is accepted. Of 59 SRRs and ERRs
  sampled with an entry (2026-10-19), **30 answered 404**, `SRR25056225`'s
  among them; all 17 DRRs downloaded. [Drafted as an upstream report](../upstream/ddbj-search-mirrored-sra-links.md).
- **A directory named `sralite` mostly holds Normalized objects.** DDBJ files
  every mirrored run under `dra/sralite/ByExp/litesra/`. Of the 29 that
  downloaded, 23 had the exact size of NCBI's Normalized object, 3 of its Lite
  object and 3 of neither, and the converter documents the file as "not
  SRA-Lite". The catalogue reads the format from the file's name, never its
  directory, so these sit on SRA Normalized.
- **ENA's `bam_ftp` and `sra_ftp` have held nothing** for any record probed;
  `sra_ftp` was empty for all 3,000 runs its route surveyed.
- **A Sanger CRAM name carries a `#`** (`27865_1#3.cram`). Asked for unescaped,
  a URL reads it as a fragment and the request is for `27865_1`, which answers
  404: a valid file reported as missing.
- **SDL answers a large project with a 504** (`PRJNA495040`, `ERP110462`), and
  retrying does not help.

**About records:**

- **ENA holds EBI's analyses and no others.** `SRZ4627089` and `DRZ010000`
  return an empty report and a search for `SRZ*` finds nothing; DDBJ's `dblink`
  answers for both, and for no `ERZ`.
- **ENA's `links` endpoint reads `limit=0` as zero rows**, and takes only
  primary accessions: `ERP017126` and `ERS1348472` return an empty table where
  `PRJEB15401` and `SAMEA4437023` return their records.
- **NCBI's `bioproject_bioproject` link answers nothing.** The umbrella relation
  is carried by the directed `_u2d` / `_d2u` pair, and NCBI records no umbrella
  for PRJNA258286 where ENA's `parent_study_accession` names PRJNA9559.
- **The BioSamples API's `extd:ArrayExpress` filter returns a sample group**
  (`SAMEG318185` for E-MTAB-5061), not samples, so it is not declared.
- **`gds` ESummary needs the entry type.** `GSM1480291[ACCN]` alone matches the
  sample, its series and its platform, and the first UID is the series'.

**About the recorded surveys** — found while splitting them, and corrected in
their `RouteEvidence` on 2026-10-19. Neither correction moves a route in the
ranking:

- The four 2026-05 BioProject → run scripts queried the `-` placeholder as a
  BioProject and recorded it as a failure, so their evidence counted 12,756
  queried and one failure too many each. It now reads 12,755.
- The two 2026-05 BioProject → GEO series routes recorded 15,186 and 15,185
  "unique results", which were rows: five series are reached from more than
  one project. The distinct series are 15,180 and 15,179.

## Gaps

Every declared link that is not drawn — its probe found nothing, found a file
or document that failed a check, or its route measured an absence. Generated
from the catalogue at build time.

<!-- catalogue-gaps -->
