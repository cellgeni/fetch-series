---
title: Link survey results
---

# Link survey results

What the [link survey](catalogue.md#link-surveys) measured, link by link, over seeded draws
of 300 accessions of each source node: 300 BioProjects per issuing archive and all 137 DDBJ
ones, 300 runs per archive, and so on. Every table below is generated at build time from
`link_splits.json`, which is generated from the committed exports. A number on this page
therefore cannot drift from the verdicts it summarises.

How to read a row:

- **Usable / queried** is the count of accessions for which the link found something
  usable: reached, and every check made on what it found passed.
- **Resolved · empty · failed** are the survey's three outcomes.
- **Status** is the link's overall [status](catalogue.md#what-a-link-survey-records-and-what-it-does-not).
  It is *surveyed* where any stratum found something usable.
- Rows are in link order. **None of this is a ranking.** At 300, a share carries up to ±5.7
  points, enough to say a link answers always, sometimes or never, and too coarse to say
  which of two links answers better.

A row of zeros is usually not a failure. Where a shared node is stratified by archive, most
cross-archive cells are empty by construction: a PRJEB's runs never name a DDBJ sample. A
link is drawn on [the route graph](../routes/graph.md) where *any* stratum found it usable.

The survey ran on 2026-10-19 and 2026-10-20 (UTC). 983 of the 1,031 links no route measures
were surveyed. The 48 that were not are:

- the six SDL-on-project and SDL-on-study units, which failed their
  [gate](catalogue.md#the-gate-2026-10-19);
- every link out of an NCBI, DDBJ or EBI analysis. Those nodes have
  [no population](catalogue.md#link-surveys) to draw from. The EBI one is recorded as such
  because its derivation found no analysis in any of 737 BioProjects.

## Wave 1: INSDC samples

Links into an INSDC sample or a BioSample, and ENA's `filereport` between INSDC records.

What it found:

- **ENA's `links` endpoint answers for a minority of projects.** BioProject → BioSample
  through `links/study` found BioSamples for 120 of 300 PRJEBs, 50 of 300 PRJNAs and none of
  137 PRJDBs. ENA's own `filereport` found them for 12,730 of 12,755 in its census
  ([BioProject to BioSample](../routes/bioproject-to-biosample/index.md)). This fits the
  gate's finding that `links/sample` stopped answering for `SAMEA4437023`: on this evidence
  the endpoint is not a route to anything.
- **Every BioSample names its INSDC sample, through three services.** NCBI's BioSample
  record did so for 300 of 300 per archive, EBI's BioSamples for 297 to 299 (with 1 to 3
  failures), and ENA's sample search for 289 to 299.
- **The reverse holds too.** An SRS, ERS or DRS gives its BioSample through EBI BioSamples'
  filter for 290 to 300 of 300, and through ENA's sample search for 288 to 300. ELink
  SRA → BioSample answered for 296 to 300 of 300 experiments per archive.
- **An ArrayExpress SDRF names an INSDC sample less often than it names a study.**
  `Comment[ENA_SAMPLE]` and its kin named an ENA sample for 129 of 300 studies and an NCBI
  one for 87. Seven studies failed outright: their SDRF is not served.
- **No BioProject in this corpus has an EBI analysis**, so `filereport`'s analysis result
  reached no sample at all. Those links are *surveyed-none*: they exist (the probe used
  `PRJEB44468`) and never answer over this project's records.

<!-- link-splits:wave=1 -->

## Wave 2: metadata documents

Links into a metadata document. A document counts as usable only if it downloads *and*
parses with the parser this project already uses for it, and names the record asked for.

What it found:

- **Every archive's own document answers for nearly every record of its own.** GEO's SOFT,
  MiniML, series matrix and `acc.cgi` answered for 300 of 300 series or samples. NCBI's SRA
  XML and runinfo answered for 299 or 300 runs and experiments in every archive. DDBJ's
  search entry answered for 296 to 300. NCBI's and EBI's BioSample records answered for 297
  to 300 BioSamples of every archive.
- **ENA does not hold about 8% of DDBJ's records.** ENA's browser XML answered 404 for 25 of
  300 DRXs and 21 of 300 DRRs. For exactly those records ENA's `filereport` answers with a
  table that does not name them, so it downloads and does not parse. For NCBI and EBI
  records both answered for 296 to 300.
- **DDBJ's DRA XML holds a fifth to a third of other archives' records less.** It answered
  404 for 66 of 300 SRRs, 80 SRXs, 60 ERRs and 103 ERXs, and for none of DDBJ's own. DDBJ's
  search entry answered for the same records. So DDBJ indexes them, and the DRA XML path
  does not serve them.
- **An ArrayExpress study's IDF or SDRF is missing for 8 to 10 of 300 studies**, as 404s.
  That is the same population as the 11 SDRF failures of the
  [ArrayExpress file route](../routes/ae-experiment-to-file/index.md).
- **runinfo** answered with a table that did not name the experiment asked for 4 of 300 SRXs.

<!-- link-splits:wave=2 -->

## Wave 3: BioProject and ArrayExpress

Links between a BioProject and an ArrayExpress study, either way.

What it found:

- **Only an EBI project points at its ArrayExpress study.** ENA's browser XML named one for
  240 of 300 PRJEBs, and `filereport`'s aliases for 238. Neither did for any PRJNA or PRJDB,
  as expected, since ArrayExpress submits through ENA.
- **BioStudies does not know its own projects.** Searching BioStudies for a BioProject found
  the study for 1 of 300 PRJEBs. ENA's cross-reference service found it for 6.
- **An ArrayExpress study's own record never names its BioProject.** BioStudies' study JSON
  named one for 0 of 300 studies; ENA's cross-references for 26. The measured route the
  other way is through the IDF's secondary accession
  ([ArrayExpress to study](../routes/ae-experiment-to-study/index.md)), which answers for
  the study. The BioProject is one more hop.
- A BioStudies record outside ArrayExpress names a BioProject for 5 of the 82 a first survey
  reached.

<!-- link-splits:wave=3 -->

## Wave 4: the ArrayExpress sample chain

Links into and out of an ArrayExpress sample: a study's SDRF row, addressed as the study
plus its Source Name (`E-MTAB-5061:AZ_A1`), because such a sample has no accession of its
own.

**This population is clustered.** It is a seeded draw of 300 from the 9,757 samples the
SDRFs of 290 studies listed, so it is 300 samples from fewer than 300 studies. A share here
describes samples weighted by how many each study has, not studies.

What it found:

- **An SDRF lists its samples** for 290 of 300 studies. The 10 failures are SDRFs that are
  not served.
- **An SDRF's fastq URIs are never usable.** They named fastqs for 253 of 300 samples, and
  the usable count is **0**:
  - none is verifiable, because no SDRF publishes an md5;
  - 93 sets did not download, because they point at the decommissioned mirror;
  - 84 sets are incomplete for 10x.

  This is [the mirror pathology](../pathologies/ae-sdrf-points-at-decommissioned-mirror.md) measured on a draw
  rather than on two studies.
- **An SDRF names the sample's ENA records far more often than NCBI's:** an ENA run for 200
  of 300 samples, an NCBI run for 94; an ENA sample for 200, an NCBI sample for 80; an ENA
  experiment for 200, an NCBI one for 100. A GEO sample is named for 100, which are the
  `E-GEOD` imports. A BioSample is named for 181.
- **Processed files** named by the SDRF were found for 72 samples, and 70 of them downloaded
  and were size-checkable.

<!-- link-splits:wave=4 -->

## Wave 5: read files from a run

Links from a run into its read files that no route measures. A file set is usable only if it
passes all three checks:

- **downloadable:** a HEAD answers at the size the archive published;
- **verifiable:** the archive publishes an md5 and a size;
- **complete for 10x:** both the barcode read and the cDNA read are there.

These checks were made on one run's set, chosen by seed. What the first reads of those
files *hold* is a separate survey, the [content survey](../benchmarks/content-survey-2026-10.md),
over the same 300 runs per run node. Its verdicts are the table's **Fit · unfit · ?**
column: runs whose listed files were read and found fit, proven unfit, or left undetermined
under rule `fit-2`.

What it found:

- **NCBI's SRA XML is a complete file route for both object types.** It lists the
  Normalized object with an md5, and the object downloads and is complete for 289, 292 and
  285 of 300 runs from DDBJ, NCBI and EBI. For the Lite object the figures are 286, 285 and
  284. The incomplete ones (4 to 9 per archive) are runs whose SRA object lost the barcode
  read, as ERR4265292's did.
- **Two sources download and can never be verified.** sra-db-be's `download_path` names the
  Lite object for 291 to 297 runs and publishes no md5. DDBJ's own fastqs download for 300 of
  300 DRRs and are complete for 298, and publish no md5 either. Both are usable in practice
  and fail the rule that a download must be checkable, so both count 0 here.
- **DDBJ's mirror of other archives' SRA objects is half there.** Its search entry names an
  object for 299 of 300 SRRs and ERRs, and 141 and 162 of those did not download. For DDBJ's
  own runs all 300 did.
- **SDL's originals are listed and cold.** SDL named the submitter's fastqs for 298 of 300
  SRRs, and none downloaded: they are in cold storage, named with an empty link. For ERRs it
  named fastqs for 232 (28 downloadable), CRAMs for 54 (3) and BAMs for 4 (0).
- **ENA lists a submitted BAM for 79 of 300 ERRs, and every one is usable.** No SRR or DRR
  has one.
- **Read, the copies mostly agree, and the mirror does not.** NCBI's SRA XML objects are fit
  for 238 of 299 NCBI runs, 226 of 299 EBI runs and 254 of 300 DDBJ runs, as the Normalized
  object SDL names is. DDBJ's mirror of NCBI objects is fit for 117: the rest are mostly
  objects that do not exist. ENA's submitted BAMs are fit for 53 of 79, and 22 are libraries
  that are not gene expression or calls left ambiguous.

<!-- link-splits:wave=5 -->

## Wave 6: everything else

The rest: NCBI's ELinks and the gds and SRA summaries, DDBJ's dblink, ENA's search and
cross-reference services, BioStudies records, submissions, and read files reached from a
record other than a run. 667 links were surveyed, as 991 link splits.

What it found:

- **The two NCBI runinfo-style answers reach everything SRA links.** sra-db-be's runinfo and
  ESummary on SRA have 93 links each, and every one of them is usable somewhere. Together
  they are the broadest single requests in the catalogue.
- **ENA publishes no SRA object.** `filereport`'s `sra_ftp` column was empty for every record
  of every node it was asked about: 28 links, 0 of 9,386 queries. The SRA objects are
  NCBI's, reached through SDL and the SRA XML ([wave 5](#wave-5-read-files-from-a-run)).
- **None of these projects is an umbrella.** ELink's umbrella-to-component link answered for
  0 of 737 BioProjects, and component-to-umbrella for 76. The one umbrella the probe used,
  `PRJNA988804`, was chosen because it is one.
- **ArrayExpress imports of GEO are historical.** BioStudies found an `E-GEOD` study for 0 of
  300 reprocessed GEO series. ArrayExpress stopped importing GEO before most of them were
  submitted.
- **DDBJ's dblink answers for 77 of the 122 links it declares.** It is the one service that
  links a DDBJ record to everything at once. Of the 45 that never answered, 20 lead into an
  analysis and 7 into a submission, which no record here reaches by any route. 6 lead into a
  BioProject and 6 into a GEO series: dblink does not carry those relations for these
  records, though other services do.
- **A record's read files from ENA are per run.** `filereport`'s fastq column reached files
  from 14 links (every record type) for 3,250 of 4,693 queries. The submitted-files column
  was usable from 19 of 42 links, for 1,572 of 14,079 queries. A BAM came from 6 of 14
  links, for EBI records only.
- **No EBI analysis was reachable from any record**, so all 22 `filereport` analysis links
  are surveyed-none, as their wave-1 neighbours were.
- **SDL from a record other than a run** answered from 25 of 44 links where its gate allowed
  it at all. SDL on a project or an EBI study failed its gate and was never drawn.

<!-- link-splits:wave=6 -->
