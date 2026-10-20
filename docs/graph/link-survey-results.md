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

<!-- link-splits:wave=3 -->

## Wave 4: the ArrayExpress sample chain

<!-- link-splits:wave=4 -->

## Wave 5: read files from a run

<!-- link-splits:wave=5 -->

## Wave 6: everything else

<!-- link-splits:wave=6 -->
