---
title: When a second route earns its place
---

# When a second route earns its place

**Status:** in force since 2026-10-29. The rule was set on 2026-10-27, measured on 2026-10-28,
and amended on 2026-10-29 by the maintainer's decision, after the numbers showed what it let
through. Six second routes are declared under it.
**Evidence:** `fetch survey promote --covered` over the `sample:3000` draws, 2026-10-28;
exports under `data/surveys/sample_3000@*/`; the confirmation check in
`data/surveys/second-routes/roundtrip-2026-10-29.json`

## The question

The catalogue holds links that answer in directions a measured route already covers. On
2026-10-28 there were 188 of them: 174 surveyed by a link survey and 14 untested. Grouped as
routes, they make 87 candidate routes in 53 survey units over 28 directions. Each is a second
(or fifth) way to the same kind of answer.

The question has two halves.

- **When does a second route earn its place?** Union mode asks every route in a direction, so
  each one declared costs requests on every resolve. A route that returns only what the
  direction already returns makes resolving slower and changes nothing.
- **When does a declared route stop earning it?** The redundancy is already here. Run → study
  has four promoted routes with identical numbers: 1,983 distinct studies over the same 3,000
  runs. They read ENA's filereport, DDBJ, sra-db-be and ESummary, so one resolve makes up to
  four requests for one answer.

## The rule, set before the numbers

Recorded on 2026-10-27, when the plan was approved, and before any covered candidate was
surveyed. Every route of the direction is measured on the same draw. A route earns its place
when it adds what none of the direction's other routes returns, for at least **0.1%** of the
accessions compared (3 of 3,000), in either of two ways:

- **values:** it returns a value none of the others returns;
- **sole resolution:** it resolves an accession where every other route was empty or failed.

0.1% is the `FAILURE_BAND` scale, the smallest difference the ranking treats as real. It is
written into `survey.directions.ADDS_SHARE` as its own constant: retuning the ranking's band
must not move this rule after the numbers are in.

A route that adds nothing stays declared and ranked. Neither the ranking nor `--explain`
changes because of this page.

Two further questions are left to be decided here from the numbers, because either one would
change what the resolver does:

- whether union mode should skip a route that adds nothing;
- whether `Mode.FIRST` is enough in a direction where every route agrees.

## The rule as amended, 2026-10-29

Applied as set, the rule passed routes whose additions were another accession's data
([below](#what-those-20-add)). On 2026-10-29 the maintainer decided that an addition counts
only when it is right. The rule now reads:

- **An addition counts only when it is confirmed.** It is confirmed when a route on another
  provider maps the value back to the accession it was added for. A file is confirmed when its
  name carries its own run.
- **Candidates are accepted in order of confirmed additions.** Among equals, the one adding
  less that is unconfirmed goes first.
- **A candidate that adds another accession's data as often as the bar is refused**, whatever
  it adds that is right. This applies the amendment's own reason: declaring such a route would
  serve the wrong values in every union-mode answer beside the right ones. It was added after
  the first run under the amendment, when `efetch_sra_files` led run → file on 2,978 confirmed
  files of their own runs while adding 37,604 files of other runs. It only refuses; it never
  admits.

The bar is unchanged: 0.1% of the accessions compared.

## How it is measured

**The candidates.** `routes.catalogue.candidates(covered=True)`: every surveyed or untested
link no route uses, in a direction a declared route covers, grouped by mechanism, source and
target. Records about the data (submission, analysis, platform) stay links, as for every other
promotion.

**The gate.** Each unit is gated on `probe-families` and `hard-cases` first. One request answers
every target a mechanism reads, and a failure is the request's. So a unit whose request already
failed its gate for another target has failed it here too, and is not asked again:

- SDL from a project or study, which answers 504 every time;
- `ena_links` from a BioSample.

**The draw.** `sample:3000` of the source's population, the draw every promoted route was
surveyed on. A declared route's verdicts on it are found in three places, the newest winning
per accession:

- the survey cache;
- the committed export of the draw;
- the committed export of the population, where the route was surveyed over all of it.

A declared route with no verdict on part of the draw is surveyed on that part, with its own
`RouteFn`, in the same run.

**Why an older verdict is good enough.** The routes of one direction were measured weeks apart:
the hand-written ones mostly on 2026-09-15, the promoted ones on 2026-10-26. A project can gain
runs between the two. A route asked later would then be credited with them, and against a bar
of 3 accessions that could pass a route on the calendar alone. So the drift was measured before
the method was chosen, in the direction most likely to grow.

`bioproject->run:ena_filereport` was asked again on 2026-10-28 for the same 3,000 projects it
answered on 2026-09-15. It returned no new run for any of them, lost one run from one project
(PRJNA1289096's SRR40164518, 1 of its 28), and changed no outcome. That is well under the bar,
so recorded verdicts are used whatever their age. `--fresh-days` would re-ask any older than a
given age, beside the candidates, for a later round where the gap is longer.

**The comparison.** Run with `fetch survey promote --compare`. It is offline and reads only
what is recorded.

- **Against what.** A candidate is measured against the declared routes it would join, and
  against the candidates already accepted. It is never measured against candidates still
  waiting. Candidates are accepted one at a time, the one adding most first, until the best one
  left clears no bar, so two candidates never join for the same values. A declared route is
  measured against the other declared routes.
- **Which accessions.** Only those every route of the direction answered. An accession a route
  was never asked is not one it answered empty, and counting it as empty would credit every
  other route with a sole resolution.
- **How files are compared.** By name, not by URL. ENA's `ftp.sra.ebi.ac.uk` and BioStudies'
  `www.ebi.ac.uk` can serve one file under two addresses. Counting the second as new would let
  every file route earn its place by spelling.
- **What the rule means for files.** For a file direction, the rule measures something
  different. Run → file's routes offer different *copies*: ENA's fastq, the submitter's files,
  SDL's `.sra` object. By name, each adds files no other route has: on 2026-10-28, `ena_fastq`
  for 2,844 of 3,000 runs and `sdl` for 2,996. That is the point of a file route.
  [The file layer](../files.md#why-every-route-is-asked-not-just-the-first) asks every one, and
  choosing between copies is a requirements question, not an evidence one. So in a file
  direction, passing the rule means offering another copy. Failing it means offering only
  copies another route already names, which is redundancy in the usual sense.

## Results

### What was surveyed

On 2026-10-28, 80 of the 87 candidate routes were surveyed on their draws, in about five hours.
The other 7 were held back by their gate:

- `sdl`, `sdl_lite` and `sdl_original`, from a project or study;
- `ena_links`, from a BioSample.

Six declared routes held no verdict on the draw and were surveyed on it in the same run:

- `bioproject->geo_series:gds_direct` and `elink`;
- `experiment->run:ena_filereport`;
- `acc_cgi` from a GEO sample, three routes.

Those six ran beside 47 survey units at once. Their failures were asked once more, at
concurrency 3. `acc_cgi` answered 184 of its 187 failures into GEO series, and `gds_direct` all
of its 189. A failure that is the survey's own load would credit a candidate with a sole
resolution it has not earned. Candidates' failures were not re-asked: a candidate's numbers
become its evidence, and its own failures only lower its credit.

### Two corrections to the comparison, made after the numbers came in

They are recorded because they changed the outcome.

- **Candidates were measured against each other.** As first written, `--compare` measured each
  candidate against every other route of the direction, other candidates included. Two candidates
  finding the same thing then cancelled each other out, and neither was credited. The rule says
  "the direction's other routes", the routes a candidate would join. Corrected, the passes went
  from 9 to 20.
- **A failure was invisible.** Every count is now split by whether another route had failed on
  that accession. Under the rule as set, `geo_sample->geo_series:gds_esummary_sample` passed on
  184 sole resolutions, all of them over `acc_cgi`'s failures under the survey's own load. Asked
  again, `acc_cgi` answered 184 of the 187, and the pass went away.

### The rule as set: 20 pass

Three of 3,000 accessions clears the bar. Twenty candidates clear it:

- sixteen in accession directions;
- four in run → file.

The bare-term ESearch, the plan's named question, passes in all four BioProject directions it
was surveyed in.

### What those 20 add

Each value a candidate adds over its direction's declared routes was asked of the reverse
direction's declared routes, by `scripts/second_routes_roundtrip.py`. It ran first over the 20
on 2026-10-28, and then over all 87 candidates on 2026-10-29, after the IDF fixes (#83, #87)
had merged. The table is the second run. The counts are against the declared routes only, so
they can exceed what the comparison credited after other candidates were accepted. It is **confirmed** when a route on another provider maps it back
to the accession it was added for. It is **contradicted** when such a route names a different
accession, and **unconfirmed** when none of them knows it.

A route on the same provider cannot confirm a value. ELink from a project adds its siblings'
experiments, and ELink from those experiments maps them straight back. Counted, that circle
"confirmed" 154 of ELink's 179.

A file has no reverse route. It is confirmed when its name carries the run it was added for, and
contradicted when it names another run.

| Candidate | Values added | Confirmed | Contradicted | Unconfirmed |
|---|---:|---:|---:|---:|
| `bioproject->experiment:elink_bioproject_sra_all` | 179 (11 projects) | 25 (3) | **154** | 0 |
| `bioproject->experiment:esearch_sra_runinfo` | 39 (16) | 25 (3) | **14** | 0 |
| `bioproject->biosample:esearch_sra_runinfo` | 23 (15) | 10 (2) | **13** | 0 |
| `bioproject->run:esearch_sra_runinfo` | 14 (13) | 0 | **14** | 0 |
| `bioproject->study:esearch_sra_runinfo` | 15 (15) | 0 | **13** | 2 |
| `bioproject->biosample:ddbj_dblink` | 70 (14) | **64 (13)** | 6 | 0 |
| `bioproject->study:ddbj_dblink` | 6 (6) | 0 | 0 | 6 |
| `bioproject->geo_series:ena_xref_target` | 4 (4) | 0 | 0 | 4 |
| `geo_series->bioproject:ena_xref_source` | 4 (4) | 0 | 0 | 4 |
| `study->ae_experiment:ena_xref_target` | 4 (4) | 0 | 0 | 4 |
| `ae_experiment->study:biostudies_study` | 6 (5); 86 (84) before #87 | 0 | 0 | 6 |
| `geo_sample->experiment:gds_esummary_sample` | 19 (5) | 0 | 0 | 19 |
| `geo_sample->experiment:sra_be_runinfo` | 7 (6) | **7 (6)** | 0 | 0 |
| `geo_sample->biosample:esummary_sra` | 4 (4) | **3 (3)** | 1 | 0 |
| `geo_series->geo_sample:sra_be_runinfo` | 53 (3) | 48 (1) | 0 | 5 |
| `experiment->run:ddbj_dblink` | 3 (3) | 1 (1) | 0 | 2 |
| `run->file:efetch_sra_files` | 40,582 (2,977) | 2,978 | **37,604** | 0 |
| `run->file:sdl_original` | 8,185 (2,926) | 0 | 0 | 8,185 |
| `run->file:sdl_lite` | 2,936 (2,936) | **2,936** | 0 | 0 |
| `run->file:ena_filereport_bam` | 61 (61) | **61** | 0 | 0 |

What each group is:

- **Another project's data.** ELink from a BioProject adds the experiments of projects under the
  same umbrella: PRJNA1033389 gains 72, and every one of them belongs to a sibling
  ([the pathology](../pathologies/elink-returns-runs-of-sibling-projects.md)). The bare-term
  ESearch matches a project's accession wherever it is written. PRJEB103088 gains ERR15943854,
  which is PRJEB102986's. Their 25 confirmed values are three NCBI projects ENA does not hold
  (PRJNA904346, PRJNA869921, PRJNA624085). `ddbj_dblink` returns the same 25 and nothing
  wrong. `esummary_sra` returns them too, along with 13 experiments of other projects: like the
  bare term, it searches the accession as text. `ddbj_dblink` was not credited only because the
  candidate adding most is accepted first, and the wrong values made the others add most.
- **Another run's files.** `efetch_sra_files` lists the files of every run in a package:
  SRR12771605 comes back with SRR12771604.lite.1 and fourteen more. 37,604 of its 40,582
  additions are other runs' files. Its own are the `.lite.1` copies `sdl_lite` offers.
- **Data only one archive knows.** The `ena_xref` routes, `gds_esummary_sample` and DDBJ's
  project → study each add values no other archive holds. GEO names SRX30213031–33 for
  GSM9209970, and SRA holds none of them: the shape of
  [experiments GEO names with no runs](../pathologies/soft-names-experiments-with-no-runs.md).
  The GEO series ENA links from PRJNA763495 and PRJNA1248477 are not served by GEO.
- **Right, and a reader's gap.** `biostudies_study`'s 86 studies are the ones an IDF names and
  `idf_secondary` does not read: the second cell of `Comment[SecondaryAccession]`, or a quoted
  row ([#83](https://github.com/cellgeni/fetch-series/pull/83) found 540 of the 551 studies the
  IDF route is said to miss). Its only reverse route is on BioStudies, so it cannot be
  confirmed here. The fix was the reader: once
  [#87](https://github.com/cellgeni/fetch-series/pull/87) read every cell, `idf_secondary`
  resolved 2,997 of the 3,000, and `biostudies_study` added 6 studies over 5.
- **Right.**
    - `bioproject->biosample:ddbj_dblink`: 64 BioSamples that NCBI ties to their project,
      mostly through the BioSample record itself. None has a run, which is why every
      run-based route misses them.
    - `sra_be_runinfo` from a GEO sample: 7 experiments over 6 samples, two of them over
      `acc_cgi` failures that did not clear when asked again.
    - `esummary_sra` from a GEO sample.
    - `sdl_lite` and `ena_filereport_bam`, which offer copies no declared route offers.
    - `sdl_original`'s files carry the submitter's names, so no run accession appears in them:
      SRR12771605's are `GAS_Day4_batch5_S5_L002_{I1,R1,R2}_001.fastq.gz`, the only route here
      to offer its index read. Unconfirmed by the name test, and not contradicted by it.

### The redundancy that is already declared

In 16 accession directions with more than one declared route, no declared route adds anything
over the others: 64 routes. Run → study, run → sample and run → bioproject each have four routes
that agree on every one of 3,000 runs, at five requests per resolve.

Adding nothing is not the same as agreeing. Take three routes answering `{x}`, `{y}` and
`{x, y}`. None of them adds anything, but the first alone is not the answer. So the two resolver
questions get different answers:

- **`Mode.FIRST` would return the same answer as union** only where every route agrees on
  outcome and values. That holds in 4 directions on this draw: run → study, run → sample, run →
  bioproject, and GEO series → GEO series. There, `Mode.FIRST` would save four requests in five
  and lose nothing, because a failure falls through to the next route.

  One thing must change first. Those routes tie on every ranking key, so `ranked()` orders them
  by route id. That puts `ddbj_dblink` first, and DDBJ is the slowest service asked, at 1.5
  requests a second.
- **Skipping a route that adds nothing** is not safe on this evidence. The example above shows
  why: dropping the third route loses `y`.

Neither is changed here. Each is a resolver change, and needs its own argument.

### Under the amended rule: six pass

Re-run with `fetch survey promote --compare --confirmed
data/surveys/second-routes/roundtrip-2026-10-29.json`.

| Route | Adds, confirmed | Over |
|---|---|---:|
| `bioproject->biosample:ddbj_dblink` | 64 BioSamples NCBI ties to their project; none has a run | 13 projects |
| `bioproject->experiment:ddbj_dblink` | 25 experiments of three projects ENA does not hold | 3 projects, the bar |
| `geo_sample->biosample:esummary_sra` | 3 BioSamples, two where `acc_cgi` fails | 3 samples |
| `geo_sample->experiment:sra_be_runinfo` | 7 experiments, two samples where `acc_cgi` fails | 6 samples |
| `run->file:sdl_lite` | the SRA Lite object, a copy no declared route offers | 2,936 runs |
| `run->file:ena_filereport_bam` | the BAM ENA holds | 61 runs |

The `acc_cgi` failures under the two GEO-sample routes were asked again at low concurrency and
did not clear. They are the route's own failures, not the survey's load.

Ten were refused for adding another accession's data at the bar or over:

- ELink from a BioProject into experiments;
- the bare-term ESearch, in all four BioProject directions;
- `esummary_sra`, in the same four;
- `efetch_sra_files` into files.

The rest add nothing confirmed over the bar.

All six are declared in `graph.PROMOTED` with the evidence of their survey on the draw.
`ranked()` places each one by that evidence, as for any route.

**One ranking needed the same draw.** `acc_cgi`'s evidence for GEO sample → BioSample and →
experiment came from `sample:1000`: 825 GEO samples, 822 distinct results. `ranked()` compares
distinct results, so against the new routes' 2,303 it came second on the size of its draw, not
its yield. It was surveyed on `sample:3000` during this run: 2,472 and 2,471 of 2,479, against
the new routes' 2,303. Its evidence in those two directions now cites that survey, and it ranks
first in both again. Its GEO-series route keeps its `sample:1000` evidence, since no second
route joined that direction.

## What was decided, 2026-10-29

1. **The amended rule above.** Six second routes are declared under it.
2. **`idf_secondary`'s reader is fixed** ([#87](https://github.com/cellgeni/fetch-series/pull/87)),
   and `biostudies_study` adds nothing confirmable after it. It stays a link.
3. **The bare-term ESearch stays a link.** Against `[GPRJ]`, everything it adds that another
   provider can place belongs to another project, in all four directions.
4. **Open:**
    - `sdl_original` offers the submitter's original files, including index reads no other
      route offers, but its names cannot carry the run, so the name test cannot confirm them.
      It needs a test of its own before it can be judged.
    - Whether union mode should skip routes that add nothing, or use `Mode.FIRST` where every
      route agrees, is still a resolver change with its own argument to make.

## What would reverse it

- **The bar is too low.** At 3 of 3,000, a route could pass on noise: three accessions whose
  record changed between two requests. If routes pass that a second draw shows adding nothing,
  the bar is wrong, and it should be raised before any further promotion. On 2026-10-28 the bar
  let wrong values through, not noise. That is the case for the amendment above.
- **The draw is not the population.** A second route that only helps in a stratum the draw
  under-samples will look redundant here. The bar is a share of the draw, so such a route needs
  a stratified draw of its own before it is turned away.
