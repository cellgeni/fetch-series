---
title: When a second route earns its place
---

# When a second route earns its place

**Status:** rule set 2026-10-27, before anything was measured. The measurement is pending:
`fetch survey promote --covered`, then `--compare`.
**Evidence:** pending

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

Pending the survey.

## What would reverse it

- **The bar is too low.** At 3 of 3,000, a route could pass on noise: three accessions whose
  record changed between two requests. If routes pass that a second draw shows adding nothing,
  the bar is wrong, and it should be raised before any further promotion.
- **The draw is not the population.** A second route that only helps in a stratum the draw
  under-samples will look redundant here. The bar is a share of the draw, so such a route needs
  a stratified draw of its own before it is turned away.
