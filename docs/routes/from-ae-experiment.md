---
title: Promoted routes from ArrayExpress experiment
---

# Promoted routes from ArrayExpress experiment

Catalogue links from an ArrayExpress experiment that were surveyed **as routes**: each at `sample:3000` of its
source's population (or the whole of it, where smaller), gated first on `probe-families` and
`hard-cases`. A route here makes the catalogue mechanism's own request and reads it with the
link survey's own extraction (`routes.catalogue`). The table is generated at build time from
`graph.PROMOTED`. Within each direction, routes are listed in the order `ranked()` tries them,
from the evidence and never by hand ([how routes are ranked](../decisions/route-ranking.md)).

<!-- promoted-routes:ae_experiment -->

## The IDF names runs in ranges

`ae_experiment->run:biostudies_idf` reads `Comment[SequenceDataURI]`, which writes an experiment's
runs as ranges: E-GEOD-29264's is `http://www.ebi.ac.uk/ena/data/view/SRR208051-SRR208059`, nine
runs. Until 2026-10-28 the route found the accessions in that text, which are the range's two ends,
and none of the seven between. Over `sample:3000@arrayexpress-ena` it returned exactly two runs for
2,159 of the 2,682 studies it resolved. The fault was this project's reader, not the archive's.

`biostudies.sequence_data` now expands a range where both ends are one kind of accession with one
prefix and it ascends, up to 50,000 accessions, and reports anything else. Re-surveyed over the same
3,000:

| | 2026-10-27 | 2026-10-28 |
|---|---:|---:|
| resolved | 2,682 | 2,667 |
| empty | 316 | 320 |
| failed | 2 | 13 |
| distinct runs | 16,873 | **111,829** |
| studies answered with exactly two runs | 2,159 | 178 |

Over the 2,667 studies it now resolves, `ae_experiment->run:biostudies_sdrf_comments` finds 111,387
runs. Each of the 2,667 answers holds every run its 2026-10-27 answer held.

- **11 more failures** are ranges the reader reports rather than guesses at: 9 written in string
  order, so that they descend, and 2 spanning over 50,000 accessions for 2 and 7 runs. Why they fail
  the link rather than answer from the rest, and why a wide range is not cut short, is argued on
  [the pathology page](../pathologies/ae-idf-run-range-is-not-a-run-list.md#why-a-wide-range-is-reported-rather-than-capped).
  The other 2 failures are the studies that register no files at all, as before.
- **4 studies went from resolved to empty**, and the reader is not why: BioStudies served their IDFs
  cut off at 4,096 bytes, before the field ([below](#an-idf-served-cut-off-at-4096-bytes)).
- The order `ranked()` gives this direction is unchanged: it ranks on how many studies resolve, and
  the two-ends reading resolved as many as the expanded one.
- **The IDF reader still misses quoted rows.** 16 of the 3,000 IDFs quote every cell
  (`"Comment [SequenceDataURI]"`, E-ERAD-355), and `biostudies.idf_comments` reads nothing from
  them. Two of the 16 name runs, which this route records as empty. The same gap reaches every link
  the IDF answers, so it is left for a fix that re-measures them all together.

A range is the first and last of a group of runs, not a list of them, so the expanded ranges also
name runs the experiment does not describe: 327 runs in 19 of the 2,654 studies whose SDRF could be
compared, and never another submitter's. That is the archive's, and is
[a pathology](../pathologies/ae-idf-run-range-is-not-a-run-list.md) the route declares.

### An IDF served cut off at 4,096 bytes

On 2026-10-28 BioStudies served some IDFs cut off at exactly 4,096 bytes, mid-line, with HTTP 200.
E-GEOD-58432's IDF is registered at 4,171 bytes and served at 4,096. Its `SequenceDataURI` reads
`http://www.ebi.ac.uk/` and stops; the legacy ArrayExpress copy, at
`ftp.ebi.ac.uk/pub/databases/arrayexpress/data/experiment/GEOD/`, still holds
`SRR1382298-SRR1382313`. E-GEOD-67915 (5,031 bytes registered) and E-MTAB-13170 (9,700) were cut the
same way. E-MTAB-15453 was cut during the survey and served whole an hour later, so the state was
changing as it was measured. These four are 4 of the route's 320 empties. On 2026-10-27 the same
four resolved.

Nothing in a cut file says it was cut. The route cannot tell it from an IDF that names no runs
without asking for the registered size, a second request. This is recorded, not worked around: it
is not the range reader's, and it reaches every route that reads an IDF.
