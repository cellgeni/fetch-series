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
- **Quoted IDFs were not read until a second fix the same day.** 16 of the 3,000 IDFs quote every
  cell (`"Comment [SequenceDataURI]"`, E-ERAD-355), and `biostudies.idf_comments` read nothing from
  them. See [below](#quoted-idfs-read-since-2026-10-28).

A range is the first and last of a group of runs, not a list of them, so the expanded ranges also
name runs the experiment does not describe: 327 runs in 19 of the 2,654 studies whose SDRF could be
compared, and never another submitter's. That is the archive's, and is
[a pathology](../pathologies/ae-idf-run-range-is-not-a-run-list.md) the route declares.

### Quoted IDFs, read since 2026-10-28

Some IDFs quote every cell and put a space in the tag: E-ERAD-129 writes
`"Comment [SecondaryAccession]"	"ERP002094"`. `biostudies.idf_comments` anchored on a bare
`Comment[` and read nothing at all from them. It now reads through `biostudies.idf_rows`, which takes
both dialects and every cell of a row, and strips each one. That reader serves every route that
reads an IDF. The same
fix took the study out of the second cell of an E-GEOD row, and rewrote
[a pathology this project's reader had made](../pathologies/ae-idf-missing-secondary-accession.md).

All three routes here were surveyed again over the same 3,000 on 2026-10-28, 14:06–14:50 UTC. Each
route's 403s from `ftp.ebi.ac.uk`, throttling while another job read the same host, were asked
again one at a time until none was left.

| Route | Resolved | Empty | Failed | Distinct | What changed |
|---|---:|---:|---:|---:|---|
| `->run` | 2,667 → **2,673** | 320 → 314 | 13 | 111,829 → **112,005** | E-ERAD-203 and E-MTAB-8121, quoted; and the 4 IDFs cut at 4,096 bytes that morning, served whole |
| `->geo_series` | 1,552 | 1,446 | 2 | 1,552 | nothing: no quoted IDF names a GEO series |
| `->ae_experiment` | 282 → **285** | 2,716 → 2,713 | 2 | 472 → **481** | E-MTAB-8121, quoted; E-MTAB-6185 and E-MTAB-6219, whose tag has a space before its tab: `Comment [RelatedExperiment] ` |

Every answer the earlier survey gave is unchanged. The other 14 quoted IDFs name a study in the
field, not runs: E-ERAD-355's is ERP009144, the study its `SecondaryAccession` names. The route
reads it as what it is and does not return it. `ranked()` is unchanged in all three directions.

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
