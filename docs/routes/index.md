---
title: The route matrix
---

# The route matrix

<!-- route-count --> Every row below was produced
by a survey recorded in the SQLite cache and exported to
[`data/surveys/`](https://github.com/cellgeni/fetch-series/tree/main/data/surveys);
the counts here are generated from the `RouteEvidence` blocks in
`src/fetch_series/graph.py`, which is what `RouteRegistry.ranked()` reads. There
is no hand-written preference order anywhere in this project.

Regenerate this table's underlying numbers with:

```bash
uv run fetch routes list
```

The same routes are drawn as a graph, with each one's resolved, empty and
failed counts, on [the route graph](graph.md).

## How to read it

- **Queried** is how many accessions the route was actually asked about. It is
  below the corpus size where the corpus mixes entity types — `reprocessed-sample`
  holds GSMs and SRSs, and a GEO route is only asked the GSMs.
- **Results** counts *distinct* target accessions across the whole survey, not
  rows. The 2026-05 predecessor to this project counted rows, and 69,923 of its
  824,216 "runs" were within-project duplicates.
- **Failed** is accessions that produced no verdict at all. It is not the same
  as empty: "the archive holds no such link" and "I could not find out" are
  different answers, and conflating them made BioProject → GEO look broken when
  most of its empties were correct.
- **Corpus** identifies the accession set exactly, including the size of a
  seeded draw. `--limit` is deliberately not used for evidence, because it
  records verdicts against the full corpus name.

<!-- route-matrix -->

## What the failures are

Every non-zero **Failed** figure in the table falls into one of two groups, and
only the first is an archive refusing an accession or a file that no longer
exists:

- **Accessions or files that are gone.** The 7 GEO series whose SOFT family file 404s,
  GSE207991 among them, which is private; the 5 ArrayExpress studies whose IDF
  is a genuine 404 while the study record and the search index still carry them;
  the 3 GEO samples whose `acc.cgi` record cannot be fetched; the 11 studies whose
  SDRF 404s on both SDRF routes, two of which register no files at all and nine
  `E-GEOD` imports whose IDF exists without one
  ([the SDRF census](ae-experiment-to-file/index.md)); and, by manual
  triage of a sample rather than of all of them, most of the 1,078–1,079
  BioProject → GEO series failures, which are re-created, withdrawn or private
  projects ([the triage](bioproject-to-geo-series/index.md#about-those-1078-failures)).
- **NCBI routes failing on the request.** None left. The four NCBI BioProject → run routes
  failed 14 to 33 projects in the 2026-05 scripts. Re-censused through the harness on
  2026-10-24 to 26, every failure was a transport error that answered on retry, and none
  remains ([the re-survey](bioproject-to-run/index.md#measured-behaviour)).

Client-side losses do not appear in the table, because each one found was fixed
and its route re-run:
[39 series lost to a UID list too long for a URL](gse-to-experiment/index.md#the-39-failures-were-ours-not-ncbis)
(that route now fails on nothing), and the
[330 BioProjects an unpaged ESummary call failed](bioproject-to-biosample/index.md#the-paging-fix-recovered-94895-biosamples)
in the 2026-05 script, which fail on nothing through the harness.
