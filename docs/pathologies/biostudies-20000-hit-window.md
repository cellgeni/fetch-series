---
title: BioStudies serves 20,000 hits, then returns HTTP 500
---

# BioStudies serves 20,000 hits, then returns HTTP 500

**Archive:** EBI BioStudies (ArrayExpress collection)
**Affected route:** [`study->ae_experiment:biostudies_search`](../routes/study-to-ae-experiment/biostudies-search.md), and any enumeration of a collection
**Status:** worked around; not yet reported upstream
**First observed:** 2026-09-15

## Symptom

A search that reports more than 20,000 `totalHits` can be paged only as far as the
20,000th. The next page is answered with **HTTP 500**, with no body explaining the
limit — not `400 Bad Request`, which is what a rejected parameter would earn, and not
a short final page, which is what an exhausted result set looks like.

The status code is the problem. `500` is the canonical *transient* failure: this
project's own `is_retryable` treats it as one, as does every HTTP retry library in
common use. So the client retries a permanently impossible request, backs off, and
retries again. The enumeration does not fail fast — it hangs.

It also fails late. The ArrayExpress collection holds 20,693 studies with an ENA
link, so the failure lands at **96.6% of the way through**, after twenty minutes of
apparently healthy paging.

## Reproducer

```bash
# 20,693 hits, and BioStudies says so
curl -s 'https://www.ebi.ac.uk/biostudies/api/v1/arrayexpress/search?pageSize=1&facet.link_type=ENA' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["totalHits"])'
# 20693

# the last page inside the window
curl -s -o /dev/null -w '%{http_code}\n' \
  'https://www.ebi.ac.uk/biostudies/api/v1/arrayexpress/search?pageSize=100&page=200&facet.link_type=ENA'
# 200

# the first page past it
curl -s -o /dev/null -w '%{http_code}\n' \
  'https://www.ebi.ac.uk/biostudies/api/v1/arrayexpress/search?pageSize=100&page=201&facet.link_type=ENA'
# 500
```

The cap is on the offset, not the page number: `pageSize=500&page=41` is the same
offset of 20,000 and fails identically. `nextCursor` is present in every response
and is always `null`, so the cursor that would sidestep the window is advertised
but never issued.

## Expected

Either a documented cap returned as `400` with a message naming it, or a working
`nextCursor`. A deep-paging limit is an entirely reasonable thing for a search
service to have; answering it with `500` is what makes it a defect.

## Workaround

Partition the query on `facet.released_year` and page each year separately. Every
study has exactly one release year, so the partitions are disjoint and exhaustive.
Implemented as `fetch_series.providers.biostudies.search_by_year`.

Verified: the union of the per-year enumerations came to exactly **20,693** distinct
accessions, matching the `totalHits` of the unpartitioned query. No year in the
ArrayExpress collection comes close to the window — the largest is under 2,200.

`search()` now raises rather than paging into the error, and refuses on the *first*
page, where `totalHits` already says the enumeration cannot complete. Discovering
this at page 201 costs 200 wasted requests and the reason is not visible in the
failure; discovering it at page 1 costs one and names the cap.

## Affected accessions

Not accession-specific: any query whose result set exceeds the window.

```
collection=arrayexpress                       80,863 hits
collection=arrayexpress&facet.link_type=ENA   20,693 hits
```

## Regression test

`tests/test_request_limits.py::TestBioStudiesSearchWindow` — asserts that a search
reporting more than `SEARCH_WINDOW` hits refuses on the first page, and that year
partitioning covers a collection exactly once.
