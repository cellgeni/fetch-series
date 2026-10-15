---
title: EBI — BioStudies search returns HTTP 500 past 20,000 hits
---

# EBI — BioStudies search returns HTTP 500 past 20,000 hits

**Status:** draft, not filed **Archive:** EBI BioStudies
**Evidence:** [pathologies/biostudies-20000-hit-window.md](../pathologies/biostudies-20000-hit-window.md)
**Where to file:** `biostudies@ebi.ac.uk`

```bash
curl -s -o /dev/null -w '%{http_code}\n' \
  'https://www.ebi.ac.uk/biostudies/api/v1/arrayexpress/search?pageSize=100&page=201&facet.link_type=ENA'
```

---

## Draft report

Subject: `/search` answers requests past the 20,000-hit window with 500 rather than 400

Paging a search result set past the 20,000th hit returns **HTTP 500** with no body. The
20,000th is the boundary exactly: `pageSize=100&page=200` succeeds and `page=201` fails,
as does `pageSize=500&page=41`, which is the same offset.

The status code is the difficulty. `500` is the canonical *transient* failure, so a client
with an ordinary retry policy retries a permanently impossible request, backs off, and
retries again. A deep-paging cap is an entirely reasonable thing for a search service to
have; answering it with `500` makes it indistinguishable from an outage.

It also fails late. The ArrayExpress collection has 20,693 studies with an ENA link, so an
enumeration fails at 96.6% — after the client has every reason to believe the run is
healthy.

`nextCursor` is present in every response and is always `null`, so the mechanism that
would sidestep the window is advertised but never issued.

### What would resolve it

Any one of:

1. Return `400` with a message naming the cap.
2. Issue a working `nextCursor`.
3. Document the window in the API reference so clients partition their queries up front.

We currently partition on `facet.released_year`, which works — the per-year enumerations
sum to exactly the 20,693 `totalHits` of the unpartitioned query — but it is a workaround
that depends on a facet happening to divide the collection finely enough.

### A related observation on the same API

`GET /studies/{accession}/files` returns the first **25** items when no explicit `limit` is
given, with nothing in the response indicating that the result is partial;
`pagination.total` is present but a client that does not think to compare it against
`len(items)` has no signal. E-MTAB-9221 has 43 files, so an unlimited request silently
reports 25 of them.
