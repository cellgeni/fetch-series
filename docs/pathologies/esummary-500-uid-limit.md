---
title: ESummary refuses more than 500 UIDs in JSON, inside an HTTP 200
---

# ESummary refuses more than 500 UIDs in JSON, inside an HTTP 200

**Archive:** NCBI E-utilities
**Affected routes:** [`bioproject->geo_series:elink`](../routes/bioproject-to-geo-series/index.md),
[`bioproject->geo_series:gds_direct`](../routes/bioproject-to-geo-series/index.md),
[`bioproject->biosample:elink`](../routes/bioproject-to-biosample/index.md)
**Status:** deterministic; worked around by paging at 500
**Measured:** reproduced 2026-10-23

## Symptom

Ask ESummary for 501 UIDs with `retmode=json` and it answers **HTTP 200** with no `result` at
all:

```json
{"header": {"type": "esummary", "version": "0.3"},
 "error": "Too many UIDs in request. Maximum number of UIDs is 500 for JSON format output."}
```

A client that checks the status code and reads `result` with a default of nothing gets an
empty answer: the project "has no GEO series" or "has no BioSamples". The same holds for a
history of more than 500 records asked for in one request.

## Reproducer

```bash
ids=$(seq -s, 10000000 10000500)          # 501 UIDs
curl -s -X POST https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi \
  -d "db=biosample&retmode=json&id=$ids" | head -c 200
# {"header":{...},"error":"Too many UIDs in request. Maximum number of UIDs is 500 for JSON format output."}
```

With 500 UIDs the same request answers with `result`. Checked on 2026-10-23.

## What it is not

- **Not a rate limit**, and not a URL length limit. A POST of 501 UIDs is refused the same way.
  The 414s a GET of a few hundred UIDs earns are a separate trap, one this project hit five
  times; that is why large lists go by POST (`POST_THRESHOLD_UIDS`).
- **Not retryable.** The same request is refused every time.

## Workaround

- **Page at 500.** `eutils.esummary_by_ids` batches a UID list at `MAX_UIDS_PER_SUMMARY`
  (500), and `eutils.esummary_by_history` pages a history by `retstart`/`retmax`.
- **Never read a missing `result` as an empty one.** `eutils.require_result` raises when an
  answer has no `result`, so the case cannot pass as an empty answer.

## Affected accessions

Any request over 500 UIDs. In the 2026-05 BioProject → BioSample ELink survey the ESummary
call was not paged: 330 projects with more than 500 BioSamples failed, and others were
silently truncated. The paged route's census of 2026-09-15 found 244,324 BioSamples where the
unpaged one had found 149,429 (the route's `RouteEvidence` notes).

## Regression test

`tests/pathologies/test_esummary_500_uid_limit.py` asserts that 501 UIDs are still refused with
the error inside an HTTP 200, and that 500 are still answered.
