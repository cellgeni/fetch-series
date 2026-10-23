---
title: BioStudies lists 25 files unless asked for more, and says so only in a field
---

# BioStudies lists 25 files unless asked for more, and says so only in a field

**Archive:** EBI BioStudies
**Affected route:** [`ae_experiment->biosample:sdrf`](../routes/ae-experiment-to-biosample/sdrf.md),
and the file-registration check the file layer makes against it
**Status:** deterministic; worked around by paging; reproduced 2026-10-23
**Measured:** 2026-10-23

## Symptom

`/biostudies/api/v1/studies/<accession>/files` with no `limit` returns the first 25 files
and stops. Only `pagination.total` says there are more. Read without it, a study with 43
files appears to register 25. In E-MTAB-9221's case, 15 of its 40 registered fastqs would
then have looked unregistered.

## Reproducer

```bash
curl -s https://www.ebi.ac.uk/biostudies/api/v1/studies/E-MTAB-9221/files \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(len(d["items"]), d["pagination"])'
# 25 {'offset': 0, 'limit': 25, 'total': 43, 'filtered': 43}

curl -s 'https://www.ebi.ac.uk/biostudies/api/v1/studies/E-MTAB-9221/files?limit=100' \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(len(d["items"]))'
# 43
```

Checked on 2026-10-23.

## What it is not

- **Not the 20,000-hit search window.** That is [a separate limit](biostudies-20000-hit-window.md)
  on search, answered with a 500.
- **Not `files` against `items`.** The listing is under `items`, and a `.get("files", [])`
  reads as an empty registration (CLAUDE.md). This page is about how many items come back.

## Workaround

`biostudies.registered_files` asks with `limit=500`, and keeps asking by `offset` until it
reaches `pagination.total`.

## Affected accessions

Every study with more than 25 files. The default is the endpoint's, not a property of studies.

## Regression test

`tests/pathologies/test_biostudies_silent_25_item_default.py` asserts that the unparameterised
listing still returns 25 of E-MTAB-9221's files while its total is higher. The non-inverted
half checks that `registered_files` returns all of them.
