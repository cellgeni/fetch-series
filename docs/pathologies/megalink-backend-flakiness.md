---
title: ELink's MegaLink backend fails in bursts, inside an HTTP 200
---

# ELink's MegaLink backend fails in bursts, inside an HTTP 200

**Archive:** NCBI E-utilities
**Affected routes:** [`bioproject->run:sra_be_elink`](../routes/bioproject-to-run/index.md),
[`bioproject->run:efetch_elink`](../routes/bioproject-to-run/index.md),
[`bioproject->biosample:elink`](../routes/bioproject-to-biosample/index.md)
**Status:** intermittent; worked around by retrying the whole chain. Not reproduced on 2026-10-23.
**Measured:** 2026-05-11, from the logs of the first BioProject surveys; re-checked live 2026-10-23

## Symptom

ELink answers HTTP 200 with a well-formed JSON body. Where its links should be, it puts a
message from the backend that failed to find them:

```
"linksetdbhistories": ... 'callMLink: Query failed on MegaLink server'
"linksetdbhistories": ... "Couldn't resolve #pmquerysrv-mz?dbaf=biosample, the address table is empty."
```

There are no links in such an answer. A client that reads only `linksetdbs` and treats a
missing list as an empty one records **"this project has no BioSamples"**: a silent empty,
indistinguishable from the real answer for a project that has none.

## The numbers

The logs of the three ELink-based BioProject surveys of 2026-05-11 survive, one run each:
the scripts then opened their logs in write mode, so earlier runs were overwritten. Lines
carrying either message:

| Survey | Lines | Projects hit at least once | Retries 1 / 2 / 3 / 4 | Failed after 5 attempts |
|---|---:|---:|---|---:|
| BioProject → SRA, ELink | 1,842 | 1,566 | 1,554 / 146 / 12 / 0 | 0 |
| BioProject → BioSample, ELink | 2,518 | 2,213 | 1,711 / 633 / 139 / 35 | 6 |
| BioProject → SRA, sra-db-be ELink | 747 | 676 | — | 5 |

In the BioProject → BioSample survey, 1,095 of the 2,518 lines are the MegaLink message and
1,423 are `#pmquerysrv-mz`. The figure of 3,135 this project quoted before is 1,712 + 1,423
from an earlier count of the same logs. Either way, it is the dominant failure signal of
those surveys, ahead of every client-side error.

**It comes in bursts.** The BioSample survey logged 316 such lines in one minute (19:21,
local time), and errors in 74 of its 103 minutes. The SRA survey peaked at 145 a minute.
Retrying a whole `esearch → elink → efetch` chain a few seconds later almost always
succeeded. Of 2,213 projects hit, 6 still failed after five attempts, and those six failed
as malformed responses.

## Reproducer

There is no deterministic one. The failure belongs to the backend at a moment, not to an
accession. Re-checked on 2026-10-23: ELink `bioproject → biosample`, `cmd=neighbor`, for 300
seeded PRJNA projects of the reprocessed corpus at 3 requests a second. 295 answered with
links, 3 empty, 2 had no BioProject UID, and **none** carried a backend error.

```bash
curl -s 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=bioproject&db=biosample&id=<uid>&cmd=neighbor&retmode=json' \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print([s.get("ERROR") for s in d.get("linksets",[])])'
# during an outage: a backend message where the links should be
```

## What it is not

- **Not rate limiting.** It arrives as HTTP 200, not 429, and at the same request rate as the
  minutes around it that succeed.
- **Not the accession.** The same project succeeds on retry seconds later.
- **Not `elink-history-missing-querykey`**, which is a valid history answer with no usable key.
  It is deterministic per project and resolved by `cmd=neighbor`.

## Workaround

- **Retry the whole chain.** A `WebEnv`/`query_key` is session-scoped, so retrying one call of
  a history chain is not enough.
- **Never read a backend message as an empty link set.** `eutils.elink_uids` raises
  `MalformedResponseError` when `linksets` is absent *or carries an `ERROR`*, and
  `survey.client.is_retryable` retries it with backoff. Until 2026-10-23 it checked only the
  first: a link set holding the message and no links read as a project with none. The routes
  ask with `cmd=neighbor`, which needs no history.
- **A project that fails after every attempt is recorded FAILED, never EMPTY**
  (`survey/runner.py`).

## Affected accessions

None standing: it is a property of the backend at a moment, not of an accession.

## Regression test

None inverted. An intermittent failure cannot be asserted to reproduce, and a test that waited
for one would skip forever. What is tested, offline, is the workaround:
`tests/test_elink_backend_errors.py` holds that an ELink answer carrying either message raises
a retryable failure, and that a link set with no links and no message is still empty.
