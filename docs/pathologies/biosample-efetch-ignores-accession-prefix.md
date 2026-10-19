---
title: efetch db=biosample ignores the accession prefix and returns a different sample
---

# `efetch db=biosample` ignores the accession prefix

**Archive:** NCBI E-utilities
**Affected source:** [`biosample:attribute`](../metadata/sources.md#biosampleattribute-is-the-richest-and-was-being-discarded)
**Status:** detected, mechanism confirmed, worked around; not yet reported upstream
**Measured:** 2026-09-16

## The symptom

`efetch db=biosample` appears to accept BioSample accessions. Asked for
`SAMEA11294529` -- a human liver sample in E-MTAB-10553 -- it returns
`SAMN11294529`, a rumen bacterium, with **HTTP 200, a well-formed
`BioSampleSet`, and 14 attributes**.

```
requested SAMEA11294529  ->  SAMN11294529, "Tissierellaceae bacterium", 14 attributes
requested SAMEA11294527  ->  SAMN11294527, "Pseudoselenomonas ruminantium", 14 attributes
```

Nothing in the response says a substitution happened. A count check passes: one
sample was asked for and one was returned.

## The reproducer

```bash
# Asked for an EBI-registered human sample; answered with an NCBI bacterium.
curl -s "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=biosample&id=SAMEA11294529&retmode=xml" \
  | grep -o 'accession="[^"]*"\|<OrganismName>[^<]*'
```

## The mechanism

**The alphabetic prefix is stripped and the remainder is read as a UID.**

```bash
# These three return the same record.
for id in SAMEA11294529 11294529 SAMN11294529; do
  curl -s "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=biosample&id=$id&retmode=xml" \
    | grep -o 'accession="[^"]*"' | head -1
done
# -> accession="SAMN11294529"  (all three)
```

`11294529` is the Entrez UID of `SAMN11294529`. The real `SAMEA11294529` is UID
`23423599`, which `esearch` finds correctly:

```bash
curl -s "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=biosample&term=SAMEA11294529&retmode=json"
# -> "idlist": ["23423599"]
```

## Why it is invisible on most samples

**For an NCBI-registered accession the number *is* the UID.**

```
SAMN12011228 -> uid 12011228
SAMN12039425 -> uid 12039425
```

So `id=SAMN12011228` returns SAMN12011228 -- not because the accession was
honoured, but because stripping the prefix happens to yield the right UID. Every
`SAMN` sample works by coincidence, and `SAMN` is most of them.

The bug bites only where the registration authority is not NCBI: **`SAMEA`
(EBI) and `SAMD` (DDBJ)**. This project's corpus is full of those -- ENA finds
**125,271 BioSamples NCBI's own link table does not**, 93.6% of them in
EBI-registered projects.

## What it is not

Not a missing record. `SAMEA11294529` is present in NCBI BioSample and
`esearch` returns exactly one hit for it. Only the `efetch` id parameter is
affected.

Not a malformed-request error. There is no 400, no `<ERROR>` element and no
warning. The substituted record is complete and internally consistent, which is
what makes it dangerous: a metadata layer reading it attributes a rumen
bacterium's `host`, `isolation_source` and `env_medium` to a human liver sample.

## The workaround

Resolve every accession through `esearch ... [accession]` first, then `efetch`
by UID, then **check every returned accession against what was asked for**.

```python
uids = await biosample_uids(client, batch, timeout)      # esearch, [accession] field
xml = await efetch_text(client, db="biosample", uids=uids, retmode="xml", ...)
for record in parse_biosample_set(xml):
    if record.accession in expected:      # the check that catches the substitution
        kept.append(record)
```

Searching on the `[accession]` field rather than as a bare term matters for the
same reason: a bare number can match a UID or an unrelated field. The term goes
by POST above 200 accessions, because 200 of them is over 5 KB and
[a UID list in a query string has cost this project data five times](../index.md).

Implemented in `eutils.fetch_biosamples`. It costs one extra request per batch
of 200, which is the price of not silently mixing organisms.

## Regression test

`tests/pathologies/test_biosample_accession_substitution.py`, inverted like the
rest of the suite: it asserts the substitution still happens, and separately
that `id=SAMEA11294529` and `id=11294529` still return the same record, so the
*mechanism* is pinned rather than only the symptom. A third test, not inverted,
asserts the workaround still defeats the live bug.

A failure of the first two is good news. Drop the `esearch` hop, update this
page, and delete them.
