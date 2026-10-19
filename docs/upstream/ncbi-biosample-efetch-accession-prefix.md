---
title: NCBI — efetch db=biosample ignores the accession prefix and returns a different sample
---

# NCBI — `efetch db=biosample` ignores the accession prefix

**Status:** draft, not filed **Archive:** NCBI E-utilities / BioSample
**Re-checked:** 2026-10-15 — `id=SAMEA11294529` still returns `SAMN11294529`, and esearch still finds the requested sample as UID 23423599. The pathology tests pass live.
**Evidence:** [pathologies/biosample-efetch-ignores-accession-prefix.md](../pathologies/biosample-efetch-ignores-accession-prefix.md)
**Where to file:** `eutilities@ncbi.nlm.nih.gov`, cc `info@ncbi.nlm.nih.gov`

```bash
# Ask for an EBI-registered human sample; get an NCBI-registered bacterium.
for id in SAMEA11294529 11294529 SAMN11294529; do
  printf '%-16s -> ' "$id"
  curl -s "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=biosample&id=$id&retmode=xml" \
    | tr '>' '>\n' | grep -m1 -o 'accession="[^"]*"'
done
# All three return accession="SAMN11294529".

# The requested sample exists and esearch finds it under a different UID.
curl -s "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=biosample&term=SAMEA11294529&retmode=json" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["esearchresult"]["idlist"])'
# -> ['23423599']
```

---

## Draft report

Subject: `efetch db=biosample` silently returns a different sample when given a non-`SAMN` accession

`efetch db=biosample` accepts an `id` parameter that appears to take BioSample
accessions, and for NCBI-registered samples it behaves as though it does. It does
not: the alphabetic prefix is discarded and the remainder is interpreted as an
Entrez UID.

For `SAMN` accessions this is invisible, because the numeric portion of a `SAMN`
accession *is* its UID — `SAMN12011228` is UID `12011228`. For accessions
registered by any other INSDC partner the number is a different, valid UID, and a
**different sample** is returned:

```
requested SAMEA11294529  ->  SAMN11294529, "Tissierellaceae bacterium", 14 attributes
requested SAMEA11294527  ->  SAMN11294527, "Pseudoselenomonas ruminantium", 14 attributes
```

`SAMEA11294529` is a human liver sample in ArrayExpress study E-MTAB-10553.
`SAMN11294529` is a rumen bacterium.

### Why this is more serious than a lookup failure

The response is **HTTP 200**, a well-formed `BioSampleSet`, containing one complete
record with a full `<Attributes>` block. There is no `<ERROR>` element, no warning,
and no indication that a substitution occurred. A client that checks how many
records came back gets the number it asked for.

The practical consequence is cross-organism metadata contamination. A pipeline
harvesting `tissue`, `isolation_source`, `host` and `env_medium` for a human
sample receives a rumen bacterium's values and has no way to detect it from the
response. Detection requires comparing the returned `accession` attribute against
the request — which is only obvious once you already know about this behaviour.

The scope is every BioSample not registered by NCBI: `SAMEA` (EBI) and `SAMD`
(DDBJ). These are not a marginal population. In a census of 12,755 BioProjects we
found 125,271 BioSamples that ENA returns and NCBI's own ELink table does not,
93.6% of them in EBI-registered projects.

### The requested sample is present and findable

This is not a coverage gap. `esearch db=biosample&term=SAMEA11294529` returns
exactly one hit, UID `23423599`, and `efetch` by that UID returns the correct
human sample. Only the accession handling in `efetch`'s `id` parameter is
affected.

### What would resolve it

Any one of these, in descending order of preference:

1. **Resolve the full accession**, including its prefix, so `id=SAMEA11294529`
   returns `SAMEA11294529`.
2. **Reject an accession-shaped `id` that cannot be resolved**, with a 400 and an
   `<ERROR>` element. An error is recoverable; a plausible wrong answer is not.
3. **Document that `id` takes UIDs only** and that accession-shaped values are
   coerced, so clients know to resolve through `esearch` first.

Option 3 alone would be enough for us. The current behaviour is worse than either
a failure or a documented limitation, because it is indistinguishable from
success.

### What we do meanwhile

Every accession is resolved through `esearch db=biosample&term=<acc>[accession]`
before `efetch`, and every returned record's `accession` attribute is checked
against the request; records that were not asked for are discarded and logged.
This costs one extra request per batch of 200 samples.
