---
title: The resolver
---

# The resolver

**Needs:** any accession and a target entity type **Feeds:** samplesheets, exports
**Code:** `src/fetch_series/resolver.py`

Four design decisions, each one the answer to something a census found. None of them is a
matter of taste, and each would be wrong if the evidence had come out differently.

## 1. Union, not a fallback chain

```python
Mode.UNION   # ask every applicable route and merge   (default)
Mode.FIRST   # stop at the first route that answers
```

The two GEO → experiment routes fail on **disjoint** populations. `elink gds→sra` returns
nothing for 2,293 of 13,045 series; GEO omits the sample-level SRA relation for 16. No
ordering of them answers every series, so the default asks all of them.

`Mode.FIRST` exists for callers who want the cheap answer and know the direction is not
contested. It is not the default because the one direction measured in full turned out to
be contested.

## 2. Provenance per value, not per record

Every resolved accession carries the routes that produced it:

```
$ fetch resolve GSE150508 --to experiment --explain
GSE150508 -> experiment (union)
  resolved gse->experiment:soft_family: 1 results
  resolved gse->experiment:soft_bioproject_ena: 1 results
  resolved gse->experiment:elink_gds_sra: 1 results
  -> 2 values, 1 confirmed to carry data
  -> 1 unconfirmed; these may resolve to no files
  -> routes disagreed; see single_route_only
SRX7571191   UNCONFIRMED   soft_family
SRX9670669   confirmed     soft_bioproject_ena,elink_gds_sra
```

`GSE114373` and `GSE150508` are why. Their routes return equal-sized, **completely
disjoint** experiment sets, and only one side has data. A merged set with no provenance
cannot express that, and either answer alone looks like an ordinary success.

`Resolution.single_route_only` is the column to read when routes disagree: a value nobody
corroborates is either a gap in the other routes or a stale record in this one, and the
resolver cannot tell which. Saying so is the honest output.

## 3. Confirmed versus unconfirmed

A route may declare `proves_data_exists`. It is true for routes built on ENA's
`result=read_run`, whose rows *are* runs — an accession cannot appear in one without
carrying data.

This matters because **an experiment accession is not evidence that data exists**. Roughly
**4,238** experiments named in GEO SOFT files — 1.37% of the 308,639 it returns — have no
runs at all. Without this split, a complete-looking answer downloads nothing and nothing
says why.

`--confirmed-only` reports just the vouched-for values, which is what a download step
wants. The default reports both, flagged, because for counting what a series *contains*
the unconfirmed values are still information.

Unconfirmed does not mean wrong. A data-proving route may simply not have been applicable,
or may key its query on something that misses the value — 82% of the SOFT/ENA gap is the
ENA route keying on the BioProject, which a series' experiments do not all sit under.

## 4. Archive-aware route selection

A route that cannot index an accession's issuing archive is never called. NCBI's ELink
resolved BioProject → BioSample for 99.6% of NCBI-issued projects and **1.4%** of
EBI-issued ones, so asking it about a `PRJEB` accession is three requests spent to learn
nothing — 497 times over in the reprocessed corpus.

```
$ fetch resolve PRJEB42537 --to biosample --explain
PRJEB42537 -> biosample (union)
  skipped  bioproject->biosample:elink: provider does not index ebi-issued accessions
  resolved bioproject->biosample:ena_filereport: 10 results
```

See [routing by archive](graph/routing-by-archive.md).

## What corroboration looks like when everything agrees

```
$ fetch resolve PRJNA988806 --to run --explain
  -> 5 values, 5 confirmed to carry data
SRR25056225   confirmed   ena_filereport,sra_be_direct_cgi,sra_be_elink,efetch_direct,efetch_elink
```

Five independent routes, the same five runs. That is the uninteresting case, and it is the
common one — which is exactly why the interesting cases need a resolver that can describe
them rather than a chain that picks one silently.

## Not yet done

`Mode.UNION` currently asks only **direct** routes. Multi-hop paths are discoverable
(`RouteRegistry.find_paths`) and the CLI suggests them when no direct route resolves, but
the resolver does not yet walk them. That matters for entry points with no direct edge to
the target — a `GSM` reaching runs, for instance.
