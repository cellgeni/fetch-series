---
title: No DDBJ provider is needed
---

# No DDBJ provider is needed

**Decided:** 2026-09-15 · **Reverses:** a plan item, on evidence ·
**Evidence:** [routing by archive](../graph/routing-by-archive.md)

The plan budgeted `providers/ddbj.py` and listed DDBJ as *"entirely
unexplored — DRA/DRR/PRJDB via DDBJ's own API vs ENA vs NCBI"*. It was explored
by measuring the alternatives first, and the provider was not written.

## The measurement

All 137 `PRJDB` projects in the reprocessed corpus, by route:

| Route | Resolves | |
|---|---:|---:|
| ENA `filereport` | **137 / 137** | **100%** |
| NCBI ELink | 45 / 137 | 32.8% |

ENA returns **2,307 `DRR` runs** for those projects. It also resolves the
`PRJDB` BioSamples that
[NCBI's link table under-reports](../routes/bioproject-to-biosample/index.md) —
1,074 of the 125,271 samples ENA finds and ELink does not are in
DDBJ-registered projects.

## The argument

A DDBJ-native provider would be a **third implementation of a route two archives
already answer**, one of them completely. It would add an API surface to
maintain, a parser, a rate limit to respect, and a fourth set of failure modes —
and buy nothing measurable.

The INSDC mirroring that makes this true is the same property the accession model
already relies on: `SRP`/`ERP`/`DRP` are one entity under three prefixes, and
ENA indexes all three. DDBJ data being reachable through ENA is not an accident
this project is exploiting; it is what INSDC is for.

## What would reverse it

**A corpus containing `PRJDB` projects ENA cannot resolve.** The figure above is
137 projects from a 10x reprocessing corpus — not a sample of DDBJ. A
DDBJ-heavy population might behave differently, and the decision is written down
precisely so that finding out is cheap:

```bash
uv run fetch survey run --route bioproject->run:ena_filereport --corpus <ddbj-corpus>
```

Also reversing: **DDBJ-only record types.** ENA mirrors the submitted data. If
this project ever needs DDBJ's own curation — DRA submission metadata that does
not cross into ENA — that lives only at DDBJ, and this page should be revisited
rather than cited.

## Since: a parser, not a provider for routes

`providers/ddbj.py` now exists, and this decision stands. It parses DDBJ
Search's `dblink` answer for [the link catalogue](../graph/catalogue.md),
which probes every link an archive API offers whether or not a route uses it.
It backs no route, and nothing that resolves an accession calls it. Writing a
route on it would need a corpus where ENA cannot resolve a `PRJDB` project,
which is still the condition above.

## Related

The same reasoning, applied to a different plan item and reaching the same
conclusion for a different reason:
[the ENA browser XML API is not needed](ena-browser-xml-not-needed.md).
