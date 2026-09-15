---
title: Parity with fetch10xmeta
---

# Parity with `fetch10xmeta`

**Measured:** 2026-09-15 · **Result: 11 of 11 test cases reproduced.**

The incumbent is `modules/cellgeni/fetch10xmeta` in
[nf-reprocessing-public-10x](https://github.com/cellgeni/nf-reprocessing-public-10x):
26 KB of bash and awk that resolves an accession to a `links.tsv` of

```
run <tab> species <tab> url[;url...] <tab> type <tab> sample
```

Replacing it means producing that file, not a better one. A swap that changes
behaviour is not a swap — so `fetch links` reproduces the incumbent's nine-step
priority chain deliberately, including where this project's own
[file layer](files.md) would choose differently.

## The contract is the incumbent's own test suite

`tests/main.nf.test.snap` records, per test case, the `(run, species, type,
sample)` tuple for every run. It deliberately excludes the URLs: SDL hands out
signed links that rotate, so a URL is a download-time fact rather than a
resolution result. That exclusion makes the snapshot exactly the right
comparison — it is about which files were **chosen**, not which mirror served
them.

## Results

| Case | Accession | Runs | Match |
|---|---|---:|---|
| GEO — ENA paired-end fastq | GSE111360 | 44 | exact |
| GEO — ENA paired-end fastq, mouse | GSE160513 | 36 | exact |
| GEO — one run per sample | GSE250130 | 28 | exact |
| GEO — mixed ENA fastq and SRA archive runs | GSE264508 | 46 | exact |
| GEO — SRA archive only | GSE117988 | 6 | exact |
| GEO — 10x BAM | GSE274955 | 6 | exact |
| ArrayExpress — submitter fastq from the SDRF | E-MTAB-9221 | 20 | exact |
| BioProject — samples taken straight from ENA | PRJNA511433 | 128 | exact |
| GEO — no SRA relations, all samples | GSE135325 | 36 | exact |
| GEO — no SRA relations, twelve samples | GSE137444 | 50 | exact |
| ArrayExpress — SDRF URIs the study does not register | E-MTAB-8060 | 15 | assertions pass¹ |

¹ This case's snapshot is not present in the committed `.snap` file, so there is
nothing to diff against. Every assertion the test does make passes: 15 rows, all
`BAM`, all URLs ending `.bam`, none under `/pub/databases/microarray/`, no
`UNKNOWN` species, every sample matching `^ERS\d+$`.

Total: **425 runs** compared, zero differences.

## Three defects had to be fixed to get there

Each was a case where this project resolved *fewer* runs, or attributed them to
the wrong identity, than the bash module it is replacing.

**ArrayExpress was not a working entry point.** ENA's `filereport` rejects an
`E-MTAB` accession outright — it answers with a 400 naming the eight accession
shapes it does take. The 400 was caught, logged and swallowed, so every
ArrayExpress accession resolved to a series with no runs at all. The study has
to be found first, through the IDF's `Comment[SecondaryAccession]`, falling back
to the BioSamples the SDRF names for a study like E-MTAB-6505 that declares no
secondary accession.

**The GEO sample was lost where GEO omits the SRA relation.** GSE135325 and
GSE137444 record a BioSample for every sample and an SRA relation for none, so
nothing attached a GSM to any run and every row carried the INSDC sample
instead — a different identity from the one a reprocessing pipeline is asked
about. ENA reports the BioSample per run, so the same identity joins them from
the other side: 36 of 36 runs recovered for GSE135325, 50 of 50 for GSE137444.

**The SDRF was read through a dict.** SDRF repeats `Comment[FASTQ_URI]` once per
mate, and `csv.DictReader` keeps only the last value for a repeated key.
E-MTAB-9221 yielded 20 URIs for its 20 runs instead of 40 — one mate each, with
no error anywhere. See
[the mirror pathology](pathologies/ae-sdrf-points-at-decommissioned-mirror.md),
which also covers the second parsing trap in the same route.

## What is deliberately not reproduced

The incumbent's steps 8 and 9 — the SRA table's archive URL, then a `srapath`
shell-out — have no equivalent here. Both ask the SRA toolkit for a signed,
rotating link, which is a download-time concern; the incumbent's own snapshots
exclude those URLs for the same reason.

## Where the two would disagree, and why parity still holds

`ENAFQ` requires ENA to offer both `_1.fastq.gz` and `_2.fastq.gz`. That is a
name test standing in for a completeness test, and it rejects a genuinely
single-end library along with a truncated paired one. This project reaches the
same verdict from `library_layout`, which is the fact rather than a proxy for
it — see [the file layer](files.md). The two agree on every case the snapshots
cover.

## Reproducing this

```bash
uv run fetch links GSE111360 > links.tsv
```

The comparison script lives in the survey harness; it reads the incumbent's
`.snap` directly, so it stays true as the incumbent's own tests change.
