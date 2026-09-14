---
title: ELink gds→sra returns no links for series that demonstrably have SRA data
---

# ELink gds→sra returns no links for series that demonstrably have SRA data

**Status:** open, unreported
**Found:** 2026-09-14 **Archive:** NCBI (Entrez) **Severity:** silent data loss

> Measured on `geo-sample-1000`, a seeded stratified draw from all 275,238 GEO series.
> Of 150 series surveyed, 24 carry SRA experiments according to their own SOFT family file.
> **ELink `gds`→`sra` returns zero links for 6 of those 24 (25%).**

## Symptom

`elink.fcgi?dbfrom=gds&db=sra` returns a `linksets` array with no `linksetdbs` for a GEO
series whose SOFT family file explicitly names SRA experiments. There is no error, no
warning, and no HTTP failure — the response is a well-formed 200 that says the series has
no SRA data.

Anything resolving GEO → SRA through ELink therefore reports these series as having no
sequencing data at all, rather than failing in a way anyone would notice.

## Affected accessions

From the 2026-09-14 survey. Each series' SOFT file names the experiment in the third column,
and that experiment exists in NCBI's own `db=sra`.

| GEO series | Experiments in SOFT | Example experiment | In `db=sra`? | ENA runs |
|---|---|---|---|---|
| GSE27679 | 9  | SRX046652 | yes (count=1) | 7 |
| GSE29158 | 20 | SRX014928 | yes (count=1) | 1 |
| GSE34359 | 2  | SRX111797 | yes (count=1) | 2 |
| GSE35884 | 4  | SRX117991 | yes (count=1) | 1 |
| GSE49732 | 4  | SRX466511 | yes (count=1) | 1 |
| GSE49780 | 6  | SRX467063 | yes (count=1) | 1 |

## Reproducer

```bash
# 1. The series resolves to a gds UID.
curl -s "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=gds&term=GSE27679%5BACCN%5D+AND+GSE%5BETYP%5D&retmode=json" \
  | jq -r '.esearchresult.idlist[0]'
# -> 200027679

# 2. ELink reports no SRA neighbours. Expected: 9 UIDs. Actual: none.
curl -s "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=gds&db=sra&id=200027679&retmode=json&cmd=neighbor" \
  | jq '.linksets[0].linksetdbs'
# -> null

# 3. But the SOFT family file names the experiments.
curl -s "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE27nnn/GSE27679/soft/GSE27679_family.soft.gz" \
  | gunzip | grep -oE 'SRX[0-9]+' | sort -u | head -3
# -> SRX046652, SRX046653, SRX046654

# 4. And that experiment is in SRA, with runs available from ENA.
curl -s "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=sra&term=SRX046652%5BACCN%5D&retmode=json" \
  | jq -r '.esearchresult.count'
# -> 1
curl -s "https://www.ebi.ac.uk/ena/portal/api/filereport?accession=SRX046652&result=read_run&fields=run_accession&format=tsv&limit=0"
# -> SRR121517 ... SRR121523
```

Passing an explicit `linkname=gds_sra` makes no difference, and searching `db=sra` for
`GSE27679[All Fields]` also returns 0 — so there is no alternative Entrez query that
recovers the link either.

## What it is not

**Not an age effect.** The first hypothesis was that the link table was never backfilled for
older submissions. It does not hold: ELink succeeds on GSE21739, which is older than all six
failures, and on GSE56066, which is newer than all of them. The accession ranges overlap
almost completely (failures 27,679–49,780; successes 21,739–56,066).

**Not a client bug.** The links are absent with and without `linkname`, under `cmd=neighbor`
and `cmd=neighbor_history`.

**Not private or withdrawn data.** Every experiment resolves in `db=sra` and every one has
downloadable runs in ENA.

## Workaround

Read the SOFT family file. `!Sample_relation = SRA:` carries the experiment accessions for
all six series. This is what `fetch10xmeta` already does, which is why the problem has not
bitten the reprocessing pipeline.

Note the failure is **not symmetric** — see
[SOFT omits the SRA relation](geo-omits-sample-sra-relation.md). SOFT and ELink fail on
disjoint populations, so neither is a safe sole source. The resolver takes the union and
records which route produced each accession.

## Upstream report

Not yet filed. Draft body in [upstream/ncbi-elink-gds-sra.md](../upstream/ncbi-elink-gds-sra.md).

Regression test: `tests/pathologies/test_elink_gds_sra_missing_links.py`. It asserts the
links are **still missing**. If it fails, NCBI has populated them — update this page and
re-rank the route.
