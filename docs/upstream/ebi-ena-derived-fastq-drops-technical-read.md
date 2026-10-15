---
title: EBI — the derived fastq omits the read carrying the cell barcode
---

# EBI — the derived fastq omits the read carrying the cell barcode

**Status:** draft, not filed **Archive:** EBI ENA
**Evidence:** [pathologies/ena-paired-library-single-fastq.md](../pathologies/ena-paired-library-single-fastq.md)
**Where to file:** the ENA support form at `https://www.ebi.ac.uk/ena/browser/support`

Regenerate the affected list before filing — archive contents drift, and a report
citing stale accessions invites the answer that the problem cannot be reproduced:

```bash
fetch survey run --route "run->file:ena_fastq" --corpus "sample:3000@reprocessed-srr"
```

Runs with exactly one entry in `fastq_ftp` are the candidates; the affected ones
are those whose `library_layout` is `PAIRED`.

---

## Draft report

Subject: `fastq_ftp` serves one file for libraries reported as PAIRED, omitting the technical read

We resolve public accessions to download links for a single-cell reprocessing pipeline.
For a large fraction of 10x runs, the ENA portal's `filereport` reports
`library_layout=PAIRED` and lists a **single** file in `fastq_ftp`, and the SRA archive
holds more bases for the same run than ENA publishes.

We measured this over a seeded random sample of **3,000 runs** drawn from the 262,690 runs
our pipeline has reprocessed, on 2026-09-15.

| | |
|---|---|
| Runs where `fastq_ftp` holds exactly one file | 1,225 of 3,000 |
| …of which `library_layout` is `PAIRED` | **1,201** |
| …of which SRA holds more bases than ENA publishes | **1,177 (98.0%)** |
| Bases SRA holds across those runs | 19.1 Tb |
| Bases ENA does not publish | **5.93 Tb (31.1%)** |
| Median ratio, SRA bases ÷ ENA bases | **1.40** |

`avgLength` in SRA is 120–139 bp for 83% of them, against a published read of roughly
91–98 bp. The difference is a ~28 bp read — for 10x, the read carrying the cell barcode
and UMI.

### A worked example

```bash
curl -s 'https://www.ebi.ac.uk/ena/portal/api/filereport?accession=SRR29534765&result=read_run&fields=library_layout,fastq_ftp,read_count,base_count&format=tsv'
# PAIRED   ...SRR29534765.fastq.gz   89275969   9016872869

curl -s 'https://trace.ncbi.nlm.nih.gov/Traces/sra-db-be/sra-db-be.cgi?rettype=runinfo&term=SRR29534765'
# spots 89275969   bases 12052255815   avgLength 135
```

Identical spot counts; 135 bases per spot in SRA against 101 published. The published
reads are tagged `/3` in their FASTQ headers — the third read of the spot:

```bash
curl -s -r 0-200000 'https://ftp.sra.ebi.ac.uk/vol1/fastq/SRR295/065/SRR29534765/SRR29534765.fastq.gz' | gzip -dc | head -1
# @SRR29534765.1 K00337:202:HWNYLBBXX:6:1101:2747:1297/3
```

### Why this is costly to consumers

The download succeeds. The `fastq_md5` ENA publishes matches. The file is valid FASTQ.
A single-cell pipeline then completes and produces a count matrix in which no read can be
assigned to a cell, because the barcode was never fetched. The failure presents as a
bad experiment rather than a bad download, and the only warning available is that
`library_layout` says `PAIRED` beside a one-file list — two fields nothing invites a
client to compare.

There is usually no alternative within ENA: only **8.2%** of runs in the same sample have
any file in `submitted_ftp`, and `sra_ftp` is empty for **all 3,000** (see the separate
note below). For the rest, the SRA object via NCBI's SDL is the only complete source.

### We are not reporting broken submissions as ENA's problem

Of the 1,201 runs, **11** hold the same number of bases in both archives. Those are
genuine submitter errors — a library declared `PAIRED` where only one read was ever
deposited. `SRR30132199` is one: 52.8 M spots of 28 bp in both ENA and SRA, unrecoverable.
We have separated those out deliberately, because only the submitter can fix them and
conflating the two categories would make this report unactionable.

### What would resolve it

Any one of:

1. Publish the technical read alongside the biological one.
2. Report `library_layout=SINGLE` for what is actually being served.
3. Add a field indicating that the derived fastq is a partial representation of the run.

Excluding technical reads is a defensible default for bulk RNA-seq. For single-cell it
removes the only thing that makes the reads single-cell, and the metadata currently gives
a client no way to detect that it has happened.

### A smaller, related observation

`sra_ftp`, `sra_md5`, `sra_bytes` and `sra_aspera` are documented returnable fields of
`result=read_run`. They were empty for all 3,000 runs in our sample, and for
`SRR25056225`, `ERR2861957`, `DRR188691` and `SRR6639101` checked individually across all
three INSDC archives. If those fields are no longer populated, removing them from
`returnFields` would save clients from implementing a route that cannot work.
