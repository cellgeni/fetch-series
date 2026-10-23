---
title: SRA's ESummary keeps every accession inside XML strings in its JSON
---

# SRA's ESummary keeps every accession inside XML strings in its JSON

**Archive:** NCBI E-utilities
**Affected route:** [`gse->experiment:elink_gds_sra`](../routes/gse-to-experiment/index.md)
**Status:** deterministic; worked around by reading the fragments by pattern
**Measured:** reproduced 2026-10-23

## Symptom

`esummary db=sra retmode=json` returns a JSON document per SRA record that holds no accession
as a field. Its keys are `uid`, `createdate`, `updatedate`, `extlinks`, `expxml` and `runs`.
The experiment, study, sample, BioProject, BioSample and run accessions are all inside
`expxml` and `runs`, which are XML fragments stored as JSON strings. `runs` is not even a
well-formed document: it has no single root.

A client that reads JSON fields finds no accessions at all.

## Reproducer

```bash
curl -s 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=sra&id=26844786&retmode=json' \
  | python3 -c 'import json,sys; r=json.load(sys.stdin)["result"]["26844786"]; print(sorted(r)); print(r["runs"].strip())'
# ['createdate', 'expxml', 'extlinks', 'runs', 'uid', 'updatedate']
# <Run acc="SRR23693882" total_spots="403887530" ... />
```

Checked on 2026-10-23.

## What it is not

- **Not missing data.** The accessions are all there, inside the strings.
- **Not a parse error.** The JSON is valid. The fragments are not meant to be parsed as
  documents.

## Workaround

`eutils.expxml_accessions` reads `<Experiment acc=…>`, `<Study acc=…>`, `<Sample acc=…>` and
`<Run acc=…>`, and `<Bioproject>` and `<Biosample>` text, by pattern from both strings. It
returns them by element, which is how `gse->experiment:elink_gds_sra` reaches experiments.

## Affected accessions

Every SRA record. It is the shape of the endpoint, not a defect of particular records.

## Regression test

`tests/pathologies/test_esummary_sra_buries_accessions_in_expxml.py` asserts that a live SRA
ESummary document still carries its run accession only inside `runs`, and that
`expxml_accessions` still finds it there.
