---
title: EBI — ArrayExpress SDRF files point at a decommissioned mirror
---

# EBI — ArrayExpress SDRF files point at a decommissioned mirror

**Status:** draft, not filed **Archive:** EBI ArrayExpress / BioStudies
**Evidence:** [pathologies/ae-sdrf-points-at-decommissioned-mirror.md](../pathologies/ae-sdrf-points-at-decommissioned-mirror.md)
**Where to file:** `biostudies@ebi.ac.uk`

```bash
for AE in E-MTAB-8060 E-MTAB-9221; do
  printf '%s: ' "$AE"
  curl -sL "https://www.ebi.ac.uk/biostudies/api/v1/studies/$AE/files?limit=500" \
    | python3 -c 'import json,sys; i=json.load(sys.stdin)["items"]; print(len(i),"files,",sum(1 for f in i if ".fastq" in f.get("path","")),"fastq")'
done
```

---

## Draft report

Subject: `Comment[FASTQ_URI]` in SDRF files points under `/pub/databases/microarray/`, which is no longer maintained

Many ArrayExpress SDRF files carry `Comment[FASTQ_URI]` values under

```
ftp://ftp.ebi.ac.uk/pub/databases/{microarray,arrayexpress}/data/experiment/...
```

the pre-BioStudies mirror. The same spelling means two different things and the SDRF gives
no way to tell them apart:

- the file still exists under BioStudies and the URI is a stale spelling of a live study
  file; or
- the file does not exist anywhere, and the study's actual data is elsewhere.

**E-MTAB-8060 and E-MTAB-9221 are indistinguishable at the SDRF level.** Both point every
`Comment[FASTQ_URI]` at the mirror; both also declare a `Comment[BAM_URI]`. Yet:

| | Files the study registers | of which fastq |
|---|---:|---:|
| E-MTAB-9221 | 43 | 40 |
| E-MTAB-8060 | 2 (the IDF and the SDRF) | 0 |

E-MTAB-8060's reads exist only on the unmaintained mirror; its real submission is the ENA
BAM. A client taking the SDRF at face value discards that BAM for all 15 runs, with
nothing anywhere recording that it did so.

### What would resolve it

Rewriting the URIs to their BioStudies paths where the file is still registered would fix
the first category outright. For the second, a study whose SDRF references files it does
not register is a data-integrity signal worth surfacing at submission time.

We currently work around it by checking each mirror URI against the study's registered
file list and dropping those that do not appear, which is only possible because the file
listing exists. It is not something every consumer of an SDRF will think to do.
