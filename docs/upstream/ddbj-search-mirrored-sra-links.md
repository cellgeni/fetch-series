---
title: DDBJ — DDBJ Search links mirrored runs' SRA objects that do not exist
---

# DDBJ — DDBJ Search links mirrored runs' SRA objects that do not exist

**Status:** draft, not filed **Archive:** DDBJ Search / DRA mirror
**Checked:** 2026-10-19 — 30 of 59 links answered 404, the same 30 on a second pass an hour later.
**Evidence:** [the link catalogue](../graph/catalogue.md#what-the-probes-found-on-the-way)
**Where to file:** an issue on [`ddbj/ddbj-search-converter`](https://github.com/ddbj/ddbj-search-converter/issues), which builds the link; otherwise the [DDBJ contact form](https://www.ddbj.nig.ac.jp/contact-e.html)

```bash
# The SRA link DDBJ Search publishes for each run, and what it answers.
for RUN in DRR345778 SRR23455953 SRR9721398 SRR25056225 ERR9866271; do
  URL=$(curl -s "https://ddbj.nig.ac.jp/search/entry/sra-run/$RUN.json" \
    | python3 -c 'import json,sys; print(next(d["contentUrl"] for d in json.load(sys.stdin)["distribution"] if d["encodingFormat"]=="SRA"))')
  curl -sI -o /dev/null -w "$RUN %{http_code} %header{content-length}\n" "$URL"
done
# DRR345778 200 3935989781
# SRR23455953 200 19411481
# SRR9721398 200 158888022
# SRR25056225 404
# ERR9866271 404

# Which object a mirrored file is: NCBI's Normalized object, or its Lite one.
for RUN in SRR23455953 SRR9721398; do
  for TYPE in "" "&filetype=sralite"; do
    curl -s -X POST https://locate.ncbi.nlm.nih.gov/sdl/2/retrieve -d "acc=$RUN$TYPE" \
      | python3 -c 'import json,sys; f=json.load(sys.stdin)["result"][0]["files"][0]; print(f["name"], f["size"])'
  done
done
# SRR23455953 19411481        <- the size DDBJ serves: Normalized
# SRR23455953.lite 14407033
# SRR9721398 316224632
# SRR9721398.lite 158888022   <- the size DDBJ serves: Lite
```

## What was measured, and how

Not a survey: this is not `RouteEvidence` and ranks nothing. DDBJ Search backs
no route here ([why](../decisions/no-ddbj-provider.md)). It is enough to
show the behaviour and its rough size:

- **60 runs drawn** with seed `20261019` from `sample:3000@reprocessed-srr`:
  40 of its 2,737 SRRs and 20 of its 246 ERRs. 59 have a DDBJ Search entry
  (`SRR31012443` has none), and each entry names one SRA object.
- **All 17 DRRs** in the same sample, as the control.
- Each link asked once with a HEAD; every link that answered 200 sized against
  NCBI's SDL, asked once plainly and once with `filetype=sralite`.

| Runs | Links | 404 | 200 |
|---|---:|---:|---:|
| SRR, mirrored from NCBI | 39 | **19** | 20 |
| ERR, mirrored from EBI | 20 | **11** | 9 |
| DRR, DDBJ's own | 17 | 0 | 17 |

The 404s were the same 30 runs on both passes:

```
ERR10036864 ERR11746906 ERR12869093 ERR13074034 ERR13936555 ERR14206262
ERR15359097 ERR15361140 ERR15487513 ERR9866271  ERR9871493  SRR17181221
SRR22276953 SRR23262644 SRR23941989 SRR24055117 SRR24362757 SRR24902884
SRR25335865 SRR26041748 SRR27308330 SRR27663522 SRR28196394 SRR30455860
SRR30692428 SRR31709642 SRR32157843 SRR34129625 SRR36149708 SRR36149741
```

Among the SRRs the gap follows the experiment accession. Sixteen of the 17 in
experiments from `SRX19751284` on answered 404, against 3 of the 22 before it.
That is consistent with the NCBI mirror largely stopping in spring 2023, but
nothing here shows why.

Of the 29 that downloaded, 23 had the exact size of NCBI's Normalized object.
Three had the exact size of its Lite object (`SRR9721398`, `SRR21657613`,
`SRR22373097`). Three ERRs matched neither: `ERR5490517` is 15.2 GB at DDBJ
against 9.5 GB at NCBI.

---

## Draft report

Subject: DDBJ Search SRA links for NCBI- and EBI-origin runs: half answer 404, and none can be told apart from one that works

For a run mirrored from NCBI or EBI, the DDBJ Search entry
(`/search/entry/sra-run/{run}.json`) publishes a `distribution` of
`encodingFormat: SRA` under
`dra/sralite/ByExp/litesra/{SRX|ERX}/…/{run}/{run}.sra`.

We understand from `docs/data-architecture.md` in `ddbj-search-converter` that
this is deliberate. The URL is built from a template without checking that the
file exists, and a 404 at download time is accepted, while a DRA-origin run's
link is checked (`dra_sra_file`). We would like to ask you to reconsider, because
of how often it happens and because a client cannot detect it.

**How often.** We drew 60 NCBI and EBI runs from a set of public single-cell
studies. Of the 59 that have an entry, **30 links answer 404**: 19 of 39 SRRs
and 11 of 20 ERRs. The same 30 failed again an hour later. All 17 DRRs we asked
about resolve. Among the SRRs the gap follows the experiment accession: 16 of
17 in experiments from `SRX19751284` on answer 404, against 3 of 22 before it.
`SRR25056225` is one example: its link answers 404, and NCBI serves the run as a
13.3 GB object.

**Why a client cannot tell.** The entry carries no size and no checksum, and a
checked link and an unchecked one look the same. The only way to find out
whether a link works is to request it. A client that builds a download list from
DDBJ Search gets half its mirrored runs failing at download time, after the list
has been accepted.

**The directory name does not describe the file.** The path says `sralite`, and
your documentation says the file is the ordinary `.sra`, "not SRA-Lite". We
compared each of the 29 files that downloaded with NCBI's two objects by exact
size:

- 23 match NCBI's Normalized object;
- 3 match its Lite object (`SRR9721398`, `SRR21657613`, `SRR22373097`);
- 3 ERRs match neither. `ERR5490517` is 15.2 GB at DDBJ against 9.5 GB at NCBI.

Lite replaces base qualities, so the difference matters to anyone realigning,
and at the moment nothing in the entry, the path or the name says which one a
user will get.

Any of these would help, most useful first:

1. **Check mirrored files too**, as for DRA-origin runs, and leave out the SRA
   distribution when the file is absent.
2. **Mark a link that was not checked.** For example, a property on the
   distribution, so a client can tell a checked link from a templated one
   without requesting it.
3. **Publish `contentSize`** (schema.org `DataDownload`) on the SRA distribution,
   and an md5 where you hold one. A size alone would let a client verify a
   download and tell Lite from Normalized.
4. **Say in the English API documentation** what the `sralite` directory holds.

### What we do meanwhile

A DDBJ SRA link is never treated as a valid file. With no checksum it cannot be
verified, so it is never recommended for download; it is listed, with the reason,
as an unconfirmed link. An object found under `sralite/` is classified by its
name, `SRR25056225.sra`, as the Normalized object, and the exceptions above are
recorded beside it.
