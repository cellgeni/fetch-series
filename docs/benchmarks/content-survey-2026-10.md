---
title: Content survey, 2026-10
---

# Content survey, 2026-10

**Instrument:** classifier 0.4.0, fit rule `fit-5`, the first 100,000 reads of each file.
**Run:** read 2026-10-22 and 23 (UTC) under `fit-3`, then judged again under `fit-5` from what
each verdict recorded, without reading anything twice. On 2026-10-23 every call was decided
again under classifier 0.4.0 from its recorded evidence (`fetch inspect survey --recall`), and
judged again; nothing was read. What that changed is [below](#what-classifier-040-changed). The first
run, 2026-10-20 under `fit-2`, is in git history. **Data:** the `screen_100000_*` exports in
`data/surveys/sample_500@sample_3000@reprocessed-srr/`,
`data/surveys/sample_300@node_run_ddbj@reprocessed-srr/`,
`data/surveys/sample_300@node_run_ebi@reprocessed-srr/` and
`data/surveys/sample_300@node_run_ncbi@reprocessed-srr/`. They hold counts, lengths, calls and
verdicts, and no sequence. **Tables:** `scripts/inspect_survey_report.py <export dir>
--anchor data/surveys/sample_3000@reprocessed-srr/run-to-file_ena_fastq.csv.gz`.

A link survey asks whether a link *answers*. This survey reads the files every run-to-reads
link lists for a run, and asks what they *hold*: which reads are there, which 10x kit made
them, and whether the [fit rule](../decisions/what-a-read-sample-can-prove.md) finds them
usable for a 10x gene-expression run. It is never evidence for a route and ranks nothing.

## How it was measured

- **Corpora.** The main draw is `sample:500@sample:3000@reprocessed-srr`: 500 runs of the
  3,000 the [ENA fastq census](../pathologies/ena-paired-library-single-fastq.md) measured,
  so the two compare run by run. It holds 457 NCBI runs, 41 EBI runs and 2 DDBJ runs, so two
  top-ups of 300 runs each, `node:run_ddbj` and `node:run_ebi` of the same corpus, cover the
  links from DDBJ and EBI runs. A third, `node:run_ncbi`, reads the 300 NCBI runs the link
  survey drew, so the [link splits](../graph/link-survey-results.md) carry a fit count for
  every run node. Its ENA fastq is unfit for 126 of 299, as the main draw's is.
- **Units.** One unit per link from a run node into a read format, `screen:100000:<link>`:
  19 or 20 per run node, measured by a route or not. A link that lists nothing for a run is
  EMPTY. A listing that fails is FAILED. A listing whose files cannot be read is RESOLVED,
  with the reason in its checks.
- **Reads.** The first 100,000 reads of each file. A FASTQ, BAM or CRAM is read from a
  range-fetched prefix. An SRA object is read by `fastq-dump -X` from its own URL, so a
  result belongs to the Normalized or Lite object it came from. Each distinct file is read
  once per run and reported against every link that lists it.
- **The gate first.** The seven runs `inspect-gate` held then were read before the corpora,
  and every expectation held under `fit-3`. Two runs added since for other technologies
  (SRR24749105, DRR346905) were read with classifier 0.3.1. The gate is never evidence.
- **Every verdict was read again under `fit-3`**, because `fit-2` had not recorded what it
  judged on. Each now records each file's format and modal length, the call, the run's mean
  spot, which listed files could not be read, and the spot-name agreement of every pair of
  files. `fetch inspect survey --rejudge` then judged all 11,436 again under `fit-5` with no
  network, and found none that lacked an input.
- **Local failures are not verdicts.** ENA's portal answered 500 to everything for ten
  minutes before the re-read began. The re-read waited for it rather than recording failures.

## The anchor: ENA's single fastq

The census found one ENA fastq listed for a paired library in 39.2% of these 3,000 runs.
The reads of ENA's fastq, for the runs both cover:

| ENA's listing in the census | Fit | Unfit | Undetermined |
|---|---:|---:|---:|
| one fastq | 0 | **183** | 0 |
| two or more | 244 | 1 | 46 |

**All 183 single-fastq runs are proven unusable by their reads**: 176 because no read starts
with a listed barcode, and 7 because no read is long enough to be the cDNA. One run with two
fastqs is unfit too: SRR14522447 (below). The 46 undetermined are:

- libraries that are not gene expression (36: feature barcode, VDJ, CRISPR);
- calls the classifier left ambiguous (7);
- SRR26701885, whose only long read is a 107-base barcode read;
- SRR11040464 and SRR34482399, sets with a listed fastq that cannot be read (below).

SRR34778689, unfit under `fit-2` for its 47-base cDNA read, is fit under `fit-3` and since.

The defect belongs to NCBI's runs. ENA's fastq is unfit for 183 of the 452 NCBI-submitted
runs that list it. For EBI-submitted runs the figure is 2 of 147, and for DDBJ-submitted runs
4 of 278. Those six are deposits that hold no cDNA read in any copy.

## What each source holds

NCBI-submitted runs, from the main draw (457 runs):

| Source | Runs listed | Fit | Unfit | Undetermined |
|---|---:|---:|---:|---:|
| ENA fastq (`ena_filereport_fastq`) | 452 | 226 | 183 | 43 |
| Normalized object (`sdl`) | 457 | 383 | 3 | 71 |
| Lite object (`sdl_lite`) | 450 | 376 | 4 | 70 |
| DDBJ's mirror (`ddbj_entry_sra`) | 452 | 171 | 3 | 278 |
| Submitted originals (`sdl_original`) | 453 | 3 | 0 | 450 |

EBI-submitted runs, from the top-up (300 runs):

| Source | Runs listed | Fit | Unfit | Undetermined |
|---|---:|---:|---:|---:|
| ENA submitted fastq | 233 | 178 | 3 | 52 |
| ENA submitted CRAM | 61 | 50 | 0 | 11 |
| ENA BAM | 79 | 53 | 4 | 22 |
| ENA fastq | 147 | 109 | 2 | 36 |
| Normalized object (`sdl`) | 300 | 234 | 2 | 64 |
| DDBJ's mirror (`ddbj_entry_sra`) | 299 | 98 | 1 | 200 |

DDBJ-submitted runs, from the top-up (300 runs): DDBJ's fastq, ENA's fastq, and the
Normalized and Lite objects agree on every run where both were read. 265 runs are fit
everywhere, 4 are unfit everywhere, and the other 31 are undetermined everywhere. ENA lists no
fastq for 22 of the 300.

The full per-link tables, Lite and Normalized separately and every mechanism, are what the
script prints.

## Findings

**NCBI's Normalized object rescues most of what ENA's fastq loses.** Run by run over the main
draw:

| ENA fastq ↓ / Normalized object → | Fit | Unfit | Undetermined |
|---|---:|---:|---:|
| fit | 244 | 0 | 0 |
| unfit | **152** | 4 | 28 |
| undetermined | 2 | 0 | 44 |

For 152 runs ENA's fastq is unfit and the Normalized object of the same run is fit. Of the
28 where the object is undetermined, 8 are calls the classifier left ambiguous and 18 are
feature-barcode or VDJ libraries, undetermined by rule. The other two are SRR12103517 and
SRR9105023, below. Where both were called, ENA and NCBI name the same kit on all 277 runs.

**ENA's fastq is often Lite.** The qualities of ENA's fastq are substituted, two values or
fewer throughout, for 122 of the 452 NCBI runs and 22 of the 278 DDBJ runs. That is what an
SRA Lite object reads as. None of the 147 EBI runs' ENA fastqs is. The reads are the same,
so fit does not change, but a quality-aware step downstream gets nothing to work with.

**ENA's split can put the barcode in the wrong read.** SRR14522447 lists two ENA fastqs, of 28
and 99 bases. The Normalized object holds the same 127 bases per spot as 8, 28 and 91. ENA's
split puts the index read and the first 20 barcode bases into one read, so no read starts
with a barcode. The Lite object carries the same split. A listing check passes this run, and
only the reads catch it.

**The Lite object answers as the Normalized one does, without real qualities.** On every run
read, the Lite object's quality is substituted. Its fit verdict matches the Normalized
object's on 492 of the 493 runs both were read for. The one that differs is SRR14522447,
whose Lite object carries ENA's split.

**DDBJ's mirror of NCBI and EBI objects is mostly missing.** For 248 of the 452 NCBI runs, and
176 of the 299 EBI runs, the object DDBJ Search links does not exist: `fastq-dump` answers
"item not found" and the URL answers 404. That is [the upstream
draft](../upstream/ddbj-search-mirrored-sra-links.md)'s finding, now over a draw rather than
59 runs. Of the 204 NCBI objects that do exist, 20 hold substituted qualities, which makes
them Lite whatever the link calls them. SRR11181954's holds its 26-base barcode read alone.
For DDBJ's own runs the mirror is complete: 300 of 300 read.

**SDL's submitted originals name files it offers no link to.** `sdl_original` lists the
submitter's own fastqs for 453 NCBI runs and 232 EBI runs. It gives no download link for 450
and 204 of them respectively, so those cannot be read here and are undetermined.

**Some deposits hold no cDNA read anywhere.** SRR11300684, SRR17100003, SRR34580087 and
ERR14686479 are 26 or 28 bases a spot, in every copy and in runinfo, as SRR30132199 on the gate
is. They are refused everywhere. SRR34778689's cDNA read is 47 bases: refused under `fit-2`,
whose floor was 50, and fit since `fit-3`, which counts a 47-base read as cDNA.

**ENA lists a file it serves as a directory.** ENA's `filereport` lists SRR11040464's
`_1.fastq.gz` with a size (583,713,371 bytes) and an md5. At that path the FTP server serves an
empty directory listing, behind a 301 to the same path with a trailing slash; the `_2` file
beside it is a real 1.3 GB file. Checked again on 2026-10-23, and measured over 4,466 listed
links on [its own page](../pathologies/ena-serves-a-listed-fastq-wrongly.md). ENA also answered
403 for one of ERR4065012's fastqs and for SRR34482399's `_1` on 2026-10-22. Both answered 200
the next day, so this was throttling, not a missing file, and the client now retries a 403 from
ENA's file server. Judged on the file that could be read, ERR4065012's and SRR11040464's sets
were refused under `fit-3` for lacking the read the missing file most likely holds. `fit-5`
makes such a set undetermined. Nothing of SRR34482399's set was read, so it was undetermined
already.

## What classifier 0.4.0 changed

Every call above was first made by classifier 0.3.x. On 2026-10-23 each was decided again
under [0.4.0](inspect-vs-starsolo-2026-10.md#classifier-040-thresholds-set-on-the-design-half)
from the evidence it recorded, and judged again under `fit-5`. Nothing was read. Of the
11,060 verdicts across the four corpora that recorded a call:

| Change | Verdicts | Runs |
|---|---:|---:|
| strand: unstranded → sense | 593 | 93 |
| strand: unstranded → antisense | 308 | 45 |
| level: ambiguous → HIGH | 270 | 42 |
| level: ambiguous → MEDIUM | 107 | 19 |
| level: ambiguous → LOW | 6 | 1 |
| layout: none → 5′ v1/v2 | 247 | 37 |
| layout: none → 3′ v2 | 136 | 19 |
| fit: undetermined → fit | 351 | 51 |
| level: HIGH → MEDIUM | 13 | 2 |

- **No call changed from one kit to another, and no verdict became unfit.** Every change
  settles something 0.3.x left open, except the last row.
- **The last row is a contradiction made visible.** ERR15405674 (3′ v2) and SRR33006063 (3′
  v3) now read antisense on the housekeeping genes, with poly-A undecided. Under 0.3.x the
  undecided poly-A vote left them unstranded. A 3′ layout read antisense is a contradiction, so
  each call drops to MEDIUM and says why. Neither has been set beside the pipeline's own
  mapping, so which signal is right is not known.
- **The TSO rule for library type changed nothing here.** These verdicts were recorded before
  0.4.0 measured the TSO share, so the rule has nothing to decide on. A run read again would
  record it.

The tables on this page are the 0.4.0 ones. The 0.3.x ones are in git history.

## What the rules got wrong, and changed

`fit-3`, `fit-4` and `fit-5` are recorded in `inspect/fit.py`. `fit-3` lowered the cDNA floor
to 30 bases and stopped the barcode read counting as its own cDNA read, which had let `fit-2`
pass 43 verdicts on a lone barcode read of 100 to 155 bases. `fit-4` makes a copy read as another technology
undetermined. `fit-5` is the directory above. The rest of this section is the first run's.

### What `fit-1` got wrong, and `fit-2` changed

The survey was first run under `fit-1`. Reading the runs it refused or passed turned up three
rules that did not hold. They became `fit-2`, and are kept in every rule since:

- **DRR878211**: its SRA object holds 26 + 8-base barcode spots first and the 98-base cDNA
  reads in spots of their own further in. Runinfo's mean is 66 bases a spot against the
  sample's 34. `fit-1` refused it for lacking a cDNA read that it holds. Two other runs of
  its study and SRR12103517 are the same shape. An SRA object is now refused only if its
  first spots account for the run's mean spot length.
- **SRR9105023** is 10x 3' v1, 8 + 98 + 14 + 10. Its barcode is the 14-base read, and no v1
  list is registered. `fit-1` refused it for having no listed barcode. A read of 11 to 25
  bases now blocks that refusal.
- **DDBJ's fastqs of DRR878211** pair the barcode read with the index read and put the 98-base
  reads in a third file of unpaired spots. `fit-1` called the set fit. Fit now needs the
  barcode read and the cDNA read to be mates, by spot name.

The first condition costs one true refusal: DDBJ's copy of SRR11181954 really does hold only
its barcode read, and it is now undetermined, because nothing in the prefix tells it apart
from an object whose cDNA reads come later. Undetermined has no effect on a recommendation,
so the cost is a source not refused, never a source wrongly refused.

## What this does not show

- **The first reads, not the file.** A verdict is about the first 100,000 reads of each
  file. The rule refuses an SRA object only when its prefix accounts for the run, but a FASTQ
  or BAM whose reads change partway through would still be judged on its start.
- **10x gene expression only.** Feature-barcode, VDJ and CRISPR libraries are undetermined by
  rule. Their usability depends on pipelines this project does not run.
- **Some reads never finished.** In the first run, the CRAMs of ERR15411117 and ERR15411164
  needed a reference from EBI's registry that did not arrive within 600 s, and four SRA
  objects did not finish `fastq-dump` within 900 s on two attempts. Reads that do not finish
  are undetermined.
- **Six runs DDBJ Search does not hold.** Its entry endpoint answers 404 for SRR28781227,
  SRR31012370, SRR31012443, SRR31012512, SRR31342328 and ERR15850668, so their
  `ddbj_entry_sra` verdict is a failure, as the link survey recorded it.
- **A draw, not a census.** 500 runs of 3,000 and 300 of each other run node, seeded, with the
  corpus name recording which.
