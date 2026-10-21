---
title: Content survey, 2026-10
---

# Content survey, 2026-10

**Instrument:** classifier 0.2.0, fit rule `fit-2`, the first 100,000 reads of each file.
**Run:** 2026-10-20 (UTC). **Data:** the `screen_100000_*` exports in
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
- **The gate first.** The seven runs of `inspect-gate` were read before each corpus. Each is
  there for what it showed, and every expectation held under `fit-2`.
- **Local failures are not verdicts.** A full disk partway through left 196 SRA reads
  failed with "no space left on device", and a handful of others failed with transient
  fastq-dump errors. Those runs were read again whole (`--reread-where`), and a full disk
  now fails a run rather than recording the file as unreadable.

## The anchor: ENA's single fastq

The census found one ENA fastq listed for a paired library in 39.2% of these 3,000 runs.
The reads of ENA's fastq, for the runs both cover:

| ENA's listing in the census | Fit | Unfit | Undetermined |
|---|---:|---:|---:|
| one fastq | 0 | **182** | 0 |
| two or more | 236 | 2 | 54 |

**All 182 single-fastq runs are proven unusable by their reads**: 175 because no read starts
with a listed barcode, and 7 because no read is long enough to be the cDNA. Two runs with
two fastqs are unfit too: SRR14522447 and SRR34778689 (below). The 54 undetermined are
libraries that are not gene expression (37: feature barcode, VDJ, CRISPR), or calls the
classifier left ambiguous (17).

The defect belongs to NCBI's runs. ENA's fastq is unfit for 183 of the 451 NCBI-submitted
runs that list it. For EBI-submitted runs the figure is 2 of 146, and for DDBJ-submitted runs
4 of 278. Those six are deposits that hold no cDNA read in any copy.

## What each source holds

NCBI-submitted runs, from the main draw (457 runs):

| Source | Runs listed | Fit | Unfit | Undetermined |
|---|---:|---:|---:|---:|
| ENA fastq (`ena_filereport_fastq`) | 451 | 221 | 183 | 47 |
| Normalized object (`sdl`) | 457 | 366 | 4 | 87 |
| Lite object (`sdl_lite`) | 450 | 359 | 5 | 86 |
| DDBJ's mirror (`ddbj_entry_sra`) | 452 | 163 | 3 | 286 |
| Submitted originals (`sdl_original`) | 453 | 3 | 0 | 450 |

EBI-submitted runs, from the top-up (300 runs):

| Source | Runs listed | Fit | Unfit | Undetermined |
|---|---:|---:|---:|---:|
| ENA submitted fastq | 233 | 175 | 3 | 55 |
| ENA submitted CRAM | 61 | 44 | 0 | 17 |
| ENA BAM | 79 | 53 | 4 | 22 |
| ENA fastq | 146 | 103 | 2 | 41 |
| Normalized object (`sdl`) | 300 | 226 | 2 | 72 |
| DDBJ's mirror (`ddbj_entry_sra`) | 299 | 95 | 1 | 203 |

DDBJ-submitted runs, from the top-up (300 runs): DDBJ's fastq, ENA's fastq, and the
Normalized and Lite objects agree on every run where both were read. 254 runs are fit
everywhere, 4 are unfit everywhere, and the rest are undetermined everywhere. ENA lists no
fastq for 22 of the 300.

The full per-link tables, Lite and Normalized separately and every mechanism, are what the
script prints.

## Findings

**NCBI's Normalized object rescues most of what ENA's fastq loses.** Run by run over the main
draw:

| ENA fastq ↓ / Normalized object → | Fit | Unfit | Undetermined |
|---|---:|---:|---:|
| fit | 236 | 0 | 0 |
| unfit | **141** | 5 | 38 |
| undetermined | 0 | 0 | 54 |

For 141 runs ENA's fastq is unfit and the Normalized object of the same run is fit. Of the
38 where the object is undetermined, 18 are calls the classifier left ambiguous and 18 are
feature-barcode or VDJ libraries, undetermined by rule. The other two are SRR12103517 and
SRR9105023, below. Where both were called, ENA and NCBI name the same kit on all 267 runs.

**ENA's fastq is often Lite.** The qualities of ENA's fastq are substituted, two values or
fewer throughout, for 121 of the 451 NCBI runs and 22 of the 278 DDBJ runs. That is what an
SRA Lite object reads as. None of the 146 EBI runs' ENA fastqs is. The reads are the same,
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
171 of the 299 EBI runs, the object DDBJ Search links does not exist: `fastq-dump` answers
"item not found" and the URL answers 404. That is [the upstream
draft](../upstream/ddbj-search-mirrored-sra-links.md)'s finding, now over a draw rather than
59 runs. Of the 204 NCBI objects that do exist, 20 hold substituted qualities, which makes
them Lite whatever the link calls them. SRR11181954's holds its 26-base barcode read alone.
For DDBJ's own runs the mirror is complete: 300 of 300 read.

**SDL's submitted originals name files it offers no link to.** `sdl_original` lists the
submitter's own fastqs for 453 NCBI runs and 232 EBI runs. It gives no download link for 450
and 204 of them respectively, so those cannot be read here and are undetermined.

**Some deposits hold no cDNA read anywhere.** SRR11300684, SRR17100003, SRR34580087 and
ERR14686479 are 26 or 28 bases a spot, in every copy and in runinfo, as SRR30132199 on the gate is.
SRR34778689's cDNA read is 47 bases, under the rule's 50. Those are refused everywhere,
correctly by the rule. Whether 47 bases is enough for STARsolo is a requirement question this
survey does not answer.

## What `fit-1` got wrong, and `fit-2` changed

The survey was first run under `fit-1`. Reading the runs it refused or passed turned up three
rules that did not hold. They are now `fit-2`, and every verdict above was made under it,
from the reads, with the inputs it judged on (`run_spot`, `mates`) recorded beside it:

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
  file. `fit-2` refuses an SRA object only when its prefix accounts for the run, but a FASTQ
  or BAM whose reads change partway through would still be judged on its start.
- **10x gene expression only.** Feature-barcode, VDJ and CRISPR libraries are undetermined by
  rule. Their usability depends on pipelines this project does not run.
- **Some reads never finished.** The CRAMs of ERR15411117 and ERR15411164 needed a reference
  from EBI's registry that did not arrive within 600 s. ERR5029524, ERR5470677, ERR6110583
  and ERR7459500's SRA objects did not finish `fastq-dump` within 900 s on two attempts.
  All of these are undetermined.
- **Six runs DDBJ Search does not hold.** Its entry endpoint answers 404 for SRR28781227,
  SRR31012370, SRR31012443, SRR31012512, SRR31342328 and ERR15850668, so their
  `ddbj_entry_sra` verdict is a failure, as the link survey recorded it.
- **A draw, not a census.** 500 runs of 3,000 and 300 of each other run node, seeded, with the
  corpus name recording which.
