---
title: ENA's fastq of an NCBI run carries SRA Lite's substituted qualities
---

# ENA's fastq of an NCBI run carries SRA Lite's substituted qualities

**Archive:** EBI ENA
**Affected route:** [`run->file:ena_fastq`](../routes/run-to-file/index.md)
**Status:** detected and measured; reported nowhere yet
**Measured:** 2026-10-22 and 23, by the [content survey](../benchmarks/content-survey-2026-10.md)

## The number

**122 of 452 NCBI-submitted runs (27%)**, and 22 of 278 DDBJ-submitted runs. That is the
share whose ENA fastq has every quality replaced by one value, `?`, throughout. None of 147
EBI-submitted runs' is.

That is what an SRA **Lite** object reads as. NCBI distributes each run as a Normalized object,
with the qualities the sequencer wrote, and as a Lite object, with every quality set to one
value to save space. For these runs ENA's fastq was derived from the Lite object. **The
Normalized object of the same run, with real qualities, is on NCBI's cloud mirror.**

## Reproducer

```bash
# ENA's fastq: every quality is '?'
curl -s -r 0-20000 https://ftp.sra.ebi.ac.uk/vol1/fastq/SRR114/050/SRR11442250/SRR11442250_1.fastq.gz \
  | gzip -dc 2>/dev/null | head -4
# @SRR11442250.1 NB501551:207:HLCW2BGX9:1:11101:4265:1048/2
# TGAGANGAGTGAACGCCGATAGGACA
# +
# ??????????????????????????

# NCBI's Normalized object of the same run: real qualities
fastq-dump -X 1 --split-files https://sra-pub-run-odp.s3.amazonaws.com/sra/SRR11442250/SRR11442250
head -4 SRR11442250_1.fastq     # a quality string of several values
```

Checked on 2026-10-23.

## What it is not

- **Not wrong reads.** The bases are the same in both copies, so a 10x set is just as fit for
  mapping, and the fit verdicts of the two agree.
- **Not NCBI's doing.** NCBI serves both objects. Which one a derived fastq is made from is
  the deriver's choice.
- **Not the barcode-read defect.** [ENA's single fastq](ena-paired-library-single-fastq.md)
  loses a read. This loses the qualities of every read, and occurs on runs with both reads.

## Why it matters

Anything quality-aware downstream gets nothing to work with:

- STAR's `--soloCBmatchWLtype 1MM_multi_Nbase_pseudocounts`, which the reprocessing pipeline
  uses, weighs barcode corrections by quality;
- quality trimming;
- Q30 reports.

`?` is Phred 30, so a run mapped from such a fastq reports every base at Q30: a number that
says nothing about the run.

## Detection and workaround

`fetch inspect` records each file's quality as `real`, `substituted` or `absent`
(`inspect/checks.py: quality_verdict`). A set whose qualities are substituted is still fit,
because the reads are the same. Where quality matters, use the Normalized object, which
`fetch files` offers beside ENA's fastq.

## Affected accessions

```bash
uv run python scripts/inspect_survey_report.py 'data/surveys/sample_500@sample_3000@reprocessed-srr'
# per-run quality is in each export's detail: checks.quality == ["substituted"]
```

## Regression test

`tests/pathologies/test_ena_fastq_is_lite_quality.py` reads the first records of SRR11442250's
ENA fastq and asserts their qualities are still one value.
