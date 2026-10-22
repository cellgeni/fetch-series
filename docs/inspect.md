---
title: Reading the reads
---

# Reading the reads: `fetch inspect`

**Needs:** a run accession, or local files. **Feeds:** the [file layer](files.md)'s
refusal path, and [the link survey's](graph/link-survey-results.md) read-file rows.
**Rule:** a source is refused only on positive evidence that it cannot be used.

The [assay layer](assays.md) reads what a submitter wrote about a library. This reads the
library itself. It reads the first 100,000 reads of every file each source offers for a
run, and says three things about each source: which reads are there, which 10x kit made
them, and whether they are fit for a 10x gene-expression run.

```bash
uv run fetch inspect lists --fetch            # once: barcode lists and references, sha256-checked
uv run fetch inspect run SRR29534765          # every source of a run
uv run fetch inspect run x_R1_001.fastq.gz x_R2_001.fastq.gz   # local files
uv run fetch files SRR29534765 --inspect --explain             # recommend, refusing what is proven unusable
```

```
SRR29534765  (classifier 0.2.1, fit-3)
  run->file:ena_fastq          fastq  101          -                      none      UNFIT        no read whose first 16 bases are a listed barcode
  run->file:sdl                sra    8+26+101     10x_3p_v2              MEDIUM    fit          10x_3p_v2 at MEDIUM
```

ENA's fastq for this run is one file of 101-base reads, every name ending `/3`. NCBI's
SRA object of the same run holds the index read, the 26-base barcode read and the cDNA
read. The metadata is identical for both, because it describes the library and not the
copy. Only the reads tell the two copies apart.

## What each source is read for

- **Files and reads.** How many reads each file holds, how long they are, the share of `N`,
  and the `/1`, `/2`, `/3` the names end with.
- **Quality:** *real*; *substituted*, where the alphabet has two values or fewer, as an SRA
  Lite object reads `?` throughout; or *absent*.
- **The barcode read:** at least two reads of 26 bases or more, one to hold the barcode and
  one to be the cDNA. A cDNA read is any read of 30 bases or more that is not the barcode
  read.
- **The kit.** Every listed 10x barcode list is tried against every read long enough. Then
  the UMI's entropy, the tail after the UMI, and the strand of the cDNA read narrow it to a
  layout, at a level: HIGH, MEDIUM, LOW or ambiguous. The method and its benchmark are in
  [the read classifier benchmark](benchmarks/inspect-2026-10.md): 0 errors in 66 held-out
  HIGH calls.
- **The library type:** gene expression, VDJ (TCR/BCR constant regions), CRISPR (the sgRNA
  scaffold), or feature barcode (a few distinct tags make up most reads).
- **BAM facts:** the sort order, the 10x tags, and whether the barcode read was rebuilt from
  them. A BAM's barcode read is often only in its tags.

Nothing read is stored. A report holds counts, lengths and calls, and no sequence.

## Fit, and the refusal path

`fit` is a requirement (`inspect/fit.py`, rule `fit-3`), not evidence. It applies what
STARsolo needs to what the reads showed:

- **unfit** needs positive evidence: no read of 30 bases or more besides the barcode read,
  or no read of 26 bases or more whose first 16 bases are a listed barcode above background.
  A barcode read long enough to carry a cDNA read after its barcode and UMI (58 bases or
  more) may be the two reads joined, so a set with no other read is undetermined, not
  unfit. An SRA object is refused
  only if its first spots account for the run's mean spot length from runinfo, and nothing
  is refused for a missing listed barcode while a read of 11 to 25 bases could be a barcode
  no list holds (10x 3' v1's is 14);
- **fit** is a called 10x gene-expression layout at HIGH or MEDIUM, whose barcode read and
  cDNA read are mates (their spot names agree);
- **undetermined** is everything else: a reader unavailable, a fetch failed, a weak call, a
  library that is not gene expression, or a set that fails one of the conditions above.

The [content survey](benchmarks/content-survey-2026-10.md) found the `fit-2` conditions,
one run each, by reading what `fit-1` had judged. `fit-3` lowered the cDNA floor from 50
bases to 30 after SRR34778689 (28 + 47), and stopped the barcode read counting as its own
cDNA read, which had let `fit-2` pass a lone 150-base barcode read (DRR259182). The argument
is in [what a read sample can prove](decisions/what-a-read-sample-can-prove.md).

Every verdict records what it was judged on: each file's format and modal length, the call,
the run's mean spot, and the spot-name agreement of every pair of files from different
sources. `fetch inspect survey --rejudge` judges recorded verdicts again under the current
rule without reading a file, and `--refit` reads again only the runs whose records lack
something the rule needs.

`fetch files --inspect` refuses an offer only when it is unfit. An undetermined offer is
ranked exactly as it would have been without reading it. If every offer is unfit, nothing is
recommended and every offer is shown, with the reason. `links.tsv` never goes through the
recommendation, so its parity with the incumbent is untouched, and a test keeps it that way.

## The report

`fetch inspect run --json PATH` writes a versioned report. `schema_version` changes when a
key is removed or changes meaning. The classifier (`classifier`) and the fit rule
(`fit_rule`) carry their own versions, so every call says which instrument made it.

| Key | What it holds |
|---|---|
| `run`, `reads` | the run, and the reads read per file |
| `sources[]` | one per offer: `source`, `kind`, `files`, `checks`, `call`, `fit`, `reason` |
| `sources[].checks` | files, read counts and lengths, quality, `N` share, roles, BAM facts, duplicates, unreadable files and why |
| `sources[].call` | `layout`, `candidates`, `level`, `barcode_list`, `evidence`, `contradictions`, `hint`, `library_type`, `version` |
| `sources[].geometry` | each read's role (R1/R2/I1/I2), lengths (mode, range, share at the mode) and FASTQ names; where the barcode and UMI sit, 1-based; `trimmed` where lengths vary |
| `sources[].mapping` | for a HIGH or MEDIUM 10x gene-expression call only: the STARsolo options, Cell Ranger's `--chemistry`, the whitelist, what was omitted and why; otherwise `null` |
| `sources[].convertible` | for a BAM, CRAM or SRA copy: whether it converts to FASTQ, how (`bamtofastq`, `tags`, `samtools fastq`, `fastq-dump --split-files`) and why |
| `agreement` | whether the sources that could be called agree, and which could not be |

Schema 2 added `geometry`, `mapping` and `convertible`. Nothing in schema 1 was removed or
changed meaning.

## Mapping a copy

```bash
uv run fetch inspect run SRR25056225 --params      # the STARsolo and Cell Ranger lines, per source
uv run fetch inspect lists --export tenx_v3 --out whitelists/   # a whitelist STARsolo reads
uv run fetch files SRR25056225 --inspect --json report.json     # the same blocks, beside the links
```

```
  run->file:sdl (sra):
    STARsolo     --readFilesIn SRR25056225_2.fastq SRR25056225_1.fastq --soloType CB_UMI_Simple
                 --soloCBwhitelist .cache/lists/export/737K-august-2016.txt --soloCBstart 1
                 --soloCBlen 16 --soloUMIstart 17 --soloUMIlen 10 --soloBarcodeReadLength 0
                 --soloStrand Reverse
    Cell Ranger  --chemistry SC5P-R2
```

The mapping block is a requirement table, not a measurement. It states what
[STARsolo's documentation](https://github.com/alexdobin/STAR/blob/master/docs/STARsolo.md) and
[10x's](https://www.10xgenomics.com/support/software/cell-ranger/latest/analysis/inputs/cr-specifying-chemistry)
say the called kit needs:

- **Barcode and UMI** come from the kit's row in the layout table. A barcode read shorter
  than barcode + UMI gets a shorter `soloUMIlen`, and `umi_short` says so. A barcode read
  that is not exactly barcode + UMI, or whose lengths vary, gets `--soloBarcodeReadLength 0`.
- **`soloStrand`** comes from the strand the cDNA read was *measured* on: sense is `Forward`,
  antisense `Reverse`. An unstranded or unmeasured read leaves it out, with the reason under
  `omitted`, rather than taking it from the kit's name.
- **`readFilesIn`** is the cDNA read, then the barcode read. A FASTQ copy uses the files as
  listed; an SRA object uses the names `fastq-dump --split-files` gives; a BAM or CRAM uses
  Cell Ranger's convention (`<run>_S1_L001_R1_001.fastq.gz`), which `rename10xrun` also
  writes.
- **The whitelist** is exported from the checked cache as sorted text. Its sha256 is in the
  register (`export_sha256`), and an export that does not match it is refused.

**The block is unvalidated.** It has not yet been compared with what the reprocessing
pipeline's own STARsolo runs used and achieved, and every block says so (`validated: false`).
The pipeline settles strand by mapping 200,000 reads both ways. Until that comparison is
published, check the first run's `Summary.csv`.

`convertible` follows the fit rule's standard and needs positive evidence. A BAM converts with
`bamtofastq` when its `10x_bam_to_fastq` header says how, and with its tags when the raw
barcode (`CR`/`CY`) and a UMI (`UR`/`UY` or `RX`/`QX`) are on its records. It splits by flag
when the barcode read and the cDNA read are mates (ERR13855116's lane CRAM). It is `false`
only when no sampled record carries a barcode tag and no read is a barcode read. An SRA object
converts with `fastq-dump --split-files` when the split reads hold the called barcode and cDNA
reads and its first spots account for the run. A Lite object is flagged: its qualities are
substituted.

## Readers, and what is optional

FASTQ (plain, gzip, bzip2) and BAM are read with the standard library. A CRAM needs pysam
(`fetch-series[inspect]`). An SRA object needs sra-tools' `fastq-dump`, which comes with the
inspect image (`docker/Dockerfile.inspect`). Without them a CRAM or an SRA object is
*undetermined*, never unfit. An SRA object is read by its own URL, so a result belongs to
the Normalized or the Lite object it came from.

## Results

The [2026-10 content survey](benchmarks/content-survey-2026-10.md) read every run-to-reads
link's files for 500 runs of the reprocessed corpus, and 300 runs from each run node:

- All 182 runs whose ENA listing is one fastq are **unfit** by their reads. For 141 runs with
  an unfit ENA fastq, NCBI's Normalized object of the same run is fit, and
  `fetch files --inspect` would choose it.
- ENA's fastq is unfit for 183 of 451 NCBI-submitted runs, and for 2 of 146 EBI-submitted
  runs. The defect is in how ENA derives fastq from NCBI's objects.
- Where ENA's fastq and the Normalized object were both called, they name the same kit on all
  267 runs. The Lite object agrees with the Normalized one on 492 of 493.
- Reading the reads changed the rule: `fit-1`'s verdicts on DRR878211, SRR9105023 and DDBJ's
  fastqs of DRR878211 were wrong, and `fit-2` is the result.
