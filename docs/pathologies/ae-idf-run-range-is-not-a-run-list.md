---
title: An IDF's run range is not a list of the experiment's runs
---

# An IDF's run range is not a list of the experiment's runs

**Archive:** EBI ArrayExpress / BioStudies
**Affected route:** `ae_experiment->run:biostudies_idf` ([promoted routes from ArrayExpress experiment](../routes/from-ae-experiment.md))
**Status:** measured; declared on the route, with the unreadable ranges reported; reproduced 2026-10-28
**Measured:** 2026-10-28, `sample:3000@arrayexpress-ena`, every IDF read once (2,996 of 3,000: two
404s, two dropped connections) and set against the same studies' SDRF `Comment[ENA_RUN]`, as
`ae_experiment->run:biostudies_sdrf_comments` recorded them on 2026-10-27

## What the field is

`Comment[SequenceDataURI]` is a link into ENA's browser over a comma-separated list of run ranges,
one run written as a range of one. MAGE-TAB does not define it; ArrayExpress writes it.
E-GEOD-29264's is `http://www.ebi.ac.uk/ena/data/view/SRR208051-SRR208059`, nine runs.

A range is the **first and last** of a group of runs, not a list of them. Where the group's
accessions are consecutive the two are the same thing, and for most studies they are. Where they
are not, the text cannot say so.

## The number

Of 2,744 IDFs that carry the field, 2,666 name runs in ranges that read. For the 2,654 of them
whose SDRF route resolved, the expanded ranges against the SDRF's runs:

| | Studies | Runs |
|---|---:|---:|
| exactly the SDRF's runs | **2,625** (98.9%) | |
| runs the SDRF does not list, in the ENA study the IDF names | 11 | 256 |
| runs the SDRF does not list, which ENA does not serve | 8 | 71 |
| runs the SDRF lists and the ranges miss | 11 | 227 |

(One study, E-MTAB-11506, is in both the first and the last row.) The rest of the 2,744 do not
name runs in a form that can be read:

| The field holds | Studies | Example |
|---|---:|---|
| a range that descends: first and last in string order | 9 | E-GEOD-47691: `SRR1204762-SRR886300` |
| a range wider than any study: the first and last of runs far apart | 2 | E-MTAB-4556: `ERR1313817-ERR1432629`, 118,813 accessions for 2 runs |
| a study accession | 66 | E-ERAD-324: `ERP007026` |
| experiments | 1 | E-TABM-722: `ERX000498-ERX000505,…` |

Not counted: IDFs BioStudies served cut off at 4,096 bytes that day, before or inside the field.
E-GEOD-58432's stops at `http://www.ebi.ac.uk/`
([the route page](../routes/from-ae-experiment.md#an-idf-served-cut-off-at-4096-bytes)).

## What a range covers that the experiment does not

- **The ENA study's other runs.** E-MTAB-3716's SDRF lists 21 of GSE30352's runs. Its range,
  `SRR306710-SRR306858`, is 149 runs, every one in SRP007412.
- **Runs ENA no longer serves.** SRR651132 lies inside E-GEOD-43654's `SRR651132-SRR651152`. ENA
  holds a record of it in the same study, SRP018092, but its portal returns no run and NCBI has no
  record of it at all. ERR145640, inside E-MTAB-1155's range, has no record at either.
- **Hundreds of thousands of accessions.** Where a group spans two submissions, its first and last
  are far apart: E-MTAB-4846's `ERR1456918-ERR1830488` spans 373,571 accessions for the 7 runs its
  SDRF lists, 4 at one end and 3 at the other.
- **Nothing, in string order.** When the first and last have different numbers of digits, the range
  is sorted as text and descends: SRR1204762 sorts before SRR886300.

## Reproducer

```bash
curl -sL https://www.ebi.ac.uk/biostudies/files/E-MTAB-4556/E-MTAB-4556.idf.txt | grep SequenceDataURI
# Comment [SequenceDataURI]	http://www.ebi.ac.uk/ena/data/view/ERR1432629-ERR1432629
# Comment [SequenceDataURI]	http://www.ebi.ac.uk/ena/data/view/ERR1313817-ERR1432629
curl -sL https://www.ebi.ac.uk/biostudies/files/E-MTAB-4556/E-MTAB-4556.sdrf.txt | cut -f30 | sort -u
# Comment[ENA_RUN]
# ERR1313817
# ERR1432629
```

Checked on 2026-10-28.

## What it is not

- **Not another submitter's runs.** Of the 327 runs a range names beyond its SDRF, 256 are in the
  ENA study the IDF itself names and 71 are not served by ENA at all. None is in another study.
- **Not this project's two-ends reading.** Until 2026-10-28 the route read the two ends of every
  range and none of the runs between, which is a reader bug, not an archive one: 2,159 of the 2,682
  studies it resolved returned exactly two runs. That is recorded on
  [the route page](../routes/from-ae-experiment.md#the-idf-names-runs-in-ranges).
- **Not in the IDF's other links.** `SecondaryAccession` and `RelatedExperiment`, which the
  study, series and related-experiment links read, held no range in the 2,996.
  `Comment[ENA_SAMPLE]` does (E-MTAB-562: `ERS025249-ERS025256`), and no link reads it.

## What the code does

`biostudies.sequence_data` expands a range only where both ends are accessions of one kind with one
prefix and it ascends, up to 50,000 accessions. Anything else is **reported**: the run link fails as
`UnreadableValue`, naming the range and why, and the same IDF's study and series are read as
before. The SDRF route resolves all 11 refused studies, so the union loses nothing by it.

A narrow range whose interior is not the experiment's cannot be told from one that is. It is
declared on the route, so `--explain` prints this page beside its runs, and the resolver's union
shows a run only this route returned as single-route and unconfirmed.

### Why a wide range is reported rather than capped

A cap would expand the first 50,000 accessions of a wider range. Both wide ranges seen are a first
and a last with almost nothing of the experiment's between them: 2 runs of 118,813, and 7 of
373,571. A capped range would be 50,000 runs that are not the experiment's, presented as an answer.

Answering from the ranges that do read is refused for the same reason. E-GEOD-47691's readable range
is 5 of the 22 runs its SDRF lists: a short answer that looks whole, which is the failure the
two-ends reading produced 2,159 times. A failed link is visible in a survey's error column; a short
answer is not.

The bound sits between the largest study in the draw, E-MTAB-5214 at 18,879 runs (it writes no
range), and the narrowest range that was not a block, 118,813. Within that gap its value changes no
verdict in the 3,000. It guards against the absurd only: E-MTAB-5101's `ERR1638087-ERR1638118` is 32
accessions, and 16 of them are runs of the same ENA study that its SDRF does not list.

**What would reverse it:** a study whose SDRF agrees, run for run, with a single range wider than
50,000. The bound would rise to cover it; the refusal to cut a range short would not change.

## Affected accessions

The refused ranges are the `UnreadableValue` failures of `ae_experiment->run:biostudies_idf` in
`data/surveys/sample_3000@arrayexpress-ena/`. The ranges that reach beyond their SDRF are found by
setting that export against `ae_experiment-to-run_biostudies_sdrf_comments.csv.gz` in the same
directory: E-GEOD-22024, E-GEOD-43654, E-GEOD-43858, E-MTAB-1155, E-MTAB-2800, E-MTAB-3453,
E-MTAB-3716, E-MTAB-4068, E-MTAB-4527, E-MTAB-4921, E-MTAB-5101, E-MTAB-5362, E-MTAB-6701, E-MTAB-691,
E-MTAB-926, E-MTAB-10034, E-MTAB-10523, E-MTAB-11506 and E-MTAB-13451.

## Regression test

`tests/pathologies/test_ae_idf_run_range_is_not_a_run_list.py` asserts that E-MTAB-4556's IDF still
spans 118,813 accessions for its 2 runs, that E-GEOD-47691's still descends, and that E-MTAB-5101's
range still covers runs its SDRF does not list.
