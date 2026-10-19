"""Read the start of a run's files and say what they are.

The metadata layer (:mod:`fetch_series.assays`) reads what a submitter *wrote*
about a library. This reads the library itself -- the first reads of each file a
route found -- and says what is actually there: which reads, how long, with
what quality, and whether a 10x pipeline could use them.

Nothing here is a ranking, and no sequence is ever stored: a check records
counts and lengths, never reads.

The core -- FASTQ, BAM, the checks and the classifier -- uses the standard
library alone. CRAM and SRA objects need ``pysam`` and sra-tools, found at
runtime; without them a file is *undetermined (reader unavailable)*, never
"unfit".
"""
