# FETCH10XMETA, backed by fetch-series

A drop-in replacement for the bash module in
[nf-reprocessing-public-10x](https://github.com/cellgeni/nf-reprocessing-public-10x).

## Swapping it in

```groovy
process {
    withName: '.*FETCH10XMETA' {
        container = 'quay.io/cellgeni/fetch-series@sha256:<digest>'
        queue     = 'transfer'
        maxForks  = 7
        cpus      = 1
        memory    = { 2.GB + 1.GB * (task.attempt - 1) }
        maxRetries    = 5
        errorStrategy = 'retry'
    }
}
```

Set `NCBI_API_KEY` in the environment, not on the command line — see
[the container notes](../../docker/README.md).

## What changes for the pipeline

**`links.tsv` is unchanged.** Same five columns, same `ORIFQ`/`ENAFQ`/`BAM`/`SRA`
vocabulary, same priority order, verified against the incumbent's own nf-test
snapshots — 14 cases, 425 runs, no differences
([the record](../../docs/fetch10xmeta-parity.md)).

**The intermediate files are gone.** The incumbent emitted `*.list`, `*.tsv`,
`*.txt` and `*.soft` as by-products of a shell pipeline. Nothing downstream
consumes them, and they are not reproduced. What replaces them is
`relations.tsv`, which is the useful part: one row per run with the GEO, SRA,
ENA and ArrayExpress identities joined, plus which route supplied each field
and where two archives disagreed.

**`screen.tsv` is new and advisory.** It says what assay each run appears to be,
from metadata, before anything is downloaded. It is deliberately `|| true`:
`fetch screen` exits non-zero when nothing looks like 10x, and that is a finding
to record rather than a reason to stop resolving. `RENAME10XRUN` stays
authoritative — it reads the FASTQ files and fails closed, which is correct for
the thing that decides how reads are parsed.

**`versions.yml` reports a real version.** The incumbent hardcoded
`reprocess_version=4.1` beside a commented-out line that would have read it from
the image.

## Two incumbent behaviours not carried over

**`errorStrategy = { task.attempt == 5 ? 'ignore' : 'retry' }`.** Ignoring on the
fifth attempt drops the dataset silently: the pipeline continues, the sample is
simply absent, and nothing downstream reports it. That is how datasets go
missing without anyone noticing. Use `'retry'` and let a genuine failure fail.

**The stub wrote file names the real script never produces** —
`${prefix}.sample.list`, `${prefix}.accessions.tsv` against the real run's
`sample.list` and `${meta.id}.accessions.tsv`. A stub that disagrees with its
process tests nothing. This one touches exactly what the script emits.
