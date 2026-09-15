# Container

```bash
docker build -t fetch-series:0.1.0 -f docker/Dockerfile .
docker run --rm fetch-series:0.1.0 links GSE111360
```

With a key, which lifts NCBI's rate limit from 3/s to 10/s:

```bash
docker run --rm -e NCBI_API_KEY="$NCBI_API_KEY" fetch-series:0.1.0 links GSE111360
```

Pass the key as an environment variable, never as an argument. It is a query
parameter, so a command line carries it into the process table and into
Nextflow's `.command.sh`, which is written into the work directory and stays
there. A key reached this repository once already by a shorter route than that.

## In Nextflow

Pin by digest, not by tag. A tag that moves under a running pipeline makes two
samples in one batch resolve by different code, and nothing in the output says
which.

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

Note the `errorStrategy`. The incumbent config uses
`{ task.attempt == 5 ? 'ignore' : 'retry' }`, which silently drops a dataset
whose metadata could not be resolved after five attempts — the pipeline
continues, the sample is simply absent, and nothing downstream reports it. That
is how datasets go missing without anyone noticing. Failing is the honest
behaviour; if a batch must survive individual failures, collect them and report
the list at the end.

## Why this image is small

It resolves accessions and writes a TSV. It downloads no data, so it needs
neither the SRA toolkit nor any of the alignment tooling the reprocessing image
carries: Python, `httpx`, `typer`, and CA certificates.
