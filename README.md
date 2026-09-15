# fetch-series

Resolve public sequencing accessions — GEO, SRA, ENA, ArrayExpress/BioStudies, BioProject, DDBJ —
to samples, runs and file links, over routes that are **documented, benchmarked and cross-checked**
rather than assumed.

The archives disagree with each other. A BioProject that resolves cleanly through one API returns
nothing through another; a GEO series may omit the SRA relation entirely; an ArrayExpress SDRF may
point at a decommissioned mirror. This project measures those disagreements, records them, and
builds a resolver that falls back deliberately instead of failing silently.

It exists to serve [nf-reprocessing-public-10x](https://github.com/cellgeni/nf-reprocessing-public-10x),
where getting the wrong file list means reprocessing the wrong data.

## Status

The accession graph, the file layer, parity with the incumbent, and the assay layer are all in.
38 routes across five archives, every one exercised against a curated pathology corpus and the
important ones measured by census rather than sample.

```bash
uv sync --all-groups

# Download links, with checksums and the reason each set was chosen
fetch files SRR25056225 --explain --verify

# links.tsv in the schema the reprocessing pipeline reads today
fetch links GSE111360

# Is this even 10x? Ask before downloading 880 runs of Smart-seq2
fetch screen GSE109816

# One row per run, joining what only GEO knows onto what ENA reports
fetch relations GSE236084 --sample-map

# Resolve between any two entity types, walking multi-hop paths when needed
fetch resolve GSM7518069 --to run --explain

# Inspect the graph and the evidence behind it
fetch routes list
fetch survey compare --route A --route B --corpus reprocessed-gse
```

`fetch links` reproduces **11 of 11** of `fetch10xmeta`'s own nf-test cases — 425 runs, no
differences. See [the parity record](docs/fetch10xmeta-parity.md).

## What the censuses found

Four of these contradicted what the code assumed before anyone measured it.

**ENA's derived fastq omits the read that carries the cell barcode — for 39.2% of runs.** Over a
random 3,000-run draw from the reprocessed corpus, 1,201 runs declare `library_layout=PAIRED` and
publish a single fastq; for **1,177 of them SRA holds more bases than ENA serves** — 5.93 Tb
unpublished, a median ratio of 1.40. The missing ~28 bp read is the 10x barcode and UMI. The
download succeeds, the published md5 matches, and every read in the result is unassignable to a
cell. Only **8.2%** of runs have any submitted original to fall back on, so for most of them the
SRA object is the sole complete source.
[The measurement](docs/pathologies/ena-paired-library-single-fastq.md).

**NCBI's `elink gds→sra` is missing 17.6% of GEO series, silently.** Over all 13,045 series in the
reprocessed corpus it resolves 82.4% against the SOFT family file's 99.8%, and returns 178,879
experiments against 308,639 — with **zero** errors, so no caller can detect the shortfall.
[The report is drafted](docs/upstream/ncbi-elink-gds-sra.md).

**An accession is not evidence that data exists.** 1.23% of the experiments named in GEO SOFT
files — roughly 3,807 of 308,639 — have no runs at all. Two series name experiment sets that are the same size as, and completely
disjoint from, what their project actually holds — so a complete-looking answer downloads nothing.
The resolver therefore splits its answer into confirmed and unconfirmed.

**Archives only index their own accessions.** NCBI's ELink resolves BioProject → BioSample for 99.3%
of NCBI-issued projects and **1.4%** of EBI-issued ones. Routes declare which archives they can
answer for, and the resolver skips the rest rather than spending three requests to learn nothing.

**A paging bug was hiding 94,895 BioSamples.** Fixing an unpaged ESummary call did not merely clear
330 failures; it recovered data that had been silently truncated on projects that reported success.
149,429 unique BioSamples became 244,324.

The comparison tables, every affected accession, and the curl reproducers are in
[the knowledge base](docs/). How it was all measured — including the methodology mistakes that cost
real time and one that understated a finding sevenfold — is in
[benchmarks](docs/benchmarks/index.md).

## Development

```bash
uv sync --all-groups
uv run ruff check src tests scripts
uv run mypy
uv run pytest                    # offline unit tests
uv run pytest -m integration     # hits live archive APIs
uv run pytest -m pathology       # known archive bugs; a FAILURE means one was fixed
uv run mkdocs serve              # the knowledge base
```

Set `NCBI_API_KEY` in a local `.env` to lift the NCBI rate limit from 3 to 10 requests/second.
Never commit it — credentials are redacted from both log output and exception messages, and CI
rejects credential-shaped strings in tracked files.

See [CLAUDE.md](CLAUDE.md) for the conventions, and for the archive behaviours that have already
cost time.

## Licence

MIT. See [LICENCE](LICENCE).
