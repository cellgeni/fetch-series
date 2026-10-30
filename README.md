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
**All 220 routes across five archives now carry measured evidence** — a census where one was
affordable, a named seeded draw where it was not, and a knowledge-base page for every one.
Forty were written by hand. The other 180 are catalogue links promoted to routes, each
surveyed as a route over 3,000 accessions, so every direction a surveyed link answers has a
measured route ([the full matrix](docs/routes/index.md)). Six of them are second routes into a
direction that already had one, declared because they add values another provider confirms
([the rule](docs/decisions/when-a-second-route-earns-its-place.md)).

Newest: [`fetch inspect`](docs/inspect.md), which reads the first 100,000 reads of every copy
of a run and says which 10x kit made them and whether they are usable. ENA's fastq of a
paired 10x run often lacks the barcode read. The metadata cannot see that, and the reads
prove it for all 182 such runs in a draw of 500
([the content survey](docs/benchmarks/content-survey-2026-10.md)). `fetch files --inspect`
refuses a copy only on that kind of proof. Every one-hop link the archive APIs offer has
also now been surveyed or probed ([link survey results](docs/graph/link-survey-results.md)).

Before that: a [metadata layer](docs/metadata.md) that resolves tissue, disease, cell type, donor and
sex per sample and grounds each value in UBERON, CL, MONDO, PATO, EFO or NCBI Taxonomy — from four
archives this project previously read for identity alone. It needs no LLM key. It asserts only
exact ontology matches, reports everything else for review, and **does not claim an accuracy
figure**, because the evaluation corpus it was designed around
[turned out to be circular in both of its halves](docs/decisions/what-counts-as-a-metadata-gold-set.md).

```bash
uv sync --all-groups

# Download links, with checksums and the reason each set was chosen
fetch files SRR25056225 --explain --verify

# links.tsv in the schema the reprocessing pipeline reads today
fetch links GSE111360

# Is this even 10x? Ask before downloading 880 runs of Smart-seq2
fetch screen GSE109816

# Read every copy of a run: which 10x kit, and is each copy usable?
fetch inspect run SRR29534765
fetch files SRR29534765 --inspect --explain   # refuses copies the reads prove unusable

# One row per run, joining what only GEO knows onto what ENA reports
fetch relations GSE236084 --sample-map

# What tissue, disease and donor is each sample? As ontology identifiers
fetch metadata PRJNA434002 --explain
fetch metadata PRJNA434002 --review      # and what a human should settle

# Resolve between any two entity types, walking multi-hop paths when needed
fetch resolve GSM7518069 --to run --explain

# Inspect the graph and the evidence behind it
fetch routes list
fetch survey compare --route A --route B --corpus reprocessed-gse

# What the archives changed since the last snapshot
fetch cache diff data/snapshots/<earlier>.sqlite
```

`fetch links` reproduces **14 of 14** of `fetch10xmeta`'s own nf-test cases — 422 snapshot rows (400 distinct
runs), no differences. See [the parity record](docs/fetch10xmeta-parity.md).

The knowledge base publishes to
[cellgeni.github.io/fetch-series](https://cellgeni.github.io/fetch-series/) on
every push to `main` — every route, every pathology, every measurement with the
date and corpus it came from. Build it locally with `uv run mkdocs serve`.

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

**Coverage is not correctness.** `esummary db=gds` resolves a BioProject for 466 GEO series whose
SOFT file names none, never fails, and never disagrees with SOFT where both answer — so it wins
every headline number. And **190 of those 466 (41%) resolve to a species-level umbrella project**:
`PRJNA30709` alone accounts for 138 of them and holds 7,591 runs, so a two-sample series routed
through it returns 7,591 and reports success. The ranking was *not* hand-tuned against this; the
defect is declared on the route and printed beside the value.
[Why](docs/decisions/route-ranking.md#coverage-is-not-correctness).

**0.77% of runs that reprocessed successfully no longer exist in either archive.** Not in ENA, and
not in NCBI's `esearch db=sra` either. Withdrawn, suppressed, or moved to controlled access — the
archives do not distinguish, and nothing announces any of them.
[The measurement](docs/routes/run-to-experiment/ena-filereport.md).

**A paging bug was hiding 94,895 BioSamples.** Fixing an unpaged ESummary call did not merely clear
330 failures; it recovered data that had been silently truncated on projects that reported success.
149,429 unique BioSamples became 244,324.

The comparison tables, every affected accession, and the curl reproducers are in
[the knowledge base](docs/index.md). How it was all measured — including the methodology mistakes
that cost real time and one that understated a finding sevenfold — is in
[benchmarks](docs/benchmarks/index.md).

## Why the routes are ordered the way they are

Nothing in this project has a hand-written preference order. `RouteRegistry.ranked()` computes the
order from the `RouteEvidence` blocks in `graph.py` — corpus, date, measured counts — and a route
nobody has surveyed sorts behind every route that has been.

That matters because the incumbent tries NCBI's `efetch` before ENA. The 2026-05 evidence said
that was backwards by **111,665 runs**, the runs ENA returns that `efetch` never does, counted as a
set difference over the same 12,755 BioProjects. Measuring why is what found the answer: one EFetch
request stops at 10,000 records. Paged, `efetch` agrees with ENA to within 0.4% (2026-10-26), and ENA
still ranks first, on cost: one request against two or three.

**[`docs/decisions/`](docs/decisions/index.md) records every such choice** — the argument, the
measurement it rests on, and what would reverse it:

| | |
|---|---|
| [How routes are ranked](docs/decisions/route-ranking.md) | why yields are **banded**, why cost only breaks ties, and why a route that answers more can still be the wrong default |
| [What counts as evidence](docs/decisions/what-counts-as-evidence.md) | three outcomes not two, distinct results not rows, why `hard-cases` is a gate and never evidence |
| [When archives disagree](docs/decisions/merging-and-provenance.md) | union over fallback, per-value provenance, conflicts surfaced rather than resolved |
| [Parity before improvement](docs/decisions/parity-before-improvement.md) | why `links.tsv` reproduces the incumbent's ordering, including where ours is better |
| [No DDBJ provider](docs/decisions/no-ddbj-provider.md), [no ENA browser XML](docs/decisions/ena-browser-xml-not-needed.md) | two planned providers dropped on measurement |

A decision with no stated failure condition is a preference, so each page ends with what would
change it.

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

See [CLAUDE.md](CLAUDE.md) for the conventions, the archive behaviours that have already cost
time, and the design decisions that look like over-complications and are not.
[CONTRIBUTING.md](CONTRIBUTING.md) has the route-, pathology- and assay-adding workflow;
[STATUS.md](STATUS.md) is a point-in-time snapshot of what is done and what is left.

## Licence

MIT. See [LICENCE](LICENCE).
