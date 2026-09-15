# CLAUDE.md

Guidance for working in this repository.

## What this is

`fetch-series` resolves public sequencing accessions — GEO, SRA, ENA, ArrayExpress/BioStudies,
BioProject, DDBJ — to samples, runs and file links, over routes that are documented, benchmarked
and cross-checked. It serves
[nf-reprocessing-public-10x](https://github.com/cellgeni/nf-reprocessing-public-10x), whose
`modules/cellgeni/fetch10xmeta` bash module it is intended to replace.

The archives genuinely disagree with each other. A BioProject that resolves cleanly through one API
returns nothing through another; a GEO series may omit the SRA relation entirely; an ArrayExpress
SDRF may point at a decommissioned mirror. This project measures those disagreements rather than
guessing at them.

## The governing rule

**Evidence before defaults.** No fallback order, route ranking or heuristic ships without a survey
behind it and a knowledge-base page citing that survey.

Concretely: when you add a route, declare it in `src/fetch_series/graph.py` with a `RouteEvidence`
block recording the corpus, the date and the measured counts. Do not hand-write a preference order.
`RouteRegistry.ranked()` derives the order from the evidence, and a route nobody has measured sorts
behind every route that has been.

The corollary: if you find yourself writing "ENA is usually better here", either measure it or say
plainly that it is unmeasured. An undated coverage number is not evidence of anything.

## Layout

```
src/fetch_series/
  accession.py     what an identifier is and which archive issued it
  graph.py         routes as declared data + evidence-based ranking
  core.py          legacy sync helpers (E-utilities, ENA portal, BioStudies)
  logging_utils.py credential redaction; use configure_logging(), not basicConfig()
scripts/           batch survey scripts (transitional — being folded into the package)
data/              corpora and survey results, Git LFS
docs/              the knowledge base (MkDocs Material)
```

## Commands

```bash
uv sync --all-groups
uv run ruff check src tests scripts
uv run ruff format src tests
uv run mypy
uv run pytest                    # offline unit tests; the default
uv run pytest -m integration     # hits live archive APIs
uv run pytest -m slow            # needs Git LFS objects fetched
uv run pytest -m pathology       # known archive bugs
```

`pytest -m pathology` is inverted from a normal suite: each test asserts that a **known archive bug
still reproduces**. A failure there is good news — the archive fixed something. Update the
knowledge-base page, promote the route, and remove the test.

## Conventions

- **Never `logging.basicConfig`.** Use `fetch_series.logging_utils.configure_logging`. The NCBI API
  key is a query parameter, so any log line echoing a request URL leaks it — which is exactly how a
  key reached this public repository once already. The redacting formatter covers logged
  tracebacks, not just format strings.
- **Redaction is not enough on its own: exceptions leak too.** httpx puts the full request URL into
  the message of every `HTTPStatusError` and most transport errors, and an *uncaught* traceback is
  printed by the interpreter without ever passing through logging. A second key was leaked this way,
  by an ad-hoc script that raised outside any handler. `SurveyClient` and `core.py` now route every
  request failure through `redact_exception`, which preserves the exception type so `is_retryable`
  still works. **Any new HTTP call site must do the same** — and never print an exception from a
  throwaway script without redacting it.
- **Log files append.** The survey scripts used `mode="w"`, so every re-run destroyed the evidence
  documenting the results sitting next to it. Use `run_logfile()` for per-run timestamped paths.
- **Parse accessions, don't regex them inline.** `fetch_series.accession.parse` is the single
  registry. It is validated against all 104,442 rows of the reprocessed corpus.
- **`-` is a placeholder, never an identifier.** It appears 18,623 times in the corpus. It must be
  reported as unparsed, never silently dropped: as a `grep -f` pattern it matches every line of a
  metadata table.
- **`zip(accessions, results)` takes `strict=True`.** A length mismatch would attribute results to
  the wrong accession, which for a provenance-focused tool is the worst kind of silent failure.
- Comments explain *why*, especially where the code looks wrong but is deliberately working around
  an archive's behaviour. Name the accession that motivated the workaround.

## Traps that have already cost time

- **httpx encodes `None` params as empty**, so `api_key=None` is sent as `api_key=` — and NCBI
  rejects that with a 400. `core.py` strips `None` params for this reason.
- **E-utilities reports errors as HTTP 200** with a JSON body that has no `result` key (for example
  "Too many UIDs in request. Maximum number of UIDs is 500"). Treat that as unresolved, not as a
  crash, and page at `retmax=500`.
- **WebEnv/query_key are session-scoped.** Retry the whole `esearch → elink → efetch` chain, never
  an individual call.
- **ArrayExpress accessions are hyphenated** (`E-MTAB-6505`). The `E-MTAB\d+` pattern this project
  used to carry could never match a real one.
- **BioStudies pages silently**: with no explicit limit it returns the first 25 of however many
  there are.
- **`[GPRJ]` is not reliable** for renamed or merged projects — the term falls through to
  `[All Fields]`.
- **MkDocs strips em dashes from anchors rather than replacing them**, so `## GPU — cellbender`
  becomes `#gpu-cellbender` with one hyphen. The build runs `--strict`, so this breaks CI.
- **`data/` is Git LFS**, but some files committed before `.gitattributes` are plain blobs. Check
  before assuming.
- **Rotating an API key mid-survey is destructive.** NCBI answers an invalid key with a 400, which
  is not retryable, so every remaining accession is recorded as a permanent failure. Stop the run,
  update `.env`, resume.
- **A pathology test that fails on a 429 is worse than no test.** The suite is
  inverted -- a failure is supposed to mean the archive fixed something -- so a
  transient failure is indistinguishable from the good news. Use the
  `run_or_skip` fixture in `tests/pathologies/conftest.py`; it skips on anything
  `is_retryable` recognises. Four false positives appeared the first time a
  survey and the suite ran at once.
- **`csv.DictReader` silently keeps only the last value of a repeated column.**
  SDRF repeats `Comment[FASTQ_URI]` once per mate, so E-MTAB-9221 read through a
  dict yielded 20 URIs for its 20 runs instead of 40 -- half the data, no error.
  Use `biostudies.sdrf_raw_rows` for anything that may repeat.
- **BioStudies file listings are under `items`, not `files`.** A
  `.get("files", [])` returns an empty list and no error, which reads as "this
  study registers nothing" -- and since that *is* the finding for E-MTAB-8060,
  the bug is invisible: it turns every study into E-MTAB-8060.
- **BioStudies serves 20,000 hits and answers the next page with a 500**, not a
  400. Every retry policy reads that as transient and retries forever. Partition
  the query; `search_by_year` does.
- **Mate markers mean nothing outside a fastq name.** E-MTAB-8060's runs deposit
  `Sample_1.bam`, where the `_1` is the submitter's sample name. A BAM carries
  both mates interleaved, so a mate number on one is meaningless.
- **Two surveys at once need a SQLite busy timeout.** WAL allows one writer and
  Python's default timeout is five seconds, so the normal way to cover NCBI and
  ENA routes in parallel would lose hours of work to "database is locked".
- **Run `hard-cases` before any full corpus.** 34 accessions and seconds. Skipping it cost a partial
  12,755-accession run on a route whose empty rate should have looked wrong immediately.

## Things to leave alone

- Don't commit `.env`, or logs containing `api_key=`. CI rejects credential-shaped strings in
  tracked files.
- Don't "fix" the bash in `nf-reprocessing-public-10x/modules/cellgeni/fetch10xmeta` from here. Its
  behaviour is the parity contract for M3; changing it moves the target.
- Don't regenerate the survey result CSVs in `data/` casually. They are dated evidence, and route
  rankings cite them.
