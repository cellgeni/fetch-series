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

**The arguments behind every ranking and fallback decision are in `docs/decisions/`.** Read
[`route-ranking.md`](docs/decisions/route-ranking.md) before changing anything about ordering;
it records why banding exists, and why the ranking was *not* hand-tuned when `gds_summary` won
every headline number and was still the more dangerous route to trust.

### What the ranking does, so you do not reinvent it

`ranked()` sorts on, in order: measured-before-unmeasured; yield **banded** at `YIELD_BAND`
(1%); failure rate banded at `FAILURE_BAND` (0.1%); cost in requests; route id for stability.

The banding is the part that looks optional and is not. Ranking on raw yield put noise above
substance twice — `sra_be_elink` beat `sra_be_direct_cgi` on **55 runs out of 754,000** while
failing twice as often, and `bioproject->geo_series:elink` beat `gds_direct` on **one accession
out of 12,114**. Routes whose yields are indistinguishable must be separated by reliability, not
by error bars.

The route id tie-break is not cosmetic either: the id is **recorded as provenance** on every
resolved value, so an unstable order makes two identical queries produce differently-attributed
answers.

### When a route answers more but answers worse

Declare the defect on the route (`known_pathologies`) and let `--explain` print it beside the
value. Do **not** demote the route by hand. Once one ranking is overridden by judgement every
ranking becomes negotiable, and the rule above stops meaning anything.

### What is and is not evidence

- **`hard-cases` never becomes `RouteEvidence`.** It is adversarial by construction — every
  accession is there because it broke something — so its empty rate would rank routes by how
  often they were pointed at withdrawn projects. It is a *gate*: a route must produce a verdict
  for all 46 before it earns a bigger run.
- **`--limit` never produces evidence.** It records verdicts against the *full* corpus name, so
  two differently-limited runs look comparable and are not. Use `sample:<n>@<corpus>`, which is
  seeded and named for its size.
- **Count distinct results, not rows.** 69,923 of the 2026-05 survey's 824,216 "runs" were
  within-BioProject duplicates.
- **Three outcomes, and absence is a fourth.** `RESOLVED` / `EMPTY` / `FAILED`; never queried is
  not the same as empty. `survey compare` keys agreement on `(outcome, results)` because an
  earlier version reported a FAILED and an EMPTY route as agreeing.

## Layout

```
src/fetch_series/
  accession.py     what an identifier is and which archive issued it
  graph.py         routes as declared data + evidence-based ranking
  resolver.py      the fallback engine: union, per-value provenance, conflicts
  relations.py     the cross-archive join, one row per run
  entities.py      RunRecord; set() is first-wins and records conflicts
  files.py         the file layer: classification, trust, ranked recommendation
  filesets.py      orchestration — files per run, verification, screening
  assays/          assay and chemistry recognition, pluggable
  export/links.py  links.tsv in the incumbent's schema (the M3 parity contract)
  providers/       one module per API surface; typed calls and parsing, no route logic
  survey/          the benchmark harness: corpora, runner, rate limiting
  cache.py         SQLite store of verdicts and raw responses
  core.py          legacy sync helpers (E-utilities, ENA portal, BioStudies)
  logging_utils.py credential redaction; use configure_logging(), not basicConfig()
scripts/           batch survey scripts (transitional — being folded into the package)
nextflow/          a drop-in FETCH10XMETA backed by the container
docker/            the image the Nextflow module runs
data/corpora/      committed accession lists, one directory per corpus
data/surveys/      every recorded verdict, gzipped CSV — `fetch survey export`
docs/              the knowledge base (MkDocs Material)
docs/decisions/    why each priority, fallback and scoping choice is what it is
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
- **A UID list in a query string has now cost this project data five times.**
  ESummary, ESearch history, two ELink run routes, and EFetch. The last one
  failed as httpx's own `InvalidURL` rather than a server 414, so the request
  was never sent and GSE241770 — 7,905 runs — was recorded as a permanent
  failure. Anything joining UIDs into a URL goes by POST above
  `POST_THRESHOLD_UIDS`.
- **Dates are UTC, everywhere.** A survey finishing at 00:12 BST is 23:12 UTC the
  day before, and `date.today()` recorded the wrong day — which a CI runner an
  hour behind then rejected as being in the future. Archive surveys are
  international by nature; a local date is ambiguous to every reader.
- **Run `hard-cases` before any full corpus.** 34 accessions and seconds. Skipping it cost a partial
  12,755-accession run on a route whose empty rate should have looked wrong immediately.

## Design decisions you will be tempted to undo

Each of these looks like an over-complication until you read why it is there.
Full arguments in `docs/decisions/`.

- **Union is the default resolver mode, not a fallback chain.** A chain can only be as complete
  as its best single route, and no route here is complete: ENA holds 110,779 runs `efetch` never
  returns, and ~1,800 runs are missing from ENA.
- **`RunRecord.set()` is first-wins *and* records the loser.** Routes are offered in measured
  order, so the first answer is the best-evidenced one — but a second, different answer goes into
  `conflicts` rather than being dropped. Those conflicts are where the pathologies came from.
- **`proves_data_exists` separates confirmed from unconfirmed.** ENA's `result=read_run` rows
  *are* runs; GEO's SOFT file names experiments from the submitter's record, and 1.23% of them
  carry no runs. Merging the two makes a complete-looking list that resolves to nothing
  indistinguishable from one that works.
- **`FileSet` keeps every offer.** Collapsing to one pick before the caller sees it is what lost
  all 36 files of E-MTAB-8060 in the incumbent.
- **Route order is evidence; *file* choice is requirements.** Which route to try first comes from
  surveys. Which file to use comes from what STARsolo needs. Do not let the second leak into the
  first — it would smuggle a preference in as a measurement.
- **Ambiguity is reported, not resolved.** Library type and chemistry come from a protocol field
  that describes a whole *study*: GSE111360 names both a 3' and a 5' kit because it used both, so
  its runs report `10x_3p_v3_family|10x_5p_v1_v2` and assert nothing. Picking one would be a coin
  flip dressed as a measurement.
- **Entity types are not merged across archives.** A `GSE` is `GEO_SERIES`, not `STUDY`. The
  INSDC triples share a type because they are genuinely one entity; GEO and ArrayExpress records
  are curated records that *point at* one, and `ranked_for()` can only filter by issuing archive
  because the model keeps them apart.
- **`links.tsv` reproduces the incumbent's ordering, including where ours is better.** That is
  the M3 parity contract, measured against `fetch10xmeta`'s own nf-test snapshots (14/14, 447
  runs). The improvements ship underneath, through `fetch files`.

## Things to leave alone

- Don't commit `.env`, or logs containing `api_key=`. CI rejects credential-shaped strings in
  tracked files.
- Don't "fix" the bash in `nf-reprocessing-public-10x/modules/cellgeni/fetch10xmeta` from here. Its
  behaviour is the parity contract for M3; changing it moves the target.
- Don't regenerate the survey result CSVs in `data/` casually. They are dated evidence, and route
  rankings cite them. `data/surveys/` is written by `fetch survey export` from the SQLite cache;
  the cache itself is deliberately **not** committed (raw response bodies, 154 MB against 16 MB
  for the exports).
- Don't delete a `docs/decisions/` page because the decision now looks obvious. The argument is
  the artefact; without it the next person re-litigates it from scratch, which is how the
  hand-written fallback order got there the first time.
- Don't file the upstream reports in `docs/upstream/` without asking. They are addressed to NCBI
  and EBI, and sending them is the maintainer's call.
