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

**A link that exists is not a route.** `src/fetch_series/catalogue/` lists every one-hop link the
archive APIs offer between 39 nodes (1,127 of them, INSDC objects split by archive), each measured by
a route's survey or confirmed by one live probe — and a probe into a file or document confirms it only
if what it found is usable. A route's per-pair split (`route_splits.json`) is filtered measurement,
not new evidence. Catalogue links are never ranked
and never enter `REGISTRY` without a survey, and **a probe never becomes `RouteEvidence`**: it is one
accession out of one, which proves existence and measures nothing. Promoting a catalogue link to a
route means surveying it like any other.

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
- **A link survey is measurement, not a ranking.** `fetch catalogue survey` measures unmeasured
  catalogue links over seeded draws of 300 per source node (BioProject and BioSample stratified by
  issuing archive), recorded under `link:<link id>`. It sets a link's status to surveyed or
  surveyed-none, and nothing in `graph.py` or `resolver.py` reads it. Promoting a link to a route
  means surveying it as a route, at `sample:3000`, with a `RouteFn` and a `RouteEvidence` block.

## Disagree when an idea looks wrong

The maintainer wants argument, not compliance. If a request contradicts the evidence or the
governing rule, or would cost more than it buys, say so plainly, with the reason and the
alternative, and let the maintainer decide. Agreeing with a weak idea to be agreeable is a
failure here, and so is recommending one of your own without re-examining it once it is
challenged.

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
  inspect/         read a run's first reads: FASTQ/BAM readers (stdlib), CRAM/SRA (optional),
                   the 10x classifier, the fit rule, the content survey, the report
  metadata/        descriptive metadata: harvest, rules, ontology grounding, measurement
  export/links.py  links.tsv in the incumbent's schema (the M3 parity contract)
  export/graph.py  the route graph as data: what the KB's route-graph page draws
  catalogue/       every one-hop link the archives offer, by mechanism, with its live probe,
                   its link survey (populations, waves, link_splits.json), never ranked
  providers/       one module per API surface; typed calls and parsing, no route logic
  survey/          the benchmark harness: corpora, runner (per route, and fan-out per
                   accession), rate limiting, the committed export format
  units.py         unit ids: a route id, `link:<link id>`, `screen:<reads>:<link id>`
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
hooks/             MkDocs build hooks: embeds the route graph from REGISTRY at build time
```

## Commands

```bash
uv sync --all-groups
uv run ruff check src tests scripts hooks
uv run ruff format src tests hooks
uv run mypy
uv run pytest                    # offline unit tests; the default
uv run pytest -m integration     # hits live archive APIs
uv run pytest -m slow            # needs Git LFS objects fetched
uv run pytest -m pathology       # known archive bugs
uv run fetch catalogue probe --write   # re-probe every untested link (live, ~1 min)
uv run fetch catalogue survey --all --gate         # every unmeasured link over the gates (live)
uv run fetch catalogue survey --all --standard --plan   # the seeded draws: units, sizes, requests
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
- **`[GPRJ]` misses experiments its project holds, and ELink adds its siblings'.** PRJNA66167's
  SRX135150 names the project in every record and is not under `PRJNA66167[GPRJ]`; ELink from
  PRJNA1033389 returns 72 runs of the projects under its umbrella. No `[GPRJ]` search was seen to
  fall through to `[All Fields]` (2026-10-26).
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
- **`efetch db=biosample` does not accept accessions, and pretends it does.** It strips the
  alphabetic prefix and reads the remainder as a UID. For `SAMN` the number *is* the UID, so the
  bug is invisible on most samples; `id=SAMEA11294529` returns `SAMN11294529` — a rumen bacterium
  where a human liver sample was asked for — with HTTP 200 and a full attribute block. Resolve
  through `esearch ... [accession]` and **check every returned accession against what was asked
  for**. Affects every EBI and DDBJ BioSample, of which this corpus has many.
- **An ontology service will always find you something.** ZOOMA's `ontologies:[...]` filter does
  not prevent a wrong answer, it selects which one you get: `male` constrained to UBERON returns
  `UBERON:0003101` *male organism* at confidence `GOOD`. And OLS's `exact=true` returns 40
  documents for `male` including *male with DSD*, while `ontology=cl` returns GO terms. Re-check
  every hit against its own label or synonyms, and against the field's declared ontology prefix.
- **An abbreviation is the most dangerous thing to ground.** `ASD` exact-matches MONDO's synonym
  for *atrial septal defect* and nothing else — in an autism study, with **one** hit, so nothing
  looks ambiguous. `PBMC` → `CL:2000001` is the same shape and correct. Nothing internal
  distinguishes them, so an abbreviation matched by synonym is never asserted without the study
  text corroborating the expansion.
- **ZOOMA echoes the queried string back as the annotated property.** Using it as the term's
  label printed `CL:0001074 (CD45+ cells from a fresh kidney tumor sample)` — a CURIE labelled
  with prose it does not mean, which hid the fact that CL:0001074 is a rare innate lymphoid
  precursor. Resolve the label through OLS.
- **`Term Source REF` qualifies the preceding *qualifiable* column, which may be a `Unit`.**
  E-MTAB-10553 runs `Characteristics[age]  Unit[time unit]  Term Source REF  Term Accession
  Number` over `84  year  EFO  UO_0000036`. `UO_0000036` is *year*. Binding term columns to the
  nearest `Characteristics[...]` grounds every such age, and grounds it to a unit.
- **ENA's `filereport` rejects an ArrayExpress accession too**, not only a GEO one. Key the
  request on the resolved project or study accession, which also returns every sample in one
  request instead of one per run.
- **A study-scoped value cannot assign a sample-scoped field, and an invariant one can.** These
  are opposite traps. PRJNA434002's `study_title` names Autism Spectrum Disorder for all 56 runs,
  controls included — never assign from it. But twenty samples all reading `tissue: lung` *are*
  all lung: the submitter wrote it once per sample, and refusing invariant values discards
  correct metadata from every single-tissue study.
- **Conflict is a property of sources disagreeing, not of a field's cardinality.** Keying it on
  `multivalued` reported "ENA says lung, GEO says brain" as a *mixture* and hid it.
- **ENA's `links` endpoint reads `limit=0` as zero rows.** `filereport` and `search` read the
  same parameter as *no limit*, so copying the parameter across returns a header and nothing,
  which reads as "no links". `links` also answers a secondary accession (`ERP`, `ERS`) with an
  empty table rather than an error: key it on the `PRJ` or `SAM`.
- **NCBI's documentation answers scripts with a browser check**, as does headless Chrome. To check
  what a page documents, read the Internet Archive's copy (`web.archive.org/web/2026id_/<url>`,
  gzipped). It matters: GEO documents none of the `_relation` lines every GEO route reads.
- **`gds` ESearch needs the entry type.** `GSM1480291[ACCN]` alone matches the sample, its series
  and its platform, and the first UID is the series'. Add `AND GSM[ETYP]`.
- **A `#` in a file link is part of the name.** Sanger names lane CRAMs `<run>_<lane>#<tag>`:
  ERR13855116's is `27865_1#3.cram`. Every HTTP client reads `#3.cram` as a fragment and asks for
  `27865_1`, which answers 404 — a valid 767 MB file reported as missing. Request a published file
  link through `files.request_url`, and keep the link itself as published: `links.tsv` reproduces it.
- **SDL answers a project or study with a 504, every time.** `sdl/2/retrieve acc=PRJNA495040` (or
  `ERP110462`) times out at the gateway after ~10 s on every attempt, and 504 is in
  `RETRYABLE_STATUS`, so a retrying client spends its whole retry budget on a request that cannot
  succeed. Ask SDL per run; a run answers in under a second.
- **Run `hard-cases` before any full corpus.** 46 accessions and seconds. Skipping it cost a partial
  12,755-accession run on a route whose empty rate should have looked wrong immediately.

## Design decisions you will be tempted to undo

Each of these looks like an over-complication until you read why it is there.
Full arguments in `docs/decisions/`.

- **Union is the default resolver mode, not a fallback chain.** A chain can only be as complete
  as its best single route, and no route here is complete: over 12,755 BioProjects the best NCBI
  route misses 162 of ENA's runs and holds 2,812 ENA lacks (2026-10-26). The 2026-05 figure of
  111,665 runs ENA held over `efetch` was EFetch's 10,000-record cap, not a route difference.
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
- **Metadata sources are deliberately unranked, and `asserted()` therefore declines to choose.**
  `RunRecord.set()` is first-wins because routes arrive in *measured* order. Nobody has surveyed
  whether BioSample's `tissue` beats GEO's, so first-wins there would be declaration order
  presented as evidence. Every `Source` carries empty `evidence` and that changes behaviour.
- **Metadata accuracy is not measured, and the reason is a finding.** The ArrayExpress gold set
  the layer was designed around does not exist: of 120 studies sampled, 119 carry curated
  characteristics and **0** carry an independently registered sample. `E-MTAB-*` records come from
  the same submission as their SDRF; `E-GEOD-*` SDRFs are a mechanical copy of GEO's own values.
  Both halves are circular. Read `docs/decisions/what-counts-as-a-metadata-gold-set.md` before
  trying to build it again.
- **Tier 1 reads keys; tier 3 reads prose.** `5864_BA9` is donor 5864, Brodmann area 9, and a
  regex for it would work on one study. It would also be indistinguishable in the output from a
  rule that generalises, which inflates tier 1 at the model's expense — and the whole point of
  building the deterministic tiers first is being able to say what the model adds.
- **`links.tsv` reproduces the incumbent's ordering, including where ours is better.** That is
  the M3 parity contract, measured against `fetch10xmeta`'s own nf-test snapshots (14/14; 422
  snapshot rows, 400 distinct runs). The improvements ship underneath, through `fetch files`.

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
