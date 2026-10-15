# Contributing

The knowledge base is the product; the CLI encodes what we know. A contribution
is welcome in either, and the bar for both is the same one rule:

> **Evidence before defaults.** No fallback order, route ranking or heuristic
> ships without a survey behind it and a knowledge-base page citing that survey.

## Adding a route

1. **Declare it** in `src/fetch_series/graph.py` with a `kb_page` and a
   `RouteCost`. Leave `evidence` off — an unmeasured route sorts behind every
   measured one, which is the correct place for it.
2. **Implement it** in `src/fetch_series/routes/__init__.py`. A route takes an
   accession and returns a list of strings; the survey harness does the rest.
3. **Run tier one**: `uv run fetch survey run --route <id> --corpus hard-cases`.
   34 accessions, seconds, and every one must produce a *verdict* — a result or
   a named pathology, never an unhandled exception. Skipping this step has
   already cost a partial 12,755-accession run.
4. **Run a representative corpus.** `reprocessed-gse`, `reprocessed-prj`,
   `arrayexpress-ena`, or a `sample:<n>@<corpus>` draw when the full set is too
   large. Tier one results are *not* evidence: `hard-cases` is adversarial by
   construction, and feeding its empty rate into the ranking would rank routes
   by how often they were pointed at broken accessions.
5. **Record the evidence**: `uv run fetch survey evidence --route <id>
   --corpus <name>` emits the block to paste into `graph.py`.
6. **Write the page.** `tests/test_routes.py` fails if a measured route has no
   knowledge-base page, or if that page cites no number.

## Adding a pathology

A pathology page is not finished until it can be handed to the archive:

- the symptom, and what makes it silent;
- a `curl` reproducer with expected against actual;
- the affected accessions, and the command that regenerates the list;
- what it is **not** — the ruled-out explanations are half the value;
- the workaround, and where it lives in the code;
- a test in `tests/pathologies/` marked `@pytest.mark.pathology`.

That suite is **inverted**: each test asserts the bug *still reproduces*. A
failure there is good news — the archive fixed something. Update the page,
promote the route, delete the test.

## Adding an assay

`Assay` is data, not code. A new one is a declaration:

```python
PARSE = Assay(name="parse-seq", phrases=("Parse Biosciences", "Evercode"))
```

Pick phrases that name a **platform**, not a kind of experiment. `scRNA-seq` is
deliberately not a 10x trigger: it appears in Smart-seq, Drop-seq and inDrop
submissions too, and adding it would buy recall by manufacturing false
positives. Measure recall against a corpus that is your assay by construction,
and say plainly that precision is unmeasured if you have no labelled negative
set.

## House rules

These exist because each one has already cost someone time. The full list is in
[CLAUDE.md](CLAUDE.md); the ones that bite newcomers:

- **Never `logging.basicConfig`.** Use `fetch_series.logging_utils.configure_logging`.
  The NCBI API key is a query parameter, so any line echoing a request URL leaks
  it — which is how a key reached this public repository once already.
- **Exceptions leak too.** httpx puts the full URL in the message of every
  `HTTPStatusError`. Route every request failure through `redact_exception`,
  and never print an exception from a throwaway script without it.
- **Parse accessions, don't regex them inline.** `fetch_series.accession.parse`
  is the single registry, validated against all 104,442 rows of the corpus.
- **`-` is a placeholder, never an identifier.** It appears 18,623 times in the
  corpus and as a `grep -f` pattern it matches every line of a metadata table.
- **`zip(a, b)` takes `strict=True`.** A length mismatch attributes results to
  the wrong accession, which for a provenance tool is the worst silent failure.
- Comments explain *why*, especially where the code looks wrong but is working
  around an archive. Name the accession that motivated it.

## Checks

```bash
uv run ruff check src tests scripts
uv run ruff format src tests
uv run mypy
uv run pytest                 # offline; the default
uv run pytest -m integration  # live archive APIs
uv run pytest -m pathology    # known archive bugs; a failure is good news
uv run mkdocs build --strict
```

## What not to touch

- `data/` survey CSVs are dated evidence, and route rankings cite them. Don't
  regenerate them casually.
- The bash in `nf-reprocessing-public-10x/modules/cellgeni/fetch10xmeta` is the
  parity contract. Changing it moves the target.
