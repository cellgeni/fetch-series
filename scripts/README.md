# scripts/

Two kinds of script live here, and they are treated differently.

## Frozen: the 2026-05 BioProject surveys

`query_bioproject*.py` and `rerun_failed.py` are the batch surveys that produced the
first route comparisons. **They are frozen.** Do not extend them, and do not use them
for a new survey: `fetch survey run` replaces them, with seeded visit order, resume,
the three-outcome record and an export that `RouteEvidence` can cite.

They are kept, not deleted, because their outputs are still cited.
`src/fetch_series/catalogue/route_splits.json` reads six of the CSVs they wrote to
`data/query/results/`, and a split that cites a file the repository can no longer
explain is not evidence of anything.

The only changes made to them since are the ones that stop them doing harm if run:

- logging goes through `fetch_series.logging_utils.configure_logging`, appending to a
  per-run file from `run_logfile()`. They used `logging.basicConfig` with `mode="w"`,
  so each re-run erased the log that documented the results beside it, and nothing
  redacted the API key from a retry warning;
- retry and failure messages pass the exception through `redact_exception`;
- outputs go to `data/query/results/`, where the cited CSVs actually are.
  `query_bioproject.py`, `query_bioproject2geo.py` and `query_bioproject2sra.py`
  still wrote to `data/query_results/`, which no longer exists, and
  `rerun_failed.py` read from it;
- an empty `NCBI_API_KEY` is treated as no key, as everywhere else.

## Live

| Script | What it does |
|---|---|
| `archive_state_monitor.sh` | re-runs the tier-one corpus and the pathology suite, and diffs against the last snapshot |
| `parity_fetch10xmeta.py` | compares `fetch links` against fetch10xmeta's nf-test snapshots (the M3 parity contract) |
| `verify_file_links.py` | checks, over a sample, that the file links the tool hands out actually work |
| `biostudies_front_door_census.py` | what BioStudies' file address answers for each ArrayExpress IDF and SDRF, without the mirror fallback, against what the study registers |
| `route_graph_layout.py` | searches node positions for the route-graph page so the layout can be reproduced |
| `benchmark_inspect.py`, `benchmark_inspect_report.py` | the read classifier benchmark: labels, calls and the held-out report |
| `inspect_survey_report.py` | the content survey's tables, from its committed `screen_*` exports |
| `biostudies_served_size_census.py` | what BioStudies serves for each ArrayExpress IDF and SDRF against the size it registers, whether it serves a directory listing instead, and its 403s under load |
