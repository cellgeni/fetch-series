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

Early. Milestone 1 (the accession-graph survey) is in progress; see
`docs/` for what has been measured so far. The CLI currently resolves BioProject → GEO only.

```bash
uv sync
uv run fetch PRJNA988806      # -> GSE236084
```

## What is already measured

Nine routes surveyed across 12,756 BioProjects. At the unique-run level:

| BioProject → runs | Unique runs | Failed accessions |
|---|---|---|
| ENA portal `filereport` | **784,026** | 15 |
| SRA `sra-db-be` (direct CGI) | 754,277 | 16 |
| SRA `sra-db-be` (via ELink) | 754,332 | 34 |
| NCBI `efetch` runinfo (via ELink) | 674,186 | 31 |
| NCBI `efetch` runinfo (direct `[GPRJ]`) | 673,157 | 15 |

ENA is a near-superset: it holds 111,665 runs `efetch` never returns, while only ~1,800 runs are
missing from it. For GEO, the ELink and direct `db=gds` routes are functionally identical — zero
accessions where they disagree.

## Development

```bash
uv sync --all-groups
uv run ruff check src tests scripts
uv run mypy
uv run pytest                    # offline unit tests
uv run pytest -m integration     # hits live archive APIs
uv run pytest -m pathology       # known archive bugs; a FAILURE means one was fixed
```

Set `NCBI_API_KEY` in a local `.env` to lift the NCBI rate limit from 3 to 10 requests/second.
Never commit it — `fetch_series.logging_utils` redacts credentials from log output, and CI rejects
credential-shaped strings in tracked files.

## Licence

MIT. See [LICENCE](LICENCE).
