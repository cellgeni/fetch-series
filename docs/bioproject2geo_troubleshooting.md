# BioProject to GSE Query Troubleshooting

This note documents the failure modes seen while resolving BioProject
accessions such as `PRJNA...` to GEO Series accessions such as `GSE...` using
NCBI E-utilities.

## Intended Query Path

The usual lookup path is:

```text
BioProject accession -> BioProject UID -> linked GDS/GEO UID -> GEO Series
```

With E-utilities, that usually means:

1. Use `ESearch` against `db=bioproject` to resolve the accession.
2. Use `ELink` from `dbfrom=bioproject` to `db=gds`.
3. Use `ESummary` against `db=gds` to fetch the GEO metadata.
4. Parse the `accession` field from the GDS summary. Series records are usually
   `GSE...`; sample records are usually `GSM...`.

## Environment and API Key Problems

The script reads the NCBI API key from the environment. In this repo we saw a
name mismatch:

```text
.env contains: NCBI_KEY
code expected: NCBI_API_KEY
```

If `load_dotenv()` returns `True` but `os.getenv("NCBI_API_KEY")` returns
`None`, inspect the parsed dotenv keys:

```python
from dotenv import dotenv_values

values = dotenv_values(".env")
for key, value in values.items():
    print(repr(key), "=", "set" if value else repr(value))
```

The script now accepts either `NCBI_API_KEY` or `NCBI_KEY`, but new code should
prefer `NCBI_API_KEY` because it is explicit.

## BioProject Search Finds Nothing

A warning like this means the BioProject accession did not resolve in
`db=bioproject`:

```text
No BioProject found for 'PRJNA...'
```

Possible causes:

- The accession is mistyped.
- The accession is not public yet.
- The accession belongs to a different archive prefix or database.
- The search field is too narrow.

The script currently searches with:

```text
PRJNA...[PRJNA]
```

If an accession looks valid but does not resolve, try the broader BioProject
field:

```text
PRJNA...[PRJA]
```

or a plain accession search in `db=bioproject`.

## No GEO Links Exist

This warning means `ELink` did not return a usable link from BioProject to
`gds`:

```text
No GEO links found for 'PRJNA...'
```

This can be a real result. Not every BioProject has a GEO Series. Many
BioProjects only link to SRA, BioSample, Assembly, or other records. Some
studies may have sequencing reads without a GEO expression submission.

For suspicious cases, check alternate routes:

```text
BioProject -> BioSample -> GEO sample IDs
BioProject -> SRA -> GEO sample/library names
BioProject -> SRA -> BioSample -> identifiers field
```

GEO links may be present at the sample level even when the project-level
BioProject to GDS link is absent.

## ELink History May Be Missing a Query Key

The normal `ELink` request uses `cmd=neighbor_history`, which should return a
history object like this:

```json
{
  "dbto": "gds",
  "linkname": "bioproject_gds",
  "querykey": "2"
}
```

Sometimes a link response can be valid but not provide a usable `querykey`.
This produced warnings like:

```text
No usable GEO link history found for 'PRJNA...'
```

These accessions may still work. In this project, all 27 BioProjects with that
warning resolved through the fallback path:

```text
ELink cmd=neighbor -> direct GEO UIDs -> ESummary by id
```

Examples that resolved through the fallback:

```text
PRJNA1214811 -> GSE287850
PRJNA1088541 -> GSE261747
PRJNA672686  -> GSE160231
PRJNA1169288 -> GSE287250, GSE287249, GSE287248
```

The script now tries this direct-UID fallback before giving up.

## Too Many UIDs in JSON ESummary

NCBI can return this error when a JSON `ESummary` request tries to return too
many linked records at once:

```json
{
  "header": {
    "type": "esummary",
    "version": "0.3"
  },
  "error": "Too many UIDs in request. Maximum number of UIDs is 500 for JSON format output."
}
```

This is an API-level JSON response, not an HTTP error. Code that blindly reads
`summary["result"]["uids"]` will fail with:

```text
KeyError: 'result'
```

Handle this by paging requests:

```text
retmax=500
retstart=0, 500, 1000, ...
```

For direct ID summaries, split `id=` lists into batches of at most 500 IDs.

## Valid JSON Without `result`

E-utilities can return a valid JSON object that has `header` and `error` but no
`result`. The parser must treat this as a failed lookup row rather than raising
an exception.

Bad assumption:

```python
for uid in summary["result"]["uids"]:
    ...
```

Safer pattern:

```python
result = summary.get("result")
if not isinstance(result, dict):
    return failure_row
```

The failure row should preserve the BioProject accession and mark
`success=False`, so downstream checks can distinguish "queried but unresolved"
from "never queried".

## Rate Limits and Transient HTTP Failures

NCBI requests may fail transiently with timeouts, connection errors, or HTTP
status codes such as:

```text
429, 500, 502, 503, 504
```

Use retries with exponential backoff for those cases. The script retries
transport errors, timeouts, and retryable HTTP status codes.

Also throttle request starts. With an API key, NCBI allows a higher request
rate, but staying below the limit is still safer for long batches. The current
script uses a small delay between request starts.

## Progress Bar and Logs

The command should keep the terminal mostly reserved for the progress bar:

```bash
uv run scripts/query_bioproject2geo.py
```

Warnings and errors are written to:

```text
data/query_results/bioproject2geo.log
```

The log file is opened in write mode, so each full run replaces the previous
log. Keep a copy before rerunning if the old warnings need to be audited.

The summary table is written to:

```text
data/query_results/bioproject2geo_summary.csv
```

## Updating Missed Rows

When a bug affects only a known class of rows, it is safer to rerun just those
BioProjects and splice the recovered rows into the existing table. For example,
the `No usable GEO link history found for` rows were recovered by:

1. Reading the affected BioProject accessions from the log.
2. Rerunning only those accessions through the fallback path.
3. Removing their old failed placeholder rows from the CSV.
4. Appending or inserting the recovered successful GSE rows.
5. Verifying that none of those accessions still have `success=False`.

In the observed run:

```text
missed_accessions: 27
resolved: 27
rows_for_missed: 29
failed_rows_for_missed: 0
```

The recovered row count was larger than the missed accession count because one
BioProject linked to three GEO Series records.

## Recommended Defensive Checks

Before treating a BioProject as unresolved, check:

- Did `ESearch` find a BioProject UID?
- Did `ELink cmd=neighbor_history` return `linksetdbhistories`?
- If history failed, did `ELink cmd=neighbor` return direct `gds` links?
- Did `ESummary` return `result.uids`?
- If not, did it return an `error` field?
- Are there more than 500 UIDs requiring pagination or batching?
- Are GEO links present through BioSample or SRA even if BioProject lacks a
  direct GDS link?

The key lesson is that "no usable history link" is not the same as "no GEO
link". Always try the direct `cmd=neighbor` fallback before marking the
BioProject as unresolved.
