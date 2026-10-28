"""Compare what BioStudies serves for each ArrayExpress IDF and SDRF with what it registers.

Usage::

    uv run python scripts/biostudies_served_size_census.py census \\
        --corpus sample:3000@arrayexpress-ena --out data/benchmarks/biostudies-served-size/<date>
    uv run python scripts/biostudies_served_size_census.py recheck --out <same dir>
    uv run python scripts/biostudies_served_size_census.py throttle --out <same dir>
    uv run python scripts/biostudies_served_size_census.py report --out <same dir>

On 2026-10-28 BioStudies served some IDFs cut off at exactly 4,096 bytes, mid-line,
with HTTP 200: E-GEOD-58432's is registered at 4,171 bytes, and its
``Comment[SequenceDataURI]`` stopped at ``http://www.ebi.ac.uk/``. Nothing in a cut
file says it was cut. This measures how often that happens and what it would cost.
That afternoon it served E-MTAB-11257's IDF as an Apache listing of the file's own
path, 837 bytes with HTTP 200, which every IDF reader read as naming nothing; the
census flags a body like that in ``directory_index``.

``census``: per study, the registered size of every IDF and SDRF -- one listing
request, filtered server-side to ``.txt`` names -- and what
``biostudies.served_file`` is served for each: the request every route's reader
makes, before ``study_file`` checks it. With its Content-Length, ETag,
Last-Modified and Content-Type, and whether it is a directory listing. For an IDF
it also records where the
last line of each field a route reads ends, so what a cut at a given length would
lose is a measurement. A body that differs from its registered size is kept under
``served/``. Resumable by study.

``recheck``: every file served short, long, as a directory listing, at exactly
4,096 bytes or not at all, asked again one at a time, with its legacy ArrayExpress
copy.

``throttle``: every IDF the census reached asked for again through the same
reader at a chosen concurrency, and every 403 asked again one at a time.
``ftp.ebi.ac.uk`` answered 244 of 3,000 such requests with a 403 at concurrency
10 on 2026-10-28.

``report``: the tables the knowledge-base page quotes, from the committed CSVs.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import re
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import httpx

from fetch_series.logging_utils import configure_logging, redact_exception, run_logfile
from fetch_series.providers import biostudies
from fetch_series.survey import corpora
from fetch_series.survey.client import Limits, SurveyClient

csv.field_size_limit(sys.maxsize)

# The length every cut copy seen on 2026-10-28 was cut to.
CUT = 4096
LEGACY = "https://ftp.ebi.ac.uk/pub/databases/arrayexpress/data/experiment"
# The last line of each IDF field a route or reader uses. Quoted rows
# (E-ERAD-355's `"Comment [SequenceDataURI]"`) count: the bytes are there whether
# or not this project's reader can parse them yet.
FIELDS = {
    "secondary": re.compile(rb'^"?Comment\s*\[SecondaryAccession\]', re.MULTILINE),
    "related": re.compile(rb'^"?Comment\s*\[RelatedExperiment\]', re.MULTILINE),
    "sequence": re.compile(rb'^"?Comment\s*\[SequenceDataURI\]', re.MULTILINE),
    "sdrf_file": re.compile(rb'^"?SDRF\s*File', re.MULTILINE | re.IGNORECASE),
}
COLUMNS = (
    "study",
    "name",
    "kind",
    "registered_bytes",
    "status",
    "served_bytes",
    "content_length",
    "etag",
    "last_modified",
    "final_host",
    "content_type",
    "directory_index",
    "ends_newline",
    *(f"{field}_end" for field in FIELDS),
    "error",
    "fetched_at",
)
RECHECK = (
    "study",
    "name",
    "registered_bytes",
    "census_bytes",
    "status",
    "served_bytes",
    "etag",
    "directory_index",
    "legacy_status",
    "legacy_bytes",
    "legacy_etag",
    "error",
    "fetched_at",
)
THROTTLE = (
    "study",
    "name",
    "concurrency",
    "status",
    "host",
    "registered_bytes",
    "served_bytes",
    "error",
    "retry_status",
    "fetched_at",
)


def now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def kind_of(name: str, declared: str = "") -> str:
    if declared == "IDF File" or name.endswith(".idf.txt"):
        return "idf"
    return "sdrf"


def field_ends(body: bytes) -> dict[str, int | str]:
    """Where the last line of each field ends, in bytes; empty if the IDF has none."""
    ends: dict[str, int | str] = {}
    for field, pattern in FIELDS.items():
        last: int | str = ""
        for match in pattern.finditer(body):
            newline = body.find(b"\n", match.start())
            last = len(body) if newline < 0 else newline
        ends[f"{field}_end"] = last
    return ends


async def registered(client: SurveyClient, study: str) -> dict[str, tuple[str, int | None]]:
    """``name -> (kind, size)`` for every IDF and SDRF the study registers.

    ``search=.txt`` filters server-side, and the listing then pages on its
    ``filtered`` count. A study whose usual IDF the filter does not return is
    listed in full, so a filter that misses a file cannot read as "unregistered".
    """
    found: dict[str, tuple[str, int | None]] = {}

    def keep(items: list[dict[str, object]]) -> None:
        for item in items:
            name = str(item.get("path") or item.get("Name") or "")
            declared = str(item.get("Type") or "")
            if declared in ("IDF File", "SDRF File") or name.endswith((".idf.txt", ".sdrf.txt")):
                size = item.get("Size")
                found[name] = (kind_of(name, declared), size if isinstance(size, int) else None)

    offset = 0
    while True:
        response = await client.get(
            f"{biostudies.API}/studies/{study}/files",
            params={"search": ".txt", "limit": biostudies.PAGE_SIZE, "offset": offset},
            timeout=60,
        )
        payload = response.json()
        page = payload.get("items") or [] if isinstance(payload, dict) else payload
        keep(page)
        pagination = payload.get("pagination") or {} if isinstance(payload, dict) else {}
        offset += biostudies.PAGE_SIZE
        if not page or offset >= pagination.get("filtered", len(page)):
            break
    if f"{study}.idf.txt" not in found:
        keep(await biostudies.registered_files(client, study, 60))
    return found


async def served(client: SurveyClient, study: str, name: str) -> tuple[dict[str, object], bytes]:
    """What BioStudies serves for one file, through ``biostudies.served_file``: unchecked."""
    row: dict[str, object] = {"study": study, "name": name, "fetched_at": now()}
    try:
        response = await biostudies.served_file(client, study, name, 60)
    except httpx.HTTPStatusError as exc:
        row["status"] = exc.response.status_code
        row["final_host"] = exc.request.url.host
        return row, b""
    except Exception as exc:
        # The message says how much of the body arrived, which is the point:
        # httpx reports a connection closed mid-body as "received N bytes,
        # expected M" (RemoteProtocolError).
        safe = redact_exception(exc)
        row["error"] = f"{type(safe).__name__}: {str(safe)[:200]}"
        return row, b""
    body = response.content
    row.update(
        status=response.status_code,
        served_bytes=len(body),
        content_length=response.headers.get("content-length", ""),
        etag=response.headers.get("etag", ""),
        last_modified=response.headers.get("last-modified", ""),
        final_host=response.url.host,
        content_type=response.headers.get("content-type", ""),
        directory_index=int(biostudies.is_directory_index(body)),
        ends_newline=int(body.endswith(b"\n")),
    )
    return row, body


async def census(args: argparse.Namespace) -> None:
    path = args.out / "files.csv"
    done: set[str] = set()
    if path.exists():
        with path.open() as handle:
            done = {r["study"] for r in csv.DictReader(handle)}
    else:
        with path.open("w", newline="") as handle:
            csv.DictWriter(handle, COLUMNS).writeheader()
    studies = [s for s in corpora.load(args.corpus).values if s not in done]
    tally: Counter[str] = Counter()
    limits = Limits(rps=args.rps, concurrency=args.concurrency, attempts=1, base_timeout=60)
    async with SurveyClient(limits) as client:

        async def one(study: str) -> list[dict[str, object]]:
            try:
                listing = await registered(client, study)
            except Exception as exc:
                error = type(redact_exception(exc)).__name__
                return [{"study": study, "error": f"listing: {error}", "fetched_at": now()}]
            names = dict(listing)
            names.setdefault(f"{study}.idf.txt", ("idf", None))
            names.setdefault(f"{study}.sdrf.txt", ("sdrf", None))
            rows = []
            for name, (kind, size) in sorted(names.items()):
                row, body = await served(client, study, name)
                row.update(kind=kind, registered_bytes="" if size is None else size)
                if kind == "idf" and body:
                    row.update(field_ends(body))
                if body and size is not None and len(body) != size:
                    kept = args.out / "served" / study / name
                    kept.parent.mkdir(parents=True, exist_ok=True)
                    kept.write_bytes(body)
                rows.append(row)
            return rows

        # One worker per connection, each taking the next study as it finishes
        # one. Batches of 24 made every study wait for the slowest: one 60 s
        # ReadTimeout (E-MTAB-15102's SDRF) held the other 23, which would have
        # made a census of arrayexpress-ena's 20,693 studies an 11-hour run.
        pending = iter(studies)
        finished = 0

        async def worker() -> None:
            nonlocal finished
            for study in pending:
                rows = await one(study)
                with path.open("a", newline="") as handle:
                    csv.DictWriter(handle, COLUMNS, restval="").writerows(rows)
                tally.update(verdict(row) for row in rows)
                finished += 1
                if finished % 100 == 0 or finished == len(studies):
                    print(f"{finished}/{len(studies)} {dict(tally)}", file=sys.stderr, flush=True)

        await asyncio.gather(*(worker() for _ in range(args.concurrency)))


def verdict(row: dict[str, object]) -> str:
    """One word for a census row: how its served size compares with its registered size."""
    if row.get("error"):
        return "error"
    if str(row.get("status")) != "200":
        return f"http_{row.get('status')}"
    # Before size: E-MTAB-11257's IDF was served as an 837-byte listing, which
    # would read as "short", and an unregistered file can be one too.
    if str(row.get("directory_index")) == "1":
        return "listing"
    if row.get("registered_bytes") in ("", None):
        return "unregistered"
    served_bytes, size = int(str(row["served_bytes"])), int(str(row["registered_bytes"]))
    return "whole" if served_bytes == size else "short" if served_bytes < size else "long"


def legacy_url(study: str, name: str) -> str:
    return f"{LEGACY}/{study.split('-')[1]}/{study}/{name}"


async def recheck(args: argparse.Namespace) -> None:
    with (args.out / "files.csv").open() as handle:
        rows = list(csv.DictReader(handle))
    wanted = [
        r
        for r in rows
        if verdict(r) not in ("whole", "unregistered", "http_404")
        or (verdict(r) == "whole" and r["served_bytes"] == str(CUT))
    ]
    path = args.out / "recheck.csv"
    async with SurveyClient(Limits(rps=1.0, concurrency=1, attempts=1, base_timeout=60)) as client:
        with path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, RECHECK, restval="")
            writer.writeheader()
            for row in wanted:
                study, name = row["study"], row["name"]
                if not name:
                    continue
                again, _ = await served(client, study, name)
                out: dict[str, object] = {
                    "study": study,
                    "name": name,
                    "registered_bytes": row["registered_bytes"],
                    "census_bytes": row["served_bytes"],
                    "status": again.get("status", ""),
                    "served_bytes": again.get("served_bytes", ""),
                    "etag": again.get("etag", ""),
                    "directory_index": again.get("directory_index", ""),
                    "error": again.get("error", ""),
                    "fetched_at": again["fetched_at"],
                }
                try:
                    legacy = await client.get(legacy_url(study, name), timeout=60)
                    out.update(
                        legacy_status=legacy.status_code,
                        legacy_bytes=len(legacy.content),
                        legacy_etag=legacy.headers.get("etag", ""),
                    )
                except httpx.HTTPStatusError as exc:
                    out["legacy_status"] = exc.response.status_code
                except Exception as exc:
                    out["legacy_status"] = type(redact_exception(exc)).__name__
                writer.writerow(out)
                handle.flush()
                print(out, file=sys.stderr, flush=True)


async def throttle(args: argparse.Namespace) -> None:
    with (args.out / "files.csv").open() as handle:
        idfs = [
            (r["study"], r["name"], r["registered_bytes"])
            for r in csv.DictReader(handle)
            if r["kind"] == "idf" and r["status"] == "200"
        ]
    limits = Limits(rps=args.rps, concurrency=args.concurrency, attempts=1, base_timeout=60)
    rows: list[dict[str, object]] = []
    async with SurveyClient(limits) as client:

        async def one(study: str, name: str, size: str) -> dict[str, object]:
            row, _ = await served(client, study, name)
            return {
                "study": study,
                "name": name,
                "concurrency": args.concurrency,
                "status": row.get("status", ""),
                "host": row.get("final_host", ""),
                "registered_bytes": size,
                "served_bytes": row.get("served_bytes", ""),
                "error": row.get("error", ""),
                "fetched_at": row["fetched_at"],
            }

        rows = list(await asyncio.gather(*(one(s, n, b) for s, n, b in idfs)))
    print(Counter((r["status"], r["host"]) for r in rows), file=sys.stderr, flush=True)
    async with SurveyClient(Limits(rps=1.0, concurrency=1, attempts=1, base_timeout=60)) as client:
        for row in rows:
            if str(row["status"]) == "200":
                continue
            again, _ = await served(client, str(row["study"]), str(row["name"]))
            row["retry_status"] = again.get("status") or again.get("error", "")
    with (args.out / f"throttle-c{args.concurrency}.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, THROTTLE, restval="")
        writer.writeheader()
        writer.writerows(rows)


def report(args: argparse.Namespace) -> None:
    with (args.out / "files.csv").open() as handle:
        rows = list(csv.DictReader(handle))
    studies = {r["study"] for r in rows}
    print(f"studies {len(studies):,}, rows {len(rows):,}")
    print(f"fetched {min(r['fetched_at'] for r in rows)} .. {max(r['fetched_at'] for r in rows)}")
    for kind in ("idf", "sdrf"):
        print(kind, dict(Counter(verdict(r) for r in rows if r["kind"] == kind)))
    listings = [r for r in rows if r.get("directory_index") == "1"]
    print(
        f"directory listings {len(listings):,} of {len(rows):,} files",
        Counter(r["kind"] for r in listings),
        [(r["study"], r["name"], r["served_bytes"], r["fetched_at"]) for r in listings],
    )
    # Whether a listing lasts: what each was served when asked again one at a time.
    again_path = args.out / "recheck.csv"
    if listings and again_path.exists():
        with again_path.open() as handle:
            again = {(r["study"], r["name"]): r for r in csv.DictReader(handle)}
        for r in listings:
            a = again.get((r["study"], r["name"]), {})
            print(
                f"  {r['name']}: asked again {a.get('fetched_at', '-')}, status "
                f"{a.get('status', '-')}, {a.get('served_bytes', '-')} bytes, listing "
                f"{a.get('directory_index', '-')}; registered {r['registered_bytes']}, "
                f"legacy {a.get('legacy_bytes', '-')}"
            )
    print(
        "served at exactly",
        CUT,
        [(r["study"], r["name"]) for r in rows if r["served_bytes"] == str(CUT)],
    )
    print(
        "not whole",
        [
            (r["study"], r["name"], r["registered_bytes"], r["served_bytes"])
            for r in rows
            if verdict(r) in ("short", "long")
        ],
    )
    idfs = [r for r in rows if r["kind"] == "idf" and r["status"] == "200" and r["served_bytes"]]
    longer = [r for r in idfs if int(r["served_bytes"]) > CUT]
    print(f"IDFs served {len(idfs):,}; longer than {CUT}: {len(longer):,}")
    for field in FIELDS:
        has = [r for r in idfs if r[f"{field}_end"]]
        past = [r for r in has if int(r[f"{field}_end"]) > CUT]
        print(f"  {field}: in {len(has):,} IDFs; ends past {CUT} in {len(past):,}")
    # What a cut at CUT would cost each IDF longer than that, and whether the
    # absence of an `SDRF File` line -- free to check -- would have shown it.
    read = ("secondary", "related", "sequence")
    exposure = Counter(
        (
            any(r[f"{f}_end"] and int(r[f"{f}_end"]) > CUT for f in read),
            bool(r["sdrf_file_end"]) and int(r["sdrf_file_end"]) > CUT,
        )
        for r in longer
    )
    print("  (loses a field a route reads, loses its SDRF File line):", dict(exposure))
    sizes = Counter(
        int(r["registered_bytes"]) for r in rows if r["kind"] == "idf" and r["registered_bytes"]
    )
    print(
        "registered IDF sizes near the cut",
        {n: sizes[n] for n in range(CUT - 8, CUT + 9) if sizes[n]},
    )
    by_study: dict[str, list[dict[str, str]]] = {}
    for r in rows:
        by_study.setdefault(r["study"], []).append(r)
    split = [
        s
        for s, rs in by_study.items()
        if not any(r["name"] == f"{s}.sdrf.txt" and r["registered_bytes"] for r in rs)
        and any(r["kind"] == "sdrf" and r["registered_bytes"] for r in rs)
    ]
    print(f"studies whose SDRF is not <study>.sdrf.txt: {len(split):,}")
    # The split-SDRF fallback reads the IDF's `SDRF File` line, so a cut that
    # lands before that line ends turns the study's SDRF into a 404.
    for study in sorted(split):
        idf = next((r for r in by_study[study] if r["name"] == f"{study}.idf.txt"), None)
        if idf is not None:
            print(
                f"  {study}: IDF {idf['served_bytes']} bytes, SDRF File ends at {idf['sdrf_file_end']}"
            )
    no_sdrf_line = [r["study"] for r in idfs if not r["sdrf_file_end"]]
    print(f"IDFs served whole with no SDRF File line: {len(no_sdrf_line):,} {no_sdrf_line[:20]}")
    print(
        "a body of exactly 4,096 bytes, and its last byte",
        Counter(r["ends_newline"] for r in idfs if r["served_bytes"] == str(CUT)),
    )
    for name in sorted(args.out.glob("throttle-c*.csv")):
        with name.open() as handle:
            t = list(csv.DictReader(handle))
        print(name.name, dict(Counter((r["status"] or r["error"], r["host"]) for r in t)))
        print(
            "  served short",
            [
                (r["study"], r["registered_bytes"], r["served_bytes"])
                for r in t
                if r["served_bytes"]
                and r["registered_bytes"]
                and int(r["served_bytes"]) < int(r["registered_bytes"])
            ],
        )
        print("  retried", dict(Counter(r["retry_status"] for r in t if r["retry_status"])))


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("phase", choices=("census", "recheck", "throttle", "report"))
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--corpus", default="sample:3000@arrayexpress-ena")
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--rps", type=float, default=8.0)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    if args.phase == "report":
        report(args)
        return
    configure_logging(logfile=run_logfile(f"biostudies-served-size-{args.phase}"))
    asyncio.run({"census": census, "recheck": recheck, "throttle": throttle}[args.phase](args))


if __name__ == "__main__":
    main()
