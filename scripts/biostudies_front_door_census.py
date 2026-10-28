"""Ask BioStudies' file address for every ArrayExpress IDF and SDRF, without the mirror.

Usage::

    uv run python scripts/biostudies_front_door_census.py census \\
        --corpus sample:3000@arrayexpress-ena --out data/benchmarks/biostudies-front-door/<date>
    uv run python scripts/biostudies_front_door_census.py recheck --out <same dir>
    uv run python scripts/biostudies_front_door_census.py report --out <same dir>

On 2026-10-26 ``biostudies.FILES`` answered 404 for 326 IDFs that 3,000
ArrayExpress studies register, and the readers have read such a file from the
FTP mirror since (docs/pathologies/biostudies-files-404s-registered-files.md).
That fallback hides the 404 from everything downstream, including the census of
served sizes: a file read through it ends on the same host as one the front
door redirected. This asks the front door alone.

``census``: per study, the names its file listing registers, and one GET for
``<study>.idf.txt`` and one for ``<study>.sdrf.txt`` at ``FILES`` with
redirects not followed, so the answer recorded is the front door's own and
only ``www.ebi.ac.uk`` is asked. Then, one at a time, the mirror for the rows
where the front door and the listing disagree: a 404 for a registered file
(the mirror its ``info`` names), and a redirect for a file the study does not
register (where the redirect points). Resumable by study.

``recheck``: the studies whose IDF or SDRF the route surveys recorded as
failing on 2026-10-26 -- re-surveyed on 2026-10-27, once the fallback had
landed, so their rows are the ones dated then: 328 IDFs, E-MTAB-13459's among
them, and 204 SDRFs. Each asked ``--tries`` times at the front door, one at a
time. Whether those files still 404 is a narrower question than whether any
file does, and it is the one the inverted test asked.

``report``: the tables the knowledge-base page quotes, from the committed CSVs.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import gzip
import sys
from collections import Counter
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path

import httpx

from fetch_series.logging_utils import configure_logging, redact_exception, run_logfile
from fetch_series.providers import biostudies
from fetch_series.survey import corpora
from fetch_series.survey.client import Limits, SurveyClient, is_retryable

csv.field_size_limit(sys.maxsize)

ATTEMPTS = 3
SURVEYS = Path("data/surveys/sample_3000@arrayexpress-ena")
FAILED_ON_1026 = {
    "idf": SURVEYS / "ae_experiment-to-run_biostudies_idf.csv.gz",
    "sdrf": SURVEYS / "ae_experiment-to-run_biostudies_sdrf_comments.csv.gz",
}
COLUMNS = (
    "study",
    "name",
    "kind",
    "registered",
    "status",
    "location",
    "attempts",
    "error",
    "fetched_at",
    "mirror",
    "mirror_status",
)
RECHECK = ("study", "name", "kind", "try", "status", "location", "error", "fetched_at")


def now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def names(study: str) -> list[tuple[str, str]]:
    return [(f"{study}.idf.txt", "idf"), (f"{study}.sdrf.txt", "sdrf")]


def described(exc: BaseException) -> str:
    safe = redact_exception(exc)
    return f"{type(safe).__name__}: {str(safe)[:200]}"


async def retrying[T](call: Callable[[], Awaitable[T]]) -> T:
    """``call()``, asked again on anything ``is_retryable`` recognises."""
    for attempt in range(1, ATTEMPTS):
        try:
            return await call()
        except Exception as exc:
            if not is_retryable(exc):
                raise
        await asyncio.sleep(2**attempt)
    return await call()


async def front_door(client: SurveyClient, study: str, name: str) -> dict[str, object]:
    """The front door's own answer for one file: its status, and where a redirect points.

    ``client`` must not follow redirects. A 3xx or a 404 is the answer; anything
    ``is_retryable`` recognises is asked again, so a 429 or a 503 is never
    recorded as the front door's verdict on a file.
    """
    row: dict[str, object] = {"study": study, "name": name}
    for attempt in range(1, ATTEMPTS + 1):
        row.update(attempts=attempt, fetched_at=now(), error="", status="", location="")
        try:
            response = await client.get(f"{biostudies.FILES}/{study}/{name}", timeout=60)
            row["status"] = response.status_code
            return row
        except httpx.HTTPStatusError as exc:
            if not is_retryable(exc) or attempt == ATTEMPTS:
                row["status"] = exc.response.status_code
                row["location"] = exc.response.headers.get("location", "")
                return row
        except Exception as exc:
            row["error"] = described(exc)
            if not is_retryable(exc) or attempt == ATTEMPTS:
                return row
        await asyncio.sleep(2**attempt)
    return row


async def listed(client: SurveyClient, study: str) -> set[str] | str:
    """The names a study's file listing registers, or why it could not be read."""
    try:
        items = await retrying(lambda: biostudies.registered_files(client, study, 60))
    except Exception as exc:
        return described(exc)
    return {str(item.get("path") or item.get("Name") or "") for item in items}


async def on_the_mirror(client: SurveyClient, row: dict[str, object]) -> None:
    """Where the front door and the listing disagree, what the mirror answers.

    A 404 for a registered file: the mirror the study's ``info`` names, which is
    what ``study_file`` falls back to. A redirect for a file the study does not
    register: wherever the redirect points.
    """
    study, name = str(row["study"]), str(row["name"])
    if str(row["status"]) == "404":
        try:
            info = await retrying(
                lambda: client.get(f"{biostudies.API}/studies/{study}/info", timeout=60)
            )
            link = info.json().get("httpLink")
        except Exception as exc:
            row["mirror"] = described(exc)
            return
        if not isinstance(link, str) or not link.startswith(biostudies.MIRROR):
            row["mirror"] = "none"
            return
        target = f"{link}/Files/{name}"
    else:
        target = str(row["location"])
    row["mirror"] = target
    try:
        row["mirror_status"] = (await client.head(target, timeout=60)).status_code
    except Exception as exc:
        row["mirror_status"] = described(exc)


async def census(args: argparse.Namespace) -> None:
    path = args.out / "front-door.csv"
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
    one_at_a_time = Limits(rps=2.0, concurrency=1, attempts=1, base_timeout=60)
    async with (
        httpx.AsyncClient(follow_redirects=False) as raw,
        SurveyClient(limits, client=raw) as door,
        SurveyClient(one_at_a_time) as mirror,
    ):

        async def one(study: str) -> list[dict[str, object]]:
            registered = await listed(door, study)
            rows = []
            for name, kind in names(study):
                row = await front_door(door, study, name)
                row["kind"] = kind
                row["registered"] = (
                    registered if isinstance(registered, str) else int(name in registered)
                )
                rows.append(row)
            return rows

        for start in range(0, len(studies), 24):
            batch = studies[start : start + 24]
            rows = [row for rows in await asyncio.gather(*(one(s) for s in batch)) for row in rows]
            for row in rows:
                redirected = str(row["status"]).startswith("3")
                if (str(row["status"]) == "404" and row["registered"] == 1) or (
                    redirected and row["registered"] == 0
                ):
                    await on_the_mirror(mirror, row)
            with path.open("a", newline="") as handle:
                csv.DictWriter(handle, COLUMNS, restval="").writerows(rows)
            tally.update(
                f"{r['kind']}:{r['registered']}:{r['status'] or str(r['error'])[:20]}" for r in rows
            )
            print(f"{start + len(batch)}/{len(studies)} {dict(tally)}", file=sys.stderr, flush=True)


def failed_on_1026(kind: str) -> list[str]:
    with gzip.open(FAILED_ON_1026[kind], "rt") as handle:
        return sorted(
            r["accession"]
            for r in csv.DictReader(handle)
            if r["fetched_at"].startswith("2026-10-27")
        )


async def recheck(args: argparse.Namespace) -> None:
    wanted = [
        (study, f"{study}.{kind}.txt", kind)
        for kind in FAILED_ON_1026
        for study in failed_on_1026(kind)
    ]
    print(dict(Counter(kind for _, _, kind in wanted)), file=sys.stderr, flush=True)
    tally: Counter[str] = Counter()
    limits = Limits(rps=2.0, concurrency=1, attempts=1, base_timeout=60)
    async with (
        httpx.AsyncClient(follow_redirects=False) as raw,
        SurveyClient(limits, client=raw) as door,
    ):
        with (args.out / "recheck.csv").open("w", newline="") as handle:
            writer = csv.DictWriter(handle, RECHECK, restval="", extrasaction="ignore")
            writer.writeheader()
            for attempt in range(1, args.tries + 1):
                for study, name, kind in wanted:
                    row = await front_door(door, study, name)
                    row.update(kind=kind)
                    row["try"] = attempt
                    writer.writerow(row)
                    tally[f"{kind}:{row['status'] or str(row['error'])[:20]}"] += 1
                handle.flush()
                print(f"try {attempt}: {dict(tally)}", file=sys.stderr, flush=True)


def report(args: argparse.Namespace) -> None:
    with (args.out / "front-door.csv").open() as handle:
        rows = list(csv.DictReader(handle))
    print(f"studies {len({r['study'] for r in rows}):,}, requests {len(rows):,}")
    print(f"fetched {min(r['fetched_at'] for r in rows)} .. {max(r['fetched_at'] for r in rows)}")
    for kind in ("idf", "sdrf"):
        of_kind = [r for r in rows if r["kind"] == kind]
        print(kind, "(registered, status):")
        for key, n in sorted(
            Counter((r["registered"], r["status"] or r["error"]) for r in of_kind).items()
        ):
            print(f"  {key} {n:,}")
        print("  attempts", dict(Counter(r["attempts"] for r in of_kind)))
        # Every redirect should point at the file under the study's mirror directory.
        elsewhere = [
            r
            for r in of_kind
            if r["location"]
            and not (
                r["location"].startswith(biostudies.MIRROR)
                and r["location"].endswith(f"/{r['study']}/Files/{r['name']}")
            )
        ]
        print("  redirected elsewhere", [(r["study"], r["location"]) for r in elsewhere])
        for r in of_kind:
            if r["mirror"]:
                print(
                    "   ", r["study"], r["name"], r["registered"], r["status"], r["mirror_status"]
                )
    recheck_path = args.out / "recheck.csv"
    if recheck_path.exists():
        with recheck_path.open() as handle:
            again = list(csv.DictReader(handle))
        print(
            f"recheck {min(r['fetched_at'] for r in again)} .. {max(r['fetched_at'] for r in again)}"
        )
        for kind in ("idf", "sdrf"):
            of_kind = [r for r in again if r["kind"] == kind]
            print(
                kind,
                f"{len({r['study'] for r in of_kind}):,} studies",
                dict(Counter((r["try"], r["status"] or r["error"]) for r in of_kind)),
            )
            print(
                "  404 at least once", sorted({r["study"] for r in of_kind if r["status"] == "404"})
            )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("phase", choices=("census", "recheck", "report"))
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--corpus", default="sample:3000@arrayexpress-ena")
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--rps", type=float, default=6.0)
    parser.add_argument("--tries", type=int, default=3)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    if args.phase == "report":
        report(args)
        return
    configure_logging(logfile=run_logfile(f"biostudies-front-door-{args.phase}"))
    asyncio.run({"census": census, "recheck": recheck}[args.phase](args))


if __name__ == "__main__":
    main()
