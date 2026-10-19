"""The committed form of the survey cache: one gzipped CSV per corpus and unit.

Writing and reading live together so the two cannot drift. ``fetch survey
export`` writes these; :func:`read_export` reads them back, which is how a
``results-of:`` corpus is rebuilt on a machine that does not hold the cache --
the cache is deliberately uncommitted, so without this a derived population
existed only on the laptop that surveyed it.
"""

from __future__ import annotations

import csv
import gzip
import json
import sys
from collections.abc import Iterable, Iterator
from datetime import datetime
from pathlib import Path

from fetch_series.results import Check, Outcome, RouteResult
from fetch_series.units import parse_unit

SURVEYS_DIR = Path("data/surveys")

SURVEY_COLUMNS = (
    "accession",
    "outcome",
    "n_results",
    "results",
    "error_class",
    "error_message",
    "latency_ms",
    "fetched_at",
)
# A link verdict carries the checks a found file or document was put through,
# and what they were made on. A route's export does not: its columns are cited
# by RouteEvidence and must stay byte-identical.
LINK_COLUMNS = (*SURVEY_COLUMNS, "checks", "checked")
SCREEN_COLUMNS = (*SURVEY_COLUMNS, "detail")

LFS_POINTER = b"version https://git-lfs"


def slug(unit_id: str) -> str:
    """Filesystem-safe form of a route id, unit id or corpus name.

    Route ids read well in a terminal ("gse->experiment:soft_family") and badly
    as directory names -- ">" needs quoting in every shell that would otherwise
    tab-complete the path.
    """
    return unit_id.replace("->", "-to-").replace(":", "_").replace("/", "_")


def columns_for(unit_id: str) -> tuple[str, ...]:
    kind = parse_unit(unit_id).kind
    if kind == "link":
        return LINK_COLUMNS
    if kind == "screen":
        return SCREEN_COLUMNS
    return SURVEY_COLUMNS


def format_checks(checks: Iterable[Check]) -> str:
    """``downloadable=true;complete=`` -- an empty value is undetermined, not false."""
    shown = {True: "true", False: "false", None: ""}
    return ";".join(f"{name}={shown[passed]}" for name, passed in checks)


def parse_checks(text: str) -> tuple[Check, ...]:
    read = {"true": True, "false": False, "": None}
    if not text:
        return ()
    return tuple(
        (name, read[value]) for name, _, value in (item.partition("=") for item in text.split(";"))
    )


def row_for(result: RouteResult) -> list[object]:
    row: list[object] = [
        result.accession,
        result.outcome.value,
        result.n_results,
        ";".join(result.results),
        result.error_class or "",
        (result.error_message or "").replace("\n", " "),
        result.latency_ms,
        result.fetched_at.isoformat(),
    ]
    kind = parse_unit(result.route_id).kind
    detail = result.detail or {}
    if kind == "link":
        row += [
            format_checks(tuple(c) for c in detail.get("checks", ())),
            detail.get("checked") or "",
        ]
    elif kind == "screen":
        row.append(json.dumps(detail, sort_keys=True) if result.detail is not None else "")
    return row


def write_export(
    directory: Path, corpus: str, unit_id: str, results: Iterable[RouteResult]
) -> tuple[Path, int]:
    """Write one unit's verdicts over one corpus, and the manifest naming the corpus."""
    directory.mkdir(parents=True, exist_ok=True)
    # Gzipped: the results column holds joined accession lists, which compress
    # 18x. 92 MB of CSV becomes 15 MB, and the user pays for LFS. `zgrep`,
    # `zcat` and `pandas.read_csv` all read it directly.
    path = directory / f"{slug(unit_id)}.csv.gz"
    rows = 0
    with gzip.open(path, "wt", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns_for(unit_id))
        for result in results:
            writer.writerow(row_for(result))
            rows += 1
    (directory / "manifest.txt").write_text(f"corpus: {corpus}\n")
    return path, rows


def is_lfs_pointer(path: Path) -> bool:
    with path.open("rb") as fh:
        return fh.read(len(LFS_POINTER)) == LFS_POINTER


def export_path(corpus: str, unit_id: str, root: Path | None = None) -> Path | None:
    """The committed export of ``unit_id`` over ``corpus``, if there is one.

    Found through the manifest, not the slug: the slug is not reversible, and
    the manifest holds the corpus name exactly as it was recorded.
    """
    directory = (root or SURVEYS_DIR) / slug(corpus)
    manifest = directory / "manifest.txt"
    path = directory / f"{slug(unit_id)}.csv.gz"
    if not manifest.exists() or not path.exists():
        return None
    if manifest.read_text().strip() != f"corpus: {corpus}":
        return None
    return path


def read_export(path: Path, corpus: str, unit_id: str) -> Iterator[RouteResult]:
    """Verdicts back out of an export. Raises on a Git LFS pointer, which is not data."""
    if is_lfs_pointer(path):
        raise FileNotFoundError(f"{path} is a Git LFS pointer; run `git lfs pull`")
    kind = parse_unit(unit_id).kind
    # The results column of a large BioProject holds thousands of joined runs,
    # far past csv's default 128 KiB field limit.
    csv.field_size_limit(sys.maxsize)
    with gzip.open(path, "rt", newline="") as handle:
        for row in csv.DictReader(handle):
            detail: dict[str, object] | None = None
            if kind == "link" and (row.get("checks") or row.get("checked")):
                detail = {
                    "checks": [list(c) for c in parse_checks(row["checks"])],
                    "checked": row["checked"] or None,
                }
            elif kind == "screen" and row.get("detail"):
                detail = json.loads(row["detail"])
            yield RouteResult(
                accession=row["accession"],
                route_id=unit_id,
                corpus=corpus,
                outcome=Outcome(row["outcome"]),
                results=tuple(v for v in row["results"].split(";") if v),
                error_class=row["error_class"] or None,
                error_message=row["error_message"] or None,
                latency_ms=int(row["latency_ms"]),
                fetched_at=datetime.fromisoformat(row["fetched_at"]),
                detail=detail,
            )


__all__ = [
    "LINK_COLUMNS",
    "SCREEN_COLUMNS",
    "SURVEYS_DIR",
    "SURVEY_COLUMNS",
    "columns_for",
    "export_path",
    "format_checks",
    "is_lfs_pointer",
    "parse_checks",
    "read_export",
    "row_for",
    "slug",
    "write_export",
]
