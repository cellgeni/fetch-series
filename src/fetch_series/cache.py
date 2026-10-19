"""SQLite store for survey results and cached responses.

Two jobs, deliberately in one file because they share a connection:

1. **Resume.** A survey over 12,756 accessions takes hours. Recording each
   verdict as it lands means an interrupted run continues rather than restarts,
   and a re-run only re-queries what has not been answered.
2. **Archive-state diffing.** Running the same corpus a month apart and
   comparing the two answers is how re-created BioProjects get caught
   automatically. ``PRJNA644294`` -> ``PRJNA644462`` and ``PRJNA735853`` ->
   ``PRJNA849641`` were both found by hand; they should not have to be.

The distinction that matters throughout is between *never queried*, *queried and
genuinely empty*, and *queried and failed*. Absence from ``survey_results`` means
the first. Conflating the second and third is what made "no GEO link exists"
look like a bug when it is usually a true negative.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fetch_series.results import Outcome, RouteResult

DEFAULT_CACHE_PATH = Path(".cache/fetch-series.sqlite")

SCHEMA = """
CREATE TABLE IF NOT EXISTS survey_results (
    corpus        TEXT NOT NULL,
    route_id      TEXT NOT NULL,
    accession     TEXT NOT NULL,
    outcome       TEXT NOT NULL,
    results       TEXT NOT NULL,   -- JSON array of target accessions
    n_results     INTEGER NOT NULL,
    error_class   TEXT,
    error_message TEXT,
    latency_ms    INTEGER NOT NULL,
    fetched_at    TEXT NOT NULL,
    detail        TEXT,            -- JSON object, or NULL: checks and what they were made on
    PRIMARY KEY (corpus, route_id, accession)
);

CREATE INDEX IF NOT EXISTS survey_results_route
    ON survey_results (route_id, outcome);

CREATE TABLE IF NOT EXISTS responses (
    cache_key  TEXT PRIMARY KEY,
    provider   TEXT NOT NULL,
    url        TEXT NOT NULL,
    status     INTEGER NOT NULL,
    body       BLOB NOT NULL,
    fetched_at TEXT NOT NULL
);
"""


class SurveyCache:
    """A SQLite-backed store of survey verdicts and raw responses."""

    def __init__(self, path: Path | str = DEFAULT_CACHE_PATH) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path)
        self._conn.row_factory = sqlite3.Row
        # WAL lets a long survey write while an analysis query reads.
        self._conn.execute("PRAGMA journal_mode=WAL")
        # WAL still allows only one writer at a time, and Python's default busy
        # timeout is five seconds. Two surveys running side by side -- the normal
        # way to cover NCBI and ENA routes at once -- would then abort hours of
        # work with "database is locked" rather than waiting its turn.
        self._conn.execute("PRAGMA busy_timeout=60000")
        self._conn.executescript(SCHEMA)
        self._migrate()
        self._conn.commit()

    def _migrate(self) -> None:
        """Bring a cache written by an earlier version up to this schema, in place.

        The cache holds months of verdicts that cannot be re-collected -- the
        archives have moved since -- so a schema change adds a column rather
        than asking for a fresh store. ``detail`` arrived with link surveys;
        every earlier row has none, which is what NULL says.
        """
        columns = {row["name"] for row in self._conn.execute("PRAGMA table_info(survey_results)")}
        if "detail" not in columns:
            self._conn.execute("ALTER TABLE survey_results ADD COLUMN detail TEXT")

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> SurveyCache:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- survey results ---------------------------------------------------

    def record(self, result: RouteResult) -> None:
        """Store one verdict, replacing any previous verdict for that cell."""
        self._write(result)
        self._conn.commit()

    def record_all(self, results: Iterable[RouteResult]) -> int:
        """Store several verdicts in one transaction: all of them, or none.

        A fan-out survey makes one request per accession and owes a verdict per
        link. Committing them one at a time would let an interruption leave an
        accession half-answered -- and resume would then ask only for the rest,
        so the two halves would come from requests made at different times.
        """
        n = 0
        with self._conn:
            for result in results:
                self._write(result)
                n += 1
        return n

    def _write(self, result: RouteResult) -> None:
        self._conn.execute(
            """
            INSERT INTO survey_results
                (corpus, route_id, accession, outcome, results, n_results,
                 error_class, error_message, latency_ms, fetched_at, detail)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (corpus, route_id, accession) DO UPDATE SET
                outcome=excluded.outcome, results=excluded.results,
                n_results=excluded.n_results, error_class=excluded.error_class,
                error_message=excluded.error_message,
                latency_ms=excluded.latency_ms, fetched_at=excluded.fetched_at,
                detail=excluded.detail
            """,
            (
                result.corpus,
                result.route_id,
                result.accession,
                result.outcome.value,
                json.dumps(list(result.results)),
                len(result.results),
                result.error_class,
                result.error_message,
                result.latency_ms,
                result.fetched_at.isoformat(),
                None if result.detail is None else json.dumps(result.detail, sort_keys=True),
            ),
        )

    def answered(self, corpus: str, route_id: str) -> set[str]:
        """Accessions already carrying a verdict, for resume.

        Failures count as answered. Re-running only the failures is
        :meth:`failed` plus an explicit re-query, not the default -- a survey
        that silently retried its failures would report a coverage number that
        no single run ever produced.
        """
        rows = self._conn.execute(
            "SELECT accession FROM survey_results WHERE corpus = ? AND route_id = ?",
            (corpus, route_id),
        )
        return {row["accession"] for row in rows}

    def answered_by(
        self, corpus: str, route_ids: Iterable[str], *, failed_only: bool = False
    ) -> dict[str, set[str]]:
        """For each accession, which of ``route_ids`` already carry a verdict.

        ``failed_only`` restricts to failed verdicts, which is what
        ``--retry-failed`` subtracts.
        """
        wanted = sorted(set(route_ids))
        if not wanted:
            return {}
        marks = ", ".join("?" for _ in wanted)
        sql = (
            f"SELECT accession, route_id FROM survey_results "
            f"WHERE corpus = ? AND route_id IN ({marks})"
        )
        params: list[str] = [corpus, *wanted]
        if failed_only:
            sql += " AND outcome = ?"
            params.append(Outcome.FAILED.value)
        found: dict[str, set[str]] = {}
        for row in self._conn.execute(sql, params):
            found.setdefault(row["accession"], set()).add(row["route_id"])
        return found

    def answered_all(self, corpus: str, route_ids: Iterable[str]) -> set[str]:
        """Accessions carrying a verdict for *every* one of ``route_ids``.

        An accession with some verdicts and not others is not done: a fan-out
        survey asks it again, for only what is missing.
        """
        wanted = set(route_ids)
        return {
            accession
            for accession, have in self.answered_by(corpus, wanted).items()
            if wanted <= have
        }

    def failed(self, corpus: str, route_id: str) -> set[str]:
        rows = self._conn.execute(
            """
            SELECT accession FROM survey_results
            WHERE corpus = ? AND route_id = ? AND outcome = ?
            """,
            (corpus, route_id, Outcome.FAILED.value),
        )
        return {row["accession"] for row in rows}

    def results(self, corpus: str, route_id: str) -> Iterator[RouteResult]:
        rows = self._conn.execute(
            "SELECT * FROM survey_results WHERE corpus = ? AND route_id = ? ORDER BY accession",
            (corpus, route_id),
        )
        for row in rows:
            yield RouteResult(
                accession=row["accession"],
                route_id=row["route_id"],
                corpus=row["corpus"],
                outcome=Outcome(row["outcome"]),
                results=tuple(json.loads(row["results"])),
                error_class=row["error_class"],
                error_message=row["error_message"],
                latency_ms=row["latency_ms"],
                fetched_at=datetime.fromisoformat(row["fetched_at"]),
                detail=None if row["detail"] is None else json.loads(row["detail"]),
            )

    def summary(self, corpus: str, route_id: str) -> dict[str, Any]:
        """Counts by outcome, plus the distinct-result total the ranking uses."""
        counts = {
            row["outcome"]: row["n"]
            for row in self._conn.execute(
                """
                SELECT outcome, COUNT(*) AS n FROM survey_results
                WHERE corpus = ? AND route_id = ? GROUP BY outcome
                """,
                (corpus, route_id),
            )
        }
        distinct: set[str] = set()
        for result in self.results(corpus, route_id):
            distinct.update(result.results)
        return {
            "corpus": corpus,
            "route_id": route_id,
            "resolved": counts.get(Outcome.RESOLVED.value, 0),
            "empty": counts.get(Outcome.EMPTY.value, 0),
            "failed": counts.get(Outcome.FAILED.value, 0),
            "queried": sum(counts.values()),
            "unique_results": len(distinct),
        }

    def routes_surveyed(self, corpus: str | None = None) -> list[tuple[str, str]]:
        sql = "SELECT DISTINCT corpus, route_id FROM survey_results"
        params: tuple[str, ...] = ()
        if corpus is not None:
            sql += " WHERE corpus = ?"
            params = (corpus,)
        return [
            (r["corpus"], r["route_id"]) for r in self._conn.execute(sql + " ORDER BY 1, 2", params)
        ]

    def diff(self, corpus: str, route_id: str, other: SurveyCache) -> dict[str, Any]:
        """Compare this store's verdicts against an earlier snapshot.

        Reports accessions whose result set changed between runs. A BioProject
        that resolved to one GEO series and now resolves to another has been
        re-created upstream; one that resolved and now does not has been
        withdrawn or made private.
        """
        mine = {r.accession: r for r in self.results(corpus, route_id)}
        theirs = {r.accession: r for r in other.results(corpus, route_id)}
        changed = {
            accession: {
                "before": sorted(theirs[accession].results),
                "after": sorted(result.results),
                "before_outcome": theirs[accession].outcome.value,
                "after_outcome": result.outcome.value,
            }
            for accession, result in mine.items()
            if accession in theirs
            and (
                set(result.results) != set(theirs[accession].results)
                or result.outcome is not theirs[accession].outcome
            )
        }
        return {
            "changed": changed,
            "added": sorted(set(mine) - set(theirs)),
            "removed": sorted(set(theirs) - set(mine)),
        }

    # -- raw responses ----------------------------------------------------

    def store_response(
        self, cache_key: str, provider: str, url: str, status: int, body: bytes
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO responses (cache_key, provider, url, status, body, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (cache_key) DO UPDATE SET
                status=excluded.status, body=excluded.body,
                fetched_at=excluded.fetched_at
            """,
            (cache_key, provider, url, status, body, datetime.now(UTC).isoformat()),
        )
        self._conn.commit()

    def load_response(self, cache_key: str) -> tuple[int, bytes] | None:
        row = self._conn.execute(
            "SELECT status, body FROM responses WHERE cache_key = ?", (cache_key,)
        ).fetchone()
        return (row["status"], row["body"]) if row else None

    def record_many(self, results: Iterable[RouteResult]) -> int:
        n = 0
        for result in results:
            self.record(result)
            n += 1
        return n


@contextmanager
def open_cache(path: Path | str = DEFAULT_CACHE_PATH) -> Iterator[SurveyCache]:
    cache = SurveyCache(path)
    try:
        yield cache
    finally:
        cache.close()
