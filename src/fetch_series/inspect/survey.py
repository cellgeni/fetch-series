"""The content survey: what the files every run-to-reads link finds actually hold.

A unit is ``screen:<reads>:<link id>`` for each link from a run node into a read
format -- measured by a route or not, because what the files *hold* is a
different question from whether the link *answers*. Per run:

* RESOLVED -- the link listed a file set, and it was read and classified, with
  the checks, the call and the fit verdict in ``detail``;
* EMPTY -- the link listed no set for this run;
* FAILED -- the listing itself failed.

Readability is a check, not an outcome: a set DDBJ lists and then answers 404
for (its mirrored-run paths) is RESOLVED with ``readable=False``. That keeps
"the archive says it has files" apart from "the files are there".

Never evidence for a route, never a ranking: ``fit`` is a requirement applied
to reads.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Iterable, Mapping
from dataclasses import replace
from typing import Any

from fetch_series.catalogue import CATALOGUE, Catalogue, Link, NodeKind
from fetch_series.catalogue.probe import answer_links, seeded_runs
from fetch_series.files import FileKind, FileRecord
from fetch_series.inspect.checks import check_set
from fetch_series.inspect.fetch import DEFAULT_READS, sample_file
from fetch_series.inspect.fit import MissingInput, fit_from_detail
from fetch_series.inspect.lists import BarcodeList
from fetch_series.inspect.model import FileSample
from fetch_series.inspect.pool import Judged, Pool, judge
from fetch_series.inspect.references import KmerIndex
from fetch_series.logging_utils import redact_exception
from fetch_series.providers import sra_be
from fetch_series.results import Outcome, RouteResult
from fetch_series.survey.client import SurveyClient
from fetch_series.survey.runner import SHUFFLE_SEED, FanoutFn
from fetch_series.units import parse_unit, screen_unit

# The gate for a content survey: each run is here for what it showed, with the
# expected outcome. Never evidence -- chosen, not sampled.
INSPECT_GATE: dict[str, str] = {
    "SRR25056225": "28 + 91 in the Normalized object, fit; the Lite object's quality substituted",
    "ERR4265292": "the submitted BAM carries the barcode read in CR/CY + RX/QX, fit; the SRA object is 91 bases only, unfit",
    "ERR13855116": "a submitted CRAM named with a #, paired 0x40/0x80, R1 26 + R2 96",
    "SRR29534765": "ENA's single fastq names its read /3 and has no barcode read, unfit; the SRA object's 8 + 26 + 101 is fit",
    "SRR30132199": "28-base reads only: no cDNA read, unfit everywhere",
    "DRR345778": "DDBJ: bzip2 fastqs, and an SRA object of 28 + 91",
    "SRR11181954": "5' v1/v2: R1 26 + R2 100 like 3' v2, the cDNA read antisense",
    "SRR24749105": "Parse Evercode: microSPLiT's linkers at 18 and 56 of R2; named, never fit",
    "DRR346905": "BD Rhapsody's original beads: both linkers in R1; named, never fit",
}


log = logging.getLogger(__name__)

# Files read at once across a whole survey: the files client's eight downloads,
# plus room for the fastq-dumps it does not see.
READS_AT_ONCE = 12

_KIND_OF_NODE = {
    "fastq": FileKind.FASTQ,
    "bam": FileKind.BAM,
    "cram": FileKind.CRAM,
    "sra_normalized": FileKind.SRA,
    "sra_lite": FileKind.SRA,
}


async def mean_spot(client: SurveyClient, run: str) -> float | None:
    """The run's bases per spot, from runinfo: what an SRA object's prefix is held to.

    ``None`` when it cannot be had, and then an SRA object refuses nothing.
    """
    try:
        rows = await client.with_retry(
            lambda t: sra_be.runinfo_by_accession(client, run, t), label=f"runinfo {run}"
        )
    except Exception as exc:
        log.warning("No runinfo for %s: %s", run, redact_exception(exc))
        return None
    row = next((r for r in rows if r.get("Run") == run), None)
    try:
        spots, bases = int(row["spots"]), int(row["bases"])  # type: ignore[index]
    except (TypeError, KeyError, ValueError):
        return None
    return round(bases / spots, 1) if spots else None


def screen_links(catalogue: Catalogue = CATALOGUE) -> list[Link]:
    """Every link from a run node into a read format."""
    return [
        link
        for link in catalogue.links
        if link.source.startswith("run_")
        and catalogue.nodes[link.target].kind is NodeKind.READ_FORMAT
    ]


def screen_fanout(
    links: list[Link],
    clients: Mapping[str, SurveyClient],
    corpus: str,
    lists: dict[str, BarcodeList],
    references: dict[str, KmerIndex],
    *,
    reads: int = DEFAULT_READS,
    catalogue: Catalogue = CATALOGUE,
    pool: Pool | None = None,
) -> FanoutFn:
    """One fan-out function for every run-to-reads link from one run node.

    Per run: each mechanism's request is made once, every file any of them
    lists is read once -- the Normalized object SDL and the SRA XML both name is
    one object, read once and reported against both links -- and each link gets
    its verdict on the files it listed. Reading by link instead read the same
    15 GB object three times per run.

    The distinct files are read at once, and a set two links share is called
    once: the reads are the same, so the call is. With a ``pool`` the parsing
    and the calls run in worker processes.
    """
    by_unit = {screen_unit(reads, link.id): link for link in links}
    files_client = clients["files"]
    # The files client bounds the downloads, but an SRA object is read by
    # fastq-dump, outside it; this bounds those too, across every run at once.
    reading = asyncio.Semaphore(READS_AT_ONCE)

    async def read(record: FileRecord) -> list[FileSample]:
        async with reading:
            return await sample_file(record, files_client, reads=reads, pool=pool)

    async def fn(accession: str, owed: frozenset[str]) -> list[RouteResult]:
        members = [by_unit[u] for u in sorted(owed) if parse_unit(u).kind == "screen"]
        by_mechanism: dict[str, list[Link]] = {}
        for link in members:
            by_mechanism.setdefault(link.mechanism, []).append(link)
        started = time.monotonic()
        findings = [
            finding
            for mechanism, group in sorted(by_mechanism.items())
            for finding in await answer_links(
                mechanism,
                accession,
                group,
                clients,
                catalogue=catalogue,
                pick_run=seeded_runs(SHUFFLE_SEED, accession),
                make_checks=False,
            )
        ]
        listed: dict[str, list[FileRecord]] = {}
        for finding in findings:
            if finding.outcome is not Outcome.RESOLVED:
                continue
            # The link's target says what format its files are: an SRA object
            # named `SRR25056225.lite.1` is "other" to a name classifier, and
            # would never be handed to a reader at all.
            kind = _KIND_OF_NODE.get(by_unit[screen_unit(reads, finding.link)].target)
            listed[finding.link] = [
                replace(r, kind=kind) if kind is not None else r
                for r in finding.files
                if r.run in ("", accession) and r.is_data
            ]
        distinct = {r.url or r.name: r for records in listed.values() for r in records}
        samples_of = dict(
            zip(
                distinct,
                await asyncio.gather(*(read(record) for record in distinct.values())),
                strict=True,
            )
        )
        run_spot = (
            await mean_spot(clients["ncbi_web"], accession)
            if any(r.kind is FileKind.SRA for r in distinct.values())
            else None
        )
        calls: dict[tuple[str, ...], Judged] = {}
        results = []
        for finding in findings:
            unit = screen_unit(reads, finding.link)
            if finding.outcome is not Outcome.RESOLVED:
                results.append(
                    RouteResult(
                        accession=accession,
                        route_id=unit,
                        corpus=corpus,
                        outcome=finding.outcome,
                        error_class=(finding.error or "").partition(":")[0] or None,
                        error_message=finding.error,
                        latency_ms=int((time.monotonic() - started) * 1000),
                    )
                )
                continue
            records = listed[finding.link]
            keys = tuple(r.url or r.name for r in records)
            samples: list[FileSample] = [s for key in keys for s in samples_of[key]]
            checks = check_set(samples, records)
            checks["reads_per_file"] = reads
            if keys not in calls:
                calls[keys] = (
                    await pool.judge(samples, run_spot=run_spot)
                    if pool is not None
                    else judge(samples, lists, references, run_spot=run_spot)
                )
            call, verdict, measures = calls[keys]
            detail: dict[str, Any] = {"checks": checks, "call": call, **verdict, **measures}
            results.append(
                RouteResult.from_results(
                    accession=accession,
                    route_id=unit,
                    corpus=corpus,
                    results=[r.url or r.name for r in records],
                    latency_ms=int((time.monotonic() - started) * 1000),
                    detail=detail,
                )
            )
        return results

    return fn


def rejudge(results: Iterable[RouteResult]) -> tuple[list[RouteResult], set[str]]:
    """Recorded verdicts judged again under the current fit rule, without a read.

    Returns the verdicts the current rule changes -- its name at least -- to be
    stored again, each keeping its ``fetched_at``, since the reads it was judged
    on are the ones fetched then; and the runs whose detail lacks something the
    rule needs, which only reading them again can settle.
    """
    changed: list[RouteResult] = []
    lacking: set[str] = set()
    for result in results:
        if result.detail is None or "rule" not in result.detail:
            continue
        try:
            verdict = fit_from_detail(result.detail)
        except MissingInput:
            lacking.add(result.accession)
            continue
        detail = {**result.detail, **verdict}
        if detail != result.detail:
            changed.append(replace(result, detail=detail))
    return changed, lacking


__all__ = ["INSPECT_GATE", "rejudge", "screen_fanout", "screen_links"]
