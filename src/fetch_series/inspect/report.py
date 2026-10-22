"""The report ``fetch inspect`` writes: every source of a run, read and called.

Versioned. ``schema_version`` changes when a key is removed or changes meaning,
never silently; the classifier and the fit rule carry their own versions, so a
report says which instrument made each call.

Keys (schema 2):

* ``run``, ``reads`` (per file), ``classifier``, ``fit_rule``, and
  ``run_spot``: the run's mean bases per spot, which an SRA object is held to;
* ``sources``: one per (source, format) offer, with ``files``, the built-in
  ``checks``, the ``call`` and its ``fit`` and ``reason``, and what the copy is
  ready for (``inspect.readiness``):

  * ``geometry`` -- each read's role (R1/R2/I1/I2), lengths and FASTQ names,
    where the barcode and UMI sit (1-based), and whether lengths vary;
  * ``mapping`` -- for a HIGH or MEDIUM 10x gene-expression call, the
    STARsolo options, the Cell Ranger chemistry and the whitelist; else null;
  * ``convertible`` -- whether a BAM, CRAM or SRA copy converts to FASTQ, how,
    and why;
  * ``qc`` -- listed-barcode rates, UMI entropy and distinct shares, the
    CB-UMI pairs' distinct share and the tail, over the sample (``inspect.qc``);
  * ``library`` -- the library type, a VDJ library's chain and a feature
    library's multiplexing tags;
* ``agreement``: whether the called sources agree, and which could not be called.

Schema 2 added ``geometry``, ``mapping``, ``convertible``, ``qc`` and
``library``; nothing of schema 1 was removed or changed meaning. No sequence is ever written.
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

from fetch_series.files import FileKind, FileRecord, FileSet, candidates
from fetch_series.files import classify as kind_of
from fetch_series.inspect.agree import across_sources
from fetch_series.inspect.checks import check_set
from fetch_series.inspect.classify import VERSION, Call, classify
from fetch_series.inspect.fetch import DEFAULT_READS, PREFIX_BYTES, sample_file, unavailable
from fetch_series.inspect.fit import RULE, fit
from fetch_series.inspect.lists import BarcodeList
from fetch_series.inspect.model import FileSample
from fetch_series.inspect.qc import library, qc
from fetch_series.inspect.readers import cram, sra
from fetch_series.inspect.readers.bam import read_bam
from fetch_series.inspect.readers.fastq import read_fastq
from fetch_series.inspect.readiness import readiness
from fetch_series.inspect.references import KmerIndex
from fetch_series.inspect.survey import mean_spot
from fetch_series.inspect.tags import available as tag_sets
from fetch_series.survey.client import SurveyClient

SCHEMA_VERSION = "2"


def _source(
    name: str,
    kind: str,
    files: list[str],
    samples: list[FileSample],
    records: Iterable[FileRecord],
    lists: dict[str, BarcodeList],
    references: dict[str, KmerIndex],
    run_spot: float | None = None,
    run: str | None = None,
) -> tuple[dict[str, Any], Call]:
    checks = check_set(samples, records)
    checks["unreadable"] = [
        {"source": s.source, "why": s.facts.get("why")}
        for s in samples
        if s.facts.get("reader") == "unavailable"
    ]
    call = classify(samples, lists, references)
    verdict = fit(samples, call, run_spot=run_spot)
    entry: dict[str, Any] = {
        "source": name,
        "kind": kind,
        "files": files,
        "checks": checks,
        "call": call.as_detail(),
        "fit": verdict["fit"],
        "reason": verdict["reason"],
        "mates": verdict.get("mates"),
    }
    entry.update(readiness(entry, run=run, run_spot=run_spot))
    entry["qc"] = qc(samples, entry["call"])
    entry["library"] = library(samples, entry["call"], references, tag_sets())
    return entry, call


async def inspect_fileset(
    fileset: FileSet,
    client: SurveyClient,
    lists: dict[str, BarcodeList],
    references: dict[str, KmerIndex],
    *,
    reads: int = DEFAULT_READS,
) -> dict[str, Any]:
    """Read every offer of one run, each distinct file once.

    An SRA object is held to the run's mean spot from runinfo (since rule fit-2), so
    a run with one is looked up once.
    """
    read: dict[str, list[FileSample]] = {}
    sources = []
    calls: dict[str, Call] = {}
    offers = candidates(fileset)
    has_sra = any(r.kind is FileKind.SRA for offer in offers for r in offer.files)
    run_spot = await mean_spot(client, fileset.run) if has_sra else None
    for offer in offers:
        samples: list[FileSample] = []
        for record in offer.files:
            key = record.url or record.name
            if key not in read:
                read[key] = await sample_file(record, client, reads=reads)
            samples.extend(read[key])
        entry, call = _source(
            offer.source,
            str(offer.kind),
            [r.url or r.name for r in offer.files],
            samples,
            offer.files,
            lists,
            references,
            run_spot,
            fileset.run,
        )
        sources.append(entry)
        calls[f"{offer.source}:{offer.kind}"] = call
    return {
        "schema_version": SCHEMA_VERSION,
        "run": fileset.run,
        "reads": reads,
        "classifier": VERSION,
        "fit_rule": RULE,
        "run_spot": run_spot,
        "sources": sources,
        "agreement": across_sources(calls),
    }


def refusals(report: dict[str, Any]) -> dict[tuple[str, FileKind], str]:
    """The offers a report proves unusable, as ``recommend`` takes them."""
    return {
        (source["source"], FileKind(source["kind"])): source["reason"]
        for source in report["sources"]
        if source["fit"] is False
    }


async def inspect_local(
    paths: list[Path],
    lists: dict[str, BarcodeList],
    references: dict[str, KmerIndex],
    *,
    reads: int = DEFAULT_READS,
) -> dict[str, Any]:
    """Local files, read as one library: the files a user is about to map."""
    samples: list[FileSample] = []
    for path in paths:
        kind = kind_of(path.name)
        record = FileRecord("", str(path), path.name, kind, "local")
        data = path.read_bytes()[: PREFIX_BYTES.get(kind, 32 * 1024 * 1024)]
        whole = path.stat().st_size <= len(data)
        if kind is FileKind.FASTQ:
            samples.append(read_fastq(data, str(path), max_reads=reads, complete=whole))
        elif kind is FileKind.BAM:
            samples.extend(read_bam(data, str(path), max_reads=reads, complete=whole))
        elif kind is FileKind.CRAM and cram.available():
            samples.extend(cram.read_cram(data, str(path), max_reads=reads, complete=whole))
        elif sra.available() and kind in (FileKind.SRA, FileKind.OTHER):
            samples.extend(await sra.read_sra(str(path), max_reads=reads))
        else:
            samples.append(unavailable(record, f"no reader for {path.name}"))
    records = [FileRecord("", str(p), p.name, kind_of(p.name), "local") for p in paths]
    entry, call = _source(
        "local", "files", [str(p) for p in paths], samples, records, lists, references
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "run": None,
        "reads": reads,
        "classifier": VERSION,
        "fit_rule": RULE,
        "sources": [entry],
        "agreement": across_sources({"local": call}),
    }


def params(report: dict[str, Any]) -> list[str]:
    """The STARsolo and Cell Ranger lines for each source a mapping block was made for."""
    lines = []
    for source in report["sources"]:
        block = source.get("mapping")
        if not block:
            continue
        lines.append(f"  {source['source']} ({source['kind']}):")
        lines.append(f"    STARsolo     {block['starsolo_args']}")
        if block["cellranger_chemistry"]:
            lines.append(f"    Cell Ranger  --chemistry {block['cellranger_chemistry']}")
        for why in block["omitted"]:
            lines.append(f"    omitted      {why}")
        measured = source.get("qc") or {}
        barcodes = measured.get("barcodes")
        if barcodes:
            pairs = (measured.get("cb_umi") or {}).get("distinct_share")
            lines.append(
                f"    sample QC    listed barcodes {barcodes['exact']:.1%} exact, "
                f"{barcodes['within_one']:.1%} within one; UMI entropy "
                f"{measured['umi'].get('entropy')}; CB-UMI pairs distinct {pairs}"
            )
        lines.append(f"    unvalidated  {block['note']}")
    return lines


def summary(report: dict[str, Any]) -> str:
    """The human-readable lines: one per source."""
    lines = [
        f"{report['run'] or 'local files'}  (classifier {report['classifier']}, {report['fit_rule']})"
    ]
    for source in report["sources"]:
        call = source["call"]
        named = call["layout"] or (" | ".join(call["candidates"]) if call["candidates"] else "-")
        files = source["checks"]["files"]
        shape = "+".join(str(f["length"]["mode"]) for f in files) or "-"
        fit_word = {True: "fit", False: "UNFIT", None: "undetermined"}[source["fit"]]
        kind = source.get("library") or {}
        finer = kind.get("chain") or kind.get("multiplexing")
        if finer and named != "-":
            named = f"{named} {kind['type']}-{finer}"
        lines.append(
            f"  {source['source']:28s} {source['kind']:6s} {shape:12s} {named:22s} "
            f"{call['level']:9s} {fit_word:12s} {source['reason']}"
        )
    agreement = report["agreement"]
    if agreement.get("agree") is False:
        lines.append(f"  sources disagree: {', '.join(agreement['layouts'])}")
    return "\n".join(lines)


__all__ = [
    "SCHEMA_VERSION",
    "inspect_fileset",
    "inspect_local",
    "params",
    "refusals",
    "summary",
]
