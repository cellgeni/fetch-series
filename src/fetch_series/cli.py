"""The ``fetch`` command line."""

from __future__ import annotations

import asyncio
import contextlib
import csv
import json
import logging
import os
import re
import tempfile
from collections import Counter
from datetime import UTC, datetime, timedelta
from importlib.metadata import version
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any

import typer
from dotenv import load_dotenv

from fetch_series.accession import Accession, EntityType, UnknownAccessionError, parse
from fetch_series.assays import AssayCall
from fetch_series.assays.chemistry import ChemistryCall
from fetch_series.cache import DEFAULT_CACHE_PATH, SurveyCache
from fetch_series.catalogue import (
    CATALOGUE,
    DRAWN,
    PROBES_PATH,
    SPLITS_PATH,
    Link,
    Probe,
    load_probes,
    merge_probes,
    save_probes,
    save_splits,
)
from fetch_series.catalogue import Status as CatalogueStatus
from fetch_series.catalogue import status as catalogue_status
from fetch_series.catalogue.link_splits import LINK_SPLITS_PATH, LinkSplitError, save_link_splits
from fetch_series.catalogue.link_splits import compute as compute_link_splits
from fetch_series.catalogue.nodes import node_for
from fetch_series.catalogue.populations import NO_POPULATION, standard_corpora
from fetch_series.catalogue.probe import host_limits, probe_links
from fetch_series.catalogue.splits import compute as compute_splits
from fetch_series.catalogue.survey import (
    GATES,
    Job,
    SelectionError,
    estimate,
    gate_passed,
    gate_report,
    memoizable,
    plan,
    run_jobs,
    select_links,
    units_of,
)
from fetch_series.catalogue.waves import links_through
from fetch_series.core import default_api_key
from fetch_series.entities import MISSING, RELATION_COLUMNS, RunRecord
from fetch_series.export.graph import route_graph
from fetch_series.export.links import LINKS_COLUMNS, LinkRow
from fetch_series.files import Recommendation, recommend
from fetch_series.filesets import (
    Verdict,
    filesets_for,
    links_table,
    recommendations_for,
    screen,
    verify_all,
)
from fetch_series.graph import REGISTRY, Route
from fetch_series.inspect.classify import NO_LISTS
from fetch_series.logging_utils import configure_logging, run_logfile
from fetch_series.metadata.collect import Collection, collect
from fetch_series.metadata.evidence import SOURCES as metadata_source_list
from fetch_series.metadata.evidence import source_of as metadata_source_of
from fetch_series.metadata.fields import FIELD_NAMES
from fetch_series.metadata.gold import agreement
from fetch_series.metadata.ground import GROUNDING_PATHOLOGIES
from fetch_series.relations import relations as build_relations
from fetch_series.relations import resolve_input, sample_to_runs
from fetch_series.resolver import Mode, Resolution, resolve_path
from fetch_series.resolver import resolve as resolver_resolve
from fetch_series.results import Outcome
from fetch_series.routes import IMPLEMENTATIONS
from fetch_series.survey import Limits, SurveyClient, corpora, run_route
from fetch_series.survey.client import MemoClient
from fetch_series.survey.export import ExportConflict, write_export
from fetch_series.survey.export import slug as export_slug
from fetch_series.survey.runner import FanoutRun, SurveyRun, run_fanout, shuffled
from fetch_series.units import parse_unit, screen_unit

if TYPE_CHECKING:
    from fetch_series.routes.catalogue import Candidate

app = typer.Typer(
    help="Resolve public sequencing accessions over documented, benchmarked routes.",
    no_args_is_help=True,
)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(version("fetch-series"))
        raise typer.Exit()


@app.callback()
def _root(
    show_version: Annotated[
        bool,
        typer.Option(
            "--version",
            callback=_version_callback,
            is_eager=True,
            help="Print the installed version and exit.",
        ),
    ] = False,
) -> None:
    """Nextflow writes a versions.yml per process, so the version has to be
    askable without running anything or knowing where the package is installed.
    """


survey_app = typer.Typer(help="Run and compare route surveys.", no_args_is_help=True)
routes_app = typer.Typer(help="Inspect the route graph.", no_args_is_help=True)
catalogue_app = typer.Typer(
    help="The link catalogue: every one-hop link the archives offer, measured or not.",
    no_args_is_help=True,
)
app.add_typer(survey_app, name="survey")
app.add_typer(routes_app, name="routes")
app.add_typer(catalogue_app, name="catalogue")
inspect_app = typer.Typer(
    help="Read the start of a run's files and say what they are.", no_args_is_help=True
)
app.add_typer(inspect_app, name="inspect")


def _client_for_one_query(concurrency: int) -> SurveyClient:
    """A client for one interactive resolution, paced by whether a key is set.

    NCBI allows 10 requests a second with a key and 3 without. A single rate for
    both either wastes the key or earns 429s without one -- and every NCBI 429
    costs a retry budget the user is waiting on.
    """
    api_key = default_api_key()
    rps = 8.0 if api_key else 2.5
    # Memoised: several routes of one direction read one document -- a runinfo,
    # a filereport, a SOFT file -- and one resolution should fetch it once.
    return MemoClient(
        limits=Limits(rps=rps, concurrency=concurrency), api_key=api_key, memoize=memoizable
    )


def _slug(route_id: str) -> str:
    return export_slug(route_id)


def _route_or_exit(route_id: str) -> Route:
    try:
        return REGISTRY[route_id]
    except KeyError:
        typer.secho(f"Unknown route: {route_id}", fg=typer.colors.RED, err=True)
        typer.echo("Known routes:", err=True)
        for route in sorted(REGISTRY, key=lambda r: r.id):
            typer.echo(f"  {route.id}", err=True)
        raise typer.Exit(2) from None


@routes_app.command("list")
def routes_list(
    source: Annotated[str | None, typer.Option(help="Filter by source entity type.")] = None,
    unmeasured: Annotated[
        bool, typer.Option(help="Show only routes with no survey behind them.")
    ] = False,
    implemented: Annotated[
        bool, typer.Option(help="Show only routes that have an implementation.")
    ] = False,
    ids_only: Annotated[
        bool, typer.Option("--ids-only", help="Emit bare route ids, for scripting.")
    ] = False,
) -> None:
    """List declared routes, with their measured evidence."""
    for route in sorted(REGISTRY, key=lambda r: (r.source, r.target, r.id)):
        if source and route.source != source:
            continue
        if unmeasured and route.is_measured:
            continue
        if implemented and route.id not in IMPLEMENTATIONS:
            continue
        if ids_only:
            typer.echo(route.id)
            continue
        marker = " " if route.id in IMPLEMENTATIONS else "!"
        if route.evidence is None:
            note = "unmeasured"
        else:
            note = (
                f"{route.evidence.unique_results or 0:,} results, "
                f"{route.evidence.failure_rate:.2%} failed, {route.evidence.corpus}"
            )
        typer.echo(f"{marker} {route.id:44s} {note}")
    # The legend would be read as a route id by anything piping --ids-only into
    # a loop, so it belongs to the human listing only.
    if not ids_only:
        typer.echo("\n'!' marks a declared route with no implementation yet.")


@routes_app.command("graph")
def routes_graph() -> None:
    """Print the route graph as JSON: nodes, ranked edges with evidence, type paths.

    This is the data the knowledge base's route-graph page is drawn from, so
    anything the page shows can be checked from here.
    """
    typer.echo(json.dumps(route_graph(), indent=2))


@catalogue_app.command("list")
def catalogue_list(
    only: Annotated[
        str | None,
        typer.Option("--status", help="Only links of this status: measured, untested, ..."),
    ] = None,
) -> None:
    """Nodes, every link with its status and mechanisms, and the gaps between them."""
    probes = load_probes()
    typer.echo(f"{len(CATALOGUE.nodes)} nodes")
    for node in CATALOGUE.nodes.values():
        typer.echo(
            f"  {node.id:20s} {node.level:13s} {node.archive or 'shared':12s} "
            f"{node.label} ({node.prefix})"
            if node.prefix
            else f"  {node.id:20s} {node.label}"
        )

    by_pair: dict[tuple[str, str], list[tuple[CatalogueStatus, str]]] = {}
    for link in CATALOGUE:
        found = catalogue_status(link, probes)
        if only and found != only:
            continue
        label = link.route_id or CATALOGUE.mechanisms[link.mechanism].id
        by_pair.setdefault((link.source, link.target), []).append((found, label))
    counts = Counter(catalogue_status(link, probes) for link in CATALOGUE)
    typer.echo(
        f"\n{len(CATALOGUE.links)} links: "
        + ", ".join(f"{counts[s]} {s}" for s in CatalogueStatus)
        + f"; {len({(link.source, link.target) for link in CATALOGUE})} node pairs"
    )
    for (source, target), entries in sorted(by_pair.items()):
        typer.echo(f"  {source} -> {target}")
        for found, label in sorted(entries):
            typer.echo(f"      {found:12s} {label}")

    if only:
        return
    drawn = {
        (link.source, link.target) for link in CATALOGUE if catalogue_status(link, probes) in DRAWN
    }
    typer.echo("\nNo confirmed one-hop link:")
    for source in CATALOGUE.nodes:
        missing = [t for t in CATALOGUE.nodes if t != source and (source, t) not in drawn]
        typer.echo(f"  {source:20s} -> {', '.join(missing) or '(reaches every node)'}")


@catalogue_app.command("splits")
def catalogue_splits(
    write: Annotated[
        bool, typer.Option("--write", help="Write route_splits.json beside the catalogue.")
    ] = False,
) -> None:
    """Split each route's recorded verdicts over the node pairs it covers.

    Reads data/surveys/ and the 2026-05 tables in data/query/results/, which are
    Git LFS objects: fetch them first. Nothing is measured; this only filters what
    each route's survey already recorded, by archive and by file format.
    """
    splits, sources = compute_splits(CATALOGUE, REGISTRY)
    for split in splits:
        share = "split unknown" if split.share is None else f"{split.share:.1%} resolved"
        typer.echo(
            f"{split.route:42s} {split.source:16s} -> {split.target:16s} "
            f"{split.reached:>7,} of {split.queried:>7,} reached  ({share})"
        )
    for route, source in sources.items():
        if source["unparsed"]:
            typer.secho(f"{route}: not accessions, set aside: {source['unparsed']}", err=True)
        if source["excluded"]:
            typer.secho(
                f"{route}: queried sources it cannot link, all empty: {source['excluded']}",
                err=True,
            )
    if write:
        save_splits(splits, sources)
        typer.echo(f"wrote {len(splits)} splits to {SPLITS_PATH}", err=True)


truth_app = typer.Typer(help="The reprocessing pipeline's own STARsolo runs, as a record.")
inspect_app.add_typer(truth_app, name="truth")


@truth_app.command("import")
def inspect_truth_import(
    bundle: Annotated[
        Path, typer.Argument(help="The bundle: one directory per dataset, as in iRODS.")
    ],
    out: Annotated[Path, typer.Option(help="Where to write truth.csv.")] = Path(
        "data/benchmarks/inspect/starsolo"
    ),
) -> None:
    """One row per sample: the whitelist, strand and pairing the pipeline used, and how it mapped.

    Read from each dataset's ``solo_qc`` table and each sample's GeneFull
    ``Summary.csv``. Split into design and held-out halves by dataset, seeded.
    """
    from collections import Counter

    from fetch_series.inspect import truth

    samples = list(truth.read_bundle(bundle))
    if not samples:
        typer.secho(f"no solo_qc rows under {bundle}", fg=typer.colors.RED, err=True)
        raise typer.Exit(2)
    # Controlled-access datasets are counted, not written: their sample names
    # are not public, and their reads cannot be read to compare anyway.
    truth.write([s for s in samples if s.public], out / "truth.csv")
    halves = Counter((s.split, s.public) for s in samples)
    typer.echo(
        f"{len(samples):,} samples from {len({s.dataset for s in samples})} datasets; "
        f"{sum(s.public for s in samples):,} public ones -> {out / 'truth.csv'}"
    )
    for (half, public), n in sorted(halves.items()):
        typer.echo(f"  {half:8s} {'public' if public else 'controlled':10s} {n:>6,}")


@inspect_app.command("lists")
def inspect_lists(
    fetch_them: Annotated[
        bool, typer.Option("--fetch", help="Download every registered list and reference.")
    ] = False,
    export_ids: Annotated[
        list[str] | None,
        typer.Option(
            "--export", help="Write this list as a text whitelist STARsolo reads (repeatable)."
        ),
    ] = None,
    out: Annotated[
        Path | None, typer.Option("--out", help="With --export: the directory to write to.")
    ] = None,
) -> None:
    """The barcode lists and references the classifier reads, and whether each is fetched.

    None is shipped: 10x's lists are under 10x's terms. Each is registered with
    its source at a pinned commit and its sha256, and a download that does not
    match is refused.
    """
    from fetch_series.inspect import lists as barcode_lists
    from fetch_series.inspect import references, tags

    if fetch_them:
        for entry, path in barcode_lists.fetch_all():
            typer.echo(f"fetched {entry.id:16s} {entry.count:>10,}  {path}", err=True)
        for reference in references.REGISTER.values():
            path = references.fetch(reference)
            typer.echo(f"fetched {reference.id:24s} {path}", err=True)
        for tag_set in tags.REGISTER.values():
            path = tags.fetch(tag_set)
            typer.echo(f"fetched {tag_set.id:24s} {path}", err=True)
    if export_ids:
        unknown = sorted(set(export_ids) - set(barcode_lists.REGISTER))
        if unknown:
            typer.secho(f"not registered: {', '.join(unknown)}", fg=typer.colors.RED, err=True)
            raise typer.Exit(2)
        for list_id in export_ids:
            listed = barcode_lists.REGISTER[list_id]
            path, digest = barcode_lists.export(listed, out or barcode_lists.cache_dir() / "export")
            typer.echo(f"{listed.id:16s} {listed.count:>10,}  {digest}  {path}")
        return
    held = barcode_lists.cache_dir()
    for entry in barcode_lists.REGISTER.values():
        state = "fetched" if (held / f"{entry.id}.u32").exists() else "missing"
        typer.echo(f"{entry.id:16s} {entry.length:>3} {entry.count:>10,}  {state:8s} {entry.terms}")
    for reference in references.REGISTER.values():
        typer.echo(f"{reference.id:24s} {reference.terms}")
    for tag_set in tags.REGISTER.values():
        state = "fetched" if (tags.cache_dir() / f"{tag_set.id}.csv").exists() else "missing"
        typer.echo(f"{tag_set.id:24s} {tag_set.kind:3s} {state:8s} {tag_set.terms}")


@inspect_app.command("run")
def inspect_run(
    targets: Annotated[
        list[str], typer.Argument(help="A run accession (every source), or local files.")
    ],
    reads: Annotated[int, typer.Option(help="Reads per file.")] = 100_000,
    json_out: Annotated[
        Path | None, typer.Option("--json", help="Write the versioned JSON report here.")
    ] = None,
    no_sdl: Annotated[bool, typer.Option("--no-sdl", help="Skip NCBI SDL.")] = False,
    metadata: Annotated[
        bool,
        typer.Option(
            help="Read the run's metadata kit too (two requests), to offer a block where "
            "the reads cannot choose between 3' v2 and 5'.",
        ),
    ] = True,
    show_params: Annotated[
        bool,
        typer.Option(
            "--params",
            help="Print the STARsolo options and Cell Ranger chemistry for each source "
            "called as 10x gene expression, and export the whitelist they name.",
        ),
    ] = False,
) -> None:
    """Read the first reads of a run's files from every source, and say what they are.

    For each offer: the reads found, their lengths and quality, the 10x layout
    at a level with its evidence, and whether the set is fit for a 10x
    gene-expression run -- proven unusable only on positive evidence.
    """
    from fetch_series.inspect import lists as barcode_lists
    from fetch_series.inspect import references
    from fetch_series.inspect.metadata import chemistry_of_run
    from fetch_series.inspect.readiness import with_whitelist
    from fetch_series.inspect.report import (
        SCHEMA_VERSION,
        inspect_fileset,
        inspect_local,
        params,
        summary,
    )

    load_dotenv()
    configure_logging(level=logging.WARNING)
    held = barcode_lists.available()
    if not held:
        typer.secho(NO_LISTS, fg=typer.colors.RED, err=True)
        raise typer.Exit(2)
    refs = {k: v for k in references.REGISTER if (v := references.index(k)) is not None}
    paths = [Path(t) for t in targets]

    async def run_it() -> list[dict[str, Any]]:
        if all(p.exists() for p in paths):
            return [await inspect_local(paths, held, refs, reads=reads)]
        if len(targets) != 1:
            typer.secho("Give one run accession, or local files.", fg="red", err=True)
            raise typer.Exit(2)
        parsed = resolve_input(targets[0])
        async with _client_for_one_query(6) as client:
            sets = await filesets_for(parsed, client, include_sdl=not no_sdl)
            return [
                await inspect_fileset(
                    s,
                    client,
                    held,
                    refs,
                    reads=reads,
                    metadata=await chemistry_of_run(s.run, client) if metadata else None,
                )
                for s in sets
            ]

    reports = asyncio.run(run_it())
    for report in reports:
        if show_params:
            for source in report["sources"]:
                for key in ("mapping", "mapping_from_metadata"):
                    if source.get(key):
                        entry = barcode_lists.REGISTER[source[key]["whitelist"]["id"]]
                        with_whitelist(source[key], str(barcode_lists.exported(entry)))
        typer.echo(summary(report))
        if show_params:
            typer.echo("\n".join(params(report)) or "  no source called as 10x gene expression")
    if json_out is not None:
        payload = (
            reports[0] if len(reports) == 1 else {"schema_version": SCHEMA_VERSION, "runs": reports}
        )
        json_out.write_text(json.dumps(payload, indent=1) + "\n")
        typer.echo(f"wrote {json_out}", err=True)


@inspect_app.command("survey")
def inspect_survey(
    corpus_name: Annotated[
        str | None, typer.Option("--corpus", help="Runs to read, e.g. sample:500@...")
    ] = None,
    gate: Annotated[bool, typer.Option("--gate", help="Over inspect-gate.")] = False,
    reads: Annotated[int, typer.Option(help="Reads per file.")] = 100_000,
    mechanism: Annotated[
        str | None, typer.Option("--mechanism", help="Only this mechanism's links.")
    ] = None,
    dry_run: Annotated[bool, typer.Option("--plan", help="Print units and sizes only.")] = False,
    retry_failed: Annotated[bool, typer.Option(help="Also re-ask stored failures.")] = False,
    rejudge: Annotated[
        bool,
        typer.Option(
            help="Judge every recorded verdict again under the current fit rule, from "
            "what it recorded. Reads nothing; says how many runs lack what the rule needs."
        ),
    ] = False,
    refit: Annotated[
        bool,
        typer.Option(
            help="Judge recorded verdicts again under the current fit rule, and read "
            "again only the runs whose records lack what it judges on."
        ),
    ] = False,
    recall_them: Annotated[
        bool,
        typer.Option(
            "--recall",
            help="Decide every recorded call again under the current classifier, from the "
            "evidence it recorded, then judge it again. Reads nothing.",
        ),
    ] = False,
    again: Annotated[
        bool,
        typer.Option(
            "--again",
            help="Read every run again, whatever is recorded: a new classifier, "
            "or a re-read that should record every verdict's inputs.",
        ),
    ] = False,
    reread_where: Annotated[
        list[str] | None,
        typer.Option(
            help="Read again every run where a file was unreadable for a reason matching "
            "this pattern (a regex; repeatable): a full disk is not the archive's answer."
        ),
    ] = None,
    workers: Annotated[int, typer.Option(help="Runs read at once.")] = 6,
    processes: Annotated[
        int, typer.Option(help="Worker processes that parse reads and call kits.")
    ] = max(1, min(8, (os.cpu_count() or 2) - 1)),
    cache_path: Annotated[Path, typer.Option("--cache", help="SQLite store.")] = DEFAULT_CACHE_PATH,
) -> None:
    """Read the first reads of what every run-to-reads link finds, and record what they are.

    Recorded under screen:<reads>:<link id>. A requirement applied to reads,
    never evidence about a route.
    """
    from fetch_series.inspect import lists as barcode_lists
    from fetch_series.inspect import references
    from fetch_series.inspect.fit import RULE as FIT_RULE
    from fetch_series.inspect.pool import Pool
    from fetch_series.inspect.survey import recall as call_again
    from fetch_series.inspect.survey import rejudge as judge_again
    from fetch_series.inspect.survey import screen_fanout, screen_links

    load_dotenv()
    if (corpus_name is None) == (not gate):
        typer.secho("Choose exactly one of --corpus or --gate", fg=typer.colors.RED, err=True)
        raise typer.Exit(2)
    name = "inspect-gate" if gate else str(corpus_name)
    corpus = corpora.load(name)
    links = [link for link in screen_links() if mechanism is None or link.mechanism == mechanism]
    # One fan-out per run node, over every mechanism's links: each run's
    # sources are listed together and each distinct file read once.
    by_node: dict[str, list[Link]] = {}
    for link in links:
        by_node.setdefault(link.source, []).append(link)
    jobs = []
    for node, node_links in sorted(by_node.items()):
        accessions = [a.value for a in corpus.accessions if node_for(a) == node]
        if accessions:
            jobs.append((node, node_links, accessions))
    for node, node_links, accessions in jobs:
        typer.echo(f"{node:12s} {len(node_links):>3} links {len(accessions):>5} runs")
    if dry_run:
        return
    if recall_them:
        with SurveyCache(cache_path) as cache:
            for node, node_links, _accessions in jobs:
                units = [screen_unit(reads, link.id) for link in node_links]
                recorded = [r for unit in units for r in cache.results(name, unit)]
                changed, short = call_again(recorded)
                cache.record_all(changed)
                typer.echo(
                    f"{node:12s} {len(changed):>6} calls decided again, "
                    f"{len(short)} runs lack what the fit rule judges on"
                )
        return
    lacking: set[str] = set()
    if rejudge or refit:
        with SurveyCache(cache_path) as cache:
            for node, node_links, _accessions in jobs:
                units = [screen_unit(reads, link.id) for link in node_links]
                recorded = [r for unit in units for r in cache.results(name, unit)]
                changed, short = judge_again(recorded)
                cache.record_all(changed)
                lacking |= short
                typer.echo(
                    f"{node:12s} {len(changed):>6} verdicts judged again under {FIT_RULE}, "
                    f"{len(short)} runs lack what it judges on"
                )
        if not refit:
            return
    lists_held = barcode_lists.available()
    if not lists_held:
        typer.secho(NO_LISTS, fg=typer.colors.RED, err=True)
        raise typer.Exit(2)
    refs = {k: v for k in references.REGISTER if (v := references.index(k)) is not None}
    logfile = run_logfile("inspect-survey")
    configure_logging(level=logging.INFO, logfile=logfile)
    api_key = default_api_key()

    async def main() -> None:
        limits = host_limits(has_api_key=bool(api_key))
        # A prefix download holds its slot for the whole 16-32 MB, so the
        # content survey streams more at once, at the same request rate.
        limits["files"] = Limits(rps=4.0, concurrency=8, base_timeout=120.0)
        async with contextlib.AsyncExitStack() as stack:
            clients = {
                bucket: await stack.enter_async_context(
                    MemoClient(
                        bucket_limits,
                        api_key=api_key if bucket == "ncbi_eutils" else None,
                        memoize=memoizable,
                    )
                )
                for bucket, bucket_limits in limits.items()
            }
            pool = stack.enter_context(Pool(lists_held, refs, processes))
            with SurveyCache(cache_path) as cache:
                for _node, node_links, accessions in jobs:
                    fn = screen_fanout(
                        node_links, clients, name, lists_held, refs, reads=reads, pool=pool
                    )
                    units = [screen_unit(reads, link.id) for link in node_links]
                    local = re.compile("|".join(reread_where)) if reread_where else None
                    stale: set[str] = set(accessions) if again else lacking & set(accessions)
                    for unit in units:
                        for r in cache.results(name, unit):
                            if r.detail is None:
                                continue
                            unreadable = (r.detail.get("checks") or {}).get("unreadable") or []
                            if local is not None and any(
                                local.search(u.get("why") or "") for u in unreadable
                            ):
                                stale.add(r.accession)
                    if stale:
                        typer.echo(f"{len(stale)} runs to read again")
                    run = await run_fanout(
                        units,
                        accessions,
                        fn,
                        cache,
                        name,
                        workers=workers,
                        retry_failed=retry_failed,
                        redo=stale,
                    )
                    for unit_id, counts in run.units.items():
                        typer.echo(
                            f"{unit_id:72s} r={counts['resolved']} e={counts['empty']} "
                            f"f={counts['failed']}"
                        )

    asyncio.run(main())


@catalogue_app.command("link-splits")
def catalogue_link_splits(
    write: Annotated[
        bool, typer.Option("--write", help="Write link_splits.json beside the catalogue.")
    ] = False,
    wave: Annotated[
        int | None, typer.Option("--wave", help="Only links reported in waves 1 to N.")
    ] = None,
) -> None:
    """Each surveyed link's verdicts per corpus, read from the committed exports.

    Measurement of whether a link answers, never a ranking: nothing in the
    resolver or the route ranking reads it. Gate corpora are set aside.
    """
    try:
        only = links_through(wave) if wave is not None else None
        splits = compute_link_splits(CATALOGUE, only=only)
    except LinkSplitError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from None
    for split in splits:
        typer.echo(
            f"{split.link:64s} {split.corpus:56s} q={split.queried} r={split.resolved} "
            f"e={split.empty} f={split.failed} usable={split.usable} d={split.distinct}"
        )
    typer.echo(f"{len(splits)} link splits", err=True)
    if write:
        save_link_splits(splits, NO_POPULATION)
        typer.echo(f"wrote {len(splits)} link splits to {LINK_SPLITS_PATH}", err=True)


@catalogue_app.command("survey")
def catalogue_survey(
    link_ids: Annotated[
        list[str] | None, typer.Option("--link", help="Survey this link; repeatable.")
    ] = None,
    mechanism: Annotated[
        str | None, typer.Option("--mechanism", help="Every unmeasured link of a mechanism.")
    ] = None,
    source: Annotated[
        str | None, typer.Option("--source", help="With --mechanism: only from this node.")
    ] = None,
    every: Annotated[bool, typer.Option("--all", help="Every unmeasured link.")] = False,
    corpus_name: Annotated[
        str | None, typer.Option("--corpus", help="Survey over this corpus.")
    ] = None,
    standard: Annotated[
        bool,
        typer.Option("--standard", help="Over each unit's seeded draw of 300 (or census)."),
    ] = False,
    gate: Annotated[
        bool,
        typer.Option("--gate", help="Over probe-families and hard-cases, checked against probes."),
    ] = False,
    dry_run: Annotated[
        bool, typer.Option("--plan", help="Print units, sizes and requests; query nothing.")
    ] = False,
    retry_failed: Annotated[bool, typer.Option(help="Also re-ask stored failures.")] = False,
    memo: Annotated[
        bool, typer.Option(help="Answer repeated document fetches from memory.")
    ] = True,
    cache_path: Annotated[Path, typer.Option("--cache", help="SQLite store.")] = DEFAULT_CACHE_PATH,
) -> None:
    """Survey unmeasured catalogue links: one request per accession, a verdict per link.

    This is measurement of whether a link answers, never a ranking: nothing
    here reaches REGISTRY, and there is deliberately no --limit. A link is
    promoted to a route only by surveying it as one.
    """
    load_dotenv()
    if sum((corpus_name is not None, standard, gate)) != 1:
        typer.secho("Choose exactly one of --corpus, --standard or --gate", fg="red", err=True)
        raise typer.Exit(2)
    try:
        links = select_links(
            link_ids=link_ids or (), mechanism=mechanism, source=source, every=every
        )
    except SelectionError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from None
    units = units_of(links)

    with SurveyCache(cache_path) as cache:
        if gate:
            survey_plan = plan(units, lambda unit: list(GATES))
        elif corpus_name is not None:
            survey_plan = plan(units, lambda unit: [corpus_name])
        else:
            # A draw is earned at the gate: a unit with any FAILED gate verdict,
            # or none at all, does not proceed (the SDL-on-project units stop here).
            gated = []
            for unit in units:
                verdict = gate_passed(unit, cache)
                if dry_run:
                    # A plan shows the whole draw; only running it needs the gate.
                    gated.append(unit)
                elif verdict is None:
                    typer.secho(f"{unit.id}: not gated yet; run --gate first", err=True)
                elif not verdict:
                    typer.secho(f"{unit.id}: failed its gate; not drawn", err=True)
                else:
                    gated.append(unit)
            survey_plan = plan(gated, lambda unit: standard_corpora(unit.source))
        for unit, reason in survey_plan.skipped:
            typer.secho(f"{unit.id}: skipped -- {reason}", fg=typer.colors.YELLOW, err=True)

        if dry_run:
            totals: Counter[str] = Counter()
            for job in survey_plan.jobs:
                cost = estimate(job)
                totals.update(cost)
                typer.echo(
                    f"{job.unit.id:52s} {len(job.unit.links):>3} links "
                    f"{len(job.accessions):>6} acc  {job.corpus}"
                )
            typer.echo(
                f"{len(survey_plan.jobs)} unit-corpora, ~{sum(totals.values()):,} requests: "
                + ", ".join(f"{bucket} {n:,}" for bucket, n in sorted(totals.items()))
            )
            return

        logfile = run_logfile("catalogue-survey")
        configure_logging(level=logging.INFO, logfile=logfile)
        typer.echo(f"{len(survey_plan.jobs)} unit-corpora -> {logfile}", err=True)

        def report(job: Job, run: FanoutRun) -> None:
            for unit_id, counts in run.units.items():
                typer.echo(
                    f"{unit_id:64s} {job.corpus:50s} "
                    f"r={counts['resolved']} e={counts['empty']} f={counts['failed']} "
                    f"d={counts['distinct']}"
                )

        api_key = default_api_key()

        async def main() -> None:
            limits = host_limits(has_api_key=bool(api_key))
            async with contextlib.AsyncExitStack() as stack:
                clients: dict[str, SurveyClient] = {}
                for bucket, bucket_limits in limits.items():
                    key = api_key if bucket == "ncbi_eutils" else None
                    client: SurveyClient = (
                        MemoClient(bucket_limits, api_key=key, memoize=memoizable)
                        if memo
                        else SurveyClient(bucket_limits, api_key=key)
                    )
                    clients[bucket] = await stack.enter_async_context(client)
                await run_jobs(
                    survey_plan.jobs, clients, cache, retry_failed=retry_failed, on_done=report
                )
                hits = sum(getattr(c, "hits", 0) for c in clients.values())
                if hits:
                    typer.echo(f"{hits:,} requests answered from memory", err=True)

        asyncio.run(main())

        if gate:
            rows = gate_report(units, cache, load_probes())
            failing = [row for row in rows if row.failed]
            disagree = [row for row in rows if row.agrees is False]
            for row in disagree:
                typer.echo(
                    f"DISAGREES {row.link}: probe {row.probe_outcome}, gate {row.gate_outcome}"
                )
            for row in failing:
                typer.echo(f"FAILED    {row.link}: {row.failed} of {row.answered} gate verdicts")
            typer.echo(
                f"{len(rows)} links gated: {len(failing)} with failures, "
                f"{len(disagree)} disagreeing with their probe",
                err=True,
            )
            if failing:
                raise typer.Exit(1)


@catalogue_app.command("probe")
def catalogue_probe(
    link_ids: Annotated[
        list[str] | None, typer.Option("--link", help="Probe only this link; repeatable.")
    ] = None,
    write: Annotated[
        bool, typer.Option("--write", help="Merge the results into probes.json.")
    ] = False,
    stale: Annotated[
        bool,
        typer.Option(
            "--stale",
            help="Only links with no probe, or whose probe used another accession.",
        ),
    ] = False,
) -> None:
    """Make one live request per untested link and report what each found.

    A probe proves a link exists and measures nothing: it is never evidence.
    """
    load_dotenv()
    configure_logging(level=logging.WARNING)
    if link_ids:
        try:
            targets = [CATALOGUE.link(link_id) for link_id in link_ids]
        except KeyError as exc:
            typer.secho(f"Unknown link: {exc.args[0]}", fg=typer.colors.RED, err=True)
            raise typer.Exit(2) from None
        # A measured link's evidence is its route's survey. A probe beside it
        # would read as a second measurement, so one is never made.
        measured = [link.id for link in targets if CATALOGUE.split(link) is not None]
        if measured:
            typer.secho(
                f"Measured by a route's survey, so never probed: {', '.join(measured)}",
                fg=typer.colors.RED,
                err=True,
            )
            raise typer.Exit(2)
    else:
        targets = CATALOGUE.untested
    if stale:
        recorded = load_probes()
        targets = [
            link
            for link in targets
            if link.id not in recorded
            or recorded[link.id].accession != CATALOGUE.probe_accession(link)
        ]
    api_key = default_api_key()

    async def main() -> list[Probe]:
        limits = host_limits(has_api_key=bool(api_key))
        async with contextlib.AsyncExitStack() as stack:
            clients = {
                bucket: await stack.enter_async_context(
                    SurveyClient(
                        bucket_limits, api_key=api_key if bucket == "ncbi_eutils" else None
                    )
                )
                for bucket, bucket_limits in limits.items()
            }
            return await probe_links(targets, clients)

    probes = asyncio.run(main())
    for probe in sorted(probes, key=lambda p: p.link):
        shown = ", ".join(probe.found[:3]) + (" ..." if probe.found_count > 3 else "")
        failed = f" FAILED CHECKS {probe.failed_checks}" if probe.failed_checks else ""
        typer.echo(
            f"{probe.outcome:9s} {probe.link:64s} {probe.accession:22s} "
            f"{probe.found_count:>6} {probe.error or shown}{failed}"
        )
    outcomes = Counter(p.outcome for p in probes)
    typer.echo(", ".join(f"{outcomes[o]} {o}" for o in Outcome), err=True)
    if write:
        kept = merge_probes(load_probes(), probes)
        save_probes(kept)
        typer.echo(f"wrote {len(kept)} probes to {PROBES_PATH}", err=True)


@survey_app.command("run")
def survey_run(
    route_id: Annotated[str, typer.Option("--route", help="Route id to survey.")],
    corpus_name: Annotated[str, typer.Option("--corpus", help="Corpus name.")] = "hard-cases",
    limit: Annotated[int | None, typer.Option(help="Query at most this many accessions.")] = None,
    concurrency: Annotated[int, typer.Option(help="Requests in flight at once.")] = 6,
    rps: Annotated[float, typer.Option(help="Requests per second.")] = 5.0,
    cache_path: Annotated[Path, typer.Option("--cache", help="SQLite store.")] = DEFAULT_CACHE_PATH,
    resume: Annotated[
        bool, typer.Option(help="Skip accessions that already have a verdict.")
    ] = True,
    retry_failed: Annotated[bool, typer.Option(help="Also re-query stored failures.")] = False,
) -> None:
    """Run one route over one corpus and record every verdict."""
    load_dotenv()
    route = _route_or_exit(route_id)
    fn = IMPLEMENTATIONS.get(route_id)
    if fn is None:
        typer.secho(
            f"Route {route_id} is declared but not implemented.", fg=typer.colors.RED, err=True
        )
        raise typer.Exit(2)

    corpus = corpora.load(corpus_name)
    # Only the accessions this route can actually accept as a source.
    accessions = list(corpus.of_type(route.source))
    if not accessions:
        typer.secho(
            f"Corpus {corpus_name!r} holds no {route.source} accessions for {route_id}.",
            fg=typer.colors.YELLOW,
        )
        raise typer.Exit(1)
    scratch: tempfile.TemporaryDirectory[str] | None = None
    if limit:
        # Sample in the survey's own visit order, never from the sorted corpus.
        # Slicing a sorted corpus takes the lowest accession numbers -- the
        # oldest submissions -- which is a biased sample dressed as a limit.
        accessions = shuffled(accessions)[:limit]
        # And never into the cache. Verdicts are keyed on the corpus name, so a
        # limited run is recorded as if it covered the whole corpus and exports
        # as evidence it is not: experiment->run:ena_filereport was a --limit
        # 3000 run cited under the full corpus name for exactly this reason.
        scratch = tempfile.TemporaryDirectory(prefix="fetch-limit-")
        cache_path = Path(scratch.name) / "scratch.sqlite"
        typer.secho(
            f"--limit is a smoke test: verdicts go to a throwaway cache, not the survey "
            f"cache. For evidence, use --corpus sample:{limit}@{corpus_name}.",
            fg=typer.colors.YELLOW,
            err=True,
        )

    logfile = run_logfile(_slug(route_id))
    configure_logging(level=logging.INFO, logfile=logfile)
    typer.echo(f"{route_id} over {corpus_name}: {len(accessions)} accessions -> {logfile}")

    async def main() -> None:
        limits = Limits(rps=rps, concurrency=concurrency)
        # SurveyCache is a plain context manager -- it wraps a sqlite3
        # connection, not an async resource.
        with SurveyCache(cache_path) as cache:
            async with SurveyClient(limits=limits, api_key=default_api_key()) as client:
                run = await run_route(
                    route,
                    accessions,
                    fn,
                    cache,
                    corpus.name,
                    client,
                    resume=resume,
                    retry_failed=retry_failed,
                )
                typer.echo(json.dumps(run.as_dict(), indent=2))

    try:
        asyncio.run(main())
    finally:
        if scratch is not None:
            scratch.cleanup()


@survey_app.command("promote")
def survey_promote(
    source: Annotated[
        str | None, typer.Option(help="Only routes from this entity type, e.g. bioproject.")
    ] = None,
    mechanism: Annotated[str | None, typer.Option(help="Only this mechanism's routes.")] = None,
    gate: Annotated[
        bool, typer.Option("--gate", help="Over probe-families and hard-cases first.")
    ] = False,
    dry_run: Annotated[bool, typer.Option("--plan", help="Print units and sizes only.")] = False,
    evidence: Annotated[
        bool, typer.Option("--evidence", help="Print the PROMOTED rows the survey supports.")
    ] = False,
    retry_failed: Annotated[bool, typer.Option(help="Also re-ask stored failures.")] = False,
    covered: Annotated[
        bool,
        typer.Option(
            "--covered",
            help="Second routes: candidates in directions a declared route already covers.",
        ),
    ] = False,
    fresh_days: Annotated[
        int | None,
        typer.Option(
            help="With --covered: a declared route's verdict on the draw older than this "
            "many days is asked again, beside the candidates."
        ),
    ] = None,
    compare: Annotated[
        bool,
        typer.Option(
            "--compare",
            help="Offline: what each route of a direction adds that its others do not.",
        ),
    ] = False,
    out: Annotated[
        Path | None, typer.Option(help="With --compare: write one row per route to this CSV.")
    ] = None,
    confirmed: Annotated[
        Path | None,
        typer.Option(
            help="With --compare: count a candidate's addition only where this check "
            "(scripts/second_routes_roundtrip.py) confirmed it."
        ),
    ] = None,
    cache_path: Annotated[Path, typer.Option("--cache", help="SQLite store.")] = DEFAULT_CACHE_PATH,
) -> None:
    """Survey surveyed catalogue links as routes, at sample:3000 of their source.

    The candidates are every surveyed link in a direction no declared route
    covers (``routes.catalogue.candidates``). One request per accession answers
    every route of a mechanism from one source. Gate first; a unit that failed
    its gate is not drawn. --evidence prints the graph.PROMOTED rows to declare.

    --covered takes the other half: candidates in directions a route already
    covers. Each is compared with its direction's routes on the same draw, so
    the declared routes that hold no verdict on the draw are surveyed on it in
    the same run. --compare reports, per direction, what each route adds
    (``survey.directions``; docs/decisions/when-a-second-route-earns-its-place.md).
    """
    from fetch_series.routes.catalogue import candidates
    from fetch_series.survey import promote

    load_dotenv()
    if compare:
        with SurveyCache(cache_path) as cache:
            _compare_directions(
                cache, candidates(covered=True), source, out, _confirmations(confirmed)
            )
        return
    chosen = [
        c
        for c in candidates(covered=covered)
        if (source is None or c.source.value == source)
        and (mechanism is None or c.mechanism == mechanism)
    ]
    if not chosen:
        typer.secho("No candidate route matches.", fg=typer.colors.RED, err=True)
        raise typer.Exit(2)
    units = promote.units_of(chosen)
    route_jobs: list[promote.RouteJob] = []

    with SurveyCache(cache_path) as cache:
        if evidence:
            # The rows to paste into graph.PROMOTED; anything held back says why.
            for candidate in chosen:
                row = promote.evidence_row(
                    candidate, promote.standard_corpus(candidate.source), cache
                )
                if row is None:
                    typer.echo(f"    # {candidate.id}: not surveyed")
                elif not promote.answered(row):
                    typer.echo(f"    # {candidate.id}: resolved nothing; stays a link")
                else:
                    typer.echo(promote.as_source(row))
            return
        if gate:
            ungated = []
            for unit in units:
                if elsewhere := promote.failed_elsewhere(unit, cache):
                    typer.secho(
                        f"{unit.id}: failed its gate as {elsewhere[0]}; not gated again", err=True
                    )
                else:
                    ungated.append(unit)
            survey_plan = promote.plan(ungated, lambda unit: list(promote.GATES))
        else:
            gated = []
            for unit in units:
                verdict = promote.gate_passed(unit, cache)
                if dry_run or verdict:
                    gated.append(unit)
                elif verdict is None:
                    typer.secho(f"{unit.id}: not gated yet; run --gate first", err=True)
                else:
                    typer.secho(f"{unit.id}: failed its gate; not drawn", err=True)
            survey_plan = promote.plan(gated, lambda unit: [promote.standard_corpus(unit.source)])
            if covered:
                since = (
                    datetime.now(UTC) - timedelta(days=fresh_days)
                    if fresh_days is not None
                    else None
                )
                route_jobs = promote.declared_jobs(promote.directions_of(gated), cache, since=since)
        for unit, reason in survey_plan.skipped:
            typer.secho(f"{unit.id}: skipped -- {reason}", fg=typer.colors.YELLOW, err=True)
        if dry_run:
            for job in survey_plan.jobs:
                typer.echo(
                    f"{job.unit.id:44s} {len(job.unit.routes):>2} routes "
                    f"{len(job.accessions):>6} acc  {job.corpus}"
                )
            typer.echo(
                f"{len(survey_plan.jobs)} unit-corpora, "
                f"{sum(len(j.accessions) for j in survey_plan.jobs):,} requests before memory"
            )
            for route_job in route_jobs:
                typer.echo(
                    f"declared {route_job.route.id:52s} {len(route_job.accessions):>6} acc  "
                    f"{route_job.corpus}"
                )
            if route_jobs:
                typer.echo(
                    f"{len(route_jobs)} declared routes owed verdicts on the draw, "
                    f"{sum(len(j.accessions) * j.route.cost.requests for j in route_jobs):,} "
                    "requests"
                )
            return

        logfile = run_logfile("promote-survey")
        configure_logging(level=logging.INFO, logfile=logfile)
        typer.echo(f"{len(survey_plan.jobs)} unit-corpora -> {logfile}", err=True)

        def report(job: promote.Job, run: FanoutRun) -> None:
            for unit_id, counts in run.units.items():
                typer.echo(
                    f"{unit_id:52s} {job.corpus:44s} r={counts['resolved']} "
                    f"e={counts['empty']} f={counts['failed']} d={counts['distinct']}"
                )

        def report_route(job: promote.RouteJob, run: SurveyRun) -> None:
            typer.echo(
                f"{job.route.id:52s} {job.corpus:44s} r={run.resolved} "
                f"e={run.empty} f={run.failed} d={run.unique_results}"
            )

        api_key = default_api_key()

        async def main() -> None:
            limits = host_limits(has_api_key=bool(api_key))
            async with contextlib.AsyncExitStack() as stack:
                clients: dict[str, SurveyClient] = {}
                for bucket, bucket_limits in limits.items():
                    key = api_key if bucket == "ncbi_eutils" else None
                    clients[bucket] = await stack.enter_async_context(
                        MemoClient(bucket_limits, api_key=key, memoize=memoizable)
                    )
                await asyncio.gather(
                    promote.run_jobs(
                        survey_plan.jobs, clients, cache, retry_failed=retry_failed, on_done=report
                    ),
                    promote.run_route_jobs(route_jobs, clients, cache, on_done=report_route),
                )

        asyncio.run(main())


def _confirmations(
    path: Path | None,
) -> tuple[dict[str, list[tuple[str, str]]], dict[str, int]] | None:
    """Per candidate route, the (accession, value) pairs the round-trip check confirmed,
    and how many accessions it was found adding another accession's data to."""
    if path is None:
        return None
    checked = json.loads(path.read_text())
    confirmed = {
        route: [
            (row["source"], row["added"]) for row in found["rows"] if row["verdict"] == "confirmed"
        ]
        for route, found in checked.items()
    }
    contradicted = {
        route: len({row["source"] for row in found["rows"] if row["verdict"] == "contradicted"})
        for route, found in checked.items()
    }
    return confirmed, contradicted


def _compare_directions(
    cache: SurveyCache,
    pool: list[Candidate],
    source: str | None,
    out: Path | None,
    confirmation: tuple[dict[str, list[tuple[str, str]]], dict[str, int]] | None = None,
) -> None:
    """Every direction with two routes or more on its draw: what each adds over the rest."""
    from fetch_series.survey import directions, promote

    pairs = {(r.source, r.target) for r in REGISTRY} | {(c.source, c.target) for c in pool}
    loaded: dict[str, tuple[str, ...]] = {}
    rows: list[list[object]] = []
    for s, t in sorted(pairs, key=lambda d: (d[0].value, d[1].value)):
        if source is not None and s.value != source:
            continue
        try:
            corpus = promote.standard_corpus(s)
        except KeyError:
            continue  # no population to draw from: nothing was surveyed on a draw
        if corpus not in loaded:
            loaded[corpus] = corpora.load(corpus).of_type(s)
        accessions = loaded[corpus]
        declared = [r.id for r in REGISTRY.ranked(s, t)]
        route_ids = declared + [c.id for c in pool if (c.source, c.target) == (s, t)]
        verdicts = {
            route_id: found
            for route_id in route_ids
            if (found := directions.draw_verdicts(route_id, corpus, accessions, cache))
        }
        if len(verdicts) < 2:
            continue
        confirmed, contradicted = confirmation if confirmation is not None else (None, None)
        direction = directions.compare(
            s,
            t,
            corpus,
            accessions,
            verdicts,
            declared=declared,
            confirmed=confirmed,
            contradicted=contradicted,
        )
        if confirmed is not None:
            for unchecked in sorted(set(verdicts) - set(declared) - set(confirmed)):
                typer.echo(f"  not checked, so counted as adding nothing: {unchecked}")
        for line in directions.report(direction):
            typer.echo(line)
        for unsurveyed in sorted(set(route_ids) - set(verdicts)):
            typer.echo(f"  not on the draw: {unsurveyed}")
        typer.echo("")
        for share in direction.routes:
            rows.append(
                [
                    s.value,
                    t.value,
                    corpus,
                    direction.compared,
                    direction.agree,
                    share.route_id,
                    "declared" if share.declared else "candidate",
                    share.resolved,
                    share.empty,
                    share.failed,
                    share.distinct,
                    share.adds_values,
                    share.added_values,
                    share.sole,
                    share.adds_over_failure,
                    share.sole_over_failure,
                    share.unconfirmed,
                    share.contradicted,
                    share.accepted is not None
                    or (share.declared and share.earns(direction.compared)),
                    share.first,
                    share.last,
                ]
            )
    if out is not None:
        with out.open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                [
                    "source",
                    "target",
                    "corpus",
                    "compared",
                    "agree",
                    "route",
                    "kind",
                    "resolved",
                    "empty",
                    "failed",
                    "distinct",
                    "adds_values",
                    "added_values",
                    "sole",
                    "adds_over_failure",
                    "sole_over_failure",
                    "unconfirmed",
                    "contradicted",
                    "earns",
                    "first",
                    "last",
                ]
            )
            writer.writerows(rows)
        typer.echo(f"wrote {len(rows)} rows to {out}", err=True)


@survey_app.command("show")
def survey_show(
    route_id: Annotated[str, typer.Option("--route")],
    corpus_name: Annotated[str, typer.Option("--corpus")] = "hard-cases",
    cache_path: Annotated[Path, typer.Option("--cache")] = DEFAULT_CACHE_PATH,
    failures: Annotated[bool, typer.Option(help="List the failures in full.")] = False,
) -> None:
    """Summarise a recorded survey."""
    with SurveyCache(cache_path) as cache:
        typer.echo(json.dumps(cache.summary(corpus_name, route_id), indent=2))
        if failures:
            for result in cache.results(corpus_name, route_id):
                if result.error_class:
                    typer.echo(
                        f"  {result.accession}: {result.error_class}: {result.error_message}"
                    )


@survey_app.command("evidence")
def survey_evidence(
    route_id: Annotated[str, typer.Option("--route")],
    corpus_name: Annotated[str, typer.Option("--corpus")],
    cache_path: Annotated[Path, typer.Option("--cache")] = DEFAULT_CACHE_PATH,
) -> None:
    """Emit the RouteEvidence block for a completed survey.

    Ranking is supposed to be derived from measurement, so the numbers in
    graph.py must come from a survey rather than from someone's recollection of
    one. This prints the block to paste, with the date and corpus already filled
    in, so the two cannot quietly drift apart.
    """
    _route_or_exit(route_id)
    with SurveyCache(cache_path) as cache:
        summary = cache.summary(corpus_name, route_id)

    if not summary["queried"]:
        typer.secho(
            f"No recorded results for {route_id} on {corpus_name}.", fg=typer.colors.RED, err=True
        )
        raise typer.Exit(1)

    # EMPTY is a real answer about the archive, not a failure, so it counts
    # towards the denominator and not towards accessions_failed -- and is
    # recorded in its own field, because the 2026-05 scripts that lumped the two
    # together left routes whose split can never be recovered (two, since the
    # BioProject -> run routes were re-surveyed on 2026-10-26).
    typer.echo(
        f"""        evidence=RouteEvidence(
            corpus="{corpus_name}",
            surveyed_on=date({datetime.now(UTC).date():%Y, %-m, %-d}),
            accessions_queried={summary["queried"]:_},
            accessions_failed={summary["failed"]:_},
            accessions_empty={summary["empty"]:_},
            unique_results={summary["unique_results"]:_},
            notes="{summary["resolved"]:,} resolved, {summary["empty"]:,} empty.",
        ),"""
    )


@survey_app.command("compare")
def survey_compare(
    route_ids: Annotated[list[str], typer.Option("--route", help="Repeat to compare several.")],
    corpus_name: Annotated[str, typer.Option("--corpus")] = "hard-cases",
    cache_path: Annotated[Path, typer.Option("--cache")] = DEFAULT_CACHE_PATH,
    examples: Annotated[int, typer.Option(help="Accessions to name per disagreement class.")] = 5,
    intersection: Annotated[
        bool,
        typer.Option(help="Compare only accessions every route has answered."),
    ] = False,
    out: Annotated[Path | None, typer.Option(help="Write every disagreement to this CSV.")] = None,
) -> None:
    """Compare routes that answer the same question.

    Agreement is cheap to report and tells you little; the disagreements are the
    evidence. Comparison is keyed on (outcome, results), not results alone: a
    route that FAILED and one that returned a genuine EMPTY both have no
    results, but they are saying entirely different things -- "I could not find
    out" versus "the archive holds no such link" -- and reporting them as
    agreeing is the confusion this project exists to remove.

    With two routes the disagreements are bucketed by direction, because at
    corpus scale the interesting question is not which accessions differ but
    whether one route is systematically missing what the other finds.
    """
    with SurveyCache(cache_path) as cache:
        per_route = {
            route_id: {
                r.accession: (r.outcome.value, frozenset(r.results))
                for r in cache.results(corpus_name, route_id)
            }
            for route_id in route_ids
        }
        for route_id in route_ids:
            typer.echo(json.dumps(cache.summary(corpus_name, route_id), indent=2))

    missing: tuple[str, frozenset[str]] = ("absent", frozenset())
    if not per_route:
        accessions: list[str] = []
    elif intersection:
        # Needed while a survey is still running: one route being further
        # through the corpus than another is not a disagreement about the
        # archive, and counting it as one would swamp the real signal.
        accessions = sorted(set.intersection(*(set(v) for v in per_route.values())))
    else:
        accessions = sorted(set().union(*(set(v) for v in per_route.values())))
    disagreements = [
        a for a in accessions if len({per_route[r].get(a, missing) for r in route_ids}) > 1
    ]

    typer.echo(f"\naccessions compared: {len(accessions):,}")
    typer.echo(f"agreements:          {len(accessions) - len(disagreements):,}")
    typer.echo(f"disagreements:       {len(disagreements):,}")

    if len(route_ids) == 2:
        left, right = route_ids
        buckets: dict[str, list[str]] = {
            f"only {left} found anything": [],
            f"only {right} found anything": [],
            "both found different sets": [],
            "same results, different outcome": [],
        }
        for accession in disagreements:
            lo, lr = per_route[left].get(accession, missing)
            ro, rr = per_route[right].get(accession, missing)
            if lr and not rr:
                buckets[f"only {left} found anything"].append(accession)
            elif rr and not lr:
                buckets[f"only {right} found anything"].append(accession)
            elif lr != rr:
                buckets["both found different sets"].append(accession)
            else:
                buckets["same results, different outcome"].append(accession)

        typer.echo("")
        for label, members in buckets.items():
            if not members:
                continue
            share = len(members) / len(accessions) if accessions else 0
            typer.echo(f"  {label}: {len(members):,} ({share:.1%})")
            for accession in members[:examples]:
                lo, lr = per_route[left].get(accession, missing)
                ro, rr = per_route[right].get(accession, missing)
                typer.echo(
                    f"      {accession:14s} {left.split(':')[-1]}={len(lr) or f'({lo})'}"
                    f"  {right.split(':')[-1]}={len(rr) or f'({ro})'}"
                )
            if len(members) > examples:
                typer.echo(f"      ... and {len(members) - examples:,} more")
    else:
        for accession in disagreements[:examples]:
            typer.echo(f"  {accession}")
            for route_id in route_ids:
                outcome, found = per_route[route_id].get(accession, missing)
                typer.echo(f"    {route_id:44s} {sorted(found) if found else f'({outcome})'}")

    if out is not None:
        import csv as _csv

        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("w", newline="") as handle:
            writer = _csv.writer(handle)
            writer.writerow(
                [
                    "accession",
                    *(f"{r}:outcome" for r in route_ids),
                    *(f"{r}:results" for r in route_ids),
                ]
            )
            for accession in disagreements:
                cells = [per_route[r].get(accession, missing) for r in route_ids]
                writer.writerow(
                    [accession, *(c[0] for c in cells), *(";".join(sorted(c[1])) for c in cells)]
                )
        typer.echo(f"\nwrote {len(disagreements):,} disagreements to {out}")


@app.command()
def resolve(
    accession: Annotated[str, typer.Argument(help="Any accession, e.g. GSE236084.")],
    to: Annotated[str, typer.Option(help="Target entity type, e.g. experiment or run.")],
    mode: Annotated[
        str, typer.Option(help="'union' asks every applicable route; 'first' stops at one.")
    ] = "union",
    explain: Annotated[
        bool, typer.Option(help="Show every route tried and where each value came from.")
    ] = False,
    confirmed_only: Annotated[
        bool,
        typer.Option(
            "--confirmed-only", help="Report only values a data-proving route vouched for."
        ),
    ] = False,
    multi_hop: Annotated[
        bool, typer.Option(help="Walk a multi-hop path when no direct route answers.")
    ] = True,
) -> None:
    """Resolve one accession to another entity type.

    Takes the union of every applicable route by default, because for the one
    direction measured in full no single route was sufficient. Values are
    reported with whether a route that only ever returns entities carrying data
    vouched for them -- an accession is not evidence that data exists.
    """
    load_dotenv()
    configure_logging(level=logging.WARNING)
    try:
        parsed = parse(accession)
    except UnknownAccessionError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from None

    try:
        target = EntityType(to)
    except ValueError:
        typer.secho(
            f"Unknown entity type {to!r}. Known: {', '.join(e.value for e in EntityType)}",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(2) from None

    try:
        resolve_mode = Mode(mode)
    except ValueError:
        typer.secho(
            f"Unknown mode {mode!r}; expected 'union' or 'first'.", fg=typer.colors.RED, err=True
        )
        raise typer.Exit(2) from None

    async def run_resolution() -> Resolution:
        async with _client_for_one_query(4) as client:
            if multi_hop and resolve_mode is Mode.UNION:
                return await resolve_path(parsed, target, client)
            return await resolver_resolve(parsed, target, client, mode=resolve_mode)

    resolution = asyncio.run(run_resolution())

    if explain:
        for line in resolution.explain():
            typer.echo(line, err=True)

    shown = resolution.confirmed if confirmed_only else resolution.values
    if not shown:
        paths = REGISTRY.find_paths(parsed.entity, target)
        if not resolution.routes_resolved and paths and len(paths[0]) > 2:
            typer.secho(
                "No direct route resolved. Known multi-hop paths:",
                fg=typer.colors.YELLOW,
                err=True,
            )
            for path in paths[:5]:
                typer.echo("  " + " -> ".join(path), err=True)
        else:
            typer.secho("No route produced a result.", fg=typer.colors.YELLOW, err=True)
        raise typer.Exit(1)

    confirmed = set(resolution.confirmed)
    for value in shown:
        if explain:
            routes = ",".join(a.route_id.split(":")[-1] for a in resolution.attributions[value])
            flag = "confirmed" if value in confirmed else "UNCONFIRMED"
            typer.echo(f"{value}\t{flag}\t{routes}")
        else:
            typer.echo(value)


@app.command()
def relations(
    accession: Annotated[str, typer.Argument(help="A series, project, study or sample accession.")],
    out: Annotated[
        Path | None, typer.Option("-o", "--out", help="Write TSV here instead of stdout.")
    ] = None,
    sample_map: Annotated[
        bool, typer.Option("--sample-map", help="Emit sample-to-runs instead of the full table.")
    ] = False,
    incomplete: Annotated[
        bool, typer.Option(help="Include runs that are not usable for reprocessing.")
    ] = True,
) -> None:
    """Cross-archive relation table: one row per run.

    Joins what only GEO knows (GSM to experiment) onto what ENA reports (run,
    experiment, sample, BioSample, study, species) in a single request. The
    experiment accession is the only key the two archives share.
    """
    load_dotenv()
    configure_logging(level=logging.WARNING)
    parsed = resolve_input(accession)

    async def run_it() -> list[RunRecord]:
        async with _client_for_one_query(4) as client:
            return await build_relations(parsed, client)

    records = asyncio.run(run_it())
    if not records:
        typer.secho(f"No runs found for {parsed}.", fg=typer.colors.YELLOW, err=True)
        raise typer.Exit(1)

    usable = [r for r in records if r.is_complete_for_reprocessing]
    skipped = [r for r in records if not r.is_complete_for_reprocessing]
    if skipped:
        gaps: dict[str, int] = {}
        for record in skipped:
            for gap in record.missing_for_reprocessing:
                gaps[gap] = gaps.get(gap, 0) + 1
        typer.secho(
            f"{len(skipped)} of {len(records)} runs are not usable for reprocessing "
            f"(missing: {', '.join(f'{k} x{v}' for k, v in sorted(gaps.items()))})",
            fg=typer.colors.YELLOW,
            err=True,
        )
    conflicted = [r for r in records if r.conflicts]
    if conflicted:
        typer.secho(
            f"{len(conflicted)} runs have fields where two archives disagree; "
            "the highest-ranked route's value is used. Fields: "
            + ", ".join(sorted({f for r in conflicted for f in r.conflicts})),
            fg=typer.colors.YELLOW,
            err=True,
        )

    chosen = records if incomplete else usable
    lines: list[str] = []
    if sample_map:
        lines.append("sample\truns")
        for sample, runs in sample_to_runs(chosen).items():
            lines.append(f"{sample}\t{','.join(runs)}")
    else:
        lines.append("\t".join(RELATION_COLUMNS))
        lines.extend("\t".join(record.as_row()) for record in chosen)

    body = "\n".join(lines) + "\n"
    if out is None:
        typer.echo(body, nl=False)
    else:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(body)
        typer.echo(f"wrote {len(chosen)} rows to {out}", err=True)


async def _inspected(
    parsed: Accession,
    client: SurveyClient,
    limit: int | None,
    include_sdl: bool,
    reads: int,
    reports: list[dict[str, Any]] | None = None,
) -> list[Recommendation]:
    """Recommendations with the refusal path: offers whose reads prove them unusable.

    Each run's inspection report is appended to ``reports`` where one is given.
    """
    from fetch_series.inspect import lists as barcode_lists
    from fetch_series.inspect import references
    from fetch_series.inspect.report import inspect_fileset, refusals, summary

    held = barcode_lists.available()
    if not held:
        typer.secho(NO_LISTS, fg=typer.colors.RED, err=True)
        raise typer.Exit(2)
    refs = {k: v for k in references.REGISTER if (v := references.index(k)) is not None}
    recommended = []
    for fileset in await filesets_for(parsed, client, limit=limit, include_sdl=include_sdl):
        report = await inspect_fileset(fileset, client, held, refs, reads=reads)
        typer.echo(summary(report), err=True)
        recommended.append(recommend(fileset, refusals(report)))
        if reports is not None:
            reports.append(report)
    return recommended


@app.command()
def files(
    accession: Annotated[str, typer.Argument(help="A run, or anything that resolves to runs.")],
    out: Annotated[
        Path | None, typer.Option("-o", "--out", help="Write TSV here instead of stdout.")
    ] = None,
    limit: Annotated[int | None, typer.Option(help="Ask about at most this many runs.")] = None,
    explain: Annotated[
        bool, typer.Option("--explain", help="Show every route's offer and why one won.")
    ] = False,
    all_offers: Annotated[
        bool, typer.Option("--all", help="Emit every offer, not just the recommended set.")
    ] = False,
    no_sdl: Annotated[
        bool, typer.Option("--no-sdl", help="Skip NCBI SDL; ENA columns only.")
    ] = False,
    check: Annotated[
        bool,
        typer.Option("--verify", help="HEAD every recommended link and check the published size."),
    ] = False,
    read_them: Annotated[
        bool,
        typer.Option(
            "--inspect", help="Read each offer's first reads; refuse a set proven unusable."
        ),
    ] = False,
    reads: Annotated[int, typer.Option(help="With --inspect: reads per file.")] = 100_000,
    json_out: Annotated[
        Path | None,
        typer.Option(
            "--json",
            help="With --inspect: write each run's inspection report, with its readiness "
            "blocks (geometry, mapping, convertible), here.",
        ),
    ] = None,
) -> None:
    """Download links for a run, with checksums and the reason for the choice.

    Asks every file route rather than stopping at the first that answers,
    because they do not answer the same question: ENA's fastq columns hold ENA's
    derivations, submitted holds the submitter's deposit, SDL holds NCBI's view
    of both. A run can have one and not the other.
    """
    load_dotenv()
    configure_logging(level=logging.WARNING)
    if json_out is not None and not read_them:
        typer.secho("--json needs --inspect", fg=typer.colors.RED, err=True)
        raise typer.Exit(2)
    parsed = resolve_input(accession)
    reports: list[dict[str, Any]] = []

    async def run_it() -> tuple[list[Recommendation], list[Verdict]]:
        async with _client_for_one_query(6) as client:
            if read_them:
                recommended = await _inspected(parsed, client, limit, not no_sdl, reads, reports)
            else:
                recommended = await recommendations_for(
                    parsed, client, limit=limit, include_sdl=not no_sdl
                )
            if not check:
                return recommended, []
            # Only the recommended files: verifying every alternative triples
            # the requests to check links the caller is not going to use.
            links = [f for r in recommended if r.chosen for f in r.chosen.files]
            return recommended, await verify_all(links, client)

    results, verdicts = asyncio.run(run_it())
    if json_out is not None:
        from fetch_series.inspect.report import SCHEMA_VERSION

        json_out.write_text(
            json.dumps({"schema_version": SCHEMA_VERSION, "runs": reports}, indent=1) + "\n"
        )
        typer.echo(f"wrote {json_out}", err=True)
    if not results:
        typer.secho(f"No runs found for {parsed}.", fg=typer.colors.YELLOW, err=True)
        raise typer.Exit(1)

    empty = [r for r in results if r.chosen is None]
    if empty:
        typer.secho(
            f"{len(empty)} of {len(results)} runs had no downloadable data file: "
            + ", ".join(r.run for r in empty[:10])
            + ("..." if len(empty) > 10 else ""),
            fg=typer.colors.YELLOW,
            err=True,
        )

    if explain:
        for rec in results:
            typer.echo(rec.explain(), err=True)

    if verdicts:
        broken = [v for v in verdicts if not v.resolves or v.size_matches is False]
        for verdict in broken:
            typer.secho("  " + verdict.describe(), fg=typer.colors.RED, err=True)
        typer.secho(
            f"verified {len(verdicts)} links: {len(verdicts) - len(broken)} good, "
            f"{len(broken)} unusable",
            fg=typer.colors.RED if broken else typer.colors.GREEN,
            err=True,
        )

    lines = ["run\tsource\tkind\tmate\turl\tmd5\tbytes"]
    for rec in results:
        offers = [rec.chosen, *rec.alternatives] if all_offers else [rec.chosen]
        for candidate in offers:
            if candidate is None:
                continue
            for record in candidate.files:
                lines.append(
                    "\t".join(
                        [
                            record.run,
                            record.source,
                            str(record.kind),
                            str(record.mate) if record.mate is not None else MISSING,
                            record.url,
                            record.md5 or MISSING,
                            str(record.size) if record.size is not None else MISSING,
                        ]
                    )
                )

    body = "\n".join(lines) + "\n"
    if out is None:
        typer.echo(body, nl=False)
    else:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(body)
        typer.echo(f"wrote {len(lines) - 1} file links to {out}", err=True)


@app.command()
def links(
    accession: Annotated[
        str, typer.Argument(help="A series, project, study or ArrayExpress experiment.")
    ],
    out: Annotated[
        Path | None, typer.Option("-o", "--out", help="Write TSV here instead of stdout.")
    ] = None,
    limit: Annotated[int | None, typer.Option(help="At most this many runs.")] = None,
    header: Annotated[bool, typer.Option(help="Emit a header row.")] = False,
    samples: Annotated[
        str | None,
        typer.Option(
            "--samples",
            help="Comma-separated sample accessions (GSM or SRS/ERS) to keep.",
        ),
    ] = None,
) -> None:
    """Emit links.tsv in the schema the reprocessing pipeline reads today.

    run / species / url(s) / type{ORIFQ,ENAFQ,BAM,SRA} / sample -- the parity
    contract with fetch10xmeta. The ordering is the incumbent's, deliberately,
    including where this project's own file layer would choose differently; see
    docs/routes/run-to-file.
    """
    load_dotenv()
    configure_logging(level=logging.WARNING)
    parsed = resolve_input(accession)

    async def run_it() -> list[LinkRow]:
        async with _client_for_one_query(6) as client:
            return await links_table(parsed, client, limit=limit)

    rows = asyncio.run(run_it())
    if not rows:
        typer.secho(f"No runs found for {parsed}.", fg=typer.colors.YELLOW, err=True)
        raise typer.Exit(1)

    if samples:
        wanted = {token.strip() for token in samples.split(",") if token.strip()}
        kept = [row for row in rows if row.sample in wanted]
        missing = wanted - {row.sample for row in rows}
        if missing:
            # Naming a sample the series does not contain is a typo or a stale
            # list, and silently returning fewer rows makes it look like the
            # archive lost data.
            typer.secho(
                f"{len(missing)} requested samples are not in {parsed}: "
                + ", ".join(sorted(missing)[:10]),
                fg=typer.colors.YELLOW,
                err=True,
            )
        if not kept:
            typer.secho("No runs left after filtering by sample.", fg=typer.colors.RED, err=True)
            raise typer.Exit(1)
        rows = kept

    unresolved = [row for row in rows if not row.urls]
    if unresolved:
        typer.secho(
            f"{len(unresolved)} of {len(rows)} runs have no download URL: "
            + ", ".join(row.run for row in unresolved[:10]),
            fg=typer.colors.YELLOW,
            err=True,
        )

    lines = ["\t".join(LINKS_COLUMNS)] if header else []
    lines.extend("\t".join(row.as_row()) for row in rows)
    body = "\n".join(lines) + "\n"
    if out is None:
        typer.echo(body, nl=False)
    else:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(body)
        typer.echo(f"wrote {len(rows)} runs to {out}", err=True)


@app.command("screen")
def screen_command(
    accession: Annotated[str, typer.Argument(help="A series, project, study or run.")],
    limit: Annotated[int | None, typer.Option(help="At most this many runs.")] = None,
    explain: Annotated[
        bool, typer.Option("--explain", help="Show the phrases each call rests on.")
    ] = False,
) -> None:
    """Say what assay each run is, from metadata, before anything is downloaded.

    Exits non-zero when no run is recognised as 10x, so a pipeline can gate on
    it. A run whose metadata carries no protocol text is *unknown*, not
    known-negative, and is reported separately -- screening on absence would
    discard every submission that left the field blank.
    """
    load_dotenv()
    configure_logging(level=logging.WARNING)
    parsed = resolve_input(accession)

    async def run_it() -> list[tuple[str, AssayCall, ChemistryCall]]:
        async with _client_for_one_query(6) as client:
            return await screen(parsed, client, limit=limit)

    calls = asyncio.run(run_it())
    if not calls:
        typer.secho(f"No runs found for {parsed}.", fg=typer.colors.YELLOW, err=True)
        raise typer.Exit(1)

    typer.echo("run\tassay\tlibrary_type\tchemistry")
    for run, call, chem in calls:
        kind = call.library_type or (
            "|".join(call.library_types) if call.ambiguous_library_type else MISSING
        )
        chemistry = chem.chemistry or ("|".join(chem.candidates) if chem.ambiguous else MISSING)
        typer.echo(f"{run}\t{call.assay or MISSING}\t{kind}\t{chemistry}")
        if explain:
            typer.echo(f"  {call.explain()}", err=True)
            typer.echo(f"  {chem.explain()}", err=True)

    recognised = [c for _, c, _ in calls if c.recognised]
    unknown = [r for r, c, _ in calls if not c.recognised and not c.had_metadata]
    ambiguous = [r for r, c, _ in calls if c.ambiguous_library_type]
    chemistries = {chem.chemistry for _, _, chem in calls if chem.chemistry}
    unsettled = [run for run, _, chem in calls if chem.ambiguous]
    notes = []
    if chemistries:
        notes.append("chemistry named: " + ", ".join(sorted(chemistries)))
    if unsettled:
        notes.append(f"{len(unsettled)} name more than one chemistry")
    if unknown:
        notes.append(f"{len(unknown)} carry no assay metadata at all")
    if ambiguous:
        notes.append(f"{len(ambiguous)} name more than one library type")
    typer.secho(
        f"{len(recognised)} of {len(calls)} runs recognised as 10x"
        + ("; " + "; ".join(notes) if notes else ""),
        fg=typer.colors.GREEN if recognised else typer.colors.YELLOW,
        err=True,
    )
    if not recognised:
        raise typer.Exit(2)


@app.command("metadata")
def metadata_command(
    accession: Annotated[str, typer.Argument(help="A series, project, study or experiment.")],
    out: Annotated[
        Path | None, typer.Option("-o", "--out", help="Write TSV here instead of stdout.")
    ] = None,
    limit: Annotated[int | None, typer.Option(help="At most this many runs.")] = None,
    explain: Annotated[
        bool, typer.Option("--explain", help="Show every claim, its source and its evidence.")
    ] = False,
    review: Annotated[
        bool, typer.Option("--review", help="The review queue, commonest reason first.")
    ] = False,
    keys: Annotated[
        bool, typer.Option("--keys", help="Raw attribute keys no rule mapped, by frequency.")
    ] = False,
    agreement_report: Annotated[
        bool, typer.Option("--agreement", help="Pairwise agreement between archives, per field.")
    ] = False,
    sources: Annotated[
        bool, typer.Option("--sources", help="Declared sources and whether each is measured.")
    ] = False,
    no_ground: Annotated[
        bool,
        typer.Option("--no-ground", help="Stop after tier 1, to measure what grounding adds."),
    ] = False,
) -> None:
    """Resolve descriptive metadata per sample, grounded in ontologies.

    Tiers 0-2: harvest what the four archives already state, normalise the keys
    and values, and ground each value in the ontology its field declares. No
    LLM key is needed and none is used.

    A value is grounded only by exact match or a curated annotation, and a
    field whose sources disagree is reported rather than resolved -- nobody has
    surveyed which archive to believe, so choosing would be a preference
    dressed as a measurement. ``--agreement`` reports how often they differ;
    ``--review`` lists what a human should settle.
    """
    load_dotenv()
    configure_logging(level=logging.WARNING)

    if sources:
        typer.echo("source\tcost\tmeasured\tknown_defects\tsummary")
        for source in metadata_source_list:
            state = "yes" if source.measured else "NO"
            defects = ",".join(source.known_pathologies) or MISSING
            typer.echo(f"{source.id}\t{source.cost}\t{state}\t{defects}\t{source.summary}")
        for slug in GROUNDING_PATHOLOGIES:
            typer.echo(
                f"(grounding)\t-\tNO\t{slug}\taffects every value grounded by the approximate step"
            )
        typer.secho(
            "No source carries a measurement: value accuracy for this layer is unmeasured, "
            "and docs/metadata.md says why. Ranking therefore falls back to source id and "
            "disagreements are reported rather than resolved.",
            fg=typer.colors.YELLOW,
            err=True,
        )
        return

    parsed = resolve_input(accession)

    async def run_it() -> Collection:
        async with _client_for_one_query(6) as client:
            return await collect(parsed, client, limit=limit, ground=not no_ground)

    collection = asyncio.run(run_it())
    if not collection.records:
        typer.secho(f"No samples found for {parsed}.", fg=typer.colors.YELLOW, err=True)
        raise typer.Exit(1)

    if keys:
        backlog = collection.unmapped_keys()
        typer.echo("key\tsamples")
        for key, count in backlog.items():
            typer.echo(f"{key}\t{count}")
        typer.secho(
            f"{len(backlog)} unmapped keys over {len(collection.records)} samples; "
            "a key on many samples earns a synonym entry, one on a single sample does not",
            err=True,
        )
        return

    if review:
        queue = collection.review_queue
        typer.echo("sample\treason")
        for sample, reason in queue:
            typer.echo(f"{sample}\t{reason}")
        for sample, conflicted in collection.conflicts.items():
            for name, said in conflicted.items():
                detail = "; ".join(f"{src}={'|'.join(v)}" for src, v in said.items())
                typer.echo(f"{sample}\t{name} disagrees between archives: {detail}")
        typer.secho(
            f"{len(queue)} flagged claims and {len(collection.conflicts)} samples with "
            "cross-archive conflicts",
            fg=typer.colors.YELLOW if queue or collection.conflicts else typer.colors.GREEN,
            err=True,
        )
        return

    if agreement_report:
        typer.echo("field\tleft\tright\tagreed\tdisagreed\tonly_left\tonly_right")
        for pair in agreement(collection.records):
            typer.echo(
                f"{pair.field}\t{pair.left}\t{pair.right}\t{pair.agreed}\t"
                f"{pair.disagreed}\t{pair.only_left}\t{pair.only_right}"
            )
        typer.secho(
            "Agreement is consistency, not correctness: two archives can agree and both be "
            "wrong. Pairs are counted only where both sources were consulted.",
            err=True,
        )
        return

    header = ["sample", *FIELD_NAMES]
    lines = ["\t".join(header)]
    for record in sorted(collection.records, key=lambda r: r.sample):
        row = [record.sample]
        for name in FIELD_NAMES:
            values = record.values(name)
            if not values:
                row.append(MISSING)
                continue
            # A CURIE is shown for every grounded value, whatever the field's
            # cardinality. Reading it off `asserted()` alone dropped the terms
            # from every multivalued field -- cell_type and tissue among them
            # -- because `asserted` is None there by design.
            #
            # A claim flagged for review renders the *submitter's* string with
            # the CURIE marked `?`, not the ontology label. The label is the
            # part under suspicion: GSE111360's `CD45+ cells from a fresh
            # kidney tumor sample` grounds to a rare innate lymphoid
            # precursor, and printing that label as the value would pass the
            # guess on as the finding.
            rendered: list[str] = []
            for value in values:
                claim = next((c for c in record.claims.get(name, ()) if c.value == value), None)
                if claim is None or claim.term is None:
                    rendered.append(value)
                elif claim.needs_review:
                    rendered.append(f"{claim.raw or claim.evidence} [{claim.term.curie}?]")
                else:
                    rendered.append(f"{value} [{claim.term.curie}]")
            # The pipe join lives here and only here: the library returns
            # candidates, and flattening them is a presentation decision.
            row.append("|".join(rendered))
        lines.append("\t".join(row))

    if out is not None:
        out.write_text("\n".join(lines) + "\n")
        typer.echo(f"wrote {len(collection.records)} samples to {out}", err=True)
    else:
        for line in lines:
            typer.echo(line)

    if explain:
        for record in sorted(collection.records, key=lambda r: r.sample):
            for line in record.explain():
                typer.echo(line, err=True)
        # Known defects of the sources that actually contributed, printed
        # beside the values as routes' pathologies are. A source that answered
        # nothing here gets no warning, because it affected nothing.
        contributed = {
            declared.id
            for record in collection.records
            for claims in record.claims.values()
            for claim in claims
            if (declared := metadata_source_of(claim.source)) is not None
        }
        for source in metadata_source_list:
            if source.id in contributed:
                for slug in source.known_pathologies:
                    typer.echo(
                        f"  known defect of {source.id}: {slug} (docs/pathologies/{slug}.md)",
                        err=True,
                    )
        if collection.stats.approximate:
            for slug in GROUNDING_PATHOLOGIES:
                typer.echo(
                    f"  known defect of the approximate grounding step: {slug} "
                    f"(docs/pathologies/{slug}.md)",
                    err=True,
                )
        if collection.study_text:
            typer.echo("  study-scoped text, never assigned to a sample:", err=True)
            for origin, text in collection.study_text.items():
                typer.echo(f"    {origin} = {text[:160]!r}", err=True)

    ungrounded = {name for record in collection.records for name in record.ungrounded}
    notes = [collection.stats.summary()]
    if ungrounded:
        notes.append("ungrounded fields: " + ", ".join(sorted(ungrounded)))
    if collection.conflicts:
        notes.append(f"{len(collection.conflicts)} samples where archives disagree (--review)")
    queue = collection.review_queue
    if queue:
        notes.append(f"{len(queue)} claims flagged for review (--review)")
    typer.secho(
        f"{len(collection.records)} samples; " + "; ".join(notes),
        fg=typer.colors.YELLOW if queue or collection.conflicts else typer.colors.GREEN,
        err=True,
    )


cache_app = typer.Typer(help="Inspect the survey cache and diff archive state over time.")
app.add_typer(cache_app, name="cache")


@cache_app.command("stats")
def cache_stats(
    cache: Annotated[Path, typer.Option(help="SQLite store.")] = DEFAULT_CACHE_PATH,
    corpus: Annotated[str | None, typer.Option(help="Only this corpus.")] = None,
) -> None:
    """What has been surveyed, and what each survey found."""
    with SurveyCache(cache) as store:
        rows = [
            store.summary(recorded_corpus, route_id)
            for recorded_corpus, route_id in store.routes_surveyed(corpus)
        ]
    typer.echo(json.dumps(rows, indent=2))


@survey_app.command("export")
def survey_export(
    out: Annotated[Path, typer.Option("-o", "--out", help="Directory to write into.")] = Path(
        "data/surveys"
    ),
    corpus_name: Annotated[str | None, typer.Option("--corpus", help="Only this corpus.")] = None,
    route_id: Annotated[str | None, typer.Option("--route", help="Only this route.")] = None,
    kind: Annotated[
        str | None, typer.Option("--kind", help="Only this kind of unit: route, link or screen.")
    ] = None,
    replace: Annotated[
        bool, typer.Option("--replace", help="Overwrite an export whose verdicts changed.")
    ] = False,
    cache_path: Annotated[Path, typer.Option("--cache", help="SQLite store.")] = DEFAULT_CACHE_PATH,
) -> None:
    """Write every recorded verdict out as CSV, one file per route and corpus.

    A committed export whose verdicts would change is refused unless --replace:
    these are dated evidence, and route rankings cite them.

    The SQLite cache is the working store and is not committed: it carries raw
    response bodies and runs to hundreds of megabytes. These CSVs are the part
    worth versioning -- one row per accession, the verdict, what it returned and
    why it failed -- which is what regenerates a comparison or an
    affected-accession list months later, when the archive no longer agrees with
    itself.

    A corpus name can contain `/` (`results-of:a->b:c@corpus`), so names are
    slugified for the filesystem and the original is written into the manifest
    beside the file. Reconstructing it from the filename would be guesswork.
    """
    written: list[tuple[str, int]] = []
    with SurveyCache(cache_path) as cache:
        pairs = [
            (recorded_corpus, recorded_route)
            for recorded_corpus, recorded_route in cache.routes_surveyed(corpus_name)
            if (route_id is None or recorded_route == route_id)
            and (kind is None or parse_unit(recorded_route).kind == kind)
        ]
        for recorded_corpus, recorded_route in pairs:
            try:
                path, rows = write_export(
                    out / _slug(recorded_corpus),
                    recorded_corpus,
                    recorded_route,
                    cache.results(recorded_corpus, recorded_route),
                    replace=replace,
                )
            except ExportConflict as exc:
                typer.secho(f"refused: {exc}", fg=typer.colors.RED, err=True)
                raise typer.Exit(1) from None
            written.append((str(path), rows))

    for written_path, rows in written:
        typer.echo(f"{rows:>8,}  {written_path}")
    typer.secho(
        f"exported {len(written)} surveys, {sum(r for _, r in written):,} verdicts", err=True
    )


@cache_app.command("diff")
def cache_diff(
    before: Annotated[Path, typer.Argument(help="The earlier snapshot.")],
    corpus: Annotated[str, typer.Option(help="Corpus to compare.")] = "hard-cases",
    route: Annotated[str | None, typer.Option(help="Only this route.")] = None,
    cache: Annotated[Path, typer.Option(help="The current store.")] = DEFAULT_CACHE_PATH,
) -> None:
    """Report what the archives changed between two runs of the same survey.

    This is the longitudinal record nobody else is keeping. A BioProject that
    resolved to one GEO series and now resolves to another has been re-created
    upstream; one that resolved and now does not has been withdrawn or made
    private. Neither event is announced anywhere, and both silently change what
    a pipeline resolves.

    Exits non-zero when anything changed, so a scheduled job can alert on it.
    """
    with SurveyCache(cache) as store, SurveyCache(before) as earlier:
        pairs = [
            (recorded_corpus, route_id)
            for recorded_corpus, route_id in store.routes_surveyed(corpus)
            if route is None or route_id == route
        ]
        report = {
            route_id: store.diff(recorded_corpus, route_id, earlier)
            for recorded_corpus, route_id in pairs
        }

    moved = {
        route_id: entry
        for route_id, entry in report.items()
        if entry["changed"] or entry["added"] or entry["removed"]
    }
    typer.echo(json.dumps(moved, indent=2))
    if not moved:
        typer.secho(f"no archive state changed across {len(report)} routes", err=True)
        return
    total = sum(len(entry["changed"]) for entry in moved.values())
    typer.secho(
        f"{total} accessions changed across {len(moved)} routes", fg=typer.colors.YELLOW, err=True
    )
    raise typer.Exit(1)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
