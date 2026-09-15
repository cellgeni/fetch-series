"""The ``fetch`` command line."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from dotenv import load_dotenv

from fetch_series.accession import EntityType, UnknownAccessionError, parse
from fetch_series.cache import DEFAULT_CACHE_PATH, SurveyCache
from fetch_series.core import default_api_key
from fetch_series.entities import MISSING, RELATION_COLUMNS, RunRecord
from fetch_series.files import Recommendation
from fetch_series.filesets import recommendations_for
from fetch_series.graph import REGISTRY, Route
from fetch_series.logging_utils import configure_logging, run_logfile
from fetch_series.relations import relations as build_relations
from fetch_series.relations import resolve_input, sample_to_runs
from fetch_series.resolver import Mode, Resolution, resolve_path
from fetch_series.resolver import resolve as resolver_resolve
from fetch_series.routes import IMPLEMENTATIONS
from fetch_series.survey import Limits, SurveyClient, corpora, run_route
from fetch_series.survey.runner import shuffled

app = typer.Typer(
    help="Resolve public sequencing accessions over documented, benchmarked routes.",
    no_args_is_help=True,
)
survey_app = typer.Typer(help="Run and compare route surveys.", no_args_is_help=True)
routes_app = typer.Typer(help="Inspect the route graph.", no_args_is_help=True)
app.add_typer(survey_app, name="survey")
app.add_typer(routes_app, name="routes")


def _slug(route_id: str) -> str:
    """Filesystem-safe form of a route id.

    Route ids read well in a terminal ("gse->experiment:soft_family") and badly
    as directory names -- ">" needs quoting in every shell that would otherwise
    tab-complete the path.
    """
    return route_id.replace("->", "-to-").replace(":", "_").replace("/", "_")


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
) -> None:
    """List declared routes, with their measured evidence."""
    for route in sorted(REGISTRY, key=lambda r: (r.source, r.target, r.id)):
        if source and route.source != source:
            continue
        if unmeasured and route.is_measured:
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
    typer.echo("\n'!' marks a declared route with no implementation yet.")


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
    if limit:
        # Sample in the survey's own visit order, never from the sorted corpus.
        # Slicing a sorted corpus takes the lowest accession numbers -- the
        # oldest submissions -- which is a biased sample dressed as a limit.
        accessions = shuffled(accessions)[:limit]

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

    asyncio.run(main())


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
    # towards the denominator and not towards accessions_failed.
    typer.echo(
        f"""        evidence=RouteEvidence(
            corpus="{corpus_name}",
            surveyed_on=date({date.today():%Y, %-m, %-d}),
            accessions_queried={summary["queried"]:_},
            accessions_failed={summary["failed"]:_},
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
        async with SurveyClient(
            limits=Limits(rps=5.0, concurrency=4), api_key=default_api_key()
        ) as client:
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
        async with SurveyClient(
            limits=Limits(rps=5.0, concurrency=4), api_key=default_api_key()
        ) as client:
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
) -> None:
    """Download links for a run, with checksums and the reason for the choice.

    Asks every file route rather than stopping at the first that answers,
    because they do not answer the same question: ENA's fastq columns hold ENA's
    derivations, submitted holds the submitter's deposit, SDL holds NCBI's view
    of both. A run can have one and not the other.
    """
    load_dotenv()
    configure_logging(level=logging.WARNING)
    parsed = resolve_input(accession)

    async def run_it() -> list[Recommendation]:
        async with SurveyClient(
            limits=Limits(rps=5.0, concurrency=6), api_key=default_api_key()
        ) as client:
            return await recommendations_for(parsed, client, limit=limit, include_sdl=not no_sdl)

    results = asyncio.run(run_it())
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


def main() -> None:
    app()


if __name__ == "__main__":
    main()
