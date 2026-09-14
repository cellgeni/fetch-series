"""The ``fetch`` command line."""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Annotated

import typer
from dotenv import load_dotenv

from fetch_series.accession import EntityType, UnknownAccessionError, parse
from fetch_series.cache import DEFAULT_CACHE_PATH, SurveyCache
from fetch_series.core import default_api_key
from fetch_series.graph import REGISTRY, Route
from fetch_series.logging_utils import configure_logging, run_logfile
from fetch_series.routes import IMPLEMENTATIONS
from fetch_series.survey import Limits, RouteFn, SurveyClient, corpora, run_route

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
        accessions = accessions[:limit]

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


@survey_app.command("compare")
def survey_compare(
    route_ids: Annotated[list[str], typer.Option("--route", help="Repeat to compare several.")],
    corpus_name: Annotated[str, typer.Option("--corpus")] = "hard-cases",
    cache_path: Annotated[Path, typer.Option("--cache")] = DEFAULT_CACHE_PATH,
) -> None:
    """Compare routes that answer the same question.

    Reports per-route coverage and, crucially, the accessions where the routes
    disagree. Agreement is cheap to report and tells you little; the
    disagreements are the evidence.
    """
    with SurveyCache(cache_path) as cache:
        # Keyed on (outcome, results), not results alone. A route that FAILED
        # and one that returned a genuine EMPTY both have no results, but they
        # are saying completely different things -- "I could not find out" versus
        # "the archive holds no such link". Comparing result sets alone reports
        # them as agreeing, which is the specific confusion this project exists
        # to remove.
        per_route = {
            route_id: {
                r.accession: (r.outcome.value, frozenset(r.results))
                for r in cache.results(corpus_name, route_id)
            }
            for route_id in route_ids
        }
        missing: tuple[str, frozenset[str]] = ("absent", frozenset())
        for route_id in route_ids:
            typer.echo(json.dumps(cache.summary(corpus_name, route_id), indent=2))

        accessions = sorted(set().union(*(set(v) for v in per_route.values())) if per_route else [])
        disagreements = [
            accession
            for accession in accessions
            if len({per_route[r].get(accession, missing) for r in route_ids}) > 1
        ]
        typer.echo(f"\naccessions compared: {len(accessions)}")
        typer.echo(f"disagreements: {len(disagreements)}")
        for accession in disagreements:
            typer.echo(f"  {accession}")
            for route_id in route_ids:
                outcome, found = per_route[route_id].get(accession, missing)
                rendered = sorted(found) if found else f"({outcome})"
                typer.echo(f"    {route_id:44s} {rendered}")


@app.command()
def resolve(
    accession: Annotated[str, typer.Argument(help="Any accession, e.g. GSE236084.")],
    to: Annotated[str, typer.Option(help="Target entity type, e.g. run.")],
    cache_path: Annotated[Path, typer.Option("--cache")] = DEFAULT_CACHE_PATH,
    explain: Annotated[
        bool, typer.Option(help="Show the route tried and where the answer came from.")
    ] = False,
) -> None:
    """Resolve one accession to another entity type, trying routes in ranked order."""
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

    candidates = [r for r in REGISTRY.ranked(parsed.entity, target) if r.id in IMPLEMENTATIONS]
    if not candidates:
        paths = REGISTRY.find_paths(parsed.entity, target)
        typer.secho(
            f"No implemented direct route from {parsed.entity} to {to}.",
            fg=typer.colors.YELLOW,
            err=True,
        )
        if paths:
            typer.echo("Known multi-hop paths:", err=True)
            for path in paths[:5]:
                typer.echo("  " + " -> ".join(path), err=True)
        raise typer.Exit(1)

    async def main() -> None:
        async with SurveyClient(
            limits=Limits(rps=5.0, concurrency=4), api_key=default_api_key()
        ) as client:
            for route in candidates:
                if explain:
                    typer.echo(f"# trying {route.id}", err=True)
                route_fn = IMPLEMENTATIONS[route.id]

                async def attempt(timeout: float, fn: RouteFn = route_fn) -> list[str]:
                    return await fn(parsed.value, client, timeout)

                try:
                    found = await client.with_retry(attempt, label=route.id)
                except Exception as exc:
                    if explain:
                        typer.echo(f"#   failed: {type(exc).__name__}: {exc}", err=True)
                    continue
                if found:
                    if explain:
                        typer.echo(f"#   resolved via {route.id}", err=True)
                    for value in found:
                        typer.echo(value)
                    return
                if explain:
                    typer.echo("#   empty (archive holds no such link)", err=True)
            typer.secho("No route produced a result.", fg=typer.colors.YELLOW, err=True)
            raise typer.Exit(1)

    asyncio.run(main())


def main() -> None:
    app()


if __name__ == "__main__":
    main()
