"""Are the values a second route adds right? Ask the reverse direction.

For each candidate route named on the command line, every value it returns on
its draw that none of its direction's declared routes returns is asked of the
reverse direction's declared routes, one by one. A value is

- **confirmed** when a reverse route on another provider maps it back to the
  accession it was added for;
- **contradicted** when such a route answers, and names another;
- **unconfirmed** when none of them knows it.

A file has no reverse route, so a file is confirmed when its name carries the
run it was added for, and contradicted when it names another run:
``efetch_sra_files`` lists SRR12771604.lite.1 among SRR12771605's files.

A route on the same provider cannot confirm a value: ELink from a project adds
its siblings' experiments (PRJNA1033389), and ELink from those experiments maps
them straight back.

This is a check on what ``fetch survey promote --compare`` credits, made live
and recorded beside the page that cites it
(docs/decisions/when-a-second-route-earns-its-place.md). It writes nothing to
the survey cache, and it never becomes RouteEvidence.

    uv run python scripts/second_routes_roundtrip.py --cache .cache/promote-20261026.sqlite \\
        --out data/surveys/second-routes/roundtrip-2026-10-29.json \\
        bioproject->biosample:ddbj_dblink ...
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
from pathlib import Path

from dotenv import load_dotenv

from fetch_series.accession import EntityType
from fetch_series.cache import SurveyCache
from fetch_series.core import default_api_key
from fetch_series.graph import REGISTRY
from fetch_series.logging_utils import configure_logging, redact
from fetch_series.results import Outcome
from fetch_series.routes import IMPLEMENTATIONS
from fetch_series.routes.catalogue import host_of
from fetch_series.survey import corpora, promote
from fetch_series.survey.client import Limits, SurveyClient
from fetch_series.survey.directions import draw_verdicts

# sra-db-be is ``sra_be`` on the hand-written routes and ``ncbi_web`` on the
# promoted ones: one service.
SAME_SERVICE = {"sra_be": "ncbi_web"}


def service(provider: str) -> str:
    return SAME_SERVICE.get(provider, provider)


def added(candidate: str, cache: SurveyCache) -> tuple[EntityType, list[tuple[str, str]]]:
    """(accession, value) for every value the candidate returns and no declared route does."""
    source, rest = candidate.split("->", 1)
    s, t = EntityType(source), EntityType(rest.split(":", 1)[0])
    corpus = promote.standard_corpus(s)
    accessions = corpora.load(corpus).of_type(s)
    declared = [route.id for route in REGISTRY.ranked(s, t)]
    verdicts = {r: draw_verdicts(r, corpus, accessions, cache) for r in [*declared, candidate]}
    found = []
    for accession in accessions:
        if any(accession not in verdicts[r] for r in verdicts):
            continue
        mine = verdicts[candidate][accession]
        if mine.outcome is not Outcome.RESOLVED:
            continue
        theirs = set().union(*(verdicts[r][accession].results for r in declared))
        found += [(accession, value) for value in sorted(set(mine.results) - theirs)]
    return t, found


RUN_ACCESSION = re.compile(r"[SED]RR\d+")


def file_verdict(accession: str, value: str) -> str:
    named = RUN_ACCESSION.findall(value.rsplit("/", 1)[-1])
    if accession in named:
        return "confirmed"
    return "contradicted" if named else "unconfirmed"


def summary(rows: list[dict[str, object]], **extra: object) -> dict[str, object]:
    counts = {
        k: sum(r["verdict"] == k for r in rows)
        for k in ("confirmed", "contradicted", "unconfirmed")
    }
    return {
        **extra,
        "values": len(rows),
        "accessions": len({r["source"] for r in rows}),
        **counts,
        "accessions_confirmed": len({r["source"] for r in rows if r["verdict"] == "confirmed"}),
        "rows": rows,
    }


async def check(candidate: str, cache: SurveyCache, client: SurveyClient) -> dict[str, object]:
    target, values = added(candidate, cache)
    if target is EntityType.FILE:
        rows = [{"source": a, "added": v, "verdict": file_verdict(a, v)} for a, v in values]
        return summary(rows, provider=None, reverse=[])
    source = EntityType(candidate.split("->", 1)[0])
    reverse = [r for r in REGISTRY.ranked(target, source) if r.id in IMPLEMENTATIONS]
    own = service(promote._PROVIDERS[host_of(candidate.rsplit(":", 1)[1])][0])
    gate = asyncio.Semaphore(client.limits.concurrency)

    async def one(accession: str, value: str) -> dict[str, object]:
        async with gate:
            answers: dict[str, object] = {}
            for route in reverse:
                try:
                    got = await client.with_retry(
                        lambda timeout, route=route: IMPLEMENTATIONS[route.id](
                            value, client, timeout
                        ),
                        label=route.id,
                    )
                    answers[route.id] = {
                        "provider": route.provider,
                        "back": accession in got,
                        "n": len(got),
                    }
                except Exception as exc:
                    answers[route.id] = {"provider": route.provider, "error": redact(str(exc))}
            independent = [a for a in answers.values() if service(a["provider"]) != own]  # type: ignore[index]
            if any(a.get("back") for a in independent):  # type: ignore[attr-defined]
                verdict = "confirmed"
            elif any(a.get("back") is False and a.get("n") for a in independent):  # type: ignore[attr-defined]
                verdict = "contradicted"
            else:
                verdict = "unconfirmed"
            return {"source": accession, "added": value, "verdict": verdict, "reverse": answers}

    rows = list(await asyncio.gather(*(one(a, v) for a, v in values)))
    return summary(rows, provider=own, reverse=[r.id for r in reverse])


async def main(args: argparse.Namespace) -> None:
    out: dict[str, object] = {}
    with SurveyCache(args.cache) as cache:
        limits = Limits(rps=7.0, concurrency=5)
        async with SurveyClient(limits, api_key=default_api_key()) as client:
            for candidate in args.candidates:
                found = await check(candidate, cache, client)
                out[candidate] = found
                print(
                    f"{candidate}: {found['values']} values over {found['accessions']} accessions; "
                    f"confirmed {found['confirmed']} ({found['accessions_confirmed']} accessions), "
                    f"contradicted {found['contradicted']}, unconfirmed {found['unconfirmed']}",
                    flush=True,
                )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(out, indent=1) + "\n")


if __name__ == "__main__":
    load_dotenv()
    configure_logging(level=logging.WARNING)
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("candidates", nargs="+")
    asyncio.run(main(parser.parse_args()))
