"""Which accessions a catalogue link is surveyed over: one population per source node.

A link is surveyed over a seeded draw of :data:`DRAW` accessions of its source
node, or over every one where there are no more than that -- a census, named
without ``sample:`` so the name says so. The draw comes from the population
this project actually resolves: the reprocessed 10x table, or what a census
over it returned. That makes every link survey a statement about *this
project's* records, and the derived ones about records reachable from them;
neither is a statement about an archive at large, and the KB says which.

Where the archives share a node -- a BioProject, a BioSample -- the population
is stratified by issuing archive. ``reprocessed-prj`` holds 12,114 PRJNAs, 504
PRJEBs and 137 PRJDBs, so an unstratified draw of 300 would hold about four
DDBJ projects and say nothing about them.

Some nodes have no population here at all: nothing this project resolves is
one. Each is recorded with its reason and date, and its links stay UNTESTED --
confirmed by a probe, measured by nothing -- which is the honest state.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from functools import cache

from fetch_series.survey import corpora

# Big enough to say whether a link answers always, sometimes or never (±5.7%
# at 95%, worst case); far too small to rank, which needs the 1% band.
DRAW = 300

ARCHIVES = ("ncbi", "ebi", "ddbj")


@dataclass(frozen=True, slots=True)
class Population:
    """Where a node's accessions come from.

    Args:
        parent: the corpus holding them, filtered to the node.
        by_archive: the node is shared by the archives, so one population per
            issuing archive.
        via: for a node nothing in the reprocessed table is, the link whose
            survey reaches it. The population is that survey's results.
        via_archive: which archive's stratum of ``via``'s source to use, where
            the source is shared.
    """

    parent: str | None = None
    by_archive: bool = False
    via: str | None = None
    via_archive: str | None = None


ENA_BIOSAMPLES = "results-of:bioproject->biosample:ena_filereport@reprocessed-prj"
ENA_STUDIES = "results-of:bioproject->study:ena_filereport@reprocessed-prj"

POPULATIONS: dict[str, Population] = {
    "bioproject": Population("reprocessed-prj", by_archive=True),
    "biosample": Population(ENA_BIOSAMPLES, by_archive=True),
    **{f"study_{a}": Population(ENA_STUDIES) for a in ARCHIVES},
    **{f"sample_{a}": Population("reprocessed-srs") for a in ARCHIVES},
    **{f"experiment_{a}": Population("reprocessed-srx") for a in ARCHIVES},
    **{f"run_{a}": Population("reprocessed-srr") for a in ARCHIVES},
    "geo_series": Population("reprocessed-gse"),
    "geo_sample": Population("reprocessed-sample"),
    "ae_experiment": Population("arrayexpress-ena"),
    # Derived: reached by the first link survey that answers for them.
    **{
        f"submission_{a}": Population(via=f"run_{a}->submission_{a}:ena_filereport")
        for a in ARCHIVES
    },
    "geo_platform": Population(via="geo_series->geo_platform:geo_soft_family"),
    "biostudy": Population(via="ae_experiment->biostudy:biostudies_search_all"),
    "ae_sample": Population(via="ae_experiment->ae_sample:biostudies_sdrf"),
}

# Nodes with no population, why, and when that was established. Their links
# stay UNTESTED rather than being surveyed over something that is not them.
NO_POPULATION: dict[str, tuple[str, date]] = {
    "analysis_ebi": (
        "Derived from BioProject -> EBI analysis (ENA filereport, result=analysis), whose "
        "survey found no analysis for any of 300 PRJEBs, 300 PRJNAs or 137 PRJDBs: nothing "
        "this project resolves reaches one. The probe's PRJEB44468 was chosen because it has 106.",
        date(2026, 10, 20),
    ),
    "analysis_ncbi": (
        "Every link into an NCBI analysis is DDBJ's dblink, and every one is unconfirmed: "
        "no record this project resolves reaches one, so there is nothing to draw.",
        date(2026, 10, 19),
    ),
    "analysis_ddbj": (
        "Every link into a DDBJ analysis is DDBJ's dblink, and every one is unconfirmed: "
        "no record this project resolves reaches one, so there is nothing to draw.",
        date(2026, 10, 19),
    ),
}


class PopulationPending(LookupError):
    """A derived node's population needs another link surveyed first."""

    def __init__(self, node: str, needs: str) -> None:
        super().__init__(f"{node}: survey {needs} first; its results are this population")
        self.node = node
        self.needs = needs


@cache
def _size(name: str) -> int:
    return len(corpora.load(name))


def population(node: str, archive: str | None = None) -> str:
    """The corpus name of ``node``'s whole population, before any draw."""
    if node in NO_POPULATION:
        raise LookupError(f"{node} has no population: {NO_POPULATION[node][0]}")
    spec = POPULATIONS.get(node)
    if spec is None:
        raise LookupError(f"{node} is not a surveyable source node")
    if spec.by_archive and archive is None:
        raise ValueError(f"{node} is shared by the archives; name one of {ARCHIVES}")
    if not spec.by_archive and archive is not None:
        raise ValueError(f"{node} is one archive's node; it takes no archive")
    label = f"{node}/{archive}" if archive else node
    if spec.via is not None:
        source = spec.via.split("->", 1)[0]
        parent = f"results-of:link:{spec.via}@{standard_corpus(source, spec.via_archive)}"
    else:
        assert spec.parent is not None
        parent = spec.parent
    return f"node:{label}@{parent}"


def standard_corpus(node: str, archive: str | None = None) -> str:
    """The corpus a link from ``node`` is surveyed over by default.

    A seeded draw of :data:`DRAW` from the node's population, or the whole
    population where it holds no more than that.
    """
    name = population(node, archive)
    try:
        size = _size(name)
    except ValueError as exc:
        spec = POPULATIONS[node]
        if spec.via is not None:
            raise PopulationPending(node, f"link:{spec.via}") from exc
        raise
    return name if size <= DRAW else f"sample:{DRAW}@{name}"


def standard_corpora(node: str) -> list[str]:
    """Every corpus a link from ``node`` is surveyed over: one per archive where shared."""
    spec = POPULATIONS.get(node)
    if spec is not None and spec.by_archive:
        return [standard_corpus(node, archive) for archive in ARCHIVES]
    return [standard_corpus(node)]


__all__ = [
    "ARCHIVES",
    "DRAW",
    "NO_POPULATION",
    "POPULATIONS",
    "Population",
    "PopulationPending",
    "population",
    "standard_corpora",
    "standard_corpus",
]
