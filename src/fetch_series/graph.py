"""The route graph: every way to get from one accession type to another.

A *route* is one declared edge -- a source entity type, a target entity type,
and the provider that answers it. Declaring routes as data rather than burying
them in ``if`` branches is what lets the same definition drive three things that
must never disagree: the resolver's fallback order, the benchmark harness, and
the knowledge-base page that justifies both.

The ordering between competing routes is **derived from measured evidence**, not
asserted. :class:`RouteEvidence` carries the result of an actual survey over a
named corpus, and :meth:`RouteRegistry.ranked` sorts by it. A route with no
evidence sorts last, behind every route that has been measured.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass, replace
from datetime import date

from fetch_series.accession import Accession, Archive, EntityType

# Two routes whose yields are within this fraction of each other are treated as
# equally complete, and separated on reliability and cost instead. 1% sits well
# above run-to-run noise and well below the ~12% gap that actually distinguishes
# the ENA and NCBI route families.
YIELD_BAND = 0.01

# Likewise for failure rates, in absolute percentage points. The two
# BioProject -> GEO routes differ by a single accession out of 12,114 (0.0008pp)
# and return identical series sets; without banding, that one accession would
# permanently outrank the fact that one route costs an extra request.
FAILURE_BAND = 0.001


@dataclass(frozen=True, slots=True)
class RouteCost:
    """What one query over this route costs.

    Args:
        requests: HTTP round trips needed to answer a single accession. ELink
            routes cost more than direct searches because they walk a history.
        rate_limit_rps: the provider's ceiling, shared across all routes that
            use the same provider.
        paginates: whether large results must be paged. Routes that do and
            don't are a recurring source of silent truncation -- BioStudies
            returns the first 25 of however many there are.
    """

    requests: int
    rate_limit_rps: float
    paginates: bool = False


@dataclass(frozen=True, slots=True)
class RouteEvidence:
    """Measured behaviour of a route over a named corpus.

    Args:
        corpus: the corpus name this was measured on.
        surveyed_on: when the survey ran. Archive contents drift, so an
            undated coverage number is not evidence of anything.
        accessions_queried: how many source accessions were attempted.
        accessions_failed: how many produced no verdict at all.
        unique_results: distinct target accessions returned, where counted.
        notes: anything that qualifies the numbers.
        accessions_empty: how many produced a verdict of "the archive holds no
            such link". ``None`` means the survey did not record the split, which
            is not the same as zero: the 2026-05 scripts kept a single boolean
            that lumped EMPTY in with FAILED, so for their routes the two cannot
            be separated after the fact.
    """

    corpus: str
    surveyed_on: date
    accessions_queried: int
    accessions_failed: int
    unique_results: int | None = None
    notes: str = ""
    # Last, not beside accessions_failed: added after the class shipped, and a
    # field inserted mid-signature would shift every positional caller's
    # unique_results into it without an error.
    accessions_empty: int | None = None

    def __post_init__(self) -> None:
        counts = (self.accessions_queried, self.accessions_failed, self.accessions_empty or 0)
        if min(counts) < 0:
            raise ValueError(f"Negative count in evidence for {self.corpus}: {counts}")
        if self.accessions_failed + (self.accessions_empty or 0) > self.accessions_queried:
            raise ValueError(
                f"Evidence for {self.corpus} records more failed and empty verdicts "
                f"than accessions queried: {counts}"
            )

    @property
    def accessions_resolved(self) -> int | None:
        """Accessions that returned at least one result, where the split is known."""
        if self.accessions_empty is None:
            return None
        return self.accessions_queried - self.accessions_failed - self.accessions_empty

    @property
    def failure_rate(self) -> float:
        if self.accessions_queried == 0:
            return 1.0
        return self.accessions_failed / self.accessions_queried

    @property
    def success_rate(self) -> float:
        return 1.0 - self.failure_rate


@dataclass(frozen=True, slots=True)
class Route:
    """One declared way of getting from ``source`` to ``target``.

    Args:
        source_archives: the issuing archives this route can actually answer
            for, empty meaning any. An archive's own index generally knows only
            its own accessions: over the reprocessed BioProjects, NCBI's ELink
            resolved BioProject -> BioSample for 100% of NCBI-issued projects,
            32.8% of DDBJ's and **1.4%** of EBI's. Declaring the restriction
            lets the resolver skip a route that provably cannot answer, instead
            of spending three requests to be told nothing.
        proves_data_exists: whether everything this route returns is known to
            carry data. True for routes built on ENA's ``result=read_run``,
            whose rows *are* runs, so an accession cannot appear without one.
            This is not a nicety: about 4,238 experiments named in GEO SOFT
            files -- 1.37% of the 308,639 it returns -- have no runs at all, so
            a complete-looking answer can still download nothing. A route that
            proves existence turns that from a silent failure into a split
            between confirmed and unconfirmed.
    """

    id: str
    source: EntityType
    target: EntityType
    provider: str
    summary: str
    kb_page: str
    cost: RouteCost
    evidence: RouteEvidence | None = None
    known_pathologies: tuple[str, ...] = ()
    source_archives: tuple[Archive, ...] = ()
    proves_data_exists: bool = False

    def accepts(self, accession: Accession) -> bool:
        """Whether this route can answer for a given accession."""
        if accession.entity is not self.source:
            return False
        return not self.source_archives or accession.archive in self.source_archives

    def with_evidence(self, evidence: RouteEvidence) -> Route:
        """Return a copy carrying a survey result."""
        return replace(self, evidence=evidence)

    @property
    def is_measured(self) -> bool:
        return self.evidence is not None

    def __str__(self) -> str:
        return self.id


class RouteRegistry:
    """Holds the declared routes and answers questions about the graph."""

    def __init__(self, routes: list[Route] | None = None) -> None:
        self._routes: dict[str, Route] = {}
        for route in routes or []:
            self.register(route)

    def register(self, route: Route) -> Route:
        if route.id in self._routes:
            raise ValueError(f"Duplicate route id: {route.id}")
        self._routes[route.id] = route
        return route

    def __len__(self) -> int:
        return len(self._routes)

    def __iter__(self) -> Iterator[Route]:
        return iter(self._routes.values())

    def __contains__(self, route_id: object) -> bool:
        return route_id in self._routes

    def __getitem__(self, route_id: str) -> Route:
        return self._routes[route_id]

    def ranked(self, source: EntityType, target: EntityType) -> list[Route]:
        """Routes from ``source`` to ``target``, best first.

        Ordering, in order of precedence:

        1. Measured routes before unmeasured ones -- even a poor measured route
           beats one nobody has tested.
        2. Yield band: results returned relative to the best route in the group,
           bucketed by :data:`YIELD_BAND`. Banding matters. Compared on raw
           counts, ``sra_be_elink`` outranks ``sra_be_direct_cgi`` on 55 extra
           runs out of 754,000 -- 0.007% -- while failing twice as often. Routes
           whose yields are indistinguishable should be separated by
           reliability, not by noise.
        3. Failure-rate band, bucketed by :data:`FAILURE_BAND` for the same reason.
        4. Cost in requests. The extra ELink hop buys nothing when yields tie,
           and adds a failure mode (MegaLink flakiness) of its own.
        5. Route id, so the order is stable -- it is recorded as provenance.
        """
        candidates = [r for r in self._routes.values() if r.source is source and r.target is target]
        best_yield = max(
            (r.evidence.unique_results or 0 for r in candidates if r.evidence is not None),
            default=0,
        )

        def sort_key(route: Route) -> tuple[int, int, int, int, str]:
            evidence = route.evidence
            if evidence is None:
                return (1, 0, 0, route.cost.requests, route.id)
            band = (
                round(((evidence.unique_results or 0) / best_yield) / YIELD_BAND)
                if best_yield
                else 0
            )
            failure_band = round(evidence.failure_rate / FAILURE_BAND)
            return (0, -band, failure_band, route.cost.requests, route.id)

        return sorted(candidates, key=sort_key)

    def ranked_for(self, accession: Accession, target: EntityType) -> list[Route]:
        """Like :meth:`ranked`, but drops routes that cannot accept ``accession``.

        This is what the resolver should use. ``ranked`` answers "which routes
        exist between these two types"; this answers "which routes could
        actually resolve this identifier".
        """
        return [r for r in self.ranked(accession.entity, target) if r.accepts(accession)]

    def targets_from(self, source: EntityType) -> set[EntityType]:
        return {r.target for r in self._routes.values() if r.source is source}

    def find_paths(
        self, source: EntityType, target: EntityType, max_hops: int = 4
    ) -> list[list[EntityType]]:
        """All simple entity-type paths from ``source`` to ``target``.

        Returned shortest-first. These are *type* paths; picking the concrete
        route for each hop is :meth:`ranked`'s job. Multi-hop paths matter
        because the direct edge is often the one that is missing: a BioProject
        with no GEO link at the project level frequently still reaches GEO
        through its BioSamples.
        """
        if source == target:
            return [[source]]
        paths: list[list[EntityType]] = []
        queue: deque[list[EntityType]] = deque([[source]])
        while queue:
            path = queue.popleft()
            if len(path) > max_hops:
                continue
            for nxt in sorted(self.targets_from(path[-1])):
                if nxt in path:  # no cycles
                    continue
                extended = [*path, nxt]
                if nxt is target:
                    paths.append(extended)
                else:
                    queue.append(extended)
        return sorted(paths, key=len)


# --------------------------------------------------------------------------
# Seeded registry.
#
# The evidence blocks dated 2026-05-12 are real measurements over the 12,755
# BioProjects in `data/All_10x.sample_table.tsv`, by the scripts in `scripts/`.
# Unique-run counts are computed at the run level, not the row level: the
# `sra-db-be` result files contain 69,923 within-BioProject duplicate rows,
# which is why the raw row counts (824k) overstate that route's advantage. The
# GEO series counts are distinct for the same reason: five series are reached
# from more than one project (GSE305915 from three), so 15,186 rows name 15,180.
#
# The scripts also sent the table's `-` placeholder as a BioProject, and every
# BioProject -> run script recorded it as a failure. It is not a BioProject, so
# it is in neither `accessions_queried` nor `accessions_failed` below, and the
# route splits report it as unparsed (`catalogue/route_splits.json`).
# --------------------------------------------------------------------------

REPROCESSED = "reprocessed"
SURVEY_DATE = date(2026, 5, 12)
_BIOPROJECTS = 12_755
_BIOPROJECTS_PRJNA = 12_114

NCBI_EUTILS_RPS = 10.0  # with an API key; 3/s without
EBI_RPS = 15.0
# The GEO FTP mirror is not an API and is slower; do not hammer it.
NCBI_FTP_RPS = 3.0

# Behaviours a route is known to exhibit that have no knowledge-base page yet.
#
# They are listed rather than quietly tolerated. `known_pathologies` is printed
# by `--explain` as a path into docs/pathologies/, so a slug with no page emits
# a dead link beside a resolved value -- which is worse than saying nothing,
# because it looks like documentation that exists.
#
# A test asserts every declared slug is either a page or in this set, so the
# backlog cannot grow silently and a written-up page cannot be forgotten here.
# Each entry is real and observed; what it lacks is the pathology-page contract:
# a curl reproducer, an affected-accession list, and a regression test.
# Empty since 2026-10-26: every declared slug has a page. A new one starts here.
PATHOLOGY_BACKLOG: frozenset[str] = frozenset()

ROUTES: list[Route] = [
    Route(
        id="bioproject->run:ena_filereport",
        source=EntityType.BIOPROJECT,
        target=EntityType.RUN,
        provider="ena_portal",
        summary="ENA portal filereport, result=read_run, one request per project.",
        kb_page="routes/bioproject-to-run/index.md",
        cost=RouteCost(requests=1, rate_limit_rps=EBI_RPS),
        evidence=RouteEvidence(
            corpus="reprocessed-prj",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=12_755,
            accessions_failed=0,
            accessions_empty=23,
            unique_results=783_936,
            notes=(
                "Census through the harness. 12,732 resolved, 23 empty, zero failures. "
                "Reproduces the 2026-05 script figure of 784,026 unique runs to within "
                "0.011% while eliminating all 14 of its failures -- the difference is "
                "four months of archive drift, not disagreement. Against the NCBI routes "
                "re-surveyed 2026-10-26 it is no longer a superset: they return 162 to "
                "1,270 of its runs fewer and 2,261 to 2,812 it lacks, six weeks later. "
                "The 111,665 runs it once held over efetch were EFetch's 10,000-record "
                "cap. The 23 empties are withdrawn, private or "
                "unreleased projects, and include the five whose GEO series also "
                "resolved to nothing in the GSE census."
            ),
        ),
        proves_data_exists=True,
    ),
    Route(
        id="bioproject->run:sra_be_direct_cgi",
        source=EntityType.BIOPROJECT,
        target=EntityType.RUN,
        provider="sra_be",
        summary="esearch db=sra [GPRJ], then the sra-db-be CGI backend by history.",
        kb_page="routes/bioproject-to-run/index.md",
        cost=RouteCost(requests=2, rate_limit_rps=NCBI_EUTILS_RPS),
        evidence=RouteEvidence(
            corpus="reprocessed-prj",
            surveyed_on=date(2026, 10, 26),
            accessions_queried=12_755,
            accessions_failed=0,
            accessions_empty=22,
            unique_results=784_935,
            notes=(
                "Re-surveyed 2026-10-24..26 on code that pages EFetch, falls back from a capped"
                " sra-db-be history to the project by accession, and fails a cut-off or empty "
                "answer instead of recording it. 2 projects failed the first pass and answered "
                "on retry. 1,262 ENA runs it does not return, 2,261 it returns that ENA's "
                "2026-09-15 census did not hold. The 2026-05 script survey's 754,277 runs were "
                "the history form's 5,000-run cap."
            ),
        ),
        known_pathologies=(
            "sra-be-history-caps-at-5000-runs",
            "gprj-misses-experiments-of-the-project",
        ),
    ),
    Route(
        id="bioproject->run:sra_be_elink",
        source=EntityType.BIOPROJECT,
        target=EntityType.RUN,
        provider="sra_be",
        summary="esearch db=bioproject, elink bioproject->sra, then the sra-db-be CGI backend.",
        kb_page="routes/bioproject-to-run/index.md",
        cost=RouteCost(requests=3, rate_limit_rps=NCBI_EUTILS_RPS),
        evidence=RouteEvidence(
            corpus="reprocessed-prj",
            surveyed_on=date(2026, 10, 26),
            accessions_queried=12_755,
            accessions_failed=0,
            accessions_empty=22,
            unique_results=785_656,
            notes=(
                "Re-surveyed 2026-10-24..26 on code that pages EFetch, falls back from a capped"
                " sra-db-be history to the project by accession, and fails a cut-off or empty "
                "answer instead of recording it. 4 projects failed the first pass; PRJNA772116 "
                "(73,888 runs) answered only on the fourth. 851 ENA runs it does not return, "
                "2,571 the other way."
            ),
        ),
        known_pathologies=(
            "sra-be-history-caps-at-5000-runs",
            "megalink-backend-flakiness",
            "elink-returns-runs-of-sibling-projects",
        ),
    ),
    Route(
        id="bioproject->run:efetch_elink",
        source=EntityType.BIOPROJECT,
        target=EntityType.RUN,
        provider="eutils",
        summary="esearch db=bioproject, elink bioproject->sra, efetch rettype=runinfo.",
        kb_page="routes/bioproject-to-run/index.md",
        cost=RouteCost(requests=3, rate_limit_rps=NCBI_EUTILS_RPS),
        evidence=RouteEvidence(
            corpus="reprocessed-prj",
            surveyed_on=date(2026, 10, 26),
            accessions_queried=12_755,
            accessions_failed=0,
            accessions_empty=22,
            unique_results=786_586,
            notes=(
                "Re-surveyed 2026-10-24..26 on code that pages EFetch, falls back from a capped"
                " sra-db-be history to the project by accession, and fails a cut-off or empty "
                "answer instead of recording it. 3 projects failed the first pass and answered "
                "on retry; 54 more failed locally when this machine lost its network, and were "
                "re-asked. Only 162 ENA runs it does not return: the 2026-05 figure of 111,665 "
                "was one EFetch request stopping at 10,000 records."
            ),
        ),
        known_pathologies=(
            "efetch-returns-fewer-runs-than-esearch-counts",
            "megalink-backend-flakiness",
            "elink-returns-runs-of-sibling-projects",
        ),
    ),
    Route(
        id="bioproject->run:efetch_direct",
        source=EntityType.BIOPROJECT,
        target=EntityType.RUN,
        provider="eutils",
        summary="esearch db=sra [GPRJ], efetch rettype=runinfo.",
        kb_page="routes/bioproject-to-run/index.md",
        cost=RouteCost(requests=2, rate_limit_rps=NCBI_EUTILS_RPS),
        evidence=RouteEvidence(
            corpus="reprocessed-prj",
            surveyed_on=date(2026, 10, 26),
            accessions_queried=12_755,
            accessions_failed=0,
            accessions_empty=23,
            unique_results=784_976,
            notes=(
                "Re-surveyed 2026-10-24..26 on code that pages EFetch, falls back from a capped"
                " sra-db-be history to the project by accession, and fails a cut-off or empty "
                "answer instead of recording it. 2 projects failed the first pass and answered "
                "on retry. 1,270 ENA runs it does not return; its ELink sibling finds 1,610 "
                "runs it does not, through experiments [GPRJ] does not match (PRJNA66167: 5,499"
                " runs against 5,910)."
            ),
        ),
        known_pathologies=(
            "efetch-returns-fewer-runs-than-esearch-counts",
            "gprj-misses-experiments-of-the-project",
        ),
    ),
    Route(
        id="bioproject->geo_series:elink",
        source=EntityType.BIOPROJECT,
        target=EntityType.GEO_SERIES,
        provider="eutils",
        summary="esearch db=bioproject, elink bioproject->gds, esummary db=gds.",
        kb_page="routes/bioproject-to-geo-series/index.md",
        cost=RouteCost(requests=3, rate_limit_rps=NCBI_EUTILS_RPS, paginates=True),
        evidence=RouteEvidence(
            corpus=REPROCESSED,
            surveyed_on=SURVEY_DATE,
            accessions_queried=_BIOPROJECTS_PRJNA,
            accessions_failed=1_078,
            unique_results=15_180,
            notes=(
                "Agrees with the direct route on every accession. Most failures are "
                "provenance, not API: re-created, withdrawn or private projects."
            ),
        ),
        known_pathologies=("esummary-500-uid-limit",),
    ),
    Route(
        id="bioproject->geo_series:gds_direct",
        source=EntityType.BIOPROJECT,
        target=EntityType.GEO_SERIES,
        provider="eutils",
        summary="esearch db=gds for the BioProject accession, esummary db=gds.",
        kb_page="routes/bioproject-to-geo-series/index.md",
        cost=RouteCost(requests=2, rate_limit_rps=NCBI_EUTILS_RPS, paginates=True),
        evidence=RouteEvidence(
            corpus=REPROCESSED,
            surveyed_on=SURVEY_DATE,
            accessions_queried=_BIOPROJECTS_PRJNA,
            accessions_failed=1_079,
            unique_results=15_179,
            notes=(
                "Functionally identical to the ELink route -- zero accessions where the "
                "two return different series -- for one fewer request."
            ),
        ),
        known_pathologies=("esummary-500-uid-limit",),
    ),
    Route(
        id="bioproject->biosample:elink",
        source=EntityType.BIOPROJECT,
        target=EntityType.BIOSAMPLE,
        provider="eutils",
        summary="esearch db=bioproject, elink bioproject->biosample, esummary db=biosample.",
        kb_page="routes/bioproject-to-biosample/index.md",
        cost=RouteCost(requests=3, rate_limit_rps=NCBI_EUTILS_RPS, paginates=True),
        # Measured: 100% of PRJNA projects resolve, 32.8% of PRJDB and 1.4% of
        # PRJEB. Asking NCBI about an EBI-native project is 3 wasted requests.
        source_archives=(Archive.NCBI,),
        evidence=RouteEvidence(
            corpus="reprocessed-prj",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=12_755,
            accessions_failed=0,
            accessions_empty=675,
            unique_results=244_324,
            notes=(
                "Census. 12,080 resolved, 675 empty, zero failures -- against the 2026-05 "
                "script's 330 failures over the same projects. That was a client bug, not "
                "an archive one: its ESummary call was not paged, so projects with more "
                "than 500 BioSamples hit the UID ceiling. Paging did not merely fix the "
                "330; it recovered 94,895 BioSamples the unpaged call had been silently "
                "truncating, 149,429 -> 244,324. Resolve rate splits hard by issuing "
                "archive: 99.3% of PRJNA, 32.8% of PRJDB, 1.4% of PRJEB."
            ),
        ),
        known_pathologies=("esummary-500-uid-limit", "megalink-backend-flakiness"),
    ),
    Route(
        id="bioproject->study:ena_filereport",
        source=EntityType.BIOPROJECT,
        target=EntityType.STUDY,
        provider="ena_portal",
        summary="ENA portal filereport, fields=secondary_study_accession.",
        kb_page="routes/bioproject-to-study/ena-filereport.md",
        cost=RouteCost(requests=1, rate_limit_rps=EBI_RPS),
        proves_data_exists=True,
        evidence=RouteEvidence(
            corpus="reprocessed-prj",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=12_755,
            accessions_failed=0,
            accessions_empty=23,
            unique_results=12_748,
            notes="12,732 resolved, 23 empty. 12,748 distinct studies for 12,732 projects: a handful of projects carry more than one.",
        ),
    ),
    Route(
        id="ae_experiment->study:idf_secondary",
        source=EntityType.AE_EXPERIMENT,
        target=EntityType.STUDY,
        provider="biostudies",
        summary="Comment[SecondaryAccession] from the ArrayExpress IDF file.",
        kb_page="routes/ae-experiment-to-study/index.md",
        cost=RouteCost(requests=1, rate_limit_rps=EBI_RPS),
        evidence=RouteEvidence(
            corpus="arrayexpress-ena",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=20_693,
            accessions_failed=5,
            accessions_empty=551,
            unique_results=20_138,
            notes=(
                "20,137 resolved, 551 empty. The corpus is every ArrayExpress "
                "study BioStudies records an ENA link for, so those 551 are a "
                "measurable recall failure rather than an unknown: the link "
                "exists and the IDF does not declare it. The BioSample fallback "
                "recovers some of them; E-MTAB-6505 resolves that way. Replaces "
                "a 2026-03 figure of 7,179/457 whose population was never "
                "recorded. The 5 failures are studies whose IDF is a genuine "
                "404: the record and the search index carry them, and they "
                "register no files at all."
            ),
        ),
        known_pathologies=(
            "ae-idf-missing-secondary-accession",
            "biostudies-files-404s-registered-files",
        ),
    ),
    Route(
        id="ae_experiment->biosample:sdrf",
        source=EntityType.AE_EXPERIMENT,
        target=EntityType.BIOSAMPLE,
        provider="biostudies",
        summary="Comment[BioSD_SAMPLE] column of the ArrayExpress SDRF file.",
        kb_page="routes/ae-experiment-to-biosample/sdrf.md",
        cost=RouteCost(requests=1, rate_limit_rps=EBI_RPS),
        known_pathologies=(
            "biostudies-silent-25-item-default",
            "biostudies-files-404s-registered-files",
        ),
        evidence=RouteEvidence(
            corpus="sample:3000@arrayexpress-ena",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=3_000,
            accessions_failed=11,
            accessions_empty=1_768,
            unique_results=63_153,
            notes=(
                "1,221 resolved, 1,768 empty. Split entirely by provenance: "
                "1,210 of 1,398 E-MTAB studies (87%) name BioSamples, and 0 of "
                "1,550 E-GEOD imports do. Adding the 450 E-GEOD studies in "
                "arrayexpress-no-secondary makes it 0 of 2,000."
            ),
        ),
    ),
    Route(
        id="biosample->run:ena_filereport",
        source=EntityType.BIOSAMPLE,
        target=EntityType.RUN,
        provider="ena_portal",
        summary="ENA portal filereport keyed on a BioSample accession.",
        kb_page="routes/biosample-to-run/ena-filereport.md",
        cost=RouteCost(requests=1, rate_limit_rps=EBI_RPS),
        proves_data_exists=True,
        evidence=RouteEvidence(
            corpus="sample:3000@results-of:bioproject->biosample:ena_filereport@reprocessed-prj",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=0,
            unique_results=4_827,
            notes="3,000 resolved, 0 empty.",
        ),
    ),
    Route(
        id="study->ae_experiment:biostudies_search",
        known_pathologies=("biostudies-20000-hit-window",),
        source=EntityType.STUDY,
        target=EntityType.AE_EXPERIMENT,
        provider="biostudies",
        summary="BioStudies search API, collection=arrayexpress.",
        kb_page="routes/study-to-ae-experiment/biostudies-search.md",
        cost=RouteCost(requests=1, rate_limit_rps=EBI_RPS, paginates=True),
        evidence=RouteEvidence(
            corpus="sample:3000@results-of:bioproject->study:ena_filereport@reprocessed-prj",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=2_900,
            unique_results=101,
            notes="100 resolved, 2,900 empty. The corpus is studies reached from the reprocessed 10x BioProjects, which are overwhelmingly NCBI submissions; a low yield here is a fact about that population, not a failure of the route.",
        ),
    ),
    Route(
        id="study->bioproject:ena_filereport",
        source=EntityType.STUDY,
        target=EntityType.BIOPROJECT,
        provider="ena_portal",
        summary="ENA portal filereport, fields=study_accession.",
        kb_page="routes/study-to-bioproject/ena-filereport.md",
        cost=RouteCost(requests=1, rate_limit_rps=EBI_RPS),
        proves_data_exists=True,
        evidence=RouteEvidence(
            corpus="sample:3000@results-of:bioproject->study:ena_filereport@reprocessed-prj",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=0,
            unique_results=2_999,
            notes="3,000 resolved, 0 empty.",
        ),
    ),
    # --- GEO series as an entry point ------------------------------------
    # The primary entry point and, until now, the least measured. GEO exposes a
    # series through two surfaces that do not agree: the SOFT family file is the
    # submitter's own record, db=gds is NCBI's index of it.
    Route(
        id="gse->bioproject:soft_family",
        source=EntityType.GEO_SERIES,
        target=EntityType.BIOPROJECT,
        provider="geo_ftp",
        summary="!Series_relation = BioProject: from the SOFT family file.",
        kb_page="routes/gse-to-bioproject/soft-family.md",
        cost=RouteCost(requests=1, rate_limit_rps=NCBI_FTP_RPS),
        evidence=RouteEvidence(
            corpus="reprocessed-gse",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=13_045,
            accessions_failed=7,
            accessions_empty=466,
            unique_results=12_572,
            notes="12,572 resolved, 466 empty.",
        ),
    ),
    Route(
        id="gse->bioproject:gds_summary",
        source=EntityType.GEO_SERIES,
        target=EntityType.BIOPROJECT,
        provider="eutils",
        summary="The bioproject field of the db=gds ESummary record.",
        kb_page="routes/gse-to-bioproject/gds-summary.md",
        cost=RouteCost(requests=2, rate_limit_rps=NCBI_EUTILS_RPS),
        evidence=RouteEvidence(
            corpus="reprocessed-gse",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=13_045,
            accessions_failed=0,
            accessions_empty=7,
            unique_results=12_830,
            notes="13,038 resolved, 7 empty. Resolves 466 series the SOFT file leaves empty, and never fails -- but 190 of those 466 resolve to a species-level umbrella project holding thousands of unrelated runs, so the extra coverage is not usable for routing without checking the project's size.",
        ),
        known_pathologies=("series-under-shared-umbrella-bioproject",),
    ),
    Route(
        id="gse->geo_sample:soft_family",
        source=EntityType.GEO_SERIES,
        target=EntityType.GEO_SAMPLE,
        provider="geo_ftp",
        summary="^SAMPLE records in the SOFT family file, in declaration order.",
        kb_page="routes/gse-to-geo-sample/soft-family.md",
        cost=RouteCost(requests=1, rate_limit_rps=NCBI_FTP_RPS),
        evidence=RouteEvidence(
            corpus="reprocessed-gse",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=13_045,
            accessions_failed=7,
            accessions_empty=0,
            unique_results=319_023,
            notes="13,038 resolved, 0 empty.",
        ),
    ),
    Route(
        id="gse->geo_sample:gds_summary",
        source=EntityType.GEO_SERIES,
        target=EntityType.GEO_SAMPLE,
        provider="eutils",
        summary="The Samples array of the db=gds ESummary record.",
        kb_page="routes/gse-to-geo-sample/gds-summary.md",
        cost=RouteCost(requests=2, rate_limit_rps=NCBI_EUTILS_RPS),
        evidence=RouteEvidence(
            corpus="reprocessed-gse",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=13_045,
            accessions_failed=0,
            accessions_empty=7,
            unique_results=319_023,
            notes="13,038 resolved, 7 empty. Identical to the SOFT route on all 13,038 series both answer -- the same 319,023 samples, sample for sample.",
        ),
    ),
    Route(
        id="gse->experiment:soft_family",
        source=EntityType.GEO_SERIES,
        target=EntityType.EXPERIMENT,
        provider="geo_ftp",
        summary="!Sample_relation = SRA: collected across every sample.",
        kb_page="routes/gse-to-experiment/index.md",
        cost=RouteCost(requests=1, rate_limit_rps=NCBI_FTP_RPS),
        evidence=RouteEvidence(
            corpus="reprocessed-gse",
            surveyed_on=date(2026, 9, 14),
            accessions_queried=13_045,
            accessions_failed=7,
            accessions_empty=16,
            unique_results=308_639,
            notes=(
                "Census of every GEO series in the reprocessed table. 13,022 resolved, "
                "16 empty, 7 failed -- all seven a 404 on the FTP mirror, i.e. private "
                "or withdrawn. Of the 16 empties, 11 are recoverable through the "
                "BioProject; the other 5 have no released runs at all."
            ),
        ),
        known_pathologies=(
            "geo-omits-sample-sra-relation",
            "soft-names-experiments-with-no-runs",
        ),
    ),
    Route(
        id="gse->experiment:elink_gds_sra",
        source=EntityType.GEO_SERIES,
        target=EntityType.EXPERIMENT,
        provider="eutils",
        summary="ELink gds->sra with cmd=neighbor, then ESummary.",
        kb_page="routes/gse-to-experiment/index.md",
        cost=RouteCost(requests=3, rate_limit_rps=NCBI_EUTILS_RPS, paginates=True),
        evidence=RouteEvidence(
            corpus="reprocessed-gse",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=13_045,
            accessions_failed=0,
            accessions_empty=2_293,
            unique_results=178_879,
            notes=(
                "Census. 10,752 resolved, 2,293 empty. Reports 17.6% of series as having "
                "no sequencing data when they demonstrably do, and returns 58% of the "
                "experiments the SOFT route finds. Zero failures -- its incompleteness is "
                "entirely silent."
            ),
        ),
        known_pathologies=(
            "esummary-sra-buries-accessions-in-expxml",
            "elink-gds-sra-missing-links",
        ),
    ),
    Route(
        id="gse->experiment:soft_bioproject_ena",
        source=EntityType.GEO_SERIES,
        target=EntityType.EXPERIMENT,
        provider="geo_ftp+ena_portal",
        summary="BioProject from the SOFT family file, then ENA filereport for its experiments.",
        kb_page="routes/gse-to-experiment/soft-bioproject-ena.md",
        cost=RouteCost(requests=2, rate_limit_rps=NCBI_FTP_RPS),
        evidence=RouteEvidence(
            corpus="reprocessed-gse",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=13_045,
            accessions_failed=7,
            accessions_empty=487,
            unique_results=290_462,
            notes=(
                "Census. 12,551 resolved, 487 empty. Covers 92.5% of the three-route "
                "union and subsumes ELink almost entirely -- only 43 of ELink's 178,879 "
                "experiments are absent from it. Because it queries result=read_run, "
                "every accession it returns provably carries data."
            ),
        ),
        known_pathologies=(
            "series-under-shared-umbrella-bioproject",
            "experiments-outside-the-series-bioproject",
        ),
        proves_data_exists=True,
    ),
    Route(
        id="gse->run:elink_gds_sra",
        source=EntityType.GEO_SERIES,
        target=EntityType.RUN,
        provider="eutils",
        summary="ELink gds->sra, then EFetch rettype=runinfo.",
        kb_page="routes/gse-to-experiment/index.md",
        cost=RouteCost(requests=3, rate_limit_rps=NCBI_EUTILS_RPS),
        known_pathologies=("efetch-returns-fewer-runs-than-esearch-counts",),
        evidence=RouteEvidence(
            corpus="reprocessed-gse",
            surveyed_on=date(2026, 9, 16),
            accessions_queried=13_045,
            accessions_failed=0,
            accessions_empty=2_294,
            unique_results=463_803,
            notes=(
                "10,751 resolved, 2,294 empty, nothing failed. The 2,294 are "
                "the same series the experiment-level ELink census reports "
                "empty, 2,281 of which the SOFT family file resolves -- so the "
                "defect is in ELink, not in the EFetch step after it."
            ),
        ),
    ),
    # --- GEO sample as an entry point -------------------------------------
    Route(
        id="geo_sample->experiment:acc_cgi",
        source=EntityType.GEO_SAMPLE,
        target=EntityType.EXPERIMENT,
        provider="geo_acc_cgi",
        summary="!Sample_relation = SRA: from GEO's own record for the sample.",
        kb_page="routes/geo-sample-to-experiment/index.md",
        cost=RouteCost(requests=1, rate_limit_rps=NCBI_FTP_RPS),
        evidence=RouteEvidence(
            corpus="sample:1000@reprocessed-sample",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=825,
            accessions_failed=3,
            accessions_empty=0,
            unique_results=822,
            notes=(
                "822 resolved, 0 empty. 825 of the 1,000 drawn are GEO samples; "
                "the other 175 are INSDC samples this route cannot accept. "
                "Replaces a run recorded against the bare corpus name, which "
                "did not say how many accessions it drew."
            ),
        ),
    ),
    Route(
        id="geo_sample->biosample:acc_cgi",
        source=EntityType.GEO_SAMPLE,
        target=EntityType.BIOSAMPLE,
        provider="geo_acc_cgi",
        summary="!Sample_relation = BioSample: from GEO's own record for the sample.",
        kb_page="routes/geo-sample-to-experiment/index.md",
        cost=RouteCost(requests=1, rate_limit_rps=NCBI_FTP_RPS),
        evidence=RouteEvidence(
            corpus="sample:1000@reprocessed-sample",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=825,
            accessions_failed=3,
            accessions_empty=0,
            unique_results=822,
            notes="822 resolved, 0 empty. 825 of the 1,000 drawn are GEO samples; the other 175 are INSDC samples this route cannot accept.",
        ),
    ),
    Route(
        id="geo_sample->geo_series:acc_cgi",
        source=EntityType.GEO_SAMPLE,
        target=EntityType.GEO_SERIES,
        provider="geo_acc_cgi",
        summary="!Sample_series_id from GEO's record; a sample can belong to several series.",
        kb_page="routes/geo-sample-to-experiment/index.md",
        cost=RouteCost(requests=1, rate_limit_rps=NCBI_FTP_RPS),
        evidence=RouteEvidence(
            corpus="sample:1000@reprocessed-sample",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=825,
            accessions_failed=3,
            accessions_empty=0,
            unique_results=849,
            notes="822 resolved, 0 empty. 825 of the 1,000 drawn are GEO samples; the other 175 are INSDC samples this route cannot accept.",
        ),
    ),
    # --- ENA portal, which answers many edges from one endpoint -----------
    Route(
        id="bioproject->experiment:ena_filereport",
        source=EntityType.BIOPROJECT,
        target=EntityType.EXPERIMENT,
        provider="ena_portal",
        summary="ENA portal filereport, experiment_accession column.",
        kb_page="routes/bioproject-to-experiment/ena-filereport.md",
        cost=RouteCost(requests=1, rate_limit_rps=EBI_RPS),
        proves_data_exists=True,
        evidence=RouteEvidence(
            corpus="reprocessed-prj",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=12_755,
            accessions_failed=0,
            accessions_empty=23,
            unique_results=481_315,
            notes="12,732 resolved, 23 empty. Nothing failed.",
        ),
    ),
    Route(
        id="bioproject->biosample:ena_filereport",
        source=EntityType.BIOPROJECT,
        target=EntityType.BIOSAMPLE,
        provider="ena_portal",
        summary="ENA portal filereport, sample_accession column.",
        kb_page="routes/bioproject-to-biosample/index.md",
        cost=RouteCost(requests=1, rate_limit_rps=EBI_RPS),
        proves_data_exists=True,
        evidence=RouteEvidence(
            corpus="reprocessed-prj",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=12_755,
            accessions_failed=0,
            accessions_empty=25,
            unique_results=363_885,
            notes="12,730 resolved, 25 empty.",
        ),
    ),
    Route(
        id="study->run:ena_filereport",
        source=EntityType.STUDY,
        target=EntityType.RUN,
        provider="ena_portal",
        summary="ENA portal filereport keyed on a study accession.",
        kb_page="routes/study-to-run/ena-filereport.md",
        cost=RouteCost(requests=1, rate_limit_rps=EBI_RPS),
        proves_data_exists=True,
        evidence=RouteEvidence(
            corpus="sample:3000@results-of:bioproject->study:ena_filereport@reprocessed-prj",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=0,
            unique_results=205_175,
            notes="3,000 resolved, 0 empty.",
        ),
    ),
    Route(
        id="experiment->run:ena_filereport",
        source=EntityType.EXPERIMENT,
        target=EntityType.RUN,
        provider="ena_portal",
        summary="ENA portal filereport keyed on an experiment accession.",
        kb_page="routes/experiment-to-run/ena-filereport.md",
        cost=RouteCost(requests=1, rate_limit_rps=EBI_RPS),
        evidence=RouteEvidence(
            corpus="sample:3000@results-of:gse->experiment:soft_family@reprocessed-gse",
            surveyed_on=date(2026, 10, 19),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=37,
            unique_results=4_811,
            notes=(
                "A seeded draw of 3,000 of the 308,639 experiments the GEO census "
                "returned. 2,963 resolved, 37 empty, zero failures. Those 37 are the "
                "measurement that matters: 1.23% of experiments GEO names carry no runs "
                "at all (95% CI 0.84-1.63%), about 3,807 of the 308,639 SOFT names. "
                "Replaces a 2026-09-15 --limit 3000 run recorded under the full corpus "
                "name; that disjoint draw also found 37 empty."
            ),
        ),
        proves_data_exists=True,
    ),
    Route(
        id="experiment->biosample:ena_filereport",
        source=EntityType.EXPERIMENT,
        target=EntityType.BIOSAMPLE,
        provider="ena_portal",
        summary="ENA portal filereport keyed on an experiment accession.",
        kb_page="routes/bioproject-to-biosample/index.md",
        cost=RouteCost(requests=1, rate_limit_rps=EBI_RPS),
        proves_data_exists=True,
        evidence=RouteEvidence(
            corpus="sample:3000@reprocessed-srx",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=41,
            unique_results=2_893,
            notes="2,959 resolved, 41 empty.",
        ),
    ),
    Route(
        id="sample->run:ena_filereport",
        source=EntityType.SAMPLE,
        target=EntityType.RUN,
        provider="ena_portal",
        summary="ENA portal filereport keyed on an INSDC sample accession.",
        kb_page="routes/sample-to-run/ena-filereport.md",
        cost=RouteCost(requests=1, rate_limit_rps=EBI_RPS),
        proves_data_exists=True,
        evidence=RouteEvidence(
            corpus="sample:3000@reprocessed-srs",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=13,
            unique_results=8_359,
            notes="2,987 resolved, 13 empty.",
        ),
    ),
    Route(
        id="run->experiment:ena_filereport",
        source=EntityType.RUN,
        target=EntityType.EXPERIMENT,
        provider="ena_portal",
        summary="ENA portal filereport keyed on a run accession.",
        kb_page="routes/run-to-experiment/ena-filereport.md",
        cost=RouteCost(requests=1, rate_limit_rps=EBI_RPS),
        proves_data_exists=True,
        evidence=RouteEvidence(
            corpus="sample:3000@reprocessed-srr",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=23,
            unique_results=2_888,
            notes="2,977 resolved, 23 empty.",
        ),
    ),
    Route(
        id="run->biosample:ena_filereport",
        source=EntityType.RUN,
        target=EntityType.BIOSAMPLE,
        provider="ena_portal",
        summary="ENA portal filereport keyed on a run accession.",
        kb_page="routes/run-to-biosample/ena-filereport.md",
        cost=RouteCost(requests=1, rate_limit_rps=EBI_RPS),
        proves_data_exists=True,
        evidence=RouteEvidence(
            corpus="sample:3000@reprocessed-srr",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=23,
            unique_results=2_880,
            notes="2,977 resolved, 23 empty. The 23 empties are the same runs that no longer exist in either archive.",
        ),
    ),
    # --- The submitter's own names --------------------------------------
    # ENA returns the submitter's alias for every run, experiment and sample,
    # and for a GEO- or ArrayExpress-brokered submission that alias is usually
    # the accession itself. One request, and it works on the series where GEO
    # records no SRA relation -- which the routes that go through GEO cannot.
    Route(
        id="run->geo_sample:ena_alias",
        source=EntityType.RUN,
        target=EntityType.GEO_SAMPLE,
        provider="ena_portal",
        summary="GSM parsed from ENA's sample_alias / experiment_alias / run_alias.",
        kb_page="routes/run-to-geo-sample/ena-alias.md",
        cost=RouteCost(requests=1, rate_limit_rps=EBI_RPS),
        evidence=RouteEvidence(
            corpus="sample:3000@reprocessed-srr",
            surveyed_on=date(2026, 9, 16),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=523,
            unique_results=2_395,
            notes="2,477 resolved, 523 empty. Coverage tracks whether the run was submitted through GEO: 82.7% of the reprocessed sample column is GEO samples, and this route resolves a comparable share. Agreement with GEO's own SRA relation is 8,604 of 8,604 over 200 series, with zero contradictions.",
        ),
    ),
    Route(
        id="run->ae_experiment:ena_alias",
        source=EntityType.RUN,
        target=EntityType.AE_EXPERIMENT,
        provider="ena_portal",
        summary="ArrayExpress accession parsed from ENA's submitter aliases.",
        kb_page="routes/run-to-geo-sample/ena-alias.md",
        cost=RouteCost(requests=1, rate_limit_rps=EBI_RPS),
        evidence=RouteEvidence(
            corpus="results-of:biosample->run:ena_filereport@results-of:ae_experiment->biosample:sdrf@arrayexpress-no-secondary",
            surveyed_on=date(2026, 9, 16),
            accessions_queried=1_547,
            accessions_failed=0,
            accessions_empty=1_456,
            unique_results=6,
            notes="91 resolved, 1,456 empty. Unlike the GEO case, ArrayExpress submitters rarely put the study accession in an alias: 91 of 1,547 runs, 6 distinct studies. E-MTAB-6505 does, most do not, so this is a lucky-dip route rather than a reliable one.",
        ),
    ),
    # --- The file layer ---------------------------------------------------
    # A file is where every other route was heading. These four disagree with
    # each other by design rather than by defect: ENA's fastq columns hold ENA's
    # own derivations, `submitted` holds what the submitter actually deposited,
    # and SDL holds NCBI's view of both. E-MTAB-8060 is the case that proves
    # they are not interchangeable -- its real data is a BAM that the fastq
    # route cannot see at all.
    Route(
        id="run->file:ena_fastq",
        source=EntityType.RUN,
        target=EntityType.FILE,
        provider="ena_portal",
        summary="ENA fastq_ftp/fastq_md5/fastq_bytes columns; ENA's own derived fastqs.",
        kb_page="routes/run-to-file/index.md",
        known_pathologies=(
            "ena-paired-library-single-fastq",
            "ena-fastq-is-lite-quality",
            "ena-fastq-splits-the-barcode-read",
            "ena-serves-a-listed-fastq-wrongly",
        ),
        cost=RouteCost(requests=1, rate_limit_rps=EBI_RPS),
        proves_data_exists=True,
        evidence=RouteEvidence(
            corpus="sample:3000@reprocessed-srr",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=156,
            unique_results=4_466,
            notes="2,844 resolved, 156 empty. 1,225 of the 2,844 resolved runs offer exactly one fastq file.",
        ),
    ),
    Route(
        id="run->file:ena_submitted",
        source=EntityType.RUN,
        target=EntityType.FILE,
        provider="ena_portal",
        summary="ENA submitted_ftp columns; the submitter's original deposit, in its original format.",
        kb_page="routes/run-to-file/index.md",
        cost=RouteCost(requests=1, rate_limit_rps=EBI_RPS),
        proves_data_exists=True,
        evidence=RouteEvidence(
            corpus="sample:3000@reprocessed-srr",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=2_754,
            unique_results=601,
            notes="246 resolved, 2,754 empty.",
        ),
    ),
    Route(
        id="run->file:ena_sra",
        source=EntityType.RUN,
        target=EntityType.FILE,
        provider="ena_portal",
        summary="ENA sra_ftp columns; the NCBI-format archive object mirrored at EBI.",
        kb_page="routes/run-to-file/index.md",
        cost=RouteCost(requests=1, rate_limit_rps=EBI_RPS),
        proves_data_exists=True,
        evidence=RouteEvidence(
            corpus="sample:3000@reprocessed-srr",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=3_000,
            unique_results=0,
            notes="0 resolved, 3,000 empty. ENA populates sra_ftp for none of them; the column is documented, returnable, and empty.",
        ),
    ),
    Route(
        id="ae_experiment->file:sdrf",
        source=EntityType.AE_EXPERIMENT,
        target=EntityType.FILE,
        provider="biostudies",
        summary="Submitter fastq URIs from the ArrayExpress SDRF, guarded by BioStudies registration.",
        kb_page="routes/ae-experiment-to-file/index.md",
        cost=RouteCost(requests=2, rate_limit_rps=EBI_RPS, paginates=True),
        known_pathologies=(
            "ae-sdrf-points-at-decommissioned-mirror",
            "biostudies-files-404s-registered-files",
        ),
        evidence=RouteEvidence(
            corpus="sample:3000@arrayexpress-ena",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=3_000,
            accessions_failed=11,
            accessions_empty=673,
            unique_results=197_089,
            notes=(
                "2,316 resolved, 673 empty. 1,385 of 1,398 E-MTAB studies (99%) "
                "name fastq URIs, against 879 of 1,550 E-GEOD imports (57%). "
                "Median 16 files per resolved study, mean 86."
            ),
        ),
    ),
    Route(
        id="run->file:sdl",
        source=EntityType.RUN,
        target=EntityType.FILE,
        provider="sdl",
        summary="NCBI Storage Data Locator; original files, cloud mirrors, and retrieval cost.",
        kb_page="routes/run-to-file/index.md",
        cost=RouteCost(requests=1, rate_limit_rps=NCBI_EUTILS_RPS),
        proves_data_exists=True,
        evidence=RouteEvidence(
            corpus="sample:3000@reprocessed-srr",
            surveyed_on=date(2026, 9, 15),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=4,
            unique_results=3_085,
            notes="2,996 resolved, 4 empty.",
        ),
    ),
]


@dataclass(frozen=True, slots=True)
class Promoted:
    """A catalogue link surveyed as a route: one row of :data:`PROMOTED`.

    Its request is a catalogue mechanism's (``routes.catalogue``), so the row
    holds only what a declaration adds to it: who answers, what it costs, and the
    evidence. The evidence is a route survey's at ``sample:3000`` -- never the
    link survey's 300, and never a probe.
    """

    mechanism: str
    source: EntityType
    target: EntityType
    provider: str
    evidence: RouteEvidence
    requests: int = 1
    rate_limit_rps: float = NCBI_EUTILS_RPS
    source_archives: tuple[Archive, ...] = ()
    known_pathologies: tuple[str, ...] = ()
    proves_data_exists: bool = False

    @property
    def id(self) -> str:
        return f"{self.source.value}->{self.target.value}:{self.mechanism}"

    def route(self) -> Route:
        return Route(
            id=self.id,
            source=self.source,
            target=self.target,
            provider=self.provider,
            summary=f"The catalogue mechanism {self.mechanism}, surveyed as a route.",
            kb_page=f"routes/from-{self.source.value.replace('_', '-')}.md",
            cost=RouteCost(requests=self.requests, rate_limit_rps=self.rate_limit_rps),
            evidence=self.evidence,
            known_pathologies=self.known_pathologies,
            source_archives=self.source_archives,
            proves_data_exists=self.proves_data_exists,
        )


# Catalogue links promoted to routes, each by its own route survey at
# sample:3000 (``fetch survey promote``; rows printed by its --evidence). A
# candidate with no evidence is not a route, and one whose survey resolved
# nothing stays a link: bioproject->ae_experiment:biostudies_search, 0 of 3,000.
# The bare-term search (esearch_sra_runinfo) is declared only where it is a
# candidate like any other; in BioProject -> run, experiment, study and
# BioSample it returned what efetch_direct's [GPRJ] does (162,808 runs against
# 162,854 over the same 3,000, and PRJNA66167's same 5,499), so it would only
# repeat an existing route's request.
PROMOTED: tuple[Promoted, ...] = (
    Promoted(
        mechanism="ena_browser_xml",
        source=EntityType.BIOPROJECT,
        target=EntityType.AE_EXPERIMENT,
        provider="ena_portal",
        requests=1,
        rate_limit_rps=15.0,
        source_archives=(),
        known_pathologies=(),
        evidence=RouteEvidence(
            corpus="sample:3000@reprocessed-prj",
            surveyed_on=date(2026, 10, 26),
            accessions_queried=3_000,
            accessions_failed=2,
            accessions_empty=2_901,
            unique_results=97,
            notes="97 resolved, 2,901 empty. A catalogue mechanism surveyed as a route.",
        ),
    ),
    Promoted(
        mechanism="ena_filereport_aliases",
        source=EntityType.BIOPROJECT,
        target=EntityType.AE_EXPERIMENT,
        provider="ena_portal",
        requests=1,
        rate_limit_rps=15.0,
        source_archives=(),
        known_pathologies=(),
        evidence=RouteEvidence(
            corpus="sample:3000@reprocessed-prj",
            surveyed_on=date(2026, 10, 26),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=2_904,
            unique_results=97,
            notes="96 resolved, 2,904 empty. A catalogue mechanism surveyed as a route.",
        ),
    ),
    Promoted(
        mechanism="ena_xref_target",
        source=EntityType.BIOPROJECT,
        target=EntityType.AE_EXPERIMENT,
        provider="ena_portal",
        requests=1,
        rate_limit_rps=15.0,
        source_archives=(),
        known_pathologies=(),
        evidence=RouteEvidence(
            corpus="sample:3000@reprocessed-prj",
            surveyed_on=date(2026, 10, 26),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=2_996,
            unique_results=4,
            notes="4 resolved, 2,996 empty. A catalogue mechanism surveyed as a route.",
        ),
    ),
    Promoted(
        mechanism="elink_bioproject_bioproject_d2u",
        source=EntityType.BIOPROJECT,
        target=EntityType.BIOPROJECT,
        provider="eutils",
        requests=3,
        rate_limit_rps=10.0,
        source_archives=(),
        known_pathologies=("megalink-backend-flakiness",),
        evidence=RouteEvidence(
            corpus="sample:3000@reprocessed-prj",
            surveyed_on=date(2026, 10, 26),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=2_470,
            unique_results=487,
            notes="530 resolved, 2,470 empty. A catalogue mechanism surveyed as a route.",
        ),
    ),
    Promoted(
        mechanism="ena_search_study_parent",
        source=EntityType.BIOPROJECT,
        target=EntityType.BIOPROJECT,
        provider="ena_portal",
        requests=1,
        rate_limit_rps=15.0,
        source_archives=(),
        known_pathologies=(),
        evidence=RouteEvidence(
            corpus="sample:3000@reprocessed-prj",
            surveyed_on=date(2026, 10, 26),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=446,
            unique_results=490,
            notes="2,554 resolved, 446 empty. A catalogue mechanism surveyed as a route.",
        ),
    ),
    Promoted(
        mechanism="ena_filereport_bam",
        source=EntityType.BIOPROJECT,
        target=EntityType.FILE,
        provider="ena_portal",
        requests=1,
        rate_limit_rps=15.0,
        source_archives=(),
        known_pathologies=(),
        evidence=RouteEvidence(
            corpus="sample:3000@reprocessed-prj",
            surveyed_on=date(2026, 10, 26),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=2_942,
            unique_results=1_470,
            notes="58 resolved, 2,942 empty. A catalogue mechanism surveyed as a route.",
        ),
    ),
    Promoted(
        mechanism="ena_filereport_fastq",
        source=EntityType.BIOPROJECT,
        target=EntityType.FILE,
        provider="ena_portal",
        requests=1,
        rate_limit_rps=15.0,
        source_archives=(),
        known_pathologies=(),
        evidence=RouteEvidence(
            corpus="sample:3000@reprocessed-prj",
            surveyed_on=date(2026, 10, 26),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=58,
            unique_results=269_128,
            notes="2,942 resolved, 58 empty. A catalogue mechanism surveyed as a route.",
        ),
    ),
    Promoted(
        mechanism="ena_filereport_submitted",
        source=EntityType.BIOPROJECT,
        target=EntityType.FILE,
        provider="ena_portal",
        requests=1,
        rate_limit_rps=15.0,
        source_archives=(),
        known_pathologies=(),
        evidence=RouteEvidence(
            corpus="sample:3000@reprocessed-prj",
            surveyed_on=date(2026, 10, 26),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=2_861,
            unique_results=29_698,
            notes="139 resolved, 2,861 empty. A catalogue mechanism surveyed as a route.",
        ),
    ),
    Promoted(
        mechanism="ena_filereport_aliases",
        source=EntityType.BIOPROJECT,
        target=EntityType.GEO_SAMPLE,
        provider="ena_portal",
        requests=1,
        rate_limit_rps=15.0,
        source_archives=(),
        known_pathologies=(),
        evidence=RouteEvidence(
            corpus="sample:3000@reprocessed-prj",
            surveyed_on=date(2026, 10, 26),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=480,
            unique_results=51_547,
            notes="2,520 resolved, 480 empty. A catalogue mechanism surveyed as a route.",
        ),
    ),
    Promoted(
        mechanism="esearch_sra_runinfo",
        source=EntityType.BIOPROJECT,
        target=EntityType.GEO_SAMPLE,
        provider="eutils",
        requests=2,
        rate_limit_rps=10.0,
        source_archives=(),
        known_pathologies=("efetch-returns-fewer-runs-than-esearch-counts",),
        evidence=RouteEvidence(
            corpus="sample:3000@reprocessed-prj",
            surveyed_on=date(2026, 10, 26),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=479,
            unique_results=51_566,
            notes="2,521 resolved, 479 empty. A catalogue mechanism surveyed as a route.",
        ),
    ),
    Promoted(
        mechanism="sra_be_runinfo",
        source=EntityType.BIOPROJECT,
        target=EntityType.GEO_SAMPLE,
        provider="ncbi_web",
        requests=1,
        rate_limit_rps=10.0,
        source_archives=(),
        known_pathologies=("sra-be-history-caps-at-5000-runs",),
        evidence=RouteEvidence(
            corpus="sample:3000@reprocessed-prj",
            surveyed_on=date(2026, 10, 26),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=480,
            unique_results=51_560,
            notes="2,520 resolved, 480 empty. A catalogue mechanism surveyed as a route.",
        ),
    ),
    Promoted(
        mechanism="ena_filereport",
        source=EntityType.BIOPROJECT,
        target=EntityType.SAMPLE,
        provider="ena_portal",
        requests=1,
        rate_limit_rps=15.0,
        source_archives=(),
        known_pathologies=(),
        evidence=RouteEvidence(
            corpus="sample:3000@reprocessed-prj",
            surveyed_on=date(2026, 10, 26),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=8,
            unique_results=84_948,
            notes="2,992 resolved, 8 empty. A catalogue mechanism surveyed as a route.",
        ),
    ),
    Promoted(
        mechanism="esearch_sra_runinfo",
        source=EntityType.BIOPROJECT,
        target=EntityType.SAMPLE,
        provider="eutils",
        requests=2,
        rate_limit_rps=10.0,
        source_archives=(),
        known_pathologies=("efetch-returns-fewer-runs-than-esearch-counts",),
        evidence=RouteEvidence(
            corpus="sample:3000@reprocessed-prj",
            surveyed_on=date(2026, 10, 26),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=6,
            unique_results=84_984,
            notes="2,994 resolved, 6 empty. A catalogue mechanism surveyed as a route.",
        ),
    ),
    Promoted(
        mechanism="esummary_sra",
        source=EntityType.BIOPROJECT,
        target=EntityType.SAMPLE,
        provider="eutils",
        requests=2,
        rate_limit_rps=10.0,
        source_archives=(),
        known_pathologies=("esummary-500-uid-limit", "esummary-sra-buries-accessions-in-expxml"),
        evidence=RouteEvidence(
            corpus="sample:3000@reprocessed-prj",
            surveyed_on=date(2026, 10, 26),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=6,
            unique_results=84_984,
            notes="2,994 resolved, 6 empty. A catalogue mechanism surveyed as a route.",
        ),
    ),
    Promoted(
        mechanism="sra_be_runinfo",
        source=EntityType.BIOPROJECT,
        target=EntityType.SAMPLE,
        provider="ncbi_web",
        requests=1,
        rate_limit_rps=10.0,
        source_archives=(),
        known_pathologies=("sra-be-history-caps-at-5000-runs",),
        evidence=RouteEvidence(
            corpus="sample:3000@reprocessed-prj",
            surveyed_on=date(2026, 10, 26),
            accessions_queried=3_000,
            accessions_failed=0,
            accessions_empty=7,
            unique_results=84_965,
            notes="2,993 resolved, 7 empty. A catalogue mechanism surveyed as a route.",
        ),
    ),
)

REGISTRY = RouteRegistry([*ROUTES, *(row.route() for row in PROMOTED)])
