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
    """

    corpus: str
    surveyed_on: date
    accessions_queried: int
    accessions_failed: int
    unique_results: int | None = None
    notes: str = ""

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
# Every evidence block below is a real measurement over the 12,756 BioProjects
# in `data/All_10x.sample_table.tsv`, surveyed 2026-05-12 by the scripts in
# `scripts/`. Unique-run counts are computed at the run level, not the row
# level: the `sra-db-be` result files contain 69,923 within-BioProject duplicate
# rows, which is why the raw row counts (824k) overstate that route's advantage.
# --------------------------------------------------------------------------

REPROCESSED = "reprocessed"
SURVEY_DATE = date(2026, 5, 12)
_BIOPROJECTS = 12_756
_BIOPROJECTS_PRJNA = 12_114

NCBI_EUTILS_RPS = 10.0  # with an API key; 3/s without
EBI_RPS = 15.0
# The GEO FTP mirror is not an API and is slower; do not hammer it.
NCBI_FTP_RPS = 3.0

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
            unique_results=783_936,
            notes=(
                "Census through the harness. 12,732 resolved, 23 empty, zero failures. "
                "Reproduces the 2026-05 script figure of 784,026 unique runs to within "
                "0.011% while eliminating all 15 of its failures -- the difference is "
                "four months of archive drift, not disagreement. Still a near-superset "
                "of every NCBI route: holds 111,665 runs efetch never returns, against "
                "~1,800 missing from it. The 23 empties are withdrawn, private or "
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
            corpus=REPROCESSED,
            surveyed_on=SURVEY_DATE,
            accessions_queried=_BIOPROJECTS,
            accessions_failed=16,
            unique_results=754_277,
            notes="Returns ~80k more runs than efetch. Times out on very large projects.",
        ),
        known_pathologies=("sra-be-timeout-on-large-projects", "gprj-falls-through-to-all-fields"),
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
            corpus=REPROCESSED,
            surveyed_on=SURVEY_DATE,
            accessions_queried=_BIOPROJECTS,
            accessions_failed=34,
            unique_results=754_332,
            notes="Marginally more runs than the direct variant, at twice the failure rate.",
        ),
        known_pathologies=("sra-be-timeout-on-large-projects", "megalink-backend-flakiness"),
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
            corpus=REPROCESSED,
            surveyed_on=SURVEY_DATE,
            accessions_queried=_BIOPROJECTS,
            accessions_failed=31,
            unique_results=674_186,
            notes="efetch returns fewer runs than esearch counted; cause not yet established.",
        ),
        known_pathologies=(
            "efetch-returns-fewer-runs-than-esearch-counts",
            "megalink-backend-flakiness",
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
            corpus=REPROCESSED,
            surveyed_on=SURVEY_DATE,
            accessions_queried=_BIOPROJECTS,
            accessions_failed=15,
            unique_results=673_157,
            notes="Lowest yield of the five. Low failure count, but it finds the least.",
        ),
        known_pathologies=("gprj-falls-through-to-all-fields",),
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
            unique_results=15_186,
            notes=(
                "Agrees with the direct route on every accession. Most failures are "
                "provenance, not API: re-created, withdrawn or private projects."
            ),
        ),
        known_pathologies=("elink-history-missing-querykey", "esummary-500-uid-limit"),
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
            unique_results=15_185,
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
            corpus="arrayexpress-all",
            surveyed_on=date(2026, 3, 6),
            accessions_queried=7_179,
            accessions_failed=457,
            unique_results=6_732,
            notes=(
                "457 experiments declare no secondary accession; the BioSample "
                "fallback recovers some of them (E-MTAB-6505 resolves that way)."
            ),
        ),
        known_pathologies=("ae-idf-missing-secondary-accession",),
    ),
    Route(
        id="ae_experiment->biosample:sdrf",
        source=EntityType.AE_EXPERIMENT,
        target=EntityType.BIOSAMPLE,
        provider="biostudies",
        summary="Comment[BioSD_SAMPLE] column of the ArrayExpress SDRF file.",
        kb_page="routes/ae-experiment-to-biosample/sdrf.md",
        cost=RouteCost(requests=1, rate_limit_rps=EBI_RPS),
        known_pathologies=("biostudies-silent-25-item-default",),
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
    ),
    Route(
        id="study->ae_experiment:biostudies_search",
        source=EntityType.STUDY,
        target=EntityType.AE_EXPERIMENT,
        provider="biostudies",
        summary="BioStudies search API, collection=arrayexpress.",
        kb_page="routes/study-to-ae-experiment/biostudies-search.md",
        cost=RouteCost(requests=1, rate_limit_rps=EBI_RPS, paginates=True),
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
        known_pathologies=("superseries-carries-no-bioproject",),
    ),
    Route(
        id="gse->bioproject:gds_summary",
        source=EntityType.GEO_SERIES,
        target=EntityType.BIOPROJECT,
        provider="eutils",
        summary="The bioproject field of the db=gds ESummary record.",
        kb_page="routes/gse-to-bioproject/gds-summary.md",
        cost=RouteCost(requests=2, rate_limit_rps=NCBI_EUTILS_RPS),
    ),
    Route(
        id="gse->geo_sample:soft_family",
        source=EntityType.GEO_SERIES,
        target=EntityType.GEO_SAMPLE,
        provider="geo_ftp",
        summary="^SAMPLE records in the SOFT family file, in declaration order.",
        kb_page="routes/gse-to-geo-sample/soft-family.md",
        cost=RouteCost(requests=1, rate_limit_rps=NCBI_FTP_RPS),
    ),
    Route(
        id="gse->geo_sample:gds_summary",
        source=EntityType.GEO_SERIES,
        target=EntityType.GEO_SAMPLE,
        provider="eutils",
        summary="The Samples array of the db=gds ESummary record.",
        kb_page="routes/gse-to-geo-sample/gds-summary.md",
        cost=RouteCost(requests=2, rate_limit_rps=NCBI_EUTILS_RPS),
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
            unique_results=308_639,
            notes=(
                "Census of every GEO series in the reprocessed table. 13,022 resolved, "
                "16 empty, 7 failed -- all seven a 404 on the FTP mirror, i.e. private "
                "or withdrawn. Of the 16 empties, 11 are recoverable through the "
                "BioProject; the other 5 have no released runs at all."
            ),
        ),
        known_pathologies=("geo-omits-sample-sra-relation",),
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
            unique_results=290_462,
            notes=(
                "Census. 12,551 resolved, 487 empty. Covers 92.5% of the three-route "
                "union and subsumes ELink almost entirely -- only 43 of ELink's 178,879 "
                "experiments are absent from it. Because it queries result=read_run, "
                "every accession it returns provably carries data."
            ),
        ),
        known_pathologies=(
            "series-under-a-shared-umbrella-bioproject",
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
        known_pathologies=("geo-omits-sample-sra-relation",),
    ),
    Route(
        id="geo_sample->biosample:acc_cgi",
        source=EntityType.GEO_SAMPLE,
        target=EntityType.BIOSAMPLE,
        provider="geo_acc_cgi",
        summary="!Sample_relation = BioSample: from GEO's own record for the sample.",
        kb_page="routes/geo-sample-to-experiment/index.md",
        cost=RouteCost(requests=1, rate_limit_rps=NCBI_FTP_RPS),
    ),
    Route(
        id="geo_sample->geo_series:acc_cgi",
        source=EntityType.GEO_SAMPLE,
        target=EntityType.GEO_SERIES,
        provider="geo_acc_cgi",
        summary="!Sample_series_id from GEO's record; a sample can belong to several series.",
        kb_page="routes/geo-sample-to-experiment/index.md",
        cost=RouteCost(requests=1, rate_limit_rps=NCBI_FTP_RPS),
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
    ),
    Route(
        id="experiment->run:ena_filereport",
        source=EntityType.EXPERIMENT,
        target=EntityType.RUN,
        provider="ena_portal",
        summary="ENA portal filereport keyed on an experiment accession.",
        kb_page="routes/bioproject-to-run/index.md",
        cost=RouteCost(requests=1, rate_limit_rps=EBI_RPS),
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
    ),
    Route(
        id="sample->run:ena_filereport",
        source=EntityType.SAMPLE,
        target=EntityType.RUN,
        provider="ena_portal",
        summary="ENA portal filereport keyed on an INSDC sample accession.",
        kb_page="routes/bioproject-to-run/index.md",
        cost=RouteCost(requests=1, rate_limit_rps=EBI_RPS),
        proves_data_exists=True,
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
    ),
]

REGISTRY = RouteRegistry(ROUTES)
