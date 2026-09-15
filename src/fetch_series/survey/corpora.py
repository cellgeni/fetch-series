"""The accession sets routes are surveyed against.

Tiered on purpose. A new route runs ``hard-cases`` first -- it is small, fast,
and made entirely of accessions known to break something, so a design error
surfaces in seconds rather than after an overnight run. Only then does it earn
a stratified sample, and only a route that is a production candidate earns the
full corpus.
"""

from __future__ import annotations

import random
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

from fetch_series.accession import Accession, EntityType, parse_all

DATA_DIR = Path("data")
SAMPLE_TABLE = DATA_DIR / "reference" / "All_10x.sample_table.tsv"
LEGACY_SAMPLE_TABLE = DATA_DIR / "All_10x.sample_table.tsv"
GSE_LIST = DATA_DIR / "reference" / "all_gse.list"
LEGACY_GSE_LIST = DATA_DIR / "all_gse.list"

SAMPLE_TABLE_COLUMNS = ("sample", "gse", "prj", "srs", "srx", "srr", "species")


@dataclass(frozen=True, slots=True)
class Corpus:
    """A named set of accessions, with a record of where it came from."""

    name: str
    description: str
    accessions: tuple[Accession, ...]
    source: str
    unparsed: tuple[str, ...] = field(default=())

    def __len__(self) -> int:
        return len(self.accessions)

    def of_type(self, entity: EntityType) -> tuple[str, ...]:
        return tuple(a.value for a in self.accessions if a.entity is entity)

    @property
    def values(self) -> tuple[str, ...]:
        return tuple(a.value for a in self.accessions)


def _build(name: str, description: str, source: str, raws: Iterable[str]) -> Corpus:
    parsed, unknown = parse_all(list(dict.fromkeys(raws)))
    return Corpus(
        name=name,
        description=description,
        accessions=tuple(parsed),
        source=source,
        # `-` and anything else unrecognised is carried, not dropped: a corpus
        # that silently discards rows has an unknowable denominator.
        unparsed=tuple(unknown),
    )


# Every accession below broke something, and the note says what. This is the
# tier-one corpus: if a route cannot produce a verdict for each of these -- a
# result, or a named pathology -- it is not ready for a bigger run.
HARD_CASES: dict[str, str] = {
    "GSE135325": "GEO records the BioSample but no !Sample_relation = SRA:; mixes BD AbSeq samples with 10x",
    "GSE137444": "BioSample-only relations, same shape as GSE135325",
    "GSE206528": "per-sample failure: no experiment or run ID found in the SRA table",
    "GSE109816": "880 runs, none of them 10x; the case for screening before download",
    "GSE207991": "private on GEO; its BioProject PRJNA857927 is gone entirely",
    "GSE203201": "points at PRJNA839063, a re-created project with no experiments",
    "GSE153824": "BioProject re-created: now PRJNA644462, not PRJNA644294",
    "E-MTAB-8060": "SDRF URIs point at the dead mirror and none of its 36 fastqs are registered; the real submission is the ENA BAM",
    "E-MTAB-9221": "same dead-mirror URIs but all 40 fastqs registered; the case that makes 8060 a trap",
    "E-MTAB-6505": "no Comment[SecondaryAccession]; only the BioSample fallback resolves it",
    "E-MTAB-9216": "no secondary accession either",
    "E-MTAB-5448": "known to break ae2secondary",
    "PRJNA644294": "re-created upstream; GSE153824 now points elsewhere",
    "PRJNA735853": "re-created upstream; GSE206528 now points at PRJNA849641",
    "PRJNA857927": "withdrawn from BioProject entirely",
    "PRJNA826352": "withdrawn; the replacement project has no experiments",
    "PRJNA1136968": "absent from the BioProject database",
    "PRJNA1190403": "absent from the BioProject database",
    "PRJNA768422": "samples submitted to SRA both directly and via GEO; more experiments than expected",
    "PRJNA940674": "esearch reports more UIDs than efetch returns rows",
    "PRJNA1169288": "one BioProject, three GEO series",
    "PRJNA1214811": "ELink history came back with no usable querykey",
    "PRJEB9859": "large project; times out on sra-db-be",
    "PRJEB82511": "large project; times out on sra-db-be",
    "PRJEB19038": "large project; times out on sra-db-be",
    "PRJEB14362": "whole blocks of ERS samples missing from the efetch result",
    "PRJEB42537": "EBI-native; reachable from E-MTAB-10018 and ERP126408",
    "PRJNA988806": "the worked example: -> GSE236084 -> SRP446371 -> SRR25056225-29",
    "SRS9161836": "CITE-seq submitted alongside scRNA-seq; two runs per experiment",
    "DRS188691": "one logical run split across separate SRA runs",
    "DRX730719": "two runs, one of average length 20 -- indexes submitted separately from reads",
    "SRS24456482": "submitted to SRA both directly and via GEO; yields more experiments than expected",
    "SRS9029085": "submitted to SRA both directly and via GEO; same pattern as SRS24456482",
    "SRS27109451": "ambiguous: PRJNA1337591 lists 4 experiments, PRJNA1345517 lists 9, same biosample",
    "GSM7518069": "belongs to two series (GSE236084 and GSE236087), so 'its' series is not singular",
    "GSM4005486": "in GSE135325, where GEO records the BioSample but no SRA relation",
    "GSM4274734": "in GSE150508, whose SOFT file names an experiment that carries no runs",
    "SRR25056225": "the worked example's first run; the end of the chain from PRJNA988806",
    "ERR2861957": "reached from E-MTAB-6505 only through its BioSample, not its IDF",
    "SRP446371": "the worked example's study, reached from PRJNA988806",
    "ERP126408": "EBI-native study; round-trips to E-MTAB-10018 and PRJEB42537",
    "SRX9670669": "the live experiment GSE150508's SOFT file does not name",
    "SRX7571191": "the experiment GSE150508 does name, which carries no runs at all",
    "SAMN36028297": "the worked example's BioSample, reached from GSM7518069",
    "SAMEA5053920": "E-MTAB-6505's BioSample; the only route to its data when the IDF declares no study",
    "SAMN12476461": "in GSE135325, which records BioSamples and no SRA relations",
}


def hard_cases() -> Corpus:
    """Tier one: accessions curated because each one broke something."""
    return _build(
        name="hard-cases",
        description="Curated pathological accessions, one per known failure mode.",
        source="fetch_series.survey.corpora.HARD_CASES",
        raws=HARD_CASES.keys(),
    )


def _sample_table_path() -> Path:
    for candidate in (SAMPLE_TABLE, LEGACY_SAMPLE_TABLE):
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"No sample table at {SAMPLE_TABLE} or {LEGACY_SAMPLE_TABLE}")


def reprocessed(column: str = "prj", path: Path | None = None) -> Corpus:
    """Everything that has actually been through reprocessing.

    The strongest corpus available: these accessions are not a sample of what
    the archives hold, they are what the pipeline really had to resolve.
    """
    if column not in SAMPLE_TABLE_COLUMNS:
        raise ValueError(f"Unknown column {column!r}; expected one of {SAMPLE_TABLE_COLUMNS}")
    index = SAMPLE_TABLE_COLUMNS.index(column)
    source = path or _sample_table_path()

    raws: list[str] = []
    with source.open() as handle:
        for line in handle:
            fields = line.rstrip("\n").split("\t")
            if index < len(fields):
                raws.extend(token.strip() for token in fields[index].split(",") if token.strip())

    return _build(
        name=f"reprocessed-{column}",
        description=f"The {column} column of the reprocessed 10x sample table.",
        source=str(source),
        raws=raws,
    )


def _gse_list_path() -> Path:
    for candidate in (GSE_LIST, LEGACY_GSE_LIST):
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"No GSE list at {GSE_LIST} or {LEGACY_GSE_LIST}")


def geo_sample(n: int = 1000, seed: int = 20260914, path: Path | None = None) -> Corpus:
    """Tier two: a reproducible random draw from every GEO series.

    Stratified by accession number, which is issued sequentially and so stands
    in for submission date. Sampling uniformly at random would over-represent
    recent years simply because GEO has grown, and route behaviour differs
    sharply between old and new submissions.

    Returns exactly ``n`` accessions or raises. The name is persisted with every
    verdict as evidence provenance, so a corpus whose size does not match its
    own label makes two runs quietly incomparable.
    """
    source = path or _gse_list_path()
    accessions = [line.strip() for line in source.read_text().splitlines() if line.strip()]
    accessions.sort(key=lambda a: int(a[3:]) if a[3:].isdigit() else 0)

    rng = random.Random(seed)
    strata = 10
    size = len(accessions)
    # Distribute the remainder across strata so the draw is exactly n. Taking
    # n // strata per stratum silently returned 10 for any n below 20 -- so
    # geo-sample-15 held 10 accessions while its name, which is persisted with
    # every verdict as provenance, claimed 15.
    quota = [n // strata + (1 if i < n % strata else 0) for i in range(strata)]
    drawn: list[str] = []
    shortfall = 0
    for i in range(strata):
        lo, hi = i * size // strata, (i + 1) * size // strata
        band = accessions[lo:hi]
        want = quota[i] + shortfall
        take = min(want, len(band))
        shortfall = want - take
        drawn.extend(rng.sample(band, take))

    if len(drawn) != n:
        raise ValueError(f"Asked for {n} GEO series but the pool of {size:,} yielded {len(drawn)}")

    return _build(
        name=f"geo-sample-{n}",
        description=f"{n} GEO series, stratified into {strata} bands by accession number (seed {seed}).",
        source=f"{source} seed={seed}",
        raws=drawn,
    )


def from_survey(route_id: str, corpus: str, cache_path: Path | None = None) -> Corpus:
    """Build a corpus out of what an earlier survey *returned*.

    The results of one survey are the inputs of the next. The experiments the
    SOFT census found are the right population for asking "how many experiments
    GEO names actually carry data" -- the reprocessed table is not, because it
    holds only accessions that already reprocessed successfully and so excludes
    the failures the question is about.
    """
    from fetch_series.cache import DEFAULT_CACHE_PATH, SurveyCache

    values: set[str] = set()
    with SurveyCache(cache_path or DEFAULT_CACHE_PATH) as cache:
        for result in cache.results(corpus, route_id):
            values.update(result.results)
    if not values:
        raise ValueError(f"No recorded results for {route_id} on {corpus}")
    return _build(
        name=f"results-of:{route_id}@{corpus}",
        description=f"Every accession {route_id} returned over {corpus}.",
        source=f"survey cache: {route_id} @ {corpus}",
        raws=sorted(values),
    )


CORPUS_BUILDERS = {
    "hard-cases": hard_cases,
    "reprocessed": reprocessed,
    "geo-sample": geo_sample,
}

KNOWN_CORPORA = (
    "hard-cases",
    "results-of:<route-id>@<corpus>",
    "reprocessed-<column>   (column: " + ", ".join(SAMPLE_TABLE_COLUMNS[:6]) + ")",
    "geo-sample[-<n>]",
)


def load(name: str) -> Corpus:
    """Build a corpus by name, accepting the parameterised forms.

    ``reprocessed-gse`` and ``geo-sample-2000`` name a corpus precisely enough
    that survey results can be attributed to it later. The corpus name is stored
    with every verdict, so it has to identify the accession set exactly -- two
    different draws recorded under one name would be uncomparable.
    """
    if name in CORPUS_BUILDERS:
        return CORPUS_BUILDERS[name]()

    if name.startswith("reprocessed-"):
        column = name.removeprefix("reprocessed-")
        if column in SAMPLE_TABLE_COLUMNS:
            return reprocessed(column)
        raise ValueError(
            f"Unknown column {column!r} in corpus {name!r}; "
            f"expected one of {', '.join(SAMPLE_TABLE_COLUMNS[:6])}"
        )

    # results-of:<route id>@<corpus>
    if name.startswith("results-of:"):
        spec = name.removeprefix("results-of:")
        route_id, _, source_corpus = spec.rpartition("@")
        if not route_id or not source_corpus:
            raise ValueError(f"Expected results-of:<route-id>@<corpus>, got {name!r}")
        return from_survey(route_id, source_corpus)

    if name.startswith("geo-sample-"):
        suffix = name.removeprefix("geo-sample-")
        if suffix.isdigit():
            return geo_sample(int(suffix))
        raise ValueError(f"Expected a sample size in corpus {name!r}, got {suffix!r}")

    raise ValueError(f"Unknown corpus {name!r}; known: {'; '.join(KNOWN_CORPORA)}")
