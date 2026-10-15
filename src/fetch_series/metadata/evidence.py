"""Which source to believe, computed from measurement -- never written down.

The exact counterpart of :mod:`fetch_series.graph` for metadata sources, and it
exists for the same reason. Four archives answer ``what tissue is this sample``
and they do not always agree; deciding between them by declaration order, or by
which archive feels more authoritative, would be the hand-written preference
this project removed from its route layer.

**Every source below is declared with no evidence**, because nobody has
surveyed them yet. That is the honest state and it has a consequence the code
enforces: :func:`ranked` puts unmeasured sources behind measured ones and
otherwise orders them by id, and
:meth:`~fetch_series.metadata.SampleMetadata.asserted` therefore declines to
pick when two disagree. Filling these in is what
:mod:`fetch_series.metadata.gold` is for.

## What a measurement here means

Agreement with an ArrayExpress curator's own annotation for the same sample --
not correctness. The distinction is stated because it is not a technicality:
SDRF curation is itself imperfect, AE-deposited studies are not a random sample
of GEO, and a source could agree with a curator by sharing her mistake. It is
the best label obtainable at scale without commissioning curation, and it is
labelled as what it is.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date

# Bands, for the same reason `graph.py` has them: ranking on raw rates put
# noise above substance twice in the route layer. Two sources whose agreement
# differs by half a percentage point are not distinguishable by it, and should
# be separated by cost instead of by an error bar.
AGREEMENT_BAND = 0.01
ANSWER_BAND = 0.01


@dataclass(frozen=True, slots=True)
class ExtractorEvidence:
    """A measurement of one source on one field, against one named corpus."""

    corpus: str
    surveyed_on: date
    field: str
    #: Samples in the corpus for which a gold label exists for this field.
    samples_queried: int
    #: Of those, how many this source answered at all. Silence is not error.
    samples_answered: int
    #: Of the answered, how many matched the gold label.
    agreements: int
    disagreements: int
    notes: str = ""

    def __post_init__(self) -> None:
        if self.agreements + self.disagreements > self.samples_answered:
            raise ValueError(
                f"{self.field}/{self.corpus}: {self.agreements} + {self.disagreements} "
                f"verdicts exceed {self.samples_answered} answered"
            )

    @property
    def answer_rate(self) -> float:
        """How often the source says anything. Recall, before correctness."""
        if not self.samples_queried:
            return 0.0
        return self.samples_answered / self.samples_queried

    @property
    def agreement_rate(self) -> float:
        """How often what it says matches the curator, *given* that it spoke.

        Conditional on answering, deliberately. A source that answers 5% of
        samples and is always right is a different thing from one that answers
        every sample and is right 5% of the time, and one rate cannot express
        both -- which is why :func:`ranked` sorts on each separately.
        """
        judged = self.agreements + self.disagreements
        if not judged:
            return 0.0
        return self.agreements / judged


@dataclass(frozen=True, slots=True)
class Source:
    """One place a field's value can come from."""

    id: str
    summary: str
    kb_page: str
    #: Requests needed to consult it for a whole accession. ENA answers every
    #: sample in one filereport; BioSample needs a batch per 200.
    cost: int = 1
    #: field name -> what has been measured for it. Empty means unmeasured,
    #: which is a state with consequences rather than a gap to be filled in
    #: by assumption.
    evidence: Mapping[str, ExtractorEvidence] = field(default_factory=dict)
    #: Knowledge-base slugs for defects this source is known to exhibit,
    #: printed by ``--explain`` beside a value it contributed. Declared for the
    #: same reason routes declare theirs: a pathology nothing admits to is one
    #: nothing will ever warn about.
    known_pathologies: tuple[str, ...] = ()

    @property
    def measured(self) -> bool:
        return bool(self.evidence)


SOURCES: tuple[Source, ...] = (
    Source(
        id="ae:characteristics",
        summary=(
            "ArrayExpress SDRF Characteristics, curated by EBI against a controlled key "
            "vocabulary and sometimes carrying an ontology term outright."
        ),
        kb_page="metadata/sources.md",
        cost=1,
    ),
    Source(
        id="biosample:attribute",
        summary=(
            "BioSample attribute pairs, carrying NCBI's own harmonized_name. The richest "
            "structured source, and the only one that recorded disease per sample for "
            "PRJNA434002 where ENA's column was empty."
        ),
        kb_page="metadata/sources.md",
        cost=1,
        known_pathologies=("biosample-efetch-ignores-accession-prefix",),
    ),
    Source(
        id="ena:filereport",
        summary=(
            "ENA's descriptive filereport columns. Structured and schema-named, but "
            "sparsely populated: ENA defines disease, cell_type and dev_stage and fills "
            "them far less often than tissue_type and sex."
        ),
        kb_page="metadata/sources.md",
        cost=1,
    ),
    Source(
        id="biosample:description",
        summary=(
            "BioSample's Description block -- title, organism, free-text paragraph. Declared "
            "separately from the attributes because it is prose the submitter wrote once, not "
            "a keyed attribute, and only `organism` comes out of it deterministically."
        ),
        kb_page="metadata/sources.md",
        cost=0,
        known_pathologies=("biosample-efetch-ignores-accession-prefix",),
    ),
    Source(
        id="geo:soft",
        summary=(
            "GEO's per-sample SOFT prose -- title, source name, organism. The source name is "
            "often the only place a tissue appears when a submitter wrote no characteristics."
        ),
        kb_page="metadata/sources.md",
        cost=0,
    ),
    Source(
        id="geo:characteristics",
        summary=(
            "GEO !Sample_characteristics_ch1, already key: value but under no vocabulary "
            "at all -- 116 distinct keys across 198 datasets, 70 of them appearing once."
        ),
        kb_page="metadata/sources.md",
        cost=1,
    ),
)

_BY_ID: Mapping[str, Source] = {s.id: s for s in SOURCES}


def source_of(claim_source: str) -> Source | None:
    """The declared source a claim's provenance string belongs to.

    ``biosample:attribute[tissue]`` -> the ``biosample:attribute`` source.
    Returns ``None`` for a provenance string no source declares, which is not
    an error: prose fields and derived claims legitimately have none, and a
    source that should be declared and is not will show up as unmeasurable
    rather than be silently attributed to its neighbour.
    """
    stem = claim_source.split("[", 1)[0]
    return _BY_ID.get(stem)


def ranked(field_name: str) -> list[Source]:
    """Sources for ``field_name``, best-evidenced first.

    Sorts on, in order: measured before unmeasured; agreement rate banded at
    :data:`AGREEMENT_BAND`; answer rate banded at :data:`ANSWER_BAND`; request
    cost; source id for stability.

    The id tie-break is not cosmetic. A source id is recorded as provenance on
    every claim, so an unstable order would make two identical queries produce
    differently-attributed answers -- the same reason ``graph.ranked()`` ends
    on ``route.id``.
    """

    def sort_key(source: Source) -> tuple[int, int, int, int, str]:
        measurement = source.evidence.get(field_name)
        if measurement is None:
            return (1, 0, 0, source.cost, source.id)
        return (
            0,
            -round(measurement.agreement_rate / AGREEMENT_BAND),
            -round(measurement.answer_rate / ANSWER_BAND),
            source.cost,
            source.id,
        )

    return sorted(SOURCES, key=sort_key)


def unmeasured_fields() -> dict[str, list[str]]:
    """Which sources have no measurement for which fields.

    Printed by ``fetch metadata evidence`` so the gap is visible rather than
    inferred from an absence. Every entry here is a place where
    ``SampleMetadata.asserted`` will decline to choose.
    """
    from fetch_series.metadata.fields import FIELD_NAMES

    return {
        source.id: [name for name in FIELD_NAMES if name not in source.evidence]
        for source in SOURCES
    }
