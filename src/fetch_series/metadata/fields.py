"""The metadata fields this project extracts, as declared data.

Two halves, chosen for two different reasons.

The **grounded** fields are the ones a search index needs: organism, tissue,
cell type, disease, developmental stage, sex, assay. Each names the ontologies
its values are permitted to come from, because a value that is merely plausible
prose is not searchable and a CURIE from the wrong ontology is worse than none.
The set and its ontologies follow the CELLxGENE schema, so output lands in the
vocabulary the single-cell community already indexes on rather than in one
invented here.

The **structural** fields -- donor, grouping, multiplexing, timepoint, age --
have no ontology and are not descriptive. They say which libraries came from the
same biological material, which is what a reprocessing pipeline has to know and
what no metadata standard records.

## Scope is a property of the field, and it is load-bearing

``disease`` is a property of a sample. A study title saying "post-mortem brain
tissue from Autism Spectrum Disorder patients" is a property of the *study*, and
PRJNA434002 is exactly that: the phrase appears in ``study_title`` for every one
of its runs while the per-sample ``disease`` column is empty -- for the controls
too. Assigning it would label the controls diseased.

So a sample-scoped field can only be assigned from a sample-scoped source, and
:mod:`fetch_series.metadata.rules` enforces it in both the static direction (a
source declared study-scoped) and the measured one (a source whose text is
identical for every sample of the study, whatever its declared scope).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum

# NCBI taxon ids, for the fields whose ontology depends on the organism.
HUMAN = "9606"
MOUSE = "10090"


class Scope(StrEnum):
    """Whether a value describes one sample or the whole study."""

    SAMPLE = "sample"
    STUDY = "study"


@dataclass(frozen=True, slots=True)
class Field:
    """One extractable field, and what a value for it must look like."""

    name: str
    scope: Scope
    summary: str
    #: OLS ontology ids a value may be grounded in. Empty means the field is
    #: structural: it carries a normalised string and no CURIE.
    ontologies: tuple[str, ...] = ()
    #: Overrides ``ontologies`` for a given NCBI taxon id. Developmental stage
    #: is the case this exists for -- HsapDv covers human and MmusDv mouse, and
    #: neither covers the other, so grounding a mouse stage against HsapDv
    #: would either fail or find a false friend.
    ontologies_by_taxon: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    #: Whether one sample may legitimately carry several values. A sample can be
    #: a mixture of cell types; it has one sex.
    multivalued: bool = False

    def permitted_ontologies(self, taxon: str | None = None) -> tuple[str, ...]:
        """The ontologies a value is allowed to come from, for this organism.

        Falls back to ``ontologies`` rather than to nothing when the taxon is
        unknown or unlisted: a generic lifecycle term from UBERON is a worse
        answer than a species-specific one, but it is still an answer, and
        refusing to ground anything for a zebrafish would be worse than both.
        """
        if taxon and (specific := self.ontologies_by_taxon.get(taxon)):
            return specific
        return self.ontologies

    @property
    def grounded(self) -> bool:
        """Whether values for this field carry an ontology term at all."""
        return bool(self.ontologies or self.ontologies_by_taxon)


FIELDS: tuple[Field, ...] = (
    Field(
        name="organism",
        scope=Scope.SAMPLE,
        summary="The species the material came from.",
        ontologies=("ncbitaxon",),
        # Already resolved as RunRecord.species from ENA's scientific_name, and
        # ENA also returns tax_id, so this is the one field that usually needs
        # no grounding request at all.
    ),
    Field(
        name="tissue",
        scope=Scope.SAMPLE,
        summary="The anatomical structure the material was taken from.",
        ontologies=("uberon",),
        multivalued=True,
    ),
    Field(
        name="cell_type",
        scope=Scope.SAMPLE,
        summary="The cell type, where the submitter sorted or selected for one.",
        ontologies=("cl",),
        # A 10x GEX library of unsorted tissue has no single cell type, and
        # saying so is the right answer. Where a study sorted CD4+ T cells and
        # CD8+ T cells into one library, it has two.
        multivalued=True,
    ),
    Field(
        name="disease",
        scope=Scope.SAMPLE,
        summary="The disease the material is from, or normal where it is a control.",
        ontologies=("mondo",),
        # Healthy is a value, not an absence: PATO_0000461 'normal' is what
        # CELLxGENE uses, and an empty disease field means "not stated", which
        # is a different claim. `vocab` maps control synonyms onto it.
        multivalued=True,
    ),
    Field(
        name="development_stage",
        scope=Scope.SAMPLE,
        summary="The developmental stage of the donor organism.",
        ontologies=("uberon",),
        ontologies_by_taxon={HUMAN: ("hsapdv",), MOUSE: ("mmusdv",)},
    ),
    Field(
        name="sex",
        scope=Scope.SAMPLE,
        summary="The phenotypic sex of the donor organism.",
        # PATO, not UBERON. Constrained to UBERON, ZOOMA answers 'male' with
        # UBERON:0003101 'male organism' at confidence GOOD -- an anatomical
        # entity where a phenotypic quality was asked for. See
        # docs/decisions/ontology-grounding.md.
        ontologies=("pato",),
    ),
    Field(
        name="assay",
        scope=Scope.SAMPLE,
        summary="The sequencing assay, as an EFO term.",
        ontologies=("efo",),
        # Deliberately overlaps the assay layer, which recognises 10x from
        # protocol prose. Two independent answers to one question is the
        # condition under which a disagreement can be found at all; merging
        # them into one would hide it.
    ),
    Field(
        name="donor_id",
        scope=Scope.SAMPLE,
        summary="The submitter's identifier for the individual the material came from.",
        # No ontology: it is an opaque submitter string. It is also the single
        # most valuable structural field and the rarest -- 22 of 198 datasets
        # name a donor at all.
    ),
    Field(
        name="sample_grouping",
        scope=Scope.SAMPLE,
        summary="Which libraries come from the same biological sample.",
        # The key a grouping was computed on, not the group. A treatment or a
        # timepoint is not a grouping key; a donor identifier is.
    ),
    Field(
        name="multiplexing",
        scope=Scope.SAMPLE,
        summary="The pooling scheme, where several donors share one library.",
    ),
    Field(
        name="timepoint",
        scope=Scope.SAMPLE,
        summary="The submitter's timepoint label, for time-course designs.",
    ),
    Field(
        name="age",
        scope=Scope.SAMPLE,
        summary="The donor's age, as the submitter wrote it, with units where given.",
        # Not normalised to a number. "E14.5", "8 weeks", "60-65", "adult" and
        # "P0" are all real values and collapsing them to a float would invent
        # precision; HsapDv/MmusDv grounding of development_stage is where the
        # comparable claim gets made.
    ),
)

FIELD_NAMES: tuple[str, ...] = tuple(f.name for f in FIELDS)
GROUNDED_FIELDS: tuple[Field, ...] = tuple(f for f in FIELDS if f.grounded)

_BY_NAME: Mapping[str, Field] = {f.name: f for f in FIELDS}


def by_name(name: str) -> Field:
    """The field declared under ``name``.

    Raises rather than returning ``None``: every caller in this package has a
    declared field name in hand, so a miss is a typo in the code and not a
    condition to handle at runtime.
    """
    try:
        return _BY_NAME[name]
    except KeyError:
        raise KeyError(f"no such metadata field: {name!r}; declared: {FIELD_NAMES}") from None
