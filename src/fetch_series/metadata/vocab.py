"""Submitter spellings, and what they mean.

Two levels, split for a reason worth keeping.

**Keys** -- ``tissue``, ``organism part``, ``source tissue``, ``tissue/organ``
-- map to a canonical field by pure logic, so they live in code. Across 198
datasets there were **116 distinct characteristic keys, of which 28 recur and 70
appear exactly once**, so a synonym table is the whole job: the long tail is
long but it is also flat, and one-offs are best left unmapped and counted.

**Values** live as data because in stage 2 they become the enum lists in the
model's JSON schema, which means a change to them changes what the model is
allowed to say. That belongs in a reviewable diff, not in a runtime string.

## Some keys are deliberately not mapped

``condition`` is the case that matters. In one study it holds a diagnosis and in
the next it holds a treatment arm, and there is nothing in the key to say which.
Mapping it to ``disease`` would label an LPS-treated wild-type mouse as
diseased; dropping it silently would lose the most common key in the corpus. So
it is recorded in :data:`AMBIGUOUS_KEYS` with the reason, reported as unmapped,
and counted -- the same treatment ``-`` gets in :mod:`fetch_series.accession`,
and for the same reason: a corpus that silently discards rows has an unknowable
denominator.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

# GEO writes !Sample_characteristics_ch1 = "tissue: lung"; ArrayExpress writes a
# Characteristics[organism part] column; BioSample writes an attribute named
# "tissue". Strip the wrappers and the channel suffix so the three arrive as one
# spelling. `(years)` goes too -- a unit is not part of a field name.
_WRAPPER = re.compile(r"^\s*(?:characteristics|factor\s*value|comment|unit)\s*\[(.+)\]\s*$", re.I)
_CHANNEL = re.compile(r"[\s_:.-]*ch\d+\s*$", re.I)
_PARENTHETICAL = re.compile(r"\s*\([^)]*\)\s*$")
_PUNCTUATION = re.compile(r"[_\-/.,]+")


def normalise_key(raw: str) -> str:
    """One spelling for an attribute key, whichever archive wrote it."""
    key = raw.strip()
    if wrapped := _WRAPPER.match(key):
        key = wrapped.group(1)
    key = _CHANNEL.sub("", key)
    key = _PARENTHETICAL.sub("", key)
    key = _PUNCTUATION.sub(" ", key)
    return " ".join(key.lower().split())


# Normalised key -> the field it means. Ordered by field for review; the lookup
# is a dict so order does not matter to behaviour.
CANONICAL_KEYS: Mapping[str, str] = {
    # organism
    "organism": "organism",
    "species": "organism",
    "scientific name": "organism",
    "organism name": "organism",
    "host": "organism",
    "host scientific name": "organism",
    # tissue
    "tissue": "tissue",
    "tissue type": "tissue",
    "tissue source": "tissue",
    "tissue origin": "tissue",
    "organism part": "tissue",
    "organ": "tissue",
    "organ part": "tissue",
    "anatomical site": "tissue",
    "anatomical region": "tissue",
    "anatomical location": "tissue",
    "body site": "tissue",
    "biopsy site": "tissue",
    "sampling site": "tissue",
    "sample site": "tissue",
    "site": "tissue",
    "region": "tissue",
    "brain region": "tissue",
    "isolation source": "tissue",
    "source tissue": "tissue",
    "tissue organ": "tissue",
    "organ tissue": "tissue",
    "tissue region": "tissue",
    # cell_type
    "cell type": "cell_type",
    "cell population": "cell_type",
    "cell subset": "cell_type",
    "cell subtype": "cell_type",
    "cell lineage": "cell_type",
    "cell fraction": "cell_type",
    "sorted cell type": "cell_type",
    "sorted population": "cell_type",
    "facs sorted cells": "cell_type",
    "cell description": "cell_type",
    # disease
    "disease": "disease",
    "disease state": "disease",
    "disease status": "disease",
    "diagnosis": "disease",
    "clinical diagnosis": "disease",
    "patient diagnosis": "disease",
    "health state": "disease",
    "health status": "disease",
    "disease diagnosis": "disease",
    "tumor type": "disease",
    "tumour type": "disease",
    "cancer type": "disease",
    # development_stage
    "developmental stage": "development_stage",
    "development stage": "development_stage",
    "dev stage": "development_stage",
    "life stage": "development_stage",
    "age group": "development_stage",
    "embryonic stage": "development_stage",
    # sex
    "sex": "sex",
    "gender": "sex",
    "donor sex": "sex",
    "patient sex": "sex",
    "subject sex": "sex",
    "host sex": "sex",
    # donor_id
    "donor": "donor_id",
    "donor id": "donor_id",
    "individual": "donor_id",
    "individual id": "donor_id",
    "patient": "donor_id",
    "patient id": "donor_id",
    "patient identifier": "donor_id",
    "subject": "donor_id",
    "subject id": "donor_id",
    "participant": "donor_id",
    "participant id": "donor_id",
    "animal": "donor_id",
    "animal id": "donor_id",
    # age
    "age": "age",
    "donor age": "age",
    "patient age": "age",
    "subject age": "age",
    "host age": "age",
    "age at diagnosis": "age",
    "age at collection": "age",
    # timepoint
    "timepoint": "timepoint",
    "time point": "timepoint",
    "time": "timepoint",
    "collection time": "timepoint",
    "sampling time point": "timepoint",
    "days post infection": "timepoint",
    "hours post treatment": "timepoint",
    # assay
    "assay": "assay",
    "assay type": "assay",
    "library type": "assay",
    "single cell platform": "assay",
    "sequencing platform": "assay",
}

# Keys that are common, meaningful, and cannot be mapped without guessing.
# Reported as unmapped with the reason, never dropped and never coerced.
AMBIGUOUS_KEYS: Mapping[str, str] = {
    "condition": (
        "holds a diagnosis in some studies and a treatment arm in others; mapping it to "
        "disease would label an LPS-treated wild-type control as diseased"
    ),
    "treatment": (
        "an intervention, not a disease or a tissue; there is no declared field for it and "
        "inventing one from this key would guess at its vocabulary"
    ),
    "group": "a study's own arm label; its meaning is defined only by that study's design",
    "genotype": (
        "a perturbation in mouse studies and a risk allele in human ones; neither is disease"
    ),
    "phenotype": "describes an observation about the donor, which may or may not be a diagnosis",
    "status": "unqualified; means disease status, enrolment status or QC status depending on study",
    "source name": (
        "a composite label, usually title-like: 'PBMC_donor3_stim_24h' carries tissue, donor, "
        "treatment and timepoint in one string with no declared separator"
    ),
    "sample type": "distinguishes tissue from cell line from organoid, which is not the tissue",
    "batch": "a processing artefact, not a property of the material",
    "cell line": (
        "a cell line is not a tissue and not a cell type; it needs its own field and CLO, "
        "which this field set does not yet declare"
    ),
}


# Keys whose meaning depends on what kind of record they appear in, with the
# field they mean where the record says so.
#
# `isolate` is the case. NCBI's Human BioSample package defines it as "the
# specific individual from which this sample was obtained" -- a donor. The
# Microbe package means a strain isolate by the same word. SAMN12011225 carries
# `isolate = 5864` beside a sample titled `5864_BA9`, which is donor 5864,
# Brodmann area 9: the most valuable structural field in the corpus, sitting
# under a key that cannot be mapped unconditionally.
#
# The archive's own declaration decides it, never the value's shape.
CONTEXT_DEPENDENT_KEYS: Mapping[str, tuple[str, str]] = {
    "isolate": (
        "donor_id",
        "the individual in a human or animal record, a strain isolate in a microbial one",
    ),
    "identifier": ("donor_id", "the donor in a human record, the specimen in a survey record"),
}

# BioSample packages whose subject is one multicellular individual. Prefixes,
# because packages are versioned (`Human.1.0`).
INDIVIDUAL_PACKAGES: tuple[str, ...] = (
    "human",
    "model.organism",
    "invertebrate",
    "plant",
    "vertebrate",
    "beta-lactamase",
)

# Taxa this project declares a field set for, used only where the package does
# not decide: SAMN12039425 is a mouse sample filed under `Generic.1.0`.
INDIVIDUAL_TAXA: frozenset[str] = frozenset({"9606", "10090"})


def resolve_with_context(
    raw_key: str, *, package: str | None = None, taxon: str | None = None
) -> tuple[str | None, str | None]:
    """A context-dependent key's field, or ``None`` and why not.

    Returns ``(field, None)`` when the record says which meaning applies, and
    ``(None, reason)`` when nothing does. Never guesses from the value: a donor
    label and a strain label look alike, and `C57BL/6` under `isolate` would
    read as a plausible donor identifier.
    """
    entry = CONTEXT_DEPENDENT_KEYS.get(normalise_key(raw_key))
    if entry is None:
        return None, None
    name, ambiguity = entry
    declared = (package or "").strip().lower()
    if declared.startswith(INDIVIDUAL_PACKAGES):
        return name, None
    if taxon and taxon.strip() in INDIVIDUAL_TAXA:
        return name, None
    where = f"package {package!r}" if package else "no package declared"
    return None, f"{ambiguity}; {where} and taxon {taxon or 'unknown'} do not say which"


def canonical_field(raw_key: str) -> str | None:
    """The field ``raw_key`` names, or ``None`` if it names nothing declared."""
    return CANONICAL_KEYS.get(normalise_key(raw_key))


def why_unmapped(raw_key: str) -> str | None:
    """Why a key this project sees often is deliberately left unmapped."""
    return AMBIGUOUS_KEYS.get(normalise_key(raw_key))


# Value synonyms, per field. Matching is on the normalised value.
#
# `sex` is safe to map aggressively because the vocabulary is only ever applied
# to a value already found under a sex-mapped key: bare "M" means male there and
# nothing else, where in free text it could be anything.
VALUE_SYNONYMS: Mapping[str, Mapping[str, str]] = {
    "sex": {
        "m": "male",
        "male": "male",
        "males": "male",
        "man": "male",
        "men": "male",
        "boy": "male",
        "xy": "male",
        "f": "female",
        "female": "female",
        "females": "female",
        "woman": "female",
        "women": "female",
        "girl": "female",
        "xx": "female",
        "mixed": "mixed",
        "both": "mixed",
        "male and female": "mixed",
        "pooled": "mixed",
        "unknown": "unknown",
        "not collected": "unknown",
        "not provided": "unknown",
        "not applicable": "unknown",
        "na": "unknown",
        "n a": "unknown",
        "missing": "unknown",
    },
    # CELLxGENE spells a healthy sample's disease as `normal`, so that is the
    # target. Bare "control" is *not* here: in a treatment study it means
    # untreated rather than healthy, which is the same trap `condition` sets.
    "disease": {
        "healthy": "normal",
        "healthy control": "normal",
        "healthy donor": "normal",
        "normal": "normal",
        "non diseased": "normal",
        "no disease": "normal",
        "unaffected": "normal",
        "disease free": "normal",
        "control healthy": "normal",
    },
}

# Underscores only. Commas, semicolons, slashes and "and" are *separators* --
# `split_values` needs them intact, and collapsing them here turned
# "Brain, prefrontal cortex" into one unfindable string instead of a whole
# string plus two parts to try.
_VALUE_PUNCTUATION = re.compile(r"_+")
# `+` is deliberately absent: it is part of a marker name, not a separator.
# Splitting on it turned "CD4+ and CD8+ T cells" into a "CD4" that no ontology
# has a term for, discarding the one piece of information in the string.
_SEPARATORS = re.compile(r"\s*(?:[,;/]|\band\b)\s*")


# Values that are common, meaningful, and cannot be normalised without
# guessing. Reported with the reason, exactly as AMBIGUOUS_KEYS is.
#
# `disease: Control` is the case this exists for, and it is not hypothetical:
# SAMN12011228 -- a sample from a study whose title names Autism Spectrum
# Disorder -- carries `disease = Control`. In that case/control design it means
# healthy. In a study where every donor is diseased and some are untreated, the
# same string under the same key means "diseased, untreated". Mapping it to
# `normal` would be right often enough to be dangerous.
AMBIGUOUS_VALUES: Mapping[str, Mapping[str, str]] = {
    "disease": {
        "control": (
            "means healthy in a case/control design and untreated in a treatment design; "
            "SAMN12011228 carries disease=Control in an autism study, where it means healthy, "
            "but nothing in the value says so"
        ),
        "ctrl": "an abbreviation of the same ambiguity",
        "normal control": "as above; the 'normal' is doing work the 'control' may not be",
        "na": "not stated, not-applicable and not-available are all written this way",
        "unknown": "an explicit absence, which is not the same claim as healthy",
        "none": "reads as 'no disease' and as 'no value recorded'",
    },
}


def why_value_unmapped(field: str, raw_value: str) -> str | None:
    """Why a value this project sees often is deliberately left unnormalised."""
    value = " ".join(_VALUE_PUNCTUATION.sub(" ", raw_value.strip()).split()).lower()
    return AMBIGUOUS_VALUES.get(field, {}).get(value)


def normalise_value(field: str, raw_value: str) -> str:
    """The canonical spelling of ``raw_value`` for ``field``.

    Returns the cleaned original when no synonym applies. Cleaning is
    conservative -- case, punctuation and whitespace only -- because the cleaned
    string is what gets looked up in an ontology, and an over-eager rewrite
    turns a findable label into an unfindable one.
    """
    value = _VALUE_PUNCTUATION.sub(" ", raw_value.strip())
    value = " ".join(value.split())
    if not value:
        return ""
    return VALUE_SYNONYMS.get(field, {}).get(value.lower(), value)


def split_values(value: str) -> tuple[str, ...]:
    """The components of a value that may name several things, longest first.

    A submitter writing ``lung, lymph node`` means two tissues; one writing
    ``Brain, prefrontal cortex`` means one, named twice at two levels. Nothing
    in the string distinguishes them, so both are offered to the ontology and
    the caller keeps what grounds -- with the whole string tried first, because
    where it grounds it is the submitter's own intent and needs no splitting.
    """
    parts = [part.strip() for part in _SEPARATORS.split(value) if part.strip()]
    if len(parts) < 2:
        return ()
    # Longest first: the specific member of a hierarchical pair is usually the
    # longer string ("prefrontal cortex" over "Brain"), and a caller taking the
    # first thing that grounds should get the specific one.
    return tuple(dict.fromkeys(sorted(parts, key=len, reverse=True)))


def split_characteristic(line: str) -> tuple[str, str] | None:
    """Split GEO's ``key: value`` characteristics line.

    On the **first** colon only. ``age: 60-65`` is fine either way, but
    ``diagnosis: stage II, ER+: yes`` is not, and a value containing a colon is
    far more common than a key containing one.
    """
    key, _, value = line.partition(":")
    if not value.strip() or not key.strip():
        return None
    return key.strip(), value.strip()
