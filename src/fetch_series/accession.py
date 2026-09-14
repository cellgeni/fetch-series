"""Accession parsing: what an identifier is, and which archive owns it.

Every route in this project is a declared edge between two :class:`EntityType`
values, so identifying an accession correctly is the first thing that has to be
right. The rules here are deliberately strict -- a prefix that merely *looks*
plausible is rejected rather than guessed at, because a misclassified accession
sends the resolver down a route that cannot possibly answer.

Worked example, the one used throughout the docs::

    PRJNA988806 -> GSE236084 -> SRP446371 -> SRR25056225-29
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Final


class Archive(StrEnum):
    """The body that issues and owns an accession namespace."""

    NCBI = "ncbi"
    EBI = "ebi"
    DDBJ = "ddbj"
    GEO = "geo"
    ARRAYEXPRESS = "arrayexpress"
    BIOSTUDIES = "biostudies"


class EntityType(StrEnum):
    """What an accession identifies, independent of which archive issued it.

    INSDC entity names are used as the canonical vocabulary because the SRA/ENA/DDBJ
    triples are genuinely the same entities under three prefixes. GEO and
    ArrayExpress get their own members: a GSE is not an SRA study, it is a
    curated record that usually *points at* one, and conflating the two is the
    root of several of the pathologies in this project's knowledge base.
    """

    BIOPROJECT = "bioproject"
    BIOSAMPLE = "biosample"
    STUDY = "study"
    SAMPLE = "sample"
    EXPERIMENT = "experiment"
    RUN = "run"
    SUBMISSION = "submission"
    ANALYSIS = "analysis"
    GEO_SERIES = "geo_series"
    GEO_SAMPLE = "geo_sample"
    GEO_PLATFORM = "geo_platform"
    AE_EXPERIMENT = "ae_experiment"
    BIOSTUDY = "biostudy"


@dataclass(frozen=True, slots=True)
class AccessionRule:
    """One namespace: a pattern, what it identifies, and who issues it."""

    name: str
    pattern: re.Pattern[str]
    entity: EntityType
    archive: Archive
    description: str


def _rule(
    name: str, regex: str, entity: EntityType, archive: Archive, description: str
) -> AccessionRule:
    return AccessionRule(name, re.compile(regex), entity, archive, description)


# Order matters only for readability -- the patterns are mutually exclusive and
# fully anchored, and `parse` asserts that no accession matches twice.
RULES: Final[tuple[AccessionRule, ...]] = (
    # --- GEO -------------------------------------------------------------
    _rule("gse", r"GSE\d+", EntityType.GEO_SERIES, Archive.GEO, "GEO series"),
    _rule("gsm", r"GSM\d+", EntityType.GEO_SAMPLE, Archive.GEO, "GEO sample"),
    _rule("gpl", r"GPL\d+", EntityType.GEO_PLATFORM, Archive.GEO, "GEO platform"),
    # --- BioProject ------------------------------------------------------
    # The third letter records the archive that *registered* the project, which
    # decides whether GEO can possibly hold a record for it. PRJEB/PRJDB
    # projects are EBI/DDBJ-native and never have a GSE.
    _rule(
        "prjna", r"PRJNA\d+", EntityType.BIOPROJECT, Archive.NCBI, "BioProject registered at NCBI"
    ),
    _rule(
        "prjea",
        r"PRJEA\d+",
        EntityType.BIOPROJECT,
        Archive.NCBI,
        "Legacy EBI project mirrored by NCBI",
    ),
    _rule(
        "prjda",
        r"PRJDA\d+",
        EntityType.BIOPROJECT,
        Archive.NCBI,
        "Legacy DDBJ project mirrored by NCBI",
    ),
    _rule("prjeb", r"PRJEB\d+", EntityType.BIOPROJECT, Archive.EBI, "BioProject registered at EBI"),
    _rule(
        "prjdb", r"PRJDB\d+", EntityType.BIOPROJECT, Archive.DDBJ, "BioProject registered at DDBJ"
    ),
    # --- BioSample -------------------------------------------------------
    _rule("samn", r"SAMN\d+", EntityType.BIOSAMPLE, Archive.NCBI, "BioSample registered at NCBI"),
    _rule("samea", r"SAMEA\d+", EntityType.BIOSAMPLE, Archive.EBI, "BioSample registered at EBI"),
    _rule("samd", r"SAMD\d+", EntityType.BIOSAMPLE, Archive.DDBJ, "BioSample registered at DDBJ"),
    # --- INSDC (SRA / ENA / DDBJ share one schema under three prefixes) ---
    _rule(
        "srp",
        r"[SED]RP\d+",
        EntityType.STUDY,
        Archive.NCBI,
        "INSDC study (secondary study accession)",
    ),
    _rule(
        "srs",
        r"[SED]RS\d+",
        EntityType.SAMPLE,
        Archive.NCBI,
        "INSDC sample (secondary sample accession)",
    ),
    _rule("srx", r"[SED]RX\d+", EntityType.EXPERIMENT, Archive.NCBI, "INSDC experiment (library)"),
    _rule(
        "srr",
        r"[SED]RR\d+",
        EntityType.RUN,
        Archive.NCBI,
        "INSDC run (the unit that carries files)",
    ),
    _rule("sra", r"[SED]RA\d+", EntityType.SUBMISSION, Archive.NCBI, "INSDC submission"),
    _rule("srz", r"[SED]RZ\d+", EntityType.ANALYSIS, Archive.NCBI, "INSDC analysis"),
    # --- EBI study collections -------------------------------------------
    # Hyphenated. The unhyphenated `E-MTAB\d+` pattern this project used to
    # carry could never match a real accession.
    _rule(
        "arrayexpress",
        r"E-[A-Z]{4}-\d+",
        EntityType.AE_EXPERIMENT,
        Archive.ARRAYEXPRESS,
        "ArrayExpress experiment (E-MTAB, E-GEOD, E-ENAD, E-PROT, ...)",
    ),
    _rule(
        "biostudies", r"S-[A-Z]+\d+", EntityType.BIOSTUDY, Archive.BIOSTUDIES, "BioStudies study"
    ),
)

_RULES_BY_NAME: Final[dict[str, AccessionRule]] = {rule.name: rule for rule in RULES}

# The archive whose prefix letter an INSDC accession actually carries. The rule
# table records NCBI for all three because they share a schema; this recovers
# the issuing archive from the first letter.
_INSDC_LETTER_ARCHIVE: Final[dict[str, Archive]] = {
    "S": Archive.NCBI,
    "E": Archive.EBI,
    "D": Archive.DDBJ,
}


class UnknownAccessionError(ValueError):
    """Raised when a string does not match any known accession namespace."""

    def __init__(self, raw: str) -> None:
        super().__init__(
            f"Not a recognised accession: {raw!r}. "
            f"Known namespaces: {', '.join(sorted(_RULES_BY_NAME))}."
        )
        self.raw = raw


@dataclass(frozen=True, slots=True)
class Accession:
    """A parsed, normalised accession."""

    value: str
    entity: EntityType
    archive: Archive
    rule: str

    def __str__(self) -> str:
        return self.value

    @property
    def is_insdc(self) -> bool:
        """True for the SRA/ENA/DDBJ run-experiment-sample-study hierarchy."""
        return self.rule in {"srp", "srs", "srx", "srr", "sra", "srz"}

    @property
    def geo_ftp_stem(self) -> str:
        """The masked directory GEO files live under, e.g. ``GSE236nnn``.

        GEO buckets its FTP tree by replacing the last three digits of the
        accession with ``nnn``; accessions with three digits or fewer sit in
        ``GSEnnn``. Needed to build a SOFT family file URL.
        """
        if self.entity not in {
            EntityType.GEO_SERIES,
            EntityType.GEO_SAMPLE,
            EntityType.GEO_PLATFORM,
        }:
            raise ValueError(f"{self.value} is not a GEO accession; it has no FTP stem")
        prefix, digits = self.value[:3], self.value[3:]
        return f"{prefix}{digits[:-3]}nnn" if len(digits) > 3 else f"{prefix}nnn"


def parse(raw: str) -> Accession:
    """Parse a single accession.

    Leading/trailing whitespace is stripped and the accession is upper-cased, so
    ``gse236084`` and ``  GSE236084 `` both parse. Nothing else is coerced.

    Raises:
        UnknownAccessionError: if the string matches no known namespace.
    """
    value = raw.strip().upper()
    matches = [rule for rule in RULES if rule.pattern.fullmatch(value)]
    if not matches:
        raise UnknownAccessionError(raw)
    # Overlapping namespaces would make classification order-dependent, which is
    # exactly the ambiguity this registry exists to remove.
    assert len(matches) == 1, f"{value} matched several namespaces: {matches}"
    rule = matches[0]

    archive = rule.archive
    if rule.name in {"srp", "srs", "srx", "srr", "sra", "srz"}:
        archive = _INSDC_LETTER_ARCHIVE[value[0]]
    return Accession(value=value, entity=rule.entity, archive=archive, rule=rule.name)


def try_parse(raw: str) -> Accession | None:
    """Like :func:`parse`, but returns None instead of raising."""
    try:
        return parse(raw)
    except UnknownAccessionError:
        return None


def parse_all(raws: list[str]) -> tuple[list[Accession], list[str]]:
    """Parse many accessions, returning the successes and the unrecognised ones.

    Batch inputs routinely contain a literal ``-`` placeholder for a missing
    value; the survey table in this repository has one. Callers need those
    separated out rather than raising, but they must never be silently dropped
    -- ``-`` used as a grep pattern matches every line of a metadata table.
    """
    parsed: list[Accession] = []
    unknown: list[str] = []
    for raw in raws:
        accession = try_parse(raw)
        if accession is None:
            unknown.append(raw)
        else:
            parsed.append(accession)
    return parsed, unknown
