"""Assay recognition from metadata, as a pluggable layer over a generic engine.

The engine below the assay layer knows about accessions, routes and files, and
nothing about chemistry. This is where an assay gets to say "that one is mine"
and "here is what kind of library it is" -- and it has to be an interface rather
than a special case, because an interface that only ever fits 10x is not an
interface. Smart-seq is implemented alongside for exactly that reason.

## What this is for

Screening before download. GSE109816 is 880 runs of Smart-seq2, and the
reprocessing pipeline downloaded every one of them in full before rejecting them
at the chemistry step. The metadata that says so is already in hand: the ENA
filereport this project fetches for every run carries
``library_construction_protocol`` and ``experiment_title``, and the difference
between the two assays is plain in both.

## What this is not

Not a replacement for empirical inference from the reads themselves. The
reprocessing pipeline's ``rename10xrun`` infers chemistry from the FASTQ files
and fails closed, which is the correct behaviour for the thing that decides how
reads are parsed. This layer is a cheap prior, computed before anything is
downloaded. Where the two disagree, the reads win and the disagreement is a
data-quality signal worth recording.

## Why the matching is what it is

These are free-text fields written by submitters, so recognition is pattern
matching and cannot be anything else. Two consequences are handled explicitly:

- A match is reported with the phrase that fired and the field it came from, so
  a classification can be argued with rather than only believed.
- Recall is measurable and is measured; the false-positive rate is not, because
  no labelled non-10x corpus exists at the scale to measure it against. That
  asymmetry is stated rather than papered over -- see the knowledge base.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field

# The metadata fields worth scanning, most specific first. The order is the
# order matches are reported in, so the most informative field leads.
SCANNED_FIELDS: tuple[str, ...] = (
    "library_construction_protocol",
    "experiment_title",
    "library_name",
    "library_strategy",
    "library_selection",
    "instrument_model",
    # GEO's SOFT spellings, for callers joining SOFT metadata in. GEO carries
    # assay information ENA's protocol field often does not: GSM5659253 names
    # CellRanger in !Sample_data_processing while its ENA protocol describes
    # only the tissue dissociation.
    "sample_library_construction_protocol",
    "sample_extract_protocol",
    "sample_data_processing",
    "sample_title",
)


@dataclass(frozen=True, slots=True)
class Match:
    """One phrase that fired, and where."""

    phrase: str
    field: str

    def __str__(self) -> str:
        return f"{self.phrase!r} in {self.field}"


@dataclass(frozen=True, slots=True)
class AssayCall:
    """What the assay layer concluded about one run, and on what grounds."""

    assay: str | None
    matches: tuple[Match, ...] = ()
    library_types: tuple[str, ...] = ()
    excluded_by: tuple[Match, ...] = ()
    #: Whether any scanned field held text at all. A run with none is *unknown*,
    #: not known-negative, and conflating the two would discard every submission
    #: that left the protocol field blank.
    had_metadata: bool = True

    @property
    def recognised(self) -> bool:
        return self.assay is not None

    @property
    def library_type(self) -> str | None:
        """The library type, only when the metadata names exactly one.

        ENA's ``library_construction_protocol`` is per-experiment in the schema
        and per-*study* in practice: submitters paste the whole methods section
        into every run. GSE111360's protocol names both a Chromium 5' gene
        expression kit and TCR V(D)J, so every one of its runs matches two
        families. Choosing between them by declaration order would attach a
        confident label to a coin flip, so an ambiguous run has no type and the
        candidates stay visible in ``library_types``.
        """
        return self.library_types[0] if len(self.library_types) == 1 else None

    @property
    def ambiguous_library_type(self) -> bool:
        return len(self.library_types) > 1

    def explain(self) -> str:
        if not self.recognised:
            if self.excluded_by:
                return "no assay recognised; ruled out by " + ", ".join(
                    str(m) for m in self.excluded_by
                )
            if not self.had_metadata:
                return "no assay metadata in any scanned field"
            return "metadata present, but no assay signature in it"
        grounds = ", ".join(str(m) for m in self.matches)
        if self.ambiguous_library_type:
            suffix = f" [ambiguous: {', '.join(self.library_types)}]"
        elif self.library_type:
            suffix = f" [{self.library_type}]"
        else:
            suffix = ""
        return f"{self.assay}{suffix}: {grounds}"


def _compile(phrases: tuple[str, ...]) -> list[tuple[str, re.Pattern[str]]]:
    # Word-boundary anchored so `10x` does not match `110x`, but tolerant of the
    # punctuation submitters put inside these names: 10X, 10x, V(D)J, 5'.
    return [
        (p, re.compile(rf"(?<![A-Za-z0-9]){re.escape(p)}(?![A-Za-z0-9])", re.I)) for p in phrases
    ]


@dataclass(frozen=True, slots=True)
class Assay:
    """One assay's recognisable signature.

    ``excludes`` exists because some phrases are decisive in the negative
    direction: a protocol naming Smart-seq2 is not a 10x run however many times
    it says "single cell".
    """

    name: str
    phrases: tuple[str, ...]
    excludes: tuple[str, ...] = ()
    library_types: Mapping[str, tuple[str, ...]] = field(default_factory=dict)

    def call(self, metadata: Mapping[str, str]) -> AssayCall:
        had_metadata = any((metadata.get(name) or "").strip() for name in SCANNED_FIELDS)
        matches = self._scan(metadata, self.phrases)
        excluded = self._scan(metadata, self.excludes)
        if excluded or not matches:
            return AssayCall(
                assay=None,
                matches=tuple(matches),
                excluded_by=tuple(excluded),
                had_metadata=had_metadata,
            )
        return AssayCall(
            assay=self.name,
            matches=tuple(matches),
            library_types=self._library_types(metadata),
            had_metadata=had_metadata,
        )

    def _scan(self, metadata: Mapping[str, str], phrases: tuple[str, ...]) -> list[Match]:
        found: list[Match] = []
        for name in SCANNED_FIELDS:
            text = (metadata.get(name) or "").strip()
            if not text:
                continue
            for phrase, pattern in _compile(phrases):
                if pattern.search(text):
                    found.append(Match(phrase=phrase, field=name))
        return found

    def _library_types(self, metadata: Mapping[str, str]) -> tuple[str, ...]:
        """Every library-type family the metadata names, in declaration order.

        All of them, not the first: the protocol field usually describes a whole
        study rather than one library, so two families matching means the text
        cannot tell them apart. Declaration order still matters -- it puts the
        specific families ahead of gene expression, which almost every protocol
        mentions -- but it decides presentation, not truth.
        """
        return tuple(
            kind for kind, phrases in self.library_types.items() if self._scan(metadata, phrases)
        )
