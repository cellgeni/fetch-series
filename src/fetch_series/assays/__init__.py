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
    # GEO's SOFT spellings, for callers joining SOFT metadata in.
    "sample_library_construction_protocol",
    "sample_extract_protocol",
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
    library_type: str | None = None
    excluded_by: tuple[Match, ...] = ()

    @property
    def recognised(self) -> bool:
        return self.assay is not None

    def explain(self) -> str:
        if not self.recognised:
            if self.excluded_by:
                return "no assay recognised; ruled out by " + ", ".join(
                    str(m) for m in self.excluded_by
                )
            return "no assay recognised in any scanned metadata field"
        grounds = ", ".join(str(m) for m in self.matches)
        suffix = f" [{self.library_type}]" if self.library_type else ""
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
        matches = self._scan(metadata, self.phrases)
        excluded = self._scan(metadata, self.excludes)
        if excluded or not matches:
            return AssayCall(assay=None, matches=tuple(matches), excluded_by=tuple(excluded))
        return AssayCall(
            assay=self.name,
            matches=tuple(matches),
            library_type=self._library_type(metadata),
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

    def _library_type(self, metadata: Mapping[str, str]) -> str | None:
        """Which kind of library, where the assay distinguishes kinds.

        Checked in declaration order, and the first hit wins, because the
        feature-barcode types are the specific ones: a CITE-seq library's
        protocol also says "gene expression" nine times out of ten, and reading
        it as GEX would send an antibody-capture library down the wrong pipeline.
        """
        for kind, phrases in self.library_types.items():
            if self._scan(metadata, phrases):
                return kind
        return None
