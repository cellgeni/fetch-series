"""10x chemistry named in metadata, in the vocabulary the pipeline already uses.

## Why this exists alongthe empirical inference

`rename10xrun`'s `infer_10x_chemistry.py` decides chemistry from the FASTQ files
themselves — barcode length, UMI length, and which whitelist the barcodes hit.
That is the right way to decide how reads are parsed, and it fails closed, and
nothing here replaces it.

But FASTQ geometry cannot distinguish every chemistry. The incumbent says so
itself, in a warning it emits:

    737K-august-2016 cannot distinguish 3prime v2 from 5prime v1/v2 by FASTQ
    geometry alone

Both use a 16 bp barcode, a 10 bp UMI and the same whitelist. Geometry is
genuinely blind here — and **metadata is not**. A protocol naming a "Chromium
Single Cell 5' Library & Gel Bead Kit" settles it, before anything is
downloaded.

So the two are complementary rather than redundant, and this module deliberately
emits the incumbent's own ``id`` strings so the two answers can be compared
directly rather than translated. Where they disagree, that is a finding about
the submission, not a tie to break.

## What this cannot do

It reads submitter free text. It is silent far more often than it speaks, and it
never guesses: a protocol that says "10x Genomics" and nothing more yields a
recognised assay and **no** chemistry.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from fetch_series.assays import Assay, AssayCall, Match

# The ids and labels are `rename10xrun`'s, verbatim, so a metadata call and an
# empirical one are directly comparable. Two are finer than any whitelist can
# be: 3' v2 and 5' v1/v2 share a whitelist, and metadata can separate them.
CHEMISTRY_LABELS: Mapping[str, str] = {
    "10x_3p_v1": "Chromium Single Cell 3prime v1",
    "10x_3p_v2": "Chromium Single Cell 3prime v2",
    "10x_3p_v3_family": "Chromium 3prime v3/v3.1/LT/HT",
    "10x_3p_v4_gemx": "GEM-X Universal 3prime v4",
    "10x_5p_v1_v2": "Chromium Single Cell 5prime v1/v2",
    "10x_5p_v3_gemx": "GEM-X Universal 5prime v3",
    "10x_multiome_arc_gex": "Chromium Single Cell Multiome Gene Expression",
    "10x_atac_v1_v1.1_v2": "Chromium Single Cell ATAC v1/v1.1/v2",
}

# The whitelist-level classes the empirical route reports, and which of the ids
# above they cover. A metadata call that lands inside one of these is *finer*
# than geometry can be, which is the whole point of having both.
GEOMETRY_AMBIGUOUS: Mapping[str, tuple[str, ...]] = {
    "10x_3p_v2_or_5p_v1_v2": ("10x_3p_v2", "10x_5p_v1_v2"),
}

# Checked in order, most specific first. GEM-X before the plain version numbers
# because a GEM-X protocol also says "3'", and v3.1 before v3 because "v3.1"
# contains "v3" -- reversing either would answer the coarser class for a
# submission that named the finer one.
CHEMISTRY_PHRASES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "10x_multiome_arc_gex",
        ("Multiome", "Single Cell Multiome", "ARC-v1", "arc-v1", "Multiome ATAC + Gene Expression"),
    ),
    ("10x_atac_v1_v1.1_v2", ("Single Cell ATAC", "scATAC-seq", "scATAC", "ATAC-seq")),
    ("10x_3p_v4_gemx", ("GEM-X Universal 3'", "GEM-X 3'", "3' v4", "3prime v4")),
    ("10x_5p_v3_gemx", ("GEM-X Universal 5'", "GEM-X 5'", "5' v3", "5prime v3")),
    (
        "10x_3p_v3_family",
        (
            "3' v3.1",
            "3prime v3.1",
            "3' v3",
            "3prime v3",
            "Single Cell 3' Reagent Kit v3",
            "Single Cell 3' v3",
            "Chromium Next GEM Single Cell 3'",
            "NextGEM 3'",
            "3' LT",
            "3' HT",
        ),
    ),
    (
        "10x_3p_v2",
        ("3' v2", "3prime v2", "Single Cell 3' Reagent Kit v2", "Single Cell 3' v2"),
    ),
    (
        "10x_5p_v1_v2",
        (
            "5' v2",
            "5prime v2",
            "5' v1",
            "5prime v1",
            "Single Cell 5'",
            "Single Cell 5prime",
            "Chromium Next GEM Single Cell 5'",
            "5' Library & Gel Bead Kit",
        ),
    ),
    ("10x_3p_v1", ("3' v1", "3prime v1", "Single Cell 3' v1")),
)


@dataclass(frozen=True, slots=True)
class ChemistryCall:
    """What the metadata said the chemistry is, and on what grounds.

    ``candidates`` holds every chemistry the text names. More than one is the
    normal case for a study that ran two: GSE111360's protocol names both a
    Chromium Single Cell 3' and a 5' kit, because the study used both, and it is
    one protocol field shared by every run. Asserting either for a given run
    would be a coin flip dressed as a measurement.
    """

    candidates: tuple[str, ...] = ()
    matches: tuple[Match, ...] = ()

    @property
    def chemistry(self) -> str | None:
        """The chemistry, only where the metadata names exactly one."""
        return self.candidates[0] if len(self.candidates) == 1 else None

    @property
    def ambiguous(self) -> bool:
        return len(self.candidates) > 1

    @property
    def named(self) -> bool:
        return self.chemistry is not None

    @property
    def label(self) -> str:
        return CHEMISTRY_LABELS.get(self.chemistry or "", "unknown")

    @property
    def splits(self) -> str | None:
        """The whitelist class this call is finer than, if it is finer than one.

        Non-None means the FASTQ files cannot reach this answer and the metadata
        can — which is the case that justifies reading metadata at all.
        """
        for ambiguous, covered in GEOMETRY_AMBIGUOUS.items():
            if self.chemistry in covered:
                return ambiguous
        return None

    def explain(self) -> str:
        if self.ambiguous:
            grounds = ", ".join(str(m) for m in self.matches)
            return (
                f"ambiguous: the metadata names {', '.join(self.candidates)} — "
                f"a study that ran both, described by one protocol field: {grounds}"
            )
        if not self.named:
            return "no chemistry named in any scanned metadata field"
        grounds = ", ".join(str(m) for m in self.matches)
        finer = (
            f" (finer than {self.splits}, which FASTQ geometry cannot split)" if self.splits else ""
        )
        return f"{self.chemistry} — {self.label}{finer}: {grounds}"


# Each chemistry is matched by the same anchored, word-boundary machinery the
# assay layer uses, so `3' v2` cannot fire on `3' v20` and the grounds are
# reported the same way.
_MATCHERS: tuple[tuple[str, Assay], ...] = tuple(
    (identifier, Assay(name=identifier, phrases=phrases))
    for identifier, phrases in CHEMISTRY_PHRASES
)


def chemistry_of(metadata: Mapping[str, str]) -> ChemistryCall:
    """Every chemistry the metadata names, in specific-to-general order.

    All of them, not the first. The protocol field describes a whole study, so a
    study that ran 3' and 5' libraries names both in every run's metadata, and
    picking one would answer a question the text cannot settle.

    The *order* still matters and is specific-first: a protocol naming GEM-X
    also says "3'", and one naming v3.1 also contains "v3". A coarser class must
    never displace a finer one that the submission actually named. But order
    decides presentation; the uniqueness rule decides whether anything is
    asserted at all.
    """
    found: list[str] = []
    grounds: list[Match] = []
    for identifier, matcher in _MATCHERS:
        call: AssayCall = matcher.call(metadata)
        if call.recognised:
            found.append(identifier)
            grounds.extend(call.matches)
    # The 3' v3 family subsumes v3.1/LT/HT, and a GEM-X kit is a 3' kit; those
    # are the same library described at two levels, not two libraries. Only a
    # genuine 3'-and-5' pairing is reported as ambiguous.
    if len(found) > 1:
        found = _drop_subsumed(found)
    return ChemistryCall(candidates=tuple(found), matches=tuple(grounds))


# A GEM-X 3' protocol also matches the plain 3' family, and a Multiome protocol
# also matches ATAC: the more specific call already implies the general one, so
# keeping both would report an ambiguity that is not there.
_SUBSUMES: Mapping[str, tuple[str, ...]] = {
    "10x_3p_v4_gemx": ("10x_3p_v3_family", "10x_3p_v2", "10x_3p_v1"),
    "10x_5p_v3_gemx": ("10x_5p_v1_v2",),
    "10x_multiome_arc_gex": ("10x_atac_v1_v1.1_v2",),
    "10x_3p_v3_family": ("10x_3p_v2", "10x_3p_v1"),
}


def _drop_subsumed(found: list[str]) -> list[str]:
    covered = {c for identifier in found for c in _SUBSUMES.get(identifier, ())}
    return [identifier for identifier in found if identifier not in covered]
