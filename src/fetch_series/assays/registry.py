"""The assays this project recognises.

Two, deliberately. 10x is the one the reprocessing pipeline exists for;
Smart-seq is here because an interface proven against a single implementation is
not proven at all, and because Smart-seq is the assay that actually turned up
uninvited -- GSE109816's 880 runs were downloaded in full before anything
noticed they were not 10x.
"""

from __future__ import annotations

from collections.abc import Mapping

from fetch_series.assays import Assay, AssayCall

# Feature-barcode types before gene expression. A CITE-seq protocol almost
# always also says "gene expression", and reading it as GEX sends an
# antibody-capture library down a pipeline that will produce nonsense from it.
TENX_LIBRARY_TYPES: dict[str, tuple[str, ...]] = {
    "ATAC": ("ATAC", "scATAC", "Single Cell ATAC", "Multiome"),
    "VDJ": ("VDJ", "V(D)J", "TCR", "BCR", "immune profiling", "immune repertoire"),
    "CRISPR": ("CRISPR", "guide capture", "Perturb-seq", "CROP-seq", "sgRNA"),
    "HTO": ("cell hashing", "hashing", "HTO", "MULTI-seq", "cell multiplexing"),
    "ADT": (
        "CITE-seq",
        "CITEseq",
        "TotalSeq",
        "antibody capture",
        "antibody-derived",
        "ADT",
        "Feature Barcode",
        "feature barcoding",
    ),
    "GEX": (
        "gene expression",
        "3' v2",
        "3' v3",
        "5' v1",
        "5' v2",
        "scRNA-seq",
        "single cell RNA",
        "snRNA-seq",
    ),
}

TENX = Assay(
    name="10x",
    # Matching is case-insensitive, so one spelling per phrase. Listing "10X"
    # beside "10x" only makes one match report as two.
    phrases=(
        "10x",
        "10x Genomics",
        "Chromium",
        "CellRanger",
        "Cell Ranger",
        "GemCode",
        "Single Cell 3'",
        "Single Cell 5'",
        "Next GEM",
    ),
    # Nothing excludes 10x outright. A protocol can legitimately mention
    # Smart-seq while describing a 10x run -- comparing the two is a common
    # study design -- so the exclusion list is empty on purpose rather than by
    # omission, and ambiguity is reported as two calls rather than resolved here.
    excludes=(),
    library_types=TENX_LIBRARY_TYPES,
)

SMARTSEQ = Assay(
    name="smart-seq",
    phrases=(
        "Smart-seq",
        "Smartseq",
        "Smart-seq2",
        "SMARTer",
        "SMARTScribe",
        "SMART-Seq v4",
    ),
    excludes=(),
    # Smart-seq is one library per cell: there is no feature-barcode family to
    # distinguish, so the interface's library_types is legitimately empty. That
    # is the point of having a second implementation -- it exercises the parts
    # of the interface 10x makes look mandatory.
    library_types={},
)

ASSAYS: tuple[Assay, ...] = (TENX, SMARTSEQ)


def call_all(metadata: Mapping[str, str]) -> list[AssayCall]:
    """Every assay that recognises this run, most specific evidence first.

    More than one call is a real answer, not an error. A study comparing 10x
    against Smart-seq2 names both in one protocol, and a caller screening for
    10x should see that the run is ambiguous rather than be handed a winner
    chosen by a tie-break it cannot inspect.
    """
    calls = [assay.call(metadata) for assay in ASSAYS]
    recognised = [c for c in calls if c.recognised]
    return sorted(recognised, key=lambda c: (-len(c.matches), c.assay or ""))


def is_10x(metadata: Mapping[str, str]) -> AssayCall:
    """The screening question the reprocessing pipeline actually asks."""
    return TENX.call(metadata)
