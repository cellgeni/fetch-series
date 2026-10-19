"""10x library layouts, as table rows: what each kit puts where in which read.

Drawn from ``rename10xrun``'s chemistry table (cellgeni/nf-reprocessing-public-10x,
GPL-3.0, at a844b5f) and cross-checked against ``tchem``'s 10x layouts
(Tussl-Co/TusslChemistry v0.1.0, Apache-2.0). Both are cited for method; the rows
are facts about the kits, restated here, not code taken from either.

A layout is identified by its barcode list, and then -- where two layouts share a
list -- by what follows the UMI in the barcode read and by which strand the cDNA
read is on. 3' v2 and 5' v1/v2 share the v2 list, a 16-base barcode and a 10-base
UMI: only the tail (poly-T against the template-switch oligo, visible only if the
barcode read was sequenced past base 26) and the strand separate them.

Flex is absent on purpose: its list ships only with Cell Ranger, and a layout
whose list cannot be fetched cannot be told from any other.

The ids are ``rename10xrun``'s -- the metadata layer's vocabulary -- so a call
made from reads and one made from a protocol compare without translation.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Tail(StrEnum):
    POLY_T = "poly-T"
    TSO = "TSO"
    NONE = "none"


class Strand(StrEnum):
    """Which strand of the transcript the cDNA read lies on."""

    SENSE = "sense"
    ANTISENSE = "antisense"
    UNSTRANDED = "unstranded"


# The 3' end of 10x's template-switch oligo as it appears in a 5' library's
# barcode read, right after the UMI.
TSO_PREFIX = "TTTCTTATATGGG"


@dataclass(frozen=True, slots=True)
class Layout:
    """One kit's read structure.

    Args:
        id: the metadata layer's id (``rename10xrun``'s).
        barcode_list: the register id of the list its cell barcodes come from.
        cb: cell-barcode length; ``umi``: UMI length (0 where there is none).
        offsets: where in the barcode read the cell barcode may start.
        tail: what follows the UMI in the barcode read.
        strand: which strand the cDNA read lies on.
        modality: ``gex`` or ``atac``.
        starsolo: whether STARsolo can count it -- ATAC it cannot.
    """

    id: str
    label: str
    barcode_list: str
    cb: int
    umi: int
    offsets: tuple[int, ...]
    tail: Tail
    strand: Strand
    modality: str
    starsolo: bool = True

    @property
    def barcode_read(self) -> int:
        """The shortest read that holds the cell barcode and the UMI."""
        return self.cb + self.umi


LAYOUTS: tuple[Layout, ...] = (
    Layout(
        "10x_3p_v1",
        "Chromium Single Cell 3' v1",
        "tenx_v1",
        14,
        10,
        (0,),
        Tail.NONE,
        Strand.SENSE,
        "gex",
    ),
    Layout(
        "10x_3p_v2",
        "Chromium Single Cell 3' v2",
        "tenx_v2",
        16,
        10,
        (0,),
        Tail.POLY_T,
        Strand.SENSE,
        "gex",
    ),
    Layout(
        "10x_5p_v1_v2",
        "Chromium Single Cell 5' v1/v2",
        "tenx_v2",
        16,
        10,
        (0,),
        Tail.TSO,
        Strand.ANTISENSE,
        "gex",
    ),
    Layout(
        "10x_3p_v3_family",
        "Chromium 3' v3/v3.1/LT/HT",
        "tenx_v3",
        16,
        12,
        (0,),
        Tail.POLY_T,
        Strand.SENSE,
        "gex",
    ),
    Layout(
        "10x_3p_v4_gemx",
        "GEM-X Universal 3' v4",
        "tenx_v4_3p",
        16,
        12,
        (0,),
        Tail.POLY_T,
        Strand.SENSE,
        "gex",
    ),
    Layout(
        "10x_5p_v3_gemx",
        "GEM-X Universal 5' v3",
        "tenx_v3_5p",
        16,
        12,
        (0,),
        Tail.TSO,
        Strand.ANTISENSE,
        "gex",
    ),
    Layout(
        "10x_multiome_arc_gex",
        "Chromium Single Cell Multiome GEX",
        "tenx_arc_gex",
        16,
        12,
        (0,),
        Tail.POLY_T,
        Strand.SENSE,
        "gex",
    ),
    # The ATAC barcode is read as the i5 index: 16 bases, or 24 with an 8-base
    # spacer first on Multiome. It carries no UMI and STARsolo cannot count it.
    Layout(
        "10x_multiome_arc_atac",
        "Chromium Single Cell Multiome ATAC",
        "tenx_arc_atac",
        16,
        0,
        (0, 8),
        Tail.NONE,
        Strand.UNSTRANDED,
        "atac",
        starsolo=False,
    ),
    Layout(
        "10x_atac_v1_v1.1_v2",
        "Chromium Single Cell ATAC v1/v1.1/v2",
        "tenx_atac",
        16,
        0,
        (0, 8),
        Tail.NONE,
        Strand.UNSTRANDED,
        "atac",
        starsolo=False,
    ),
)

BY_ID = {layout.id: layout for layout in LAYOUTS}
LISTS = sorted({layout.barcode_list for layout in LAYOUTS})


def sharing(barcode_list: str) -> tuple[Layout, ...]:
    """Every layout that draws its barcodes from one list."""
    return tuple(layout for layout in LAYOUTS if layout.barcode_list == barcode_list)


def list_class(layout_id: str) -> str | None:
    """The list a chemistry id draws from: the finest thing barcodes alone can say."""
    layout = BY_ID.get(layout_id)
    return layout.barcode_list if layout else None


__all__ = [
    "BY_ID",
    "LAYOUTS",
    "LISTS",
    "TSO_PREFIX",
    "Layout",
    "Strand",
    "Tail",
    "list_class",
    "sharing",
]
