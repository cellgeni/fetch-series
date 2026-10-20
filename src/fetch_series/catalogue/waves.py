"""Which wave of the link survey reports each link.

The link survey runs every unit at once, and is reported in six waves, each
its own change to ``link_splits.json`` and the knowledge base, so each result
is read and explained before the next lands. The assignment is declared, in the
order the waves were planned, so a link's wave never depends on when its
survey happened to finish.

1. INSDC samples, and ENA's ``filereport`` between INSDC records;
2. metadata documents;
3. BioProject and ArrayExpress, either way;
4. the ArrayExpress sample chain;
5. read files from a run;
6. everything else.
"""

from __future__ import annotations

from fetch_series.catalogue import CATALOGUE, Catalogue, Link, NodeKind

WAVES = {
    1: "INSDC samples",
    2: "metadata documents",
    3: "BioProject and ArrayExpress",
    4: "the ArrayExpress sample chain",
    5: "read files from a run",
    6: "everything else",
}


def wave_of(link: Link, catalogue: Catalogue = CATALOGUE) -> int:
    source, target = catalogue.nodes[link.source], catalogue.nodes[link.target]
    if "ae_sample" in (link.source, link.target):
        return 4
    if {link.source, link.target} in ({"bioproject", "ae_experiment"}, {"bioproject", "biostudy"}):
        return 3
    if target.kind is NodeKind.METADATA:
        return 2
    if link.source.startswith("run_") and target.kind is NodeKind.READ_FORMAT:
        return 5
    if link.target.startswith("sample_") or link.target == "biosample":
        return 1
    if (
        link.mechanism == "ena_filereport"
        and source.kind is NodeKind.RECORD
        and target.kind is NodeKind.RECORD
        and (source.insdc_object or link.source in ("bioproject", "biosample"))
        and (target.insdc_object or link.target in ("bioproject", "biosample"))
    ):
        return 1
    return 6


def links_through(wave: int, catalogue: Catalogue = CATALOGUE) -> set[str]:
    """Every link id reported in waves 1 to ``wave``."""
    return {link.id for link in catalogue.links if wave_of(link, catalogue) <= wave}


__all__ = ["WAVES", "links_through", "wave_of"]
