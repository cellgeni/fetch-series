"""What a metadata document names: the second half of the request that fetched it.

The catalogue declares each request once. ``geo_series -> geo_sample`` over the
SOFT family file is one link, and so is ``geo_series -> metadata_geo``: the
same request, read for the samples it names and kept as the document it is. Drawn
that way the document is a dead end, though the reprocessing pipeline gets from
a dataset to its samples exactly by reading SDRF, IDF and SOFT family files.

A continuation joins the two: ``metadata_geo -> geo_sample`` *because*
``geo_series -> metadata_geo`` fetched the document that ``geo_series ->
geo_sample`` reads. It is not a new link and makes no request, so it is not in
``Catalogue.links`` and the catalogue's counts do not change. It carries the
record link's own evidence, and it is drawn only where that link is.

Only a reader that parses **the same document** continues it: sra-db-be runinfo
read for its columns, a SOFT family read for its relation lines, an SDRF read
for its comment columns. A document no declared mechanism reads for records --
MINiML, a series matrix, SRA XML, a DDBJ entry -- has no continuation yet.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from fetch_series.catalogue import CATALOGUE, DRAWN, Catalogue, Link, Probe, Status
from fetch_series.catalogue.model import NodeKind

# The mechanism that fetches a document, and every mechanism that reads that
# same document for the records it names.
DOCUMENT_READERS: dict[str, tuple[str, ...]] = {
    "sra_be_runinfo_document": ("sra_be_runinfo",),
    "ena_filereport_document": ("ena_filereport", "ena_filereport_aliases"),
    "ena_browser_xml_document": ("ena_browser_xml",),
    "geo_soft_family": ("geo_soft_family", "geo_soft_family_relations"),
    "geo_acc_series": ("geo_acc_series",),
    "geo_acc_sample": ("geo_acc_sample",),
    "biostudies_idf": ("biostudies_idf",),
    "biostudies_sdrf": ("biostudies_sdrf", "biostudies_sdrf_comments"),
    "biosample_record": ("biosample_record",),
    "biosamples_record": ("biosamples_record",),
}

# Strongest first: a continuation carries the best status among its record links.
_ORDER = (Status.MEASURED, Status.SURVEYED, Status.UNTESTED)


@dataclass(frozen=True, slots=True)
class Continuation:
    """``document -> target``: what one fetched document names, by the link that reads it."""

    document: str
    target: str
    fetched_by: Link
    read_by: Link

    @property
    def id(self) -> str:
        return f"{self.document}->{self.target}:{self.read_by.mechanism}"


def continuations(
    catalogue: Catalogue = CATALOGUE, probes: Mapping[str, Probe] | None = None
) -> list[Continuation]:
    """Every document -> record continuation whose record link is drawn."""
    from fetch_series.catalogue import load_probes

    known = load_probes() if probes is None else probes
    statuses = {link.id: catalogue.status(link, known) for link in catalogue}
    by_mechanism_source: dict[tuple[str, str], list[Link]] = {}
    for link in catalogue:
        by_mechanism_source.setdefault((link.mechanism, link.source), []).append(link)
    found = []
    for fetcher, readers in DOCUMENT_READERS.items():
        for fetched in catalogue:
            if fetched.mechanism != fetcher:
                continue
            if catalogue.nodes[fetched.target].kind is not NodeKind.METADATA:
                continue
            for reader in readers:
                for read in by_mechanism_source.get((reader, fetched.source), []):
                    if catalogue.nodes[read.target].kind is not NodeKind.RECORD:
                        continue
                    if statuses[read.id] not in DRAWN:
                        continue
                    found.append(Continuation(fetched.target, read.target, fetched, read))
    return sorted(found, key=lambda c: (c.document, c.target, c.read_by.id))


def status_of(continuations_: list[Continuation], statuses: Mapping[str, Status]) -> Status:
    """A pair's status: the strongest of the record links it continues."""
    held = {statuses[c.read_by.id] for c in continuations_}
    return next(s for s in _ORDER if s in held)


__all__ = ["DOCUMENT_READERS", "Continuation", "continuations", "status_of"]
