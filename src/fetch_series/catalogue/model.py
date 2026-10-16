"""What the catalogue is made of: nodes, mechanisms, links and probes.

Re-exported from :mod:`fetch_series.catalogue`; kept in its own module only so
that ``nodes`` and ``mechanisms`` can import it without importing the package
that assembles them.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum

from fetch_series.accession import EntityType
from fetch_series.results import Outcome


class Level(StrEnum):
    """How fine-grained a record is, coarsest first.

    The same six rows the route graph draws: a dataset holds samples, a sample
    is sequenced as one or more libraries, a library is run one or more times,
    and a run carries files. ``OTHER`` is for records that describe the data
    rather than being a level of it -- a submission, an analysis, a platform.
    """

    DATASET = "dataset"
    SAMPLE = "sample"
    LIBRARY = "library"
    RUN = "run"
    FILE = "file"
    OTHER = "other"


@dataclass(frozen=True, slots=True)
class Node:
    """One kind of record, in one database.

    Args:
        id: stable identifier. Where a node is exactly one :class:`EntityType`
            it is that type's value, so a route's endpoints map onto it without
            a lookup table.
        database: the archive whose record this is, with INSDC merged: an
            ``SRS``, ``ERS`` and ``DRS`` are one entity under three prefixes,
            and one node.
        family: the colour the graph draws it in. Matches the route graph's
            families, so the two views agree.
        entity: what a parsed accession of this node is, or ``None`` for a
            record with no accession of its own (an ArrayExpress SDRF row). Every
            file node is :attr:`EntityType.FILE`; they differ by which archive's
            record lists the file, which an accession cannot say.
    """

    id: str
    label: str
    level: Level
    database: str
    family: str
    entity: EntityType | None
    note: str = ""


class Basis(StrEnum):
    """What a mechanism's citation is evidence of.

    ``DOCUMENTED``: the archive's own documentation describes the field or
    parameter the link is read from. ``LIVE_ONLY``: it does not, and the link is
    known only because a live response carries it -- the citation is then the
    endpoint's general page and the probe is the whole of the evidence.
    """

    DOCUMENTED = "documented"
    LIVE_ONLY = "live-only"


@dataclass(frozen=True, slots=True)
class Mechanism:
    """One way an archive API answers: a request shape, and what it returns.

    Args:
        name: the short form drawn along a line, e.g. ``filereport``.
        endpoint: the request, with ``{acc}`` for the input accession.
        citation_url: the documentation page naming the field, set exactly
            when ``basis`` is ``DOCUMENTED``. A live-only link cites nothing:
            its probe is the evidence, and a citation to a page that is silent
            about the field would read as though it were not.
        inputs: node ids the request accepts.
        outputs: target node id -> the field the target is read from. Every
            input is linked to every output except itself (see ``self_link``),
            so a response that fans out is declared once.
        self_link: whether the output of the source's own type names a
            *different* record -- a SubSeries, a related experiment -- rather
            than echoing the input back.
        via: nodes a chained mechanism passes through without the caller
            seeing them, e.g. a measured route that reads the BioProject out of
            a SOFT file and then asks ENA about it.
        routes: ``(source, target) -> route id`` for the links a declared
            route measures. A link carries a route id exactly when one does.
        probe_with: overrides for the default probe accession, keyed
            ``"source>target"`` for one link or ``"source"`` for every link
            from that node. Needed where a mechanism only answers for a
            particular record: an alias names an ArrayExpress experiment only on
            an ArrayExpress-brokered run, and a SubSeries only exists under a
            SuperSeries.
    """

    id: str
    name: str
    api: str
    endpoint: str
    citation_url: str | None
    basis: Basis
    inputs: tuple[str, ...]
    outputs: Mapping[str, str]
    self_link: bool = False
    via: tuple[str, ...] = ()
    routes: Mapping[tuple[str, str], str] = field(default_factory=dict)
    probe_with: Mapping[str, str] = field(default_factory=dict)
    note: str = ""

    def pairs(self) -> Iterator[tuple[str, str, str]]:
        """``(source, target, field)`` for every link this mechanism offers."""
        for source in self.inputs:
            for target, read_from in self.outputs.items():
                if target == source and not self.self_link:
                    continue
                yield source, target, read_from

    def probe_accession(self, source: str, target: str, default: str) -> str:
        """The accession that probes ``source -> target`` over this mechanism."""
        return self.probe_with.get(f"{source}>{target}") or self.probe_with.get(source) or default


@dataclass(frozen=True, slots=True)
class Link:
    """A statement that ``source`` reaches ``target`` in one request.

    Not a statement about whether to use it: that is a :class:`Route`, and only
    a route has evidence. ``route_id`` is set exactly when a declared route
    measures this link.
    """

    source: str
    target: str
    mechanism: str
    read_from: str
    route_id: str | None = None

    @property
    def id(self) -> str:
        return f"{self.source}->{self.target}:{self.mechanism}"

    @property
    def measured(self) -> bool:
        return self.route_id is not None


@dataclass(frozen=True, slots=True)
class Probe:
    """One live request made to see whether a link answers at all.

    A probe proves a link exists and measures nothing. One accession says
    nothing about how often the link answers, so a probe never becomes
    :class:`~fetch_series.graph.RouteEvidence` and never affects ranking.

    Args:
        found: the values the declared field held, sorted, at most
            :data:`PROBE_KEEP`; ``found_count`` is the full number.
        outcome: the survey's three outcomes. ``EMPTY`` leaves the link
            documented but not confirmed; ``FAILED`` means no verdict at all,
            which is not the same thing and is kept apart.
    """

    link: str
    accession: str
    checked_on: date
    outcome: Outcome
    found: tuple[str, ...] = ()
    found_count: int = 0
    error: str | None = None

    @property
    def confirmed(self) -> bool:
        return self.outcome is Outcome.RESOLVED


# A probe keeps a sample of what it found, not all of it: a platform lists
# thousands of samples, and the catalogue needs to show what the field holds,
# not to be a copy of the archive.
PROBE_KEEP = 12
