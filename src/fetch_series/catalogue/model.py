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

from fetch_series.accession import Archive, EntityType
from fetch_series.results import Outcome

# The three INSDC archives. Each issues its own study, sample, experiment, run,
# submission and analysis accessions under its own prefix letter, and the
# routes that answer for one are not always the routes that answer for another.
INSDC: tuple[Archive, ...] = (Archive.NCBI, Archive.EBI, Archive.DDBJ)

# Which INSDC archives a curated archive's records point at. GEO brokers its
# submissions to NCBI's SRA alone; ArrayExpress brokers to ENA and imports
# GEO's series, so points at EBI's and NCBI's. The recorded verdicts agree and
# never once disagree: gse->experiment:soft_bioproject_ena reached no ERX or
# DRX from 13,045 series, run->geo_sample:ena_alias no GSM from 263 EBI and
# DDBJ runs, ae_experiment->study:idf_secondary no DRP from 20,693
# experiments, and study->ae_experiment:biostudies_search no ArrayExpress
# experiment from 38 DRPs. A link the archive cannot hold is not declared.
POINTS_AT: dict[Archive, tuple[Archive, ...]] = {
    Archive.GEO: (Archive.NCBI,),
    Archive.ARRAYEXPRESS: (Archive.NCBI, Archive.EBI),
}


class Level(StrEnum):
    """Which row of the graph a node sits on, top to bottom.

    The first five are records: a dataset holds samples, a sample is sequenced
    as one or more libraries, a library is run one or more times, and ``OTHER``
    is a record *about* the data -- a submission, an analysis, a platform. The
    last three are what a record leads to rather than records themselves: read
    files by format, supplementary files, and metadata documents.
    """

    DATASET = "dataset"
    SAMPLE = "sample"
    LIBRARY = "library"
    RUN = "run"
    OTHER = "other"
    FILE = "file"
    SUPPLEMENTARY = "supplementary"
    METADATA = "metadata"


class NodeKind(StrEnum):
    """What a node is, which decides what confirms a link into it.

    A link into a record is confirmed by finding an accession of it. A link
    into a read format is confirmed only by a *valid* file: downloadable,
    verifiable and complete for 10x. A supplementary file needs to download and
    be size-checkable, and a metadata document to download and parse.
    """

    RECORD = "record"
    READ_FORMAT = "read-format"
    SUPPLEMENTARY = "supplementary"
    METADATA = "metadata"


_KIND_OF_LEVEL = {
    Level.FILE: NodeKind.READ_FORMAT,
    Level.SUPPLEMENTARY: NodeKind.SUPPLEMENTARY,
    Level.METADATA: NodeKind.METADATA,
}


@dataclass(frozen=True, slots=True)
class Node:
    """One kind of record, file or document, in one archive.

    Args:
        id: stable identifier. A node that is all of one :class:`EntityType`
            is named after it; an INSDC object split by archive is named
            ``<entity>_<archive>`` (``run_ebi``), so a parsed accession lands on
            its node from its entity type and its prefix letter alone.
        database: the archive whose record this is, as a reader would name it.
        family: the colour the graph draws it in: its archive, or ``shared``
            for the BioProject and BioSample records all three INSDC archives
            hold, or ``file`` for a read format no archive owns.
        entity: what a parsed accession of this node is, or ``None`` for a
            record with no accession of its own (an ArrayExpress SDRF row), a
            supplementary file or a metadata document. Every read format is
            :attr:`EntityType.FILE`: they differ by format, which an accession
            cannot say.
        archive: the INSDC archive that issued the record, for the objects
            split by archive; GEO or ArrayExpress for theirs; ``None`` for a
            BioProject, a BioSample or a read format, which are one node across
            archives.
        prefix: how an identifier of this node is spelt, shown under its label.
    """

    id: str
    label: str
    level: Level
    database: str
    family: str
    entity: EntityType | None
    archive: Archive | None = None
    prefix: str = ""
    note: str = ""

    @property
    def kind(self) -> NodeKind:
        return _KIND_OF_LEVEL.get(self.level, NodeKind.RECORD)

    @property
    def insdc_object(self) -> bool:
        """An INSDC record issued by one archive: an SRR, an ERS, a DRX."""
        return self.kind is NodeKind.RECORD and self.archive in INSDC


class Basis(StrEnum):
    """What a mechanism's citation is evidence of.

    ``DOCUMENTED``: the archive's own documentation describes the field or
    parameter the link is read from. ``LIVE_ONLY``: it does not, and the link is
    known only because a live response carries it -- the citation is then the
    endpoint's general page and the probe is the whole of the evidence.
    """

    DOCUMENTED = "documented"
    LIVE_ONLY = "live-only"


class Origin(StrEnum):
    """Who made a read file a mechanism lists, drawn on the line rather than the node.

    ``SUBMITTED``: the files the submitter deposited, as deposited.
    ``GENERATED``: files the archive derived from them -- ENA's fastqs.
    ``ARCHIVE_FORMAT``: the archive's own container, an SRA object, which must
    be dumped before it is a read file at all. ``MIXED``: the field names
    whichever copy a curator chose, and which one it is varies by study.

    The same format can arrive by any of the three, and they are not the same
    offer: a submitted 10x BAM holds every read, and a fastq dumped from an SRA
    object may not hold the barcode read.
    """

    SUBMITTED = "submitted"
    GENERATED = "archive-generated"
    ARCHIVE_FORMAT = "archive format"
    MIXED = "mixed"


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
            input is linked to every output except itself (see ``self_link``)
            and except an INSDC object of another archive (see :meth:`pairs`),
            so a response that fans out is declared once.
        self_link: whether the output of the source's own type names a
            *different* record -- a SubSeries, a related experiment -- rather
            than echoing the input back.
        via: nodes a chained mechanism passes through without the caller
            seeing them, e.g. a measured route that reads the BioProject out of
            a SOFT file and then asks ENA about it.
        routes: ``(source, target) -> route id`` for the links a declared
            route measures, each side a node id or an entity type's value. An
            entity type stands for every node of it the mechanism links, so
            ``("bioproject", "run")`` is BioProject to SRR, ERR and DRR; ``file``
            is every read format. Which of those pairs the route's survey
            actually measured is the split's business, not the declaration's.
        probe_with: overrides for the default probe accession, keyed
            ``"source>target"`` for one link or ``"source"`` for every link
            from that node. Needed where a mechanism only answers for a
            particular record: an alias names an ArrayExpress experiment only on
            an ArrayExpress-brokered run, and a SubSeries only exists under a
            SuperSeries.
        origin: for a mechanism that lists read files, who made them; by target
            where one answer holds both an archive's object and the
            submitter's files, as SDL's does.
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
    origin: Origin | Mapping[str, Origin] | None = None
    note: str = ""

    def origin_of(self, target: str) -> Origin | None:
        """Who made the files this mechanism lists on ``target``."""
        if isinstance(self.origin, Mapping):
            return self.origin.get(target)
        return self.origin if target in self.outputs else None

    def pairs(self, nodes: Mapping[str, Node] | None = None) -> Iterator[tuple[str, str, str]]:
        """``(source, target, field)`` for every link this mechanism offers.

        Given the nodes, an INSDC object is never linked to another archive's:
        one submission's study, samples, experiments and runs are all issued by
        the archive it was submitted to. That is not assumed lightly. Every
        recorded verdict in ``data/surveys/`` was scanned for a pair that
        crosses archives and none does, and a slow test repeats the scan, so
        the first one that appears fails the suite rather than going undrawn.
        Nor is a GEO or ArrayExpress record linked to an archive's objects it
        never points at (:data:`POINTS_AT`).
        """
        for source in self.inputs:
            for target, read_from in self.outputs.items():
                if target == source and not self.self_link:
                    continue
                if nodes is not None and not _may_link(nodes.get(source), nodes.get(target)):
                    continue
                yield source, target, read_from

    def probe_accession(self, source: str, target: str, default: str) -> str:
        """The accession that probes ``source -> target`` over this mechanism."""
        return self.probe_with.get(f"{source}>{target}") or self.probe_with.get(source) or default


def _may_link(a: Node | None, b: Node | None) -> bool:
    """Whether two records can be linked at all, by who issued them."""
    if a is None or b is None or a.kind is not NodeKind.RECORD or b.kind is not NodeKind.RECORD:
        return True
    if a.insdc_object and b.insdc_object:
        return a.archive == b.archive
    for curated, other in ((a, b), (b, a)):
        if curated.archive in POINTS_AT and other.insdc_object:
            return other.archive in POINTS_AT[curated.archive]
    return True


@dataclass(frozen=True, slots=True)
class Link:
    """A statement that ``source`` reaches ``target`` in one request.

    Not a statement about whether to use it: that is a :class:`Route`, and only
    a route has evidence. ``route_id`` is set where a declared route covers this
    link; whether its survey actually queried any accession of ``source`` is the
    route split's to say (:meth:`Catalogue.split`).
    """

    source: str
    target: str
    mechanism: str
    read_from: str
    route_id: str | None = None

    @property
    def id(self) -> str:
        return f"{self.source}->{self.target}:{self.mechanism}"


# What a check on a found file or document said. ``None`` is "could not be
# determined", which is not a pass: a file whose completeness is unknown is not
# known to be valid.
Check = tuple[str, bool | None]


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
        checks: for a link into a file or a document, each check made on what
            was found, in order. A read file is checked for being downloadable,
            verifiable and complete for 10x; a supplementary file for
            downloading and being size-checkable; a metadata document for
            downloading and parsing.
        checked: what the checks were made on -- a run's file set, a document.
    """

    link: str
    accession: str
    checked_on: date
    outcome: Outcome
    found: tuple[str, ...] = ()
    found_count: int = 0
    error: str | None = None
    checks: tuple[Check, ...] = ()
    checked: str | None = None

    @property
    def confirmed(self) -> bool:
        """Found something, and everything found passed every check made on it."""
        return self.outcome is Outcome.RESOLVED and all(passed is True for _, passed in self.checks)

    @property
    def failed_checks(self) -> list[str]:
        """The checks that did not pass, an undetermined one included."""
        return [name for name, passed in self.checks if passed is not True]


# A probe keeps a sample of what it found, not all of it: a platform lists
# thousands of samples, and the catalogue needs to show what the field holds,
# not to be a copy of the archive.
PROBE_KEEP = 12
