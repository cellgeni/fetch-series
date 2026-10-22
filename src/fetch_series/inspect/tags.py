"""Sample-multiplexing tag sets: registered, fetched at runtime, never shipped.

A feature-barcode library whose reads carry a handful of distinct tags is
called ``feature barcode`` by the classifier. Whether those tags are cell
hashing (BioLegend's TotalSeq hashtags) or 10x's CellPlex CMOs can only be said
against the tags themselves, so each set is registered (``tags.tsv``) at a
pinned commit of the source that publishes it, with its sha256, and
``fetch inspect lists --fetch`` downloads it. Without a fetched set a library
stays ``feature barcode``: an unlisted tag is not evidence of anything.

Both sets come from Cell Ranger's own source, which lists the tags with where
they sit in the read: a CMO at the start of the feature read (``5P(BC)``), a
hashtag after ten bases (``5PNNNNNNNNNN(BC)``, TotalSeq-B and -C). TotalSeq-A
puts the same hashtag sequences at the start of the read, so hashtags are
looked for at both.
"""

from __future__ import annotations

import csv
import hashlib
import io
import os
from dataclasses import dataclass
from functools import cache
from pathlib import Path

import httpx

from fetch_series.logging_utils import redact_exception

REGISTER_PATH = Path(__file__).with_name("tags.tsv")
DEFAULT_CACHE = Path(".cache/tags")


@dataclass(frozen=True, slots=True)
class TagSet:
    """One registered set: what it marks (``CMO`` or ``HTO``), and where its tags may sit."""

    id: str
    kind: str
    offsets: tuple[int, ...]
    count: int
    sha256: str
    source: str
    terms: str


def load_register(path: Path = REGISTER_PATH) -> dict[str, TagSet]:
    with path.open(newline="") as handle:
        return {
            row["id"]: TagSet(
                id=row["id"],
                kind=row["kind"],
                offsets=tuple(int(x) for x in row["offsets"].split(",")),
                count=int(row["count"]),
                sha256=row["sha256"],
                source=row["source"],
                terms=row["terms"],
            )
            for row in csv.DictReader(handle, delimiter="\t")
        }


REGISTER = load_register()


def cache_dir() -> Path:
    return Path(os.environ.get("FETCH_SERIES_TAGS", DEFAULT_CACHE))


def parse(text: str, tag_set: TagSet) -> frozenset[str]:
    """The tag sequences of a Cell Ranger feature reference, checked against the register."""
    found = frozenset(row["sequence"].strip().upper() for row in csv.DictReader(io.StringIO(text)))
    if len(found) != tag_set.count:
        raise ValueError(f"{tag_set.id}: {len(found)} tags, the register says {tag_set.count}")
    return found


def fetch(tag_set: TagSet, directory: Path | None = None) -> Path:
    """Download one set once, refusing it unless the sha256 matches the register."""
    directory = directory or cache_dir()
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{tag_set.id}.csv"
    if not path.exists():
        try:
            with httpx.Client(follow_redirects=True, timeout=60.0) as client:
                response = client.get(tag_set.source)
                response.raise_for_status()
        except httpx.HTTPError as exc:
            raise redact_exception(exc) from None
        digest = hashlib.sha256(response.content).hexdigest()
        if digest != tag_set.sha256:
            raise ValueError(f"{tag_set.id}: sha256 {digest}, the register says {tag_set.sha256}")
        path.write_bytes(response.content)
    return path


@cache
def available(directory: str | None = None) -> dict[str, tuple[TagSet, frozenset[str]]]:
    """Every registered set that has been fetched, with its tags, read once per process."""
    found = {}
    for tag_set in REGISTER.values():
        path = (Path(directory) if directory else cache_dir()) / f"{tag_set.id}.csv"
        if path.exists():
            found[tag_set.id] = (tag_set, parse(path.read_text(), tag_set))
    return found


__all__ = ["REGISTER", "TagSet", "available", "cache_dir", "fetch", "load_register", "parse"]
