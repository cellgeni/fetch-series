"""Barcode lists: registered, fetched at runtime, never shipped.

10x Genomics' lists are distributed under 10x's terms, so this repository
carries only a **register** (``lists.tsv``): each list's source at a pinned
commit, its sha256, its barcode count and length, and the terms it comes under.
``fetch inspect lists --fetch`` downloads them into a local cache and refuses
any whose bytes do not match the register.

A list is held as a sorted array of 2-bit-packed barcodes and searched with
``bisect``: the 6.8 million barcodes of 10x's v3 list take 27 MB that way,
against several hundred as a Python set of strings.
"""

from __future__ import annotations

import array
import bisect
import csv
import gzip
import hashlib
import itertools
import json
import os
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from functools import cache
from pathlib import Path

import httpx

from fetch_series.logging_utils import redact_exception

REGISTER_PATH = Path(__file__).with_name("lists.tsv")
DEFAULT_CACHE = Path(".cache/lists")
_BASES = "ACGT"
_ACGT = frozenset(_BASES)
_DIGITS = str.maketrans(_BASES, "0123")
# 4 bytes per barcode: a 16-base barcode is 32 bits.
_TYPECODE = "I"


@dataclass(frozen=True, slots=True)
class Entry:
    """One registered list.

    ``sha256`` is the download's; ``export_sha256`` is that of the text
    :func:`export` writes -- the barcodes sorted, one a line -- so an exported
    whitelist can be checked against the register too.
    """

    id: str
    length: int
    count: int
    sha256: str
    source: str
    terms: str
    export_sha256: str = ""

    @property
    def file_name(self) -> str:
        """The name 10x gives the list, as its source names it: ``3M-february-2018.txt``."""
        return self.source.rsplit("/", 1)[-1].removesuffix(".gz")


def load_register(path: Path = REGISTER_PATH) -> dict[str, Entry]:
    with path.open(newline="") as handle:
        return {
            row["id"]: Entry(
                id=row["id"],
                length=int(row["length"]),
                count=int(row["count"]),
                sha256=row["sha256"],
                source=row["source"],
                terms=row["terms"],
                export_sha256=row.get("export_sha256") or "",
            )
            for row in csv.DictReader(handle, delimiter="\t")
        }


REGISTER = load_register()


def cache_dir() -> Path:
    return Path(os.environ.get("FETCH_SERIES_LISTS", DEFAULT_CACHE))


def exported(entry: Entry, out: Path | None = None) -> Path:
    """The list as a text whitelist, exported once into ``out`` (default: the cache's ``export/``)."""
    out = out or cache_dir() / "export"
    target = out / entry.file_name
    return target if target.exists() else export(entry, out)[0]


def encode(seq: str) -> int | None:
    """A barcode as an integer, two bits a base; ``None`` if it holds anything but ACGT.

    Read as a base-4 numeral, in C. A loop over the bases in Python was most of
    a content survey's CPU -- every barcode window of 100,000 reads is encoded
    against every list -- and one asyncio process doing that starved its own
    downloads into read errors.
    """
    if not _ACGT.issuperset(seq):
        return None
    return int(seq.translate(_DIGITS), 4) if seq else 0


class ListMismatch(ValueError):
    """A fetched list is not the one the register names."""


def pack(lines: Iterable[str], entry: Entry) -> array.array[int]:
    """Barcodes, sorted and packed, checked against the register's count and length."""
    codes: list[int] = []
    for line in lines:
        barcode = line.strip()
        if not barcode:
            continue
        if len(barcode) != entry.length:
            raise ListMismatch(
                f"{entry.id}: a {len(barcode)}-base barcode in a {entry.length}-base list"
            )
        code = encode(barcode)
        if code is None:
            raise ListMismatch(f"{entry.id}: {barcode!r} is not ACGT")
        codes.append(code)
    if len(codes) != entry.count:
        raise ListMismatch(
            f"{entry.id}: {len(codes):,} barcodes, the register says {entry.count:,}"
        )
    packed = array.array(_TYPECODE, sorted(codes))
    assert packed.itemsize == 4, "an unsigned int must be four bytes"
    return packed


def fetch(
    entry: Entry, directory: Path | None = None, *, client: httpx.Client | None = None
) -> Path:
    """Download one list, check its sha256 against the register, and cache it packed."""
    directory = directory or cache_dir()
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{entry.id}.u32"
    if target.exists():
        return target
    owned = client is None
    http = client or httpx.Client(follow_redirects=True, timeout=120.0)
    try:
        response = http.get(entry.source)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise redact_exception(exc) from None
    finally:
        if owned:
            http.close()
    digest = hashlib.sha256(response.content).hexdigest()
    if digest != entry.sha256:
        raise ListMismatch(f"{entry.id}: sha256 {digest}, the register says {entry.sha256}")
    text = gzip.decompress(response.content).decode("ascii")
    packed = pack(text.splitlines(), entry)
    partial = target.with_suffix(".partial")
    with partial.open("wb") as handle:
        packed.tofile(handle)
    partial.rename(target)
    target.with_suffix(".json").write_text(
        json.dumps({"id": entry.id, "sha256": entry.sha256, "count": entry.count}) + "\n"
    )
    return target


class BarcodeList:
    """A packed, sorted list, and the lookups a classifier makes against it."""

    def __init__(self, entry: Entry, codes: array.array[int]) -> None:
        self.entry = entry
        self.length = entry.length
        self._codes = codes

    def __len__(self) -> int:
        return len(self._codes)

    def __contains__(self, code: int) -> bool:
        i = bisect.bisect_left(self._codes, code)
        return i < len(self._codes) and self._codes[i] == code

    def exact(self, barcode: str) -> bool:
        code = encode(barcode)
        return code is not None and code in self

    def near(self, barcode: str) -> bool:
        """Listed, or one substitution from a listed barcode; an ``N`` is a wildcard.

        Sequencers write N for a base they could not call, and a single
        miscalled base is the common error. Two or more Ns are no barcode at all.
        """
        n_count = barcode.count("N")
        if n_count > 1:
            return False
        if n_count == 1:
            at = barcode.index("N")
            return any(self.exact(barcode[:at] + base + barcode[at + 1 :]) for base in _BASES)
        if self.exact(barcode):
            return True
        for at, original in enumerate(barcode):
            for base in _BASES:
                if base != original and self.exact(barcode[:at] + base + barcode[at + 1 :]):
                    return True
        return False

    def barcodes(self) -> Iterator[str]:
        """Every listed barcode, in sorted order: what a pipeline reads as a whitelist."""
        return unpack(self._codes, self.length)

    @property
    def background(self) -> float:
        """The chance a random barcode-length string is listed exactly."""
        return float(len(self._codes)) / float(4**self.length)


def unpack(codes: Iterable[int], length: int) -> Iterator[str]:
    """Packed barcodes back to bases: :func:`encode` reversed, by table, eight bases at a time."""
    low = min(length, 8)
    low_bases = ["".join(p) for p in itertools.product(_BASES, repeat=low)]
    high_bases = ["".join(p) for p in itertools.product(_BASES, repeat=length - low)]
    shift, mask = 2 * low, (1 << 2 * low) - 1
    for code in codes:
        yield high_bases[code >> shift] + low_bases[code & mask]


def export(entry: Entry, out: Path, directory: Path | None = None) -> tuple[Path, str]:
    """Write a fetched list as the text STARsolo reads (``--soloCBwhitelist``).

    One barcode a line, sorted -- not in 10x's order, which no aligner reads --
    from the cache that was checked against the register when it was fetched.
    The text is checked against the register's ``export_sha256`` before it
    takes its name. Returns the path and that sha256.
    """
    barcodes = load(entry, directory)
    out.mkdir(parents=True, exist_ok=True)
    target = out / entry.file_name
    partial = target.with_suffix(".partial")
    digest = hashlib.sha256()
    with partial.open("w") as handle:
        for barcode in barcodes.barcodes():
            line = barcode + "\n"
            handle.write(line)
            digest.update(line.encode("ascii"))
    written = digest.hexdigest()
    if entry.export_sha256 and written != entry.export_sha256:
        partial.unlink()
        raise ListMismatch(
            f"{entry.id}: exported sha256 {written}, the register says {entry.export_sha256}"
        )
    partial.rename(target)
    return target, written


def load(entry: Entry, directory: Path | None = None) -> BarcodeList:
    path = (directory or cache_dir()) / f"{entry.id}.u32"
    if not path.exists():
        raise FileNotFoundError(
            f"barcode list {entry.id} is not fetched; run `fetch inspect lists --fetch`"
        )
    codes: array.array[int] = array.array(_TYPECODE)
    with path.open("rb") as handle:
        codes.fromfile(handle, path.stat().st_size // codes.itemsize)
    if len(codes) != entry.count:
        raise ListMismatch(f"{path}: {len(codes):,} barcodes, the register says {entry.count:,}")
    return BarcodeList(entry, codes)


@cache
def available(directory: str | None = None) -> dict[str, BarcodeList]:
    """Every registered list that has been fetched, loaded once per process."""
    found = {}
    for entry in REGISTER.values():
        try:
            found[entry.id] = load(entry, Path(directory) if directory else None)
        except FileNotFoundError:
            continue
    return found


def fetch_all(directory: Path | None = None) -> Iterator[tuple[Entry, Path]]:
    with httpx.Client(follow_redirects=True, timeout=120.0) as client:
        for entry in REGISTER.values():
            yield entry, fetch(entry, directory, client=client)


__all__ = [
    "REGISTER",
    "BarcodeList",
    "Entry",
    "ListMismatch",
    "available",
    "cache_dir",
    "encode",
    "export",
    "exported",
    "fetch",
    "fetch_all",
    "load",
    "load_register",
    "pack",
    "unpack",
]
