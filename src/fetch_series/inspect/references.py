"""Reference sequences the classifier measures strand and library type against.

None is shipped. Each is registered (``references.tsv``) at a pinned version
with the sha256 of what is downloaded: two RefSeq mitochondrial genomes at
versioned accessions, which never change, and two small extracts of GENCODE
release 44's spliced transcripts -- 40 ubiquitously expressed human genes, and
the 20 genes of the TCR and BCR constant regions.

The method is ``tchem``'s (Apache-2.0), reimplemented: a cDNA read that shares
k-mers with a transcript in its sense orientation is a sense read, and the
share of sense reads says which strand the library reads -- without aligning
anything. That is what separates 3' v2 from 5' v1/v2, which share a barcode
list, a barcode length and a UMI length.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import os
from dataclasses import dataclass
from functools import cache
from pathlib import Path

import httpx

from fetch_series.logging_utils import redact_exception

REGISTER_PATH = Path(__file__).with_name("references.tsv")
DEFAULT_CACHE = Path(".cache/references")
K = 21
# Every eighth reference k-mer is kept. A read is checked at every position, so
# a read lying on the transcript still meets several; memory falls eightfold.
STRIDE = 8
_COMPLEMENT = str.maketrans("ACGTN", "TGCAN")


@dataclass(frozen=True, slots=True)
class Reference:
    id: str
    sha256: str
    source: str
    extract: str
    terms: str


def load_register(path: Path = REGISTER_PATH) -> dict[str, Reference]:
    with path.open(newline="") as handle:
        return {row["id"]: Reference(**row) for row in csv.DictReader(handle, delimiter="\t")}


REGISTER = load_register()


def cache_dir() -> Path:
    return Path(os.environ.get("FETCH_SERIES_REFERENCES", DEFAULT_CACHE))


def revcomp(seq: str) -> str:
    return seq.translate(_COMPLEMENT)[::-1]


def _source_path(reference: Reference, directory: Path) -> Path:
    name = reference.source.rsplit("/", 1)[-1]
    if "efetch" in reference.source:
        name = f"{reference.id}.fa"
    return directory / name


def fetch(reference: Reference, directory: Path | None = None) -> Path:
    """Download a reference's source once, refusing it unless the sha256 matches.

    The API key is never needed: E-utilities serves a single efetch without one.
    """
    directory = directory or cache_dir()
    directory.mkdir(parents=True, exist_ok=True)
    path = _source_path(reference, directory)
    if not path.exists():
        try:
            with httpx.Client(follow_redirects=True, timeout=300.0) as client:
                response = client.get(reference.source)
                response.raise_for_status()
        except httpx.HTTPError as exc:
            raise redact_exception(exc) from None
        path.write_bytes(response.content)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != reference.sha256:
        raise ValueError(f"{reference.id}: sha256 {digest}, the register says {reference.sha256}")
    return path


def sequences(reference: Reference, directory: Path | None = None) -> list[str]:
    """The reference's sequences, in sense orientation."""
    path = _source_path(reference, directory or cache_dir())
    if not path.exists():
        raise FileNotFoundError(f"reference {reference.id} is not fetched")
    raw = path.read_bytes()
    text = (gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw).decode("ascii")
    wanted: set[str] | None = None
    if reference.extract.startswith("genes:"):
        wanted = set(reference.extract.removeprefix("genes:").split(","))
    found: list[str] = []
    keep = False
    chunk: list[str] = []
    for line in text.splitlines():
        if line.startswith(">"):
            if keep and chunk:
                found.append("".join(chunk).upper())
            chunk = []
            fields = line[1:].split("|")
            keep = wanted is None or (len(fields) > 5 and fields[5] in wanted)
        elif keep:
            chunk.append(line.strip())
    if keep and chunk:
        found.append("".join(chunk).upper())
    return found


@dataclass(frozen=True, slots=True)
class KmerIndex:
    """Sampled k-mers of a reference, sense and antisense apart."""

    id: str
    sense: frozenset[str]
    antisense: frozenset[str]

    def orient(self, read: str) -> int:
        """+1 if the read lies sense on the reference, -1 antisense, 0 if neither or both."""
        sense = anti = 0
        for i in range(len(read) - K + 1):
            kmer = read[i : i + K]
            sense += kmer in self.sense
            anti += kmer in self.antisense
        if sense and not anti:
            return 1
        if anti and not sense:
            return -1
        return 0


def build_index(reference_id: str, seqs: list[str]) -> KmerIndex:
    sense = {s[i : i + K] for s in seqs for i in range(0, len(s) - K + 1, STRIDE)}
    sense = {k for k in sense if "N" not in k}
    antisense = {revcomp(k) for k in sense}
    # A palindromic k-mer orients nothing.
    both = sense & antisense
    return KmerIndex(reference_id, frozenset(sense - both), frozenset(antisense - both))


@cache
def index(reference_id: str) -> KmerIndex | None:
    """The k-mer index of a fetched reference, or ``None`` if it has not been fetched."""
    reference = REGISTER[reference_id]
    try:
        return build_index(reference_id, sequences(reference))
    except FileNotFoundError:
        return None


__all__ = [
    "REGISTER",
    "K",
    "KmerIndex",
    "Reference",
    "build_index",
    "fetch",
    "index",
    "revcomp",
    "sequences",
]
