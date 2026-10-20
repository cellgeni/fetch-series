"""Decompress the start of a file, tolerating its being only the start.

A prefix of a gzip or bzip2 file ends mid-stream by construction, which the
standard library's one-shot functions treat as corruption. These decompress
member after member and return what was recovered before the cut.
"""

from __future__ import annotations

import bz2
import zlib

GZIP_MAGIC = b"\x1f\x8b"
BZIP2_MAGIC = b"BZh"


def compression(data: bytes) -> str | None:
    if data.startswith(GZIP_MAGIC):
        return "gzip"
    if data.startswith(BZIP2_MAGIC):
        return "bzip2"
    return None


def gunzip_prefix(data: bytes, limit: int | None = None) -> tuple[bytes, bool]:
    """Every gzip member in ``data``, concatenated, and whether the input ended cleanly.

    Multi-member, because BGZF (BAM) is a chain of small gzip members and ENA
    serves some fastqs the same way. ``limit`` stops after that many output
    bytes: a 32 MB prefix of a well-compressed file inflates to far more than a
    sample needs.
    """
    out: list[bytes] = []
    produced = 0
    rest = data
    clean = True
    while rest:
        inflater = zlib.decompressobj(wbits=31)
        try:
            chunk = inflater.decompress(rest)
        except zlib.error:
            clean = False
            break
        out.append(chunk)
        produced += len(chunk)
        if not inflater.eof:
            clean = False  # the prefix ended inside this member
            break
        rest = inflater.unused_data
        if limit is not None and produced >= limit:
            break
    return b"".join(out), clean


def bunzip_prefix(data: bytes, limit: int | None = None) -> tuple[bytes, bool]:
    """Every bzip2 stream in ``data``. DDBJ serves its fastqs as bzip2."""
    out: list[bytes] = []
    produced = 0
    rest = data
    clean = True
    while rest:
        inflater = bz2.BZ2Decompressor()
        try:
            chunk = inflater.decompress(rest)
        except OSError:
            clean = False
            break
        out.append(chunk)
        produced += len(chunk)
        if not inflater.eof:
            clean = False
            break
        rest = inflater.unused_data
        if limit is not None and produced >= limit:
            break
    return b"".join(out), clean


def inflate(data: bytes, limit: int | None = None) -> tuple[bytes, bool]:
    """Whatever the compression, the decompressed prefix and whether it ended cleanly."""
    kind = compression(data)
    if kind == "gzip":
        return gunzip_prefix(data, limit)
    if kind == "bzip2":
        return bunzip_prefix(data, limit)
    return data, True
