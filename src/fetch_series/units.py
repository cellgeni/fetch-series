"""The ids a link survey and a content survey record their verdicts under.

The survey cache is keyed on ``(corpus, unit id, accession)``, and a route's
unit id is its route id. A link's id can be **identical** to a route's:
``bioproject->biosample:ena_filereport`` is a route and also the link that
route covers, because the mechanism is named for the request. Recording link
verdicts under the bare link id would overwrite the route's evidence in place,
silently, the first time both were surveyed over one corpus.

So link and content verdicts carry a prefix no route id can start with:

* ``link:<link id>`` -- did the link answer, and did what it found pass its checks;
* ``screen:<reads>:<link id>`` -- what the first ``<reads>`` reads of the files
  the link found turned out to be. The read count is part of the id because it
  is part of what was measured: a call made on 100,000 reads and one made on
  200,000 are different measurements, and must not resume into each other.
"""

from __future__ import annotations

from dataclasses import dataclass

LINK_PREFIX = "link:"
SCREEN_PREFIX = "screen:"


def link_unit(link_id: str) -> str:
    """The unit id a link survey records ``link_id``'s verdicts under."""
    return f"{LINK_PREFIX}{link_id}"


def screen_unit(reads: int, link_id: str) -> str:
    """The unit id a content survey records under, at ``reads`` reads per file set."""
    if reads <= 0:
        raise ValueError(f"A content survey reads at least one read, not {reads}")
    return f"{SCREEN_PREFIX}{reads}:{link_id}"


@dataclass(frozen=True, slots=True)
class Unit:
    """A parsed unit id: what kind of survey, of which link, at how many reads."""

    kind: str  # "route", "link" or "screen"
    id: str  # the route id or the link id
    reads: int | None = None


def parse_unit(unit_id: str) -> Unit:
    """Read a unit id back. Anything unprefixed is a route id."""
    if unit_id.startswith(LINK_PREFIX):
        return Unit("link", unit_id.removeprefix(LINK_PREFIX))
    if unit_id.startswith(SCREEN_PREFIX):
        reads, sep, link_id = unit_id.removeprefix(SCREEN_PREFIX).partition(":")
        if not sep or not reads.isdigit() or not link_id:
            raise ValueError(f"Expected screen:<reads>:<link id>, got {unit_id!r}")
        return Unit("screen", link_id, int(reads))
    return Unit("route", unit_id)


__all__ = ["LINK_PREFIX", "SCREEN_PREFIX", "Unit", "link_unit", "parse_unit", "screen_unit"]
