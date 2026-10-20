"""Whether a call made from reads agrees with another call: another source's, or the metadata's.

Two levels of agreement, because the two calls can be made at different
resolutions:

* **exact** -- the same layout id;
* **class** -- the same barcode list. 3' v2 and 5' v1/v2 share the v2 list, so a
  read call of "3' v2" and a protocol saying "5' v1" agree on the list and
  disagree on the kit. That is a class match and an exact mismatch, and both
  are reported: the first says the barcodes will be read correctly, the second
  that the submission and the reads tell different stories.

A metadata call naming several kits (GSE111360's protocol names a 3' and a 5'
kit) agrees with a read call that is one of them, and is reported as such.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from fetch_series.assays.chemistry import ChemistryCall
from fetch_series.inspect.classify import Call
from fetch_series.inspect.classify.layouts import list_class


@dataclass(frozen=True, slots=True)
class Agreement:
    exact: bool | None
    same_class: bool | None
    note: str

    def as_dict(self) -> dict[str, object]:
        return {"exact": self.exact, "class": self.same_class, "note": self.note}


def with_metadata(call: Call, metadata: ChemistryCall) -> Agreement:
    """A read call against what the submission's metadata says the kit is."""
    if call.layout is None and not call.candidates:
        return Agreement(None, None, "the reads made no call")
    if not metadata.candidates:
        return Agreement(None, None, "the metadata names no kit")
    read_ids = [call.layout] if call.layout else list(call.candidates)
    read_classes = {list_class(i) for i in read_ids}
    meta_classes = {list_class(i) for i in metadata.candidates}
    same_class = bool(read_classes & meta_classes)
    if metadata.chemistry is not None and call.layout is not None:
        exact = call.layout == metadata.chemistry
        note = (
            "same kit"
            if exact
            else ("same list, different kit" if same_class else "different list")
        )
        return Agreement(exact, same_class, note)
    overlap = set(read_ids) & set(metadata.candidates)
    if overlap:
        return Agreement(None, True, f"consistent: {sorted(overlap)} in both")
    return Agreement(False if call.layout else None, same_class, "no kit in common")


def across_sources(calls: Mapping[str, Call]) -> dict[str, object]:
    """Whether every readable source of one run's files makes the same call.

    The same library read through ENA's fastqs, the SRA object and the submitted
    BAM should classify alike. Where one source cannot be classified -- ENA's
    fastq that dropped the barcode read -- that is not a disagreement but a
    source that is unfit, and it is reported apart.
    """
    made = {source: call for source, call in calls.items() if call.layout is not None}
    layouts = {call.layout for call in made.values()}
    lists = {call.barcode_list for call in made.values()}
    return {
        "sources": sorted(calls),
        "called": sorted(made),
        "uncalled": sorted(set(calls) - set(made)),
        "agree": len(layouts) <= 1 if made else None,
        "agree_on_list": len(lists) <= 1 if made else None,
        "layouts": sorted(layout for layout in layouts if layout is not None),
    }


def summarise(calls: Sequence[Call]) -> list[str]:
    return sorted({call.layout for call in calls if call.layout})


__all__ = ["Agreement", "across_sources", "summarise", "with_metadata"]
