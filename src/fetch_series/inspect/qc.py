"""Basic QC of a copy's first reads: barcode and UMI metrics, and the library's finer type.

Every number is over the sample -- the first reads of each file -- and says so:
a CB-UMI pair's distinct share over 100,000 reads is a proxy for saturation,
not the saturation STARsolo reports over the whole run. What it is good for is
telling a copy whose barcodes and UMIs look like a 10x library from one that
does not, and setting one copy beside another.

The listed-barcode rates are the classifier's own (``call.evidence.hits``):
the share of barcode windows that are a listed barcode exactly, and within one
substitution with ``N`` as a wildcard -- the nearest thing a sample has to
STARsolo's "valid barcodes", and the number the comparison with past STARsolo
runs holds against it.

The library's finer type goes one step past the classifier's:

* a VDJ library's chain, T or B, from the TCR and BCR constant regions apart;
* a feature-barcode library's multiplexing, cell hashing (``HTO``) or CellPlex
  (``CMO``), where its feature reads carry a registered tag set's tags
  (``inspect.tags``). Without a fetched set it stays ``feature barcode``.

Both thresholds are provisional: no labelled set has measured them yet, and
the block says so.
"""

from __future__ import annotations

from typing import Any

from fetch_series.inspect.classify.layouts import BY_ID
from fetch_series.inspect.model import FileSample
from fetch_series.inspect.readiness import barcode_hit
from fetch_series.inspect.references import KmerIndex
from fetch_series.inspect.tags import TagSet

# A sample of the feature or cDNA read, as the classifier's library typing takes.
SAMPLE = 20_000
# Provisional: one chain's share of the constant-region reads that names it.
CHAIN_SHARE = 0.8
# Provisional: the share of feature reads carrying a set's tags that names it.
MULTIPLEX_SHARE = 0.5
TAG_LENGTH = 15
# 10x's template-switch oligo, past its first bases. A gene-expression
# library's TSO artefacts make its cDNA reads start alike, which reads as a
# feature library's few tags: SRR30069691's and SRR23446052's R2 start
# AAGCAGTGGTATCAACGCAGAGTAC in a third of reads, and neither is one.
TSO_CORE = "GTGGTATCAACGCAGAGTAC"
TSO_SHARE = 0.2


def _geometry(call: dict[str, Any]) -> tuple[int, int] | None:
    """Barcode and UMI lengths: the layout's, or what every candidate shares."""
    ids = [call["layout"]] if call.get("layout") else list(call.get("candidates") or [])
    layouts = [BY_ID[i] for i in ids if i in BY_ID]
    shapes = {(layout.cb, layout.umi) for layout in layouts}
    return shapes.pop() if len(shapes) == 1 else None


def qc(samples: list[FileSample], call: dict[str, Any]) -> dict[str, Any] | None:
    """Barcode, UMI and tail metrics over the sample; ``None`` with no barcode read."""
    samples = [s for s in samples if s.records]
    hit = barcode_hit(call)
    if hit is None:
        return None
    evidence = call.get("evidence") or {}
    out: dict[str, Any] = {
        "barcodes": {
            "list": hit["barcode_list"],
            "read": hit["read"],
            "exact": hit["exact"],
            "within_one": hit["near"],
            "sampled": hit.get("sampled"),
        },
        "umi": {"entropy": evidence.get("umi_entropy")},
        "tail": evidence.get("tail"),
    }
    shape = _geometry(call)
    if shape is None or not shape[1]:
        return out
    cb, umi = shape
    start = hit["offset"]
    seqs = [r.seq for r in samples[hit["read"]].records]
    whole = [s for s in seqs if len(s) >= start + cb + umi]
    if not whole:
        return out
    umis = [s[start + cb : start + cb + umi] for s in whole]
    pairs = {(s[start : start + cb], s[start + cb : start + cb + umi]) for s in whole}
    out["umi"].update(
        distinct_share=round(len(set(umis)) / len(umis), 4),
        with_n=round(sum("N" in u for u in umis) / len(umis), 4),
    )
    out["cb_umi"] = {"reads": len(whole), "distinct_share": round(len(pairs) / len(whole), 4)}
    return out


def _within_one(tags: frozenset[str]) -> frozenset[str]:
    """Every tag, and every sequence one substitution or one ``N`` from one."""
    near = set(tags)
    for tag in tags:
        for i, base in enumerate(tag):
            for other in "ACGTN":
                if other != base:
                    near.add(tag[:i] + other + tag[i + 1 :])
    return frozenset(near)


def library(
    samples: list[FileSample],
    call: dict[str, Any],
    references: dict[str, KmerIndex],
    tag_sets: dict[str, tuple[TagSet, frozenset[str]]],
) -> dict[str, Any]:
    """The classifier's library type, with a VDJ library's chain and a feature library's tags."""
    samples = [s for s in samples if s.records]
    kind = call.get("library_type")
    out: dict[str, Any] = {"type": kind, "provisional": True}
    index = call.get("cdna_read")
    if index is None or index >= len(samples):
        return out
    seqs = [r.seq for r in samples[index].records[:SAMPLE]]
    if kind == "VDJ":
        tcr, bcr = references.get("tcr_constant_human"), references.get("bcr_constant_human")
        if tcr is None or bcr is None:
            out["chain"] = None
            out["why"] = "TCR and BCR references not fetched"
            return out
        t = sum(tcr.orient(s) != 0 for s in seqs)
        b = sum(bcr.orient(s) != 0 for s in seqs)
        out.update(tcr_reads=t, bcr_reads=b)
        total = t + b
        out["chain"] = (
            "T"
            if total and t / total >= CHAIN_SHARE
            else "B"
            if total and b / total >= CHAIN_SHARE
            else ("T and B" if total else None)
        )
    elif kind == "feature barcode":
        if not tag_sets:
            out["multiplexing"] = None
            out["why"] = "no tag set fetched"
            return out
        tso = sum(TSO_CORE in s[:45] for s in seqs) / len(seqs) if seqs else 0.0
        if tso >= TSO_SHARE:
            out["tso_share"] = round(tso, 4)
            out["suspect"] = (
                "the reads start with 10x's template-switch oligo, as a gene-expression "
                "library's artefacts do: the few distinct tags may be those"
            )
        shares = {}
        for set_id, (tag_set, tags) in tag_sets.items():
            near = _within_one(tags)
            share = 0.0
            for offset in tag_set.offsets:
                windows = [
                    s[offset : offset + TAG_LENGTH] for s in seqs if len(s) >= offset + TAG_LENGTH
                ]
                if windows:
                    share = max(share, sum(w in near for w in windows) / len(windows))
            shares[set_id] = round(share, 4)
        out["tag_shares"] = shares
        best = max(shares, key=lambda k: shares[k], default=None)
        out["multiplexing"] = (
            tag_sets[best][0].kind if best is not None and shares[best] >= MULTIPLEX_SHARE else None
        )
    return out


__all__ = ["CHAIN_SHARE", "MULTIPLEX_SHARE", "library", "qc"]
