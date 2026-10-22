"""Say which 10x layout a run's reads are, from the reads, with the evidence.

The method is ``tchem``'s (Tussl-Co/TusslChemistry, Apache-2.0), reimplemented,
with ``rename10xrun``'s role assignment and mate checks
(cellgeni/nf-reprocessing-public-10x, GPL-3.0): nothing is copied, both are
cited, and both run as reference instruments in the benchmark only.

1. **Barcodes.** For each read long enough to hold one, the share of its first
   bases that are a listed barcode -- exactly, and within one substitution with
   ``N`` as a wildcard -- against every fetched list, at each start the layout
   allows. The list with the most hits names the layouts it can be.
2. **UMI entropy.** The bases after the barcode must vary: at least 1.5 bits a
   position. A constant run there is a sample index or a spacer, not a UMI.
3. **Tail and strand.** Where two layouts share a list, what follows the UMI
   (poly-T for 3', the template-switch oligo for 5') and the strand the cDNA
   read lies on (sense for 3', antisense for 5') separate them. Strand is
   measured without a reference genome: k-mers of the mitochondrial genomes
   and of a spliced housekeeping set, in each orientation.
4. **Library type.** TCR/BCR constant regions, the sgRNA scaffold, and a
   feature-barcode library's handful of distinct tags.
5. **Level.** HIGH, MEDIUM, LOW or ambiguous, with every piece of evidence and
   every contradiction listed. A level is a measured confidence once the
   thresholds are set on the benchmark's design half; until then it is a
   provisional one, and the call says which.
"""

from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import asdict, dataclass, field
from typing import Any

from fetch_series.inspect.checks import CDNA_READ_MIN
from fetch_series.inspect.classify.layouts import (
    BY_ID,
    LAYOUTS,
    TSO_PREFIX,
    Layout,
    Strand,
    Tail,
    sharing,
)
from fetch_series.inspect.lists import BarcodeList
from fetch_series.inspect.model import FileSample
from fetch_series.inspect.references import KmerIndex

# The classifier is a versioned instrument: a call records the version that
# made it, and a threshold change is a new version. 0.2.1 takes a cDNA read
# from 30 bases rather than 50 (rule fit-3, SRR34778689's 28 + 47); a call on
# a set with no read of 30 to 49 bases is unchanged from 0.2.0.
VERSION = "0.2.1"
NO_LISTS = "no barcode list fetched; run `fetch inspect lists --fetch`"
THRESHOLDS_SET_ON = "the 2026-10-19 benchmark's design half: 121 runs"

# Set on the benchmark's design half (docs/benchmarks/inspect-2026-10.md): the
# lowest bars at which the design half's HIGH calls carried no error and its
# MEDIUM calls one in twenty at most. LOW only needs barcodes listed far above
# chance (``background``): a set where 30% of reads carry a listed barcode is a
# 10x set read badly, not a coincidence.
HIGH_NEAR = 0.70
MEDIUM_NEAR = 0.70
LOW_NEAR = 0.20
MIN_MARGIN = 0.20
MIN_STRAND_VOTES = 2
UMI_TRIM = 2
MIN_UMI_ENTROPY = 1.5
NEAR_SAMPLE = 2_000
STRAND_SAMPLE = 20_000
VDJ_SHARE = 0.15
CRISPR_SHARE = 0.05
TAG_CONCENTRATION = 0.50
SCAFFOLD = "GTTTTAGAGCTAGAAATAGCAAGTT"
POLY_A = "A" * 15
POLY_T = "T" * 15

# Illumina instrument ids, by prefix, longest first. A run's reads keep the
# sequencer's name where the archive kept the original header.
_INSTRUMENTS = (
    ("LH", "NovaSeq X"),
    ("VH", "NextSeq 1000/2000"),
    ("NB", "NextSeq 500/550"),
    ("NS", "NextSeq 500/550"),
    ("MN", "MiniSeq"),
    ("FS", "iSeq 100"),
    ("SN", "HiSeq 2000/2500"),
    ("A", "NovaSeq 6000"),
    ("K", "HiSeq 3000/4000"),
    ("J", "HiSeq 3000"),
    ("E", "HiSeq X"),
    ("D", "HiSeq 2000/2500"),
    ("M", "MiSeq"),
)
_ILLUMINA = re.compile(r"^([A-Z]{1,2})\d+:\d+:[A-Z0-9-]+:\d+:\d+:\d+:\d+")


@dataclass(frozen=True, slots=True)
class ListHit:
    """How one read's first bases matched one barcode list at one start."""

    barcode_list: str
    read: int  # index into the samples
    offset: int
    exact: float
    near: float
    sampled: int
    background: float


@dataclass(slots=True)
class Call:
    """A classification, with what it rests on and what argues against it."""

    layout: str | None
    candidates: list[str]
    level: str
    barcode_list: str | None = None
    barcode_read: int | None = None
    cdna_read: int | None = None
    evidence: dict[str, Any] = field(default_factory=dict)
    contradictions: list[str] = field(default_factory=list)
    hint: str | None = None
    library_type: str | None = None
    version: str = VERSION

    def as_detail(self) -> dict[str, Any]:
        return asdict(self)


def _entropy(column: list[str]) -> float:
    counts = Counter(column)
    total = sum(counts.values())
    return -sum(n / total * math.log2(n / total) for n in counts.values()) if total else 0.0


def umi_entropy(seqs: list[str], start: int, length: int) -> float | None:
    """Mean Shannon entropy, in bits, of the UMI positions across reads."""
    if length <= 0:
        return None
    usable = [s for s in seqs if len(s) >= start + length]
    if len(usable) < 100:
        return None
    return round(
        sum(_entropy([s[i] for s in usable]) for i in range(start, start + length)) / length, 3
    )


def _mismatches(a: str, b: str) -> int:
    return sum(x != y for x, y in zip(a, b, strict=False)) + abs(len(a) - len(b))


def tail_of(seqs: list[str], start: int) -> dict[str, float] | None:
    """What follows the UMI: the share of reads with poly-T, and with the TSO."""
    window = [s[start : start + len(TSO_PREFIX)] for s in seqs if len(s) >= start + len(TSO_PREFIX)]
    if len(window) < 100:
        return None
    poly_t = sum(w[:10].count("T") >= 8 for w in window) / len(window)
    tso = sum(_mismatches(w, TSO_PREFIX) <= 2 for w in window) / len(window)
    return {"poly_t": round(poly_t, 3), "tso": round(tso, 3), "reads": len(window)}


def instrument(sample: FileSample) -> str | None:
    ids = sample.facts.get("instrument_ids")
    if not isinstance(ids, list) or not ids:
        return None
    match = _ILLUMINA.match(str(ids[0]))
    if match is None:
        return None
    prefix = match.group(1)
    for start, name in _INSTRUMENTS:
        if prefix.startswith(start) and (len(start) == 2 or len(prefix) == 1):
            return name
    return None


def barcode_hits(samples: list[FileSample], lists: dict[str, BarcodeList]) -> list[ListHit]:
    """Every (read, list, start) that a layout allows, with its hit shares."""
    offsets: dict[str, set[int]] = {}
    for layout in LAYOUTS:
        offsets.setdefault(layout.barcode_list, set()).update(layout.offsets)
    hits = []
    for index, sample in enumerate(samples):
        seqs = [r.seq for r in sample.records]
        if not seqs:
            continue
        for list_id, barcodes in lists.items():
            for offset in sorted(offsets.get(list_id, {0})):
                windows = [s[offset : offset + barcodes.length] for s in seqs]
                windows = [w for w in windows if len(w) == barcodes.length]
                if len(windows) < 100:
                    continue
                exact = sum(barcodes.exact(w) for w in windows) / len(windows)
                subsample = windows[:NEAR_SAMPLE]
                near = sum(barcodes.near(w) for w in subsample) / len(subsample)
                hits.append(
                    ListHit(
                        list_id,
                        index,
                        offset,
                        round(exact, 4),
                        round(near, 4),
                        len(windows),
                        barcodes.background,
                    )
                )
    return sorted(hits, key=lambda h: (h.near, h.exact), reverse=True)


# Each strand signal votes, with its own threshold, and needs its own minimum.
# chrM and poly-A against poly-T were clean on every gate run; the housekeeping
# set is noisier -- ERR4265292 (3' v3) reads 1,176 sense to 1,076 antisense on it
# while chrM reads 997 to 20 -- so it votes at a lower bar and never alone
# overrides the other two.
STRAND_VOTES = {
    "chrM": (30, 0.75),
    "housekeeping_human": (50, 0.70),
    "poly_a": (50, 0.80),
}
# The two mitochondrial genomes share conserved stretches, so a human library
# meets mouse chrM k-mers too, at a muddled share (SRR25056225, a mouse run, has
# 39 human-chrM hits at 69% antisense against 671 mouse ones at 94%). Only the
# genome with the most hits -- the library's own species -- votes.
CHRM = ("human_chrM", "mouse_chrM")


def _vote(sense: int, anti: int, minimum: int, bar: float) -> str | None:
    total = sense + anti
    if total < minimum:
        return None
    if sense / total >= bar:
        return str(Strand.SENSE)
    if anti / total >= bar:
        return str(Strand.ANTISENSE)
    return str(Strand.UNSTRANDED)


def strand_of(seqs: list[str], references: dict[str, KmerIndex]) -> dict[str, Any] | None:
    """Which strand the cDNA read lies on, by a vote of independent signals.

    Each reference with a known strand orients the reads it shares k-mers with;
    poly-A runs in a cDNA read are sense to the transcript, poly-T runs
    antisense. A strand is called only where every signal that votes agrees;
    one voting the other way leaves it unstranded.
    """
    if not seqs:
        return None
    sample = seqs[:STRAND_SAMPLE]
    by_reference: dict[str, list[int]] = {}
    for ref_id, ref in references.items():
        if ref_id not in STRAND_VOTES and ref_id not in CHRM:
            continue
        counts = [0, 0]
        for seq in sample:
            orientation = ref.orient(seq)
            if orientation:
                counts[0 if orientation > 0 else 1] += 1
        by_reference[ref_id] = counts
    poly_a = sum(POLY_A in s for s in sample)
    poly_t = sum(POLY_T in s for s in sample)
    by_reference["poly_a"] = [poly_a, poly_t]
    present = [ref for ref in CHRM if ref in by_reference]
    signals = {k: v for k, v in by_reference.items() if k not in CHRM}
    if present:
        species = max(present, key=lambda ref: sum(by_reference[ref]))
        signals["chrM"] = by_reference[species]
    votes = {
        signal: vote
        for signal, (sense, anti) in signals.items()
        if (vote := _vote(sense, anti, *STRAND_VOTES[signal])) is not None
    }
    directional = {v for v in votes.values() if v != str(Strand.UNSTRANDED)}
    undecided = {signal for signal, v in votes.items() if v == str(Strand.UNSTRANDED)}
    if not votes:
        strand = None
    elif len(directional) == 1 and undecided <= {"housekeeping_human"}:
        # The housekeeping set undecided does not overrule chrM and poly-A.
        strand = directional.pop()
    else:
        strand = str(Strand.UNSTRANDED)
    sense = sum(c[0] for c in by_reference.values())
    oriented = sense + sum(c[1] for c in by_reference.values())
    return {
        "strand": strand,
        "votes": votes,
        "by_reference": by_reference,
        "oriented": oriented,
        "sense_share": round(sense / oriented, 3) if oriented else None,
    }


def library_type(
    seqs: list[str], references: dict[str, KmerIndex]
) -> tuple[str | None, dict[str, Any]]:
    """GEX, VDJ, CRISPR or feature barcode, from the cDNA read alone."""
    sample = seqs[:STRAND_SAMPLE]
    if not sample:
        return None, {}
    facts: dict[str, Any] = {}
    constant = references.get("tcr_bcr_constant_human")
    if constant is not None:
        facts["tcr_bcr_share"] = round(
            sum(constant.orient(s) != 0 for s in sample) / len(sample), 4
        )
    scaffold_rc = SCAFFOLD.translate(str.maketrans("ACGT", "TGCA"))[::-1]
    facts["scaffold_share"] = round(
        sum(SCAFFOLD in s or scaffold_rc in s for s in sample) / len(sample), 4
    )
    concentrations = []
    for start in (0, 10):
        tags = Counter(s[start : start + 15] for s in sample if len(s) >= start + 15)
        total = sum(tags.values())
        if total:
            concentrations.append(sum(n for _, n in tags.most_common(100)) / total)
    facts["tag_concentration"] = round(max(concentrations), 4) if concentrations else None
    if facts.get("tcr_bcr_share", 0) >= VDJ_SHARE:
        return "VDJ", facts
    if facts["scaffold_share"] >= CRISPR_SHARE:
        return "CRISPR", facts
    if (facts["tag_concentration"] or 0) >= TAG_CONCENTRATION:
        return "feature barcode", facts
    return "GEX", facts


def _no_barcode_hint(samples: list[FileSample]) -> str:
    modes = [Counter(s.lengths).most_common(1)[0][0] for s in samples if s.records]
    if not any(m >= 16 for m in modes):
        return "no read long enough for a barcode"
    if sum(m >= 26 for m in modes) < 2:
        return "barcode read missing: this source dropped it; use the SRA Normalized object or the submitted files"
    return "no listed 10x barcode at any start: not a 10x library, or a list not fetched"


def classify(
    samples: list[FileSample],
    lists: dict[str, BarcodeList],
    references: dict[str, KmerIndex] | None = None,
) -> Call:
    """The layout a run's reads are, at a level, with evidence and contradictions."""
    references = references or {}
    samples = [s for s in samples if s.records]
    if not samples:
        return Call(None, [], "none", hint="nothing could be read")
    if not lists:
        return Call(
            None, [], "none", hint="no barcode list fetched; run `fetch inspect lists --fetch`"
        )

    hits = barcode_hits(samples, lists)
    evidence: dict[str, Any] = {
        "hits": [asdict(h) for h in hits[:6]],
        "instrument": next((i for s in samples if (i := instrument(s))), None),
        "files": [
            {
                "source": s.source,
                "role_hint": s.role_hint,
                "derived": s.derived,
                "mode": Counter(s.lengths).most_common(1)[0][0],
            }
            for s in samples
        ],
    }
    above = [h for h in hits if h.near >= LOW_NEAR and h.exact >= 10 * h.background]
    if not above:
        return Call(None, [], "none", evidence=evidence, hint=_no_barcode_hint(samples))

    def feasible(hit: ListHit) -> list[Layout]:
        """The layouts a hit can be, given the read it is on.

        A gene-expression layout needs room for its UMI, less two bases a
        submitter may have trimmed: SRR31955810's 16-base i5 read matched the
        ARC gene-expression list at 0.94 and is the ATAC library's barcode, with
        no UMI after it at all.
        """
        length = Counter(samples[hit.read].lengths).most_common(1)[0][0]
        return [
            layout
            for layout in sharing(hit.barcode_list)
            if hit.offset in layout.offsets
            and length >= hit.offset + layout.cb
            and (layout.modality != "gex" or length >= layout.cb + layout.umi - UMI_TRIM)
        ]

    usable = [h for h in above if feasible(h)]
    if not usable:
        return Call(None, [], "none", evidence=evidence, hint=_no_barcode_hint(samples))
    best = usable[0]
    rival = next((h for h in above if h.barcode_list != best.barcode_list), None)
    evidence["margin"] = round(best.near - rival.near, 4) if rival else None

    barcode = samples[best.read]
    seqs = [r.seq for r in barcode.records]
    contradictions: list[str] = []
    if barcode.role_hint in ("R2", "I1", "I2"):
        contradictions.append(
            f"the barcode is in the file named {barcode.role_hint}: files swapped"
        )

    read_length = Counter(barcode.lengths).most_common(1)[0][0]
    candidates: list[Layout] = feasible(best)
    gex = [layout for layout in candidates if layout.modality == "gex"]
    if gex:
        umi_len = max(layout.umi for layout in gex)
        cb = gex[0].cb
        entropy = umi_entropy(seqs, best.offset + cb, min(umi_len, max(read_length - cb, 0)))
        evidence["umi_entropy"] = entropy
        if entropy is not None and entropy < MIN_UMI_ENTROPY:
            contradictions.append(f"UMI entropy {entropy} bits is below {MIN_UMI_ENTROPY}")
        tail = tail_of(seqs, best.offset + cb + umi_len)
        evidence["tail"] = tail
    else:
        tail = None

    cdna_index = next(
        (
            i
            for i, s in sorted(
                enumerate(samples), key=lambda kv: -Counter(kv[1].lengths).most_common(1)[0][0]
            )
            if i != best.read and Counter(s.lengths).most_common(1)[0][0] >= CDNA_READ_MIN
        ),
        None,
    )
    kind: str | None = None
    strand: dict[str, Any] | None = None
    if cdna_index is not None and samples[cdna_index].facts.get("sort_order") == "coordinate":
        # A coordinate-sorted BAM starts at the start of chr1: SRR10377488's
        # first 680,000 records are all within its first megabase, 97% of them
        # secondary alignments to repeats and a mitochondrial insertion. Those
        # reads say nothing about the library's strand or type; its barcodes
        # are unaffected, because every read of every cell carries one.
        evidence["strand"] = {
            "skipped": "coordinate-sorted BAM: the first reads are chr1, not a sample"
        }
    elif cdna_index is not None:
        cdna = [r.seq for r in samples[cdna_index].records]
        strand = strand_of(cdna, references)
        evidence["strand"] = strand
        kind, facts = library_type(cdna, references)
        evidence["library"] = facts
    else:
        contradictions.append(f"no cDNA read of {CDNA_READ_MIN} bases or more")

    # Where layouts share a list, the tail and then the strand decide.
    remaining = list(candidates)
    if len(remaining) > 1:
        by_tail = {Tail.POLY_T: "poly_t", Tail.TSO: "tso"}
        if tail is not None:
            seen = [x for x in remaining if x.tail in by_tail and tail[by_tail[x.tail]] >= 0.5]
            if len(seen) == 1:
                remaining = seen
        # Strand settles it only on two agreeing signals. Poly-A alone -- the
        # only vote a library of an unreferenced species gets -- called
        # SRR19242687 3' v2 against a 5' label, where tchem called it ambiguous.
        directional = [
            v
            for v in (strand or {}).get("votes", {}).values()
            if v in (Strand.SENSE, Strand.ANTISENSE)
        ]
        if (
            len(remaining) > 1
            and strand
            and strand.get("strand") in (Strand.SENSE, Strand.ANTISENSE)
            and len(directional) >= MIN_STRAND_VOTES
        ):
            seen = [x for x in remaining if str(x.strand) == strand["strand"]]
            if len(seen) == 1:
                remaining = seen
    if len(remaining) == 1 and strand and strand.get("strand") in (Strand.SENSE, Strand.ANTISENSE):
        layout = remaining[0]
        # A VDJ or feature-barcode library's cDNA read is not a transcript's.
        opposed = (
            layout.strand in (Strand.SENSE, Strand.ANTISENSE)
            and str(layout.strand) != strand["strand"]
        )
        if opposed and kind in (None, "GEX"):
            contradictions.append(
                f"{layout.id} reads {layout.strand}; the cDNA read is {strand['strand']}"
            )

    ids = [x.id for x in remaining]
    if len(ids) > 1:
        level = "ambiguous"
    elif (
        best.near >= HIGH_NEAR
        and (evidence["margin"] is None or evidence["margin"] >= MIN_MARGIN)
        and not contradictions
    ):
        level = "HIGH"
    elif best.near >= MEDIUM_NEAR and (
        evidence["margin"] is None or evidence["margin"] >= MIN_MARGIN
    ):
        # A rival list close behind is not a MEDIUM call: SRR31168852's 26-base
        # barcode read matched v3 at 0.66 with the v4 list near it.
        level = "MEDIUM"
    else:
        level = "LOW"
    return Call(
        layout=ids[0] if len(ids) == 1 else None,
        candidates=ids,
        level=level,
        barcode_list=best.barcode_list,
        barcode_read=best.read,
        cdna_read=cdna_index,
        evidence=evidence,
        contradictions=contradictions,
        library_type=kind,
    )


__all__ = [
    "BY_ID",
    "NO_LISTS",
    "VERSION",
    "Call",
    "ListHit",
    "barcode_hits",
    "classify",
    "instrument",
    "library_type",
    "strand_of",
    "tail_of",
    "umi_entropy",
]
