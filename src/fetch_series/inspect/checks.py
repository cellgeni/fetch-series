"""What a run's file set holds, as counts: the checks every inspection makes.

These are facts about reads, not a verdict on a library: the classifier
(``inspect.classify``) and the fit rule turn them into one. The output is
stored as survey ``detail`` and committed, so it holds counts and lengths and
**never a sequence**.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from typing import Any

from fetch_series.files import FileRecord
from fetch_series.inspect.model import FileSample
from fetch_series.inspect.readers.fastq import spot_name

# 10x's barcode read is cell barcode + UMI: 16 + 10 on v2 and 5' v1, 16 + 12 on
# v3. Anything shorter cannot hold one; an 8-base i7 index read certainly not.
BARCODE_READ_MIN = 26
# A cDNA read long enough to align. 10x's own floor for R2 is far above this;
# the point is to tell a data read from a barcode or index read. It was 50
# until SRR34778689: 28 + 47, a 3' v3 library whose 47-base reads map, and
# which the maintainer counts usable (rule fit-3).
CDNA_READ_MIN = 30
# Spot names compared across mates.
CONCORDANCE_READS = 1_000


def quality_verdict(sample: FileSample) -> str:
    """``real``, ``substituted`` or ``absent``.

    SRA Lite replaces every quality with one value: SRR25056225's Lite object
    reads ``?`` throughout, where its Normalized object has real scores. An
    alphabet of two or fewer is not a quality track anyone measured.
    """
    quals = [r.qual for r in sample.records if r.qual]
    if not quals:
        return "absent"
    alphabet = set("".join(quals[:2_000]))
    return "substituted" if len(alphabet) <= 2 else "real"


def _mode(lengths: list[int]) -> int | None:
    return Counter(lengths).most_common(1)[0][0] if lengths else None


def file_facts(sample: FileSample) -> dict[str, Any]:
    lengths = sample.lengths
    bases = sum(lengths)
    n = sum(r.seq.upper().count("N") for r in sample.records)
    return {
        "source": sample.source,
        "format": sample.format,
        "role_hint": sample.role_hint,
        "derived": sample.derived,
        "reads": len(sample.records),
        "reached_end": sample.reached_end,
        "length": {
            "min": min(lengths) if lengths else None,
            "max": max(lengths) if lengths else None,
            "mode": _mode(lengths),
            "distinct": len(set(lengths)),
        },
        "quality": quality_verdict(sample),
        "n_fraction": round(n / bases, 5) if bases else None,
        "suffixes": dict(sorted(sample.suffixes.items())),
    }


def concordance(a: FileSample, b: FileSample) -> float | None:
    """Share of the first reads whose spot names agree across two files of one run.

    Mates of one spot share a name. Two files whose names disagree are not two
    reads of the same spots, whatever their file names say.
    """
    pairs = list(zip(a.records[:CONCORDANCE_READS], b.records[:CONCORDANCE_READS], strict=False))
    if not pairs:
        return None
    same = sum(spot_name(x.name) == spot_name(y.name) for x, y in pairs)
    return round(same / len(pairs), 4)


def duplicates(records: Iterable[FileRecord]) -> list[list[str]]:
    """Files that are the same bytes under different names: same md5 and size.

    Reading both would count one read twice and could make a single-read set
    look paired.
    """
    groups: dict[tuple[str, int], list[str]] = {}
    for record in records:
        if record.md5 and record.size:
            groups.setdefault((record.md5, record.size), []).append(record.url or record.name)
    return [sorted(names) for names in groups.values() if len(names) > 1]


def concordances(samples: list[FileSample]) -> list[list[Any]]:
    """Spot-name agreement for every pair of read files from different sources.

    ``[i, j, share]`` with ``i < j`` indexing the files read, as ``files`` and
    a call's ``hits`` do. Two reads of one source are mates by construction,
    so they are not compared. Recorded so a fit rule can say which reads are
    mates without the reads (``fit.fit_from_detail``).
    """
    read = [s for s in samples if s.records]
    return [
        [i, j, concordance(read[i], read[j])]
        for i in range(len(read))
        for j in range(i + 1, len(read))
        if read[i].source != read[j].source
    ]


def check_set(samples: list[FileSample], records: Iterable[FileRecord] = ()) -> dict[str, Any]:
    """Every built-in check on one run's set of files.

    ``barcode_read`` needs *two* reads of at least 26 bases: one to be the
    barcode read and another to be the cDNA read. A set whose only long read is
    the cDNA -- ENA's lone fastq of SRR29534765, SRA's 91-base object of
    ERR4265292 -- has lost the barcode read, however the files are named.
    """
    files = [file_facts(s) for s in samples if s.records]
    modes = [f["length"]["mode"] for f in files if f["length"]["mode"] is not None]
    long_enough = [m for m in modes if m >= BARCODE_READ_MIN]
    checks: dict[str, Any] = {
        "readable": bool(files),
        "files": files,
        "barcode_read": len(long_enough) >= 2,
        "cdna_read": any(m >= CDNA_READ_MIN for m in modes),
        "quality": sorted({f["quality"] for f in files}),
        "duplicates": duplicates(records),
    }
    plain = [s for s in samples if s.records and not s.derived]
    if len(plain) >= 2 and len({s.source for s in plain}) >= 2:
        checks["spot_names_agree"] = concordance(plain[0], plain[1])
    checks["concordance"] = concordances(samples)
    bam = next((s.facts for s in samples if s.format in ("bam", "cram") and s.facts), None)
    if bam is not None:
        checks["bam"] = bam
    return checks


__all__ = [
    "BARCODE_READ_MIN",
    "CDNA_READ_MIN",
    "check_set",
    "concordance",
    "concordances",
    "duplicates",
    "file_facts",
    "quality_verdict",
]
