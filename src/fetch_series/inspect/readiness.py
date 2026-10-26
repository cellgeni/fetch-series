"""What one copy of a run is ready for: its geometry, how to map it, how to convert it.

``fetch inspect`` reads a copy's first reads and calls its kit; this turns that
call into what a pipeline consumes. Each function is pure, a function of what
a report or a content-survey verdict records -- the ``checks``, the ``call``,
the ``fit`` verdict -- so a recorded verdict yields the same block as a fresh
read.

These are **requirement tables, not evidence.** Which STARsolo option a kit
needs is what STAR's and 10x's documentation say it needs:

* STARsolo: https://github.com/alexdobin/STAR/blob/master/docs/STARsolo.md --
  ``CB_UMI_Simple``, 1-based ``soloCBstart``/``soloUMIstart``, the cDNA read
  first in ``readFilesIn`` and the barcode read last, and
  ``soloBarcodeReadLength 0`` where the barcode read is not exactly barcode
  plus UMI;
* Cell Ranger: https://www.10xgenomics.com/support/software/cell-ranger/latest/analysis/inputs/cr-specifying-chemistry
  -- the ``--chemistry`` names.

``soloStrand`` comes from the strand the reads were measured on, never from
the kit's name: sense is ``Forward``, antisense ``Reverse``, and an unstranded
or unmeasured read leaves it out rather than guessed. The reprocessing
pipeline (``starsolo_10x_auto.sh``) settles the same question by mapping
200,000 reads both ways.

A block says whether its layout has been checked against what that pipeline
used (``VALIDATION``): on a fresh held-out draw of its past STARsolo runs, read
once under classifier 0.4.0 (``docs/benchmarks/inspect-vs-starsolo-2026-10.md``),
every one of 404 blocks named the whitelist the pipeline used, and every one of
386 with a ``soloStrand`` named its strand. A layout no held-out sample was
called as is unvalidated, and says so.
"""

from __future__ import annotations

from typing import Any

from fetch_series.assays.chemistry import ChemistryCall
from fetch_series.inspect.checks import CDNA_READ_MIN
from fetch_series.inspect.classify.layouts import BY_ID, TSO_PREFIX, Layout, Tail
from fetch_series.inspect.fit import INDEX_READ_MAX, short_of_run
from fetch_series.inspect.lists import REGISTER

# Cell Ranger's --chemistry for each layout that Cell Ranger counts.
CELLRANGER_CHEMISTRY = {
    "10x_3p_v1": "SC3Pv1",
    "10x_3p_v2": "SC3Pv2",
    "10x_3p_v3_family": "SC3Pv3",
    "10x_3p_v4_gemx": "SC3Pv4",
    "10x_5p_v1_v2": "SC5P-R2",
    "10x_5p_v3_gemx": "SC5P-R2-v3",
    "10x_multiome_arc_gex": "ARC-v1",
}
SOLO_STRAND = {"sense": "Forward", "antisense": "Reverse"}
UNVALIDATED = (
    "not yet compared with the reprocessing pipeline's own STARsolo runs: "
    "check the first run's Summary.csv"
)
# Blocks set beside the reprocessing pipeline's own STARsolo runs: the second
# held-out draw, read once under classifier 0.4.0 on 2026-10-23. Per layout, the
# samples, the blocks whose whitelist was the pipeline's, the blocks whose
# soloStrand was (or that omitted it), and the blocks whose layout implies the
# other strand from the one the pipeline found (ERS5577770: 3' v3 at HIGH,
# soloStrand omitted, mapped Reverse).
VALIDATION_SOURCE = "docs/benchmarks/inspect-vs-starsolo-2026-10.md"
VALIDATION = {
    "10x_3p_v1": {
        "samples": 1,
        "blocks": 1,
        "same_list": 1,
        "same_strand": 1,
        "strand_omitted": 0,
        "layout_other_strand": 0,
    },
    "10x_3p_v2": {
        "samples": 182,
        "blocks": 272,
        "same_list": 272,
        "same_strand": 270,
        "strand_omitted": 2,
        "layout_other_strand": 0,
    },
    "10x_3p_v3_family": {
        "samples": 56,
        "blocks": 92,
        "same_list": 92,
        "same_strand": 87,
        "strand_omitted": 5,
        "layout_other_strand": 3,
    },
    "10x_5p_v1_v2": {
        "samples": 21,
        "blocks": 34,
        "same_list": 34,
        "same_strand": 23,
        "strand_omitted": 11,
        "layout_other_strand": 0,
    },
    "10x_multiome_arc_gex": {
        "samples": 2,
        "blocks": 5,
        "same_list": 5,
        "same_strand": 5,
        "strand_omitted": 0,
        "layout_other_strand": 0,
    },
}


def _name(source: str) -> str:
    """A file's name as a mapper will be given it: a link's last part, a local path whole."""
    return source.rsplit("/", 1)[-1] if "://" in source else source


def barcode_hit(call: dict[str, Any]) -> dict[str, Any] | None:
    """The hit the call rests on: its list, on its barcode read."""
    hits = (call.get("evidence") or {}).get("hits") or []
    return next(
        (
            h
            for h in hits
            if h["barcode_list"] == call.get("barcode_list")
            and h["read"] == call.get("barcode_read")
        ),
        None,
    )


def roles(files: list[dict[str, Any]], call: dict[str, Any]) -> list[str | None]:
    """R1/R2/I1/I2 for each file read, as Cell Ranger names them.

    The barcode read and the cDNA read come from the call: 10x's R1 is the
    barcode read and R2 the cDNA, except for ATAC, where the barcode is read as
    the i5 index (R2) between two genomic reads (R1, R3). Another file of the
    same length, name and role from a different source is another lane of it.
    Index reads are the short ones, I1 then I2 unless their names say. What is
    left is not guessed.
    """
    modes = [f["length"]["mode"] for f in files]
    found: list[str | None] = [None] * len(files)
    barcode, cdna = call.get("barcode_read"), call.get("cdna_read")
    layout = BY_ID.get(call.get("layout") or "")
    atac = layout is not None and layout.modality == "atac"
    if barcode is not None:
        found[barcode] = "R2" if atac else "R1"
    if atac:
        genomic = [i for i, m in enumerate(modes) if i != barcode and m >= CDNA_READ_MIN]
        for i, genomic_role in zip(genomic, ("R1", "R3"), strict=False):
            found[i] = genomic_role
    elif cdna is not None:
        found[cdna] = "R2"
    for i, f in enumerate(files):
        if found[i] is not None:
            continue
        for j in (barcode, cdna):
            if (
                j is not None
                and modes[i] == modes[j]
                and f.get("role_hint") == files[j].get("role_hint")
                and f["source"] != files[j]["source"]
            ):
                found[i] = found[j]
    free = ["I1", "I2"]
    for i, mode in enumerate(modes):
        if found[i] is None and mode <= INDEX_READ_MAX:
            hint = files[i].get("role_hint")
            index = hint if hint in free else (free[0] if free else None)
            if index is not None:
                free.remove(index)
            found[i] = index
    return found


def _fastq_names(
    files: list[dict[str, Any]], found: list[str | None], run: str | None
) -> list[dict[str, Any]]:
    """What each read is called as a FASTQ: as listed, as dumped or rebuilt, and renamed.

    ``renamed`` is Cell Ranger's convention, which ``rename10xrun`` writes and
    STARsolo and Cell Ranger both read: ``<run>_S1_L001_R1_001.fastq.gz``, a
    lane number per further file of one role.
    """
    prefix = run or "sample"
    lanes: dict[str, int] = {}
    within: dict[str, int] = {}
    named = []
    for f, role in zip(files, found, strict=True):
        source = f["source"]
        within[source] = within.get(source, 0) + 1
        if f["format"] == "fastq":
            listed = _name(source)
        elif f["format"] == "sra":
            # fastq-dump --split-files names each read of the spot by its place.
            listed = f"{run or _name(source)}_{within[source]}.fastq"
        else:
            listed = None
        renamed = None
        if role is not None:
            lanes[role] = lanes.get(role, 0) + 1
            renamed = f"{prefix}_S1_L{lanes[role]:03d}_{role}_001.fastq.gz"
        named.append({"listed": listed, "renamed": renamed})
    return named


def geometry(
    checks: dict[str, Any], call: dict[str, Any], *, run: str | None = None
) -> dict[str, Any]:
    """Each read's role and lengths, and where the barcode and UMI sit.

    Positions are 1-based, as STARsolo's are. ``trimmed`` says a read's lengths
    vary -- with the share at the mode and the range -- and not why.
    """
    files = checks.get("files") or []
    found = roles(files, call)
    names = _fastq_names(files, found, run)
    reads = []
    for i, (f, role, name) in enumerate(zip(files, found, names, strict=True)):
        length = f["length"]
        reads.append(
            {
                "read": i,
                "source": f["source"],
                "role": role,
                "name": name,
                "length": {
                    "mode": length["mode"],
                    "min": length["min"],
                    "max": length["max"],
                    "at_mode": length.get("at_mode"),
                },
                "trimmed": length["min"] != length["max"],
            }
        )
    out: dict[str, Any] = {
        "reads": reads,
        "barcode": None,
        "cdna": None,
        "trimmed": any(r["trimmed"] for r in reads),
    }
    layout = BY_ID.get(call.get("layout") or "")
    hit = barcode_hit(call)
    if layout is not None and hit is not None:
        mode = files[hit["read"]]["length"]["mode"]
        umi = max(0, min(layout.umi, mode - hit["offset"] - layout.cb))
        out["barcode"] = {
            "read": hit["read"],
            "list": layout.barcode_list,
            "cb_start": hit["offset"] + 1,
            "cb_len": layout.cb,
            "umi_start": hit["offset"] + layout.cb + 1 if layout.umi else None,
            "umi_len": umi if layout.umi else None,
            # A 12-base-UMI kit's barcode read sequenced to 26 bases holds 10
            # UMI bases; the reprocessing pipeline shortens soloUMIlen the same
            # way rather than refuse the run.
            "umi_short": umi < layout.umi,
        }
    if call.get("cdna_read") is not None:
        out["cdna"] = {"read": call["cdna_read"]}
    return out


def _countable(call: dict[str, Any], fit: bool | None) -> Layout | None:
    """The layout, if the call is one STARsolo can be told how to count."""
    layout = BY_ID.get(call.get("layout") or "")
    if (
        layout is None
        or not layout.starsolo
        or layout.modality != "gex"
        or call.get("level") not in ("HIGH", "MEDIUM")
        or call.get("library_type") not in (None, "GEX")
        or fit is False
    ):
        return None
    return layout


def mapping(
    shape: dict[str, Any],
    call: dict[str, Any],
    fit: bool | None,
    *,
    whitelists: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    """The STARsolo options and Cell Ranger chemistry for a HIGH or MEDIUM 10x GEX call.

    ``shape`` is :func:`geometry`'s. ``whitelists`` maps a list id to the path
    it was exported to; without one the block names the list's file.
    ``None`` for anything else -- ATAC, VDJ, a weak call, an unfit copy.
    """
    layout = _countable(call, fit)
    barcode = shape.get("barcode")
    if layout is None or barcode is None:
        return None
    entry = REGISTER[layout.barcode_list]
    reads = shape["reads"]
    omitted: list[str] = []
    solo: dict[str, Any] = {
        "soloType": "CB_UMI_Simple",
        "soloCBwhitelist": (whitelists or {}).get(entry.id, entry.file_name),
        "soloCBstart": barcode["cb_start"],
        "soloCBlen": barcode["cb_len"],
        "soloUMIstart": barcode["umi_start"],
        "soloUMIlen": barcode["umi_len"],
    }
    bc_read = reads[barcode["read"]]
    if bc_read["trimmed"] or bc_read["length"]["mode"] != (
        barcode["cb_start"] - 1 + barcode["cb_len"] + barcode["umi_len"]
    ):
        # STAR checks the barcode read is exactly CB + UMI unless told not to.
        solo["soloBarcodeReadLength"] = 0
    strand = ((call.get("evidence") or {}).get("strand") or {}).get("strand")
    if strand in SOLO_STRAND:
        solo["soloStrand"] = SOLO_STRAND[strand]
    else:
        omitted.append(f"soloStrand: the cDNA read's strand was {strand or 'not measured'}")
    # The files as they will be on disk: a FASTQ as listed, an SRA object as
    # fastq-dump splits it, a BAM's reads under the names its conversion gives.
    by_role: dict[str, list[str]] = {}
    for r in reads:
        name = r["name"]["listed"] or r["name"]["renamed"]
        if r["role"] in ("R1", "R2") and name:
            by_role.setdefault(r["role"], []).append(name)
    if "R1" in by_role and "R2" in by_role:
        solo["readFilesIn"] = [",".join(by_role["R2"]), ",".join(by_role["R1"])]
    else:
        omitted.append("readFilesIn: no cDNA read and barcode read pair")
    paired = _paired(layout, barcode, bc_read, solo)
    return {
        "starsolo": solo,
        "starsolo_args": starsolo_args(solo),
        "starsolo_paired": paired,
        "starsolo_paired_args": starsolo_args(paired) if paired else None,
        "cellranger_chemistry": CELLRANGER_CHEMISTRY.get(layout.id),
        "whitelist": {
            "id": entry.id,
            "file": entry.file_name,
            "sha256": entry.export_sha256,
            "count": entry.count,
            "path": (whitelists or {}).get(entry.id),
        },
        "omitted": omitted,
        **_validation(layout.id),
    }


def _validation(layout_id: str) -> dict[str, Any]:
    measured = VALIDATION.get(layout_id)
    if measured is None:
        return {"validated": False, "validation": None, "note": UNVALIDATED}
    return {
        "validated": True,
        "validation": {**measured, "source": VALIDATION_SOURCE},
        "note": (
            f"set beside {measured['samples']} held-out samples the reprocessing pipeline mapped: "
            f"the same whitelist in {measured['same_list']} of {measured['blocks']} blocks, "
            f"the same strand in {measured['same_strand']} of "
            f"{measured['blocks'] - measured['strand_omitted']} that named one"
            + (
                f"; in {measured['layout_other_strand']} the layout implies the other strand"
                if measured["layout_other_strand"]
                else ""
            )
        ),
    }


# Above this, a 5' library's barcode read carries cDNA after the barcode, UMI
# and TSO, and the reprocessing pipeline maps the two reads as a pair.
PAIRED_BARCODE_READ = 50


def _paired(
    layout: Layout, barcode: dict[str, Any], bc_read: dict[str, Any], solo: dict[str, Any]
) -> dict[str, Any] | None:
    """The paired-end form the reprocessing pipeline maps a long-R1 5' library with.

    ``starsolo_10x_auto.sh`` runs ``--readFilesIn R1 R2 --soloBarcodeMate 1
    --clip5pNbases 39 0 --soloStrand Forward`` when R1 is longer than 50 bases
    and the test mapping finds the library Reverse: the barcode read's cDNA,
    after barcode, UMI and the 13-base TSO, is mapped as the first mate, and the
    second mate then reads forward. This is the script's choice, given as an
    alternative, not this project's judgement of which is better.
    """
    if layout.tail is not Tail.TSO or bc_read["length"]["mode"] <= PAIRED_BARCODE_READ:
        return None
    files = solo.get("readFilesIn")
    if not files:
        return None
    paired = {k: v for k, v in solo.items() if k not in ("readFilesIn", "soloBarcodeReadLength")}
    clip = barcode["cb_start"] - 1 + barcode["cb_len"] + barcode["umi_len"] + len(TSO_PREFIX)
    paired.update(
        readFilesIn=[files[1], files[0]],
        soloBarcodeMate=1,
        clip5pNbases=[clip, 0],
        soloStrand="Forward",
    )
    return paired


def with_whitelist(block: dict[str, Any], path: str) -> None:
    """Point a mapping block at an exported whitelist file."""
    block["whitelist"]["path"] = path
    block["starsolo"]["soloCBwhitelist"] = path
    block["starsolo_args"] = starsolo_args(block["starsolo"])
    if block.get("starsolo_paired"):
        block["starsolo_paired"]["soloCBwhitelist"] = path
        block["starsolo_paired_args"] = starsolo_args(block["starsolo_paired"])


def starsolo_args(solo: dict[str, Any]) -> str:
    """The block as STAR's command-line options, ``readFilesIn`` first."""
    order = ["readFilesIn", *[k for k in solo if k != "readFilesIn"]]
    parts = []
    for key in order:
        if key not in solo:
            continue
        value = solo[key]
        text = " ".join(str(v) for v in value) if isinstance(value, list) else str(value)
        parts.append(f"--{key} {text}")
    return " ".join(parts)


def convertible(
    kind: str,
    checks: dict[str, Any],
    call: dict[str, Any],
    fit: bool | None,
    reason: str,
    run_spot: float | None = None,
) -> dict[str, Any]:
    """Whether a BAM, CRAM or SRA copy can be turned into the FASTQs a mapper reads.

    Positive evidence only, as for fit: ``no`` needs a sampled record to show
    the barcode is absent, and a copy that cannot be judged is ``None``. An SRA
    object converts when its split reads hold the called barcode and cDNA
    reads and its first spots account for the run (``run_spot``): DRR878211's
    first spots are 26 + 8 and its cDNA reads come later, in spots of their
    own, so splitting it gives files whose reads are not mates.
    """
    formats = {f["format"] for f in checks.get("files") or []}
    if kind not in ("fastq", "bam", "cram", "sra") and len(formats) == 1:
        kind = formats.pop()  # local files: what was read says what they are
    if kind == "fastq":
        return {"method": None, "convertible": None, "reason": "already FASTQ"}
    if kind in ("bam", "cram"):
        bam = checks.get("bam")
        if not bam:
            return {"method": None, "convertible": None, "reason": "no record was read"}
        records = bam.get("records") or 0
        tags = bam.get("tags") or {}
        if bam.get("bamtofastq"):
            return {
                "method": "bamtofastq",
                "convertible": True,
                "reason": "the 10x_bam_to_fastq header says how to rebuild "
                + ", ".join(sorted(bam["bamtofastq"])),
            }
        if tags.get("CR") and (tags.get("UR") or tags.get("RX")):
            umi = "UR/UY" if tags.get("UR") else "RX/QX"
            return {
                "method": "tags",
                "convertible": True,
                "reason": f"raw barcode CR/CY and UMI {umi} on {tags['CR']} of {records} sampled "
                "records: R1 is rebuilt from them, with no header to say so",
            }
        paired = bam.get("paired") or {}
        if (
            paired.get("first")
            and paired.get("second")
            and call.get("barcode_read") is not None
            and call.get("cdna_read") is not None
        ):
            # ERR13855116's lane CRAM holds both reads as mates (0x40/0x80):
            # 26-base barcode reads and 96-base cDNA reads, no tags needed.
            return {
                "method": "samtools fastq",
                "convertible": True,
                "reason": "the barcode read and the cDNA read are the records' mates "
                "(0x40/0x80): split by flag",
            }
        if (
            records
            and not tags.get("CR")
            and not tags.get("CB")
            and call.get("barcode_read") is None
        ):
            return {
                "method": None,
                "convertible": False,
                "reason": f"none of {records} sampled records carries a barcode tag, "
                "and no read of them is a barcode read",
            }
        return {"method": None, "convertible": None, "reason": "no way to rebuild the barcode read"}
    if kind == "sra":
        quality = sorted(set(checks.get("quality") or []))
        out: dict[str, Any] = {"method": "fastq-dump --split-files", "quality": quality}
        if "substituted" in quality:
            out["note"] = "an SRA Lite object: every quality is substituted"
        if fit is False:
            return {**out, "convertible": False, "reason": reason}
        short = short_of_run(checks.get("files") or [], run_spot)
        if short:
            return {**out, "convertible": None, "reason": short}
        if (
            call.get("layout")
            and call.get("barcode_read") is not None
            and call.get("cdna_read") is not None
        ):
            return {
                **out,
                "convertible": True,
                "reason": "the split reads hold the called barcode read and cDNA read",
            }
        return {**out, "convertible": None, "reason": reason}
    return {"method": None, "convertible": None, "reason": f"no conversion known for {kind}"}


# Where the reads cannot choose between 3' v2 and 5' v1/v2, the metadata may,
# on the terms of docs/decisions/when-metadata-may-break-a-tie.md: only that
# pair, only when the metadata names exactly one of the two, and offered beside
# the read-derived answer, never merged into it. Measured on the pipeline's own
# record (docs/benchmarks/metadata-vs-reads-2026-10.md, 2026-10-23).
TIE = frozenset({"10x_3p_v2", "10x_5p_v1_v2"})
TIE_VALIDATION = {
    "ties_settled": 9,
    "ties_as_the_pipeline_mapped": 9,
    "strand_agrees_at_scale": 946,
    "strand_compared_at_scale": 948,
    "source": "docs/benchmarks/metadata-vs-reads-2026-10.md",
}


def from_metadata(
    checks: dict[str, Any],
    call: dict[str, Any],
    fit: bool | None,
    metadata: ChemistryCall | None,
    *,
    run: str | None = None,
    whitelists: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    """A mapping block for a read call tied between 3' v2 and 5', with the metadata's kit.

    ``None`` unless the reads left exactly that tie and the metadata names one
    of the two and not the other. The block's ``soloStrand`` is the kit's, and
    says so: the read-derived block's rule, a strand only from the reads, is
    why this one is separate.
    """
    if metadata is None or call.get("level") != "ambiguous":
        return None
    if set(call.get("candidates") or []) != TIE:
        return None
    named = [c for c in metadata.candidates if c in TIE]
    if len(named) != 1:
        return None
    layout = BY_ID[named[0]]
    measured = ((call.get("evidence") or {}).get("strand") or {}).get("strand")
    if measured in SOLO_STRAND and measured != str(layout.strand):
        # One strand vote, too few to decide, against the metadata's kit: no tie
        # is broken against what the reads did measure.
        return None
    # The tied call places no UMI, so the geometry is laid out for the kit named.
    chosen = {**call, "layout": layout.id, "level": "MEDIUM"}
    shape = geometry(checks, chosen, run=run)
    block = mapping(shape, chosen, fit, whitelists=whitelists)
    if block is None:
        return None
    solo = dict(block["starsolo"])
    solo["soloStrand"] = SOLO_STRAND[str(layout.strand)]
    paired = _paired(layout, shape["barcode"], shape["reads"][shape["barcode"]["read"]], solo)
    return {
        **block,
        "layout": layout.id,
        "source": "metadata",
        "grounds": [str(m) for m in metadata.matches],
        "starsolo": solo,
        "starsolo_args": starsolo_args(solo),
        "starsolo_paired": paired,
        "starsolo_paired_args": starsolo_args(paired) if paired else None,
        "omitted": [w for w in block["omitted"] if not w.startswith("soloStrand")],
        "validated": True,
        "validation": TIE_VALIDATION,
        "note": (
            f"the reads could not choose between 3' v2 and 5' v1/v2; the metadata names "
            f"{layout.id}, and soloStrand is that kit's. Where the reads abstained, the "
            "metadata chose as the pipeline mapped in 9 of 9; at scale its strand agreed in "
            "946 of 948, both errors one study's protocol applied to every sample"
        ),
    }


def readiness(
    source: dict[str, Any],
    *,
    run: str | None = None,
    run_spot: float | None = None,
    whitelists: dict[str, str] | None = None,
    metadata: ChemistryCall | None = None,
) -> dict[str, Any]:
    """Geometry, mapping and conversion for one report source."""
    shape = geometry(source["checks"], source["call"], run=run)
    return {
        "geometry": shape,
        "mapping": mapping(shape, source["call"], source["fit"], whitelists=whitelists),
        "mapping_from_metadata": from_metadata(
            source["checks"],
            source["call"],
            source["fit"],
            metadata,
            run=run,
            whitelists=whitelists,
        ),
        "convertible": convertible(
            source["kind"],
            source["checks"],
            source["call"],
            source["fit"],
            source["reason"],
            run_spot,
        ),
    }


__all__ = [
    "CELLRANGER_CHEMISTRY",
    "SOLO_STRAND",
    "barcode_hit",
    "convertible",
    "from_metadata",
    "geometry",
    "mapping",
    "readiness",
    "roles",
    "starsolo_args",
    "with_whitelist",
]
