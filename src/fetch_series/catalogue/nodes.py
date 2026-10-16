"""The 18 kinds of record the catalogue links, one per level and database.

INSDC is merged: an ``SRS``, ``ERS`` and ``DRS`` are one entity under three
prefixes and one node, as are the experiment, run, study, submission and
analysis triples. GEO, ArrayExpress and BioStudies records are kept apart from
them, because they are curated records that *point at* INSDC ones rather than
being mirrors of them -- the same distinction :class:`EntityType` draws.
"""

from __future__ import annotations

from fetch_series.accession import EntityType
from fetch_series.catalogue.model import Level, Node

# Probe family, one fixed accession per node. Every one of them belongs to the
# same two linked studies the knowledge base already cites, so a probe that
# finds something can be checked by eye against the rest of its family:
#
#   GSE60450 -> PRJNA258286 -> SRP045534 -> GSM1480291 -> SAMN02991530
#            -> SRS685538 -> SRX681985 -> SRR1552444, submitted as SRA179382
#            on GPL13112.
#
# Its EBI counterpart, used wherever a mechanism only answers for an
# EBI-issued record, is E-MTAB-5061 -> PRJEB15401 / ERP017126 -> SAMEA4437023
# / ERS1348472 -> ERX1700348 -> ERR1630015, submitted as ERA705448.
#
# An ArrayExpress sample has no accession, so it is addressed as the study plus
# the row's Source Name -- the same spelling ENA uses for the sample's alias,
# `E-MTAB-5061:AZ_A11`.
NODES: tuple[Node, ...] = (
    # --- datasets ---------------------------------------------------------
    Node("bioproject", "BioProject", Level.DATASET, "BioProject", "insdc", EntityType.BIOPROJECT),
    Node("study", "INSDC study", Level.DATASET, "INSDC", "insdc", EntityType.STUDY),
    Node("geo_series", "GEO series", Level.DATASET, "GEO", "geo", EntityType.GEO_SERIES),
    Node(
        "ae_experiment",
        "ArrayExpress experiment",
        Level.DATASET,
        "ArrayExpress",
        "arrayexpress",
        EntityType.AE_EXPERIMENT,
    ),
    Node(
        "biostudy",
        "BioStudies study",
        Level.DATASET,
        "BioStudies",
        "arrayexpress",
        EntityType.BIOSTUDY,
        note=(
            "An S- accession: a BioStudies record outside the ArrayExpress collection. "
            "Europe PMC's S-EPMC records are the common case -- a paper's data "
            "citations, declared as links."
        ),
    ),
    # --- samples ----------------------------------------------------------
    Node("biosample", "BioSample", Level.SAMPLE, "BioSample", "insdc", EntityType.BIOSAMPLE),
    Node("sample", "INSDC sample", Level.SAMPLE, "INSDC", "insdc", EntityType.SAMPLE),
    Node("geo_sample", "GEO sample", Level.SAMPLE, "GEO", "geo", EntityType.GEO_SAMPLE),
    Node(
        "ae_sample",
        "ArrayExpress sample",
        Level.SAMPLE,
        "ArrayExpress",
        "arrayexpress",
        None,
        note=(
            "A row of an ArrayExpress SDRF. It has no accession of its own: it is "
            "addressed by the experiment plus the row's Source Name, so every link "
            "from it starts with fetching the experiment's SDRF. One row is one "
            "assay, so several rows can describe one specimen."
        ),
    ),
    # --- libraries, runs --------------------------------------------------
    Node("experiment", "INSDC experiment", Level.LIBRARY, "INSDC", "insdc", EntityType.EXPERIMENT),
    Node("run", "INSDC run", Level.RUN, "INSDC", "insdc", EntityType.RUN),
    # --- files ------------------------------------------------------------
    # Named after the archive whose record lists the file, not the host that
    # serves it: an SDRF's FASTQ_URI usually points at ENA's FTP server, and is
    # still ArrayExpress's statement about which files belong to the run.
    Node(
        "ena_files",
        "ENA files",
        Level.FILE,
        "ENA",
        "file",
        EntityType.FILE,
        note="Files ENA's own record lists: its derived fastqs, the submitted files, the SRA object.",
    ),
    Node(
        "sra_files",
        "NCBI SRA files",
        Level.FILE,
        "NCBI SRA",
        "file",
        EntityType.FILE,
        note="Files NCBI's Storage Data Locator lists, including cloud copies and their cost.",
    ),
    Node(
        "ae_files",
        "ArrayExpress files",
        Level.FILE,
        "ArrayExpress",
        "file",
        EntityType.FILE,
        note="Files an ArrayExpress study names in its SDRF or registers with BioStudies.",
    ),
    Node(
        "geo_files",
        "GEO supplementary files",
        Level.FILE,
        "GEO",
        "file",
        EntityType.FILE,
        note="A series' or sample's supplementary files on GEO's FTP server: processed data, not reads.",
    ),
    # --- other ------------------------------------------------------------
    Node(
        "submission",
        "INSDC submission",
        Level.OTHER,
        "INSDC",
        "insdc",
        EntityType.SUBMISSION,
    ),
    Node("analysis", "INSDC analysis", Level.OTHER, "INSDC", "insdc", EntityType.ANALYSIS),
    Node("geo_platform", "GEO platform", Level.OTHER, "GEO", "geo", EntityType.GEO_PLATFORM),
)

PROBE_ACCESSIONS: dict[str, str] = {
    "bioproject": "PRJNA258286",
    "study": "SRP045534",
    "geo_series": "GSE60450",
    "ae_experiment": "E-MTAB-5061",
    "biostudy": "S-EPMC9302581",
    "biosample": "SAMN02991530",
    "sample": "SRS685538",
    "geo_sample": "GSM1480291",
    "ae_sample": "E-MTAB-5061:AZ_A1",
    "experiment": "SRX681985",
    "run": "SRR1552444",
    "submission": "SRA179382",
    # GSE60450's family has no analysis object; this one is a public ATAC-seq
    # BAM whose report names its study, sample, experiment and four runs.
    "analysis": "ERZ10183054",
    "geo_platform": "GPL13112",
}

# The EBI half of the probe family, for mechanisms that only answer about an
# EBI-issued or ArrayExpress-brokered record.
EBI_PROBES: dict[str, str] = {
    "bioproject": "PRJEB15401",
    "study": "ERP017126",
    "biosample": "SAMEA4437023",
    "sample": "ERS1348472",
    "experiment": "ERX1700348",
    "run": "ERR1630015",
    "submission": "ERA705448",
}

# The analysis' own family, for mechanisms that report analyses.
ANALYSIS_PROBES: dict[str, str] = {
    "analysis": "ERZ10183054",
    "bioproject": "PRJEB44468",
    "study": "ERP128521",
    "biosample": "SAMEA7629265",
    "sample": "ERS5620409",
}
