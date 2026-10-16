"""Every one-hop link the archives offer, declared by the mechanism that answers it.

A mechanism is one request shape and the fields of its answer. Most of them fan
out -- one ENA ``filereport`` request returns seven INSDC levels from any one of
them -- so links are generated from ``inputs x outputs`` rather than listed, and
a response's fan-out is declared exactly once. The INSDC objects are split by
archive (``nodes.py``), and the fan-out between them stays within one archive:
an SRP links to SRRs, never to ERRs (:meth:`Mechanism.pairs`).

What this module claims, and does not:

* **That a link exists.** Each is confirmed by one live request (``probe.py``)
  or is measured by a declared route. Neither says how often it answers.
* **What the documentation says.** ``basis`` is ``DOCUMENTED`` only where the
  archive's own documentation names the field or parameter the link is read
  from, and that was checked against the page on 2026-10-16. NCBI serves its
  documentation to scripts as a browser check, so its pages were read through
  the Internet Archive's copies: GEO's SOFT page documents ``!Series_sample_id``,
  ``!Sample_platform_id`` and ``!Sample_supplementary_file`` and none of the
  ``_relation`` lines, and the E-utilities book documents ``linkname`` and
  ``cmd=neighbor`` but not ``rettype=runinfo``. ``LIVE_ONLY`` links have no
  citation; their probe is the whole of the evidence.
* **Nothing about which link to use.** That is :mod:`fetch_series.graph`, and
  only a route with ``RouteEvidence`` can influence it. ``routes`` here only
  records which links a declared route covers.

Route-specific chains -- an ELink followed by EFetch, a SOFT file followed by an
ENA lookup -- declare only the link their route measures. The general fan-out of
the response at the end of the chain is declared once, on its plainest
mechanism (``sra_be_runinfo`` for a runinfo table), so the same link is not
counted three times under three spellings of the request that reaches it.

**Which archives a mechanism answers for** is declared from what it holds, not
from who runs it. ENA's portal answers for all three INSDC archives because it
mirrors them, and so do NCBI's SRA database and DDBJ's search; GEO only ever
brokers to NCBI's SRA, so a GEO record points at NCBI-issued objects and no
others. Where a field holds "an INSDC accession" without saying which archive's
(an SDRF's ``Comment[ENA_RUN]`` holds an SRR on an E-GEOD import), all three
are declared and the probes say which ever answer.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from fetch_series.accession import Archive
from fetch_series.catalogue.model import INSDC, Basis, Mechanism, Origin
from fetch_series.catalogue.nodes import (
    ANALYSIS_PROBES,
    SPLIT,
    TEN_X_CRAM_EBI,
    TEN_X_FAMILIES,
    TEN_X_FASTQ_EBI,
    TEN_X_ORIGINALS_NCBI,
    objects,
)

# --------------------------------------------------------------------------
# Citations, each checked to load on 2026-10-16.
# --------------------------------------------------------------------------

ENA_FILE_REPORTS = (
    "https://ena-docs.readthedocs.io/en/latest/retrieval/programmatic-access/file-reports.html"
)
ENA_FIELDS = "https://www.ebi.ac.uk/ena/portal/api/returnFields?result={result}"
ENA_PORTAL_SPEC = "https://www.ebi.ac.uk/ena/portal/api/swagger-ui/index.html"
ENA_XREF_SPEC = "https://www.ebi.ac.uk/ena/xref/rest/swagger-ui/index.html"
ENA_BROWSER_SPEC = "https://www.ebi.ac.uk/ena/browser/api/swagger-ui/index.html"
ENTREZ_LINKS = "https://eutils.ncbi.nlm.nih.gov/entrez/query/static/entrezlinks.html"
GEO_SOFT = "https://www.ncbi.nlm.nih.gov/geo/info/soft.html"
GEO_DOWNLOAD = "https://www.ncbi.nlm.nih.gov/geo/info/download.html"
MAGE_TAB = "https://www.fged.org/projects/mage-tab/"
DDBJ_SEARCH_SPEC = "https://github.com/ddbj/ddbj-search-api/blob/main/docs/api-spec.md"

ENA = "ENA Portal API"
NCBI = "NCBI E-utilities"

READ_FORMATS = ("fastq", "bam", "cram")
SRA_FORMATS = ("sra_normalized", "sra_lite")


def nodes(*groups: str, archives: tuple[Archive, ...] = INSDC) -> tuple[str, ...]:
    """Node ids for groups: an INSDC entity type's value stands for its archives' nodes.

    ``nodes("bioproject", "run")`` is the BioProject node and SRR, ERR and DRR;
    ``nodes("run", archives=(Archive.NCBI,))`` is SRR alone.
    """
    found: list[str] = []
    for group in groups:
        entity = next((e for e in SPLIT if e.value == group), None)
        found.extend(objects(entity, archives=archives) if entity else (group,))
    return tuple(found)


def fields(by_group: Mapping[str, str], archives: tuple[Archive, ...] = INSDC) -> dict[str, str]:
    """Expand ``group -> field`` into ``node -> field``."""
    return {
        node: read for group, read in by_group.items() for node in nodes(group, archives=archives)
    }


def each(targets: Iterable[str], read_from: str) -> dict[str, str]:
    """Several targets read from one field: a file column holds every format."""
    return dict.fromkeys(targets, read_from)


_EBI = (Archive.EBI,)
_NCBI_ONLY = (Archive.NCBI,)
_SHARED = ("bioproject", "biosample", "geo_series", "geo_sample", "ae_experiment", "ae_sample")


def _shared(family: Mapping[str, str]) -> dict[str, str]:
    """Probe the records shared across archives with one family's members."""
    return {node: family[node] for node in _SHARED if node in family}


def _small(family: Mapping[str, str]) -> dict[str, str]:
    """A family without its project and study, for a service that cannot list them."""
    return {k: v for k, v in family.items() if k != "bioproject" and not k.startswith("study_")}


def _into(target: str, family: Mapping[str, str]) -> dict[str, str]:
    """Probe every link into ``target`` from a family's members with that member."""
    return {f"{node}>{target}": accession for node, accession in family.items()}


# The seven INSDC levels ENA's read_run report accepts as `accession`. Its error
# message lists them: project, study, sample (both forms), experiment, run and
# submission. An analysis or an ArrayExpress accession is refused with a 400.
INSDC_FIELDS = {
    "bioproject": "study_accession",
    "study": "secondary_study_accession",
    "biosample": "sample_accession",
    "sample": "secondary_sample_accession",
    "experiment": "experiment_accession",
    "run": "run_accession",
    "submission": "submission_accession",
}
INSDC_INPUTS = nodes(*INSDC_FIELDS)
INSDC_OUTPUTS = fields(INSDC_FIELDS)


def _filereport(fields: str, result: str = "read_run") -> str:
    return f"GET ena/portal/api/filereport?accession={{acc}}&result={result}&fields={fields}"


# --------------------------------------------------------------------------
# ENA
# --------------------------------------------------------------------------

_ENA: tuple[Mechanism, ...] = (
    Mechanism(
        id="ena_filereport",
        name="filereport",
        api=ENA,
        endpoint=_filereport(",".join(INSDC_FIELDS.values())),
        citation_url=ENA_FILE_REPORTS,
        basis=Basis.DOCUMENTED,
        inputs=INSDC_INPUTS,
        outputs=INSDC_OUTPUTS,
        routes={
            ("bioproject", "run"): "bioproject->run:ena_filereport",
            ("bioproject", "study"): "bioproject->study:ena_filereport",
            ("bioproject", "experiment"): "bioproject->experiment:ena_filereport",
            ("bioproject", "biosample"): "bioproject->biosample:ena_filereport",
            ("biosample", "run"): "biosample->run:ena_filereport",
            ("study", "bioproject"): "study->bioproject:ena_filereport",
            ("study", "run"): "study->run:ena_filereport",
            ("experiment", "run"): "experiment->run:ena_filereport",
            ("experiment", "biosample"): "experiment->biosample:ena_filereport",
            ("sample", "run"): "sample->run:ena_filereport",
            ("run", "experiment"): "run->experiment:ena_filereport",
            ("run", "biosample"): "run->biosample:ena_filereport",
        },
        note=(
            "One row per run, carrying every INSDC level at once, from any one of "
            "them, for all three archives' records."
        ),
    ),
    Mechanism(
        id="ena_filereport_aliases",
        name="aliases",
        api=ENA,
        endpoint=_filereport("study_alias,sample_alias,experiment_alias,run_alias"),
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=INSDC_INPUTS,
        outputs={
            "geo_series": "study_alias",
            "geo_sample": "sample_alias / experiment_alias / run_alias",
            "ae_experiment": "study_alias, or an alias prefix",
        },
        routes={
            ("run", "geo_sample"): "run->geo_sample:ena_alias",
            ("run", "ae_experiment"): "run->ae_experiment:ena_alias",
        },
        note=(
            "ENA documents the aliases as the submitter's names for each record. That "
            "a GEO- or ArrayExpress-brokered submission names its records by their "
            "GEO or ArrayExpress accessions is a convention, not documented."
        ),
    ),
    Mechanism(
        id="ena_filereport_fastq",
        name="fastq_ftp",
        api=ENA,
        endpoint=_filereport("fastq_ftp,fastq_md5,fastq_bytes"),
        citation_url=ENA_FILE_REPORTS,
        basis=Basis.DOCUMENTED,
        inputs=INSDC_INPUTS,
        outputs={"fastq": "fastq_ftp"},
        routes={("run", "file"): "run->file:ena_fastq"},
        origin=Origin.GENERATED,
        # The EBI 10x family was submitted as a BAM, and ENA dumps no fastq
        # from one; E-MTAB-12893's runs have both.
        probe_with=_into("fastq", {k: v for k, v in TEN_X_FASTQ_EBI.items() if "_ebi" in k}),
        note="ENA's own fastqs, dumped from the run. Whether the barcode read survived is a check.",
    ),
    Mechanism(
        id="ena_filereport_submitted",
        name="submitted_ftp",
        api=ENA,
        endpoint=_filereport("submitted_ftp,submitted_md5,submitted_bytes,submitted_format"),
        citation_url=ENA_FILE_REPORTS,
        basis=Basis.DOCUMENTED,
        inputs=INSDC_INPUTS,
        outputs=each(READ_FORMATS, "submitted_ftp"),
        routes={("run", "file"): "run->file:ena_submitted"},
        origin=Origin.SUBMITTED,
        # A record shared across archives is probed with a 10x family that
        # has submitted files at ENA at all -- GEO's submissions to SRA have
        # none -- and each format with a family that submitted it.
        probe_with={
            **_shared(TEN_X_FAMILIES[Archive.EBI]),
            **_into("fastq", TEN_X_FASTQ_EBI),
            **_into("cram", TEN_X_CRAM_EBI),
        },
        note=(
            "What the submitter deposited, in whatever format. A run submitted to "
            "NCBI or DDBJ has none here: 0 of 2,754 such runs in the surveyed draw."
        ),
    ),
    Mechanism(
        id="ena_filereport_bam",
        name="bam_ftp",
        api=ENA,
        endpoint=_filereport("bam_ftp,bam_md5,bam_bytes"),
        citation_url=ENA_FIELDS.format(result="read_run"),
        basis=Basis.DOCUMENTED,
        inputs=INSDC_INPUTS,
        outputs={"bam": "bam_ftp"},
        origin=Origin.GENERATED,
    ),
    Mechanism(
        id="ena_filereport_sra",
        name="sra_ftp",
        api=ENA,
        endpoint=_filereport("sra_ftp,sra_md5,sra_bytes"),
        citation_url=ENA_FILE_REPORTS,
        basis=Basis.DOCUMENTED,
        inputs=INSDC_INPUTS,
        outputs=each(SRA_FORMATS, "sra_ftp"),
        routes={("run", "file"): "run->file:ena_sra"},
        origin=Origin.ARCHIVE_FORMAT,
        note="Documented and returnable, and empty for all 3,000 runs its route surveyed.",
    ),
    Mechanism(
        id="ena_filereport_analysis",
        name="filereport analysis",
        api=ENA,
        endpoint=_filereport("…", result="analysis"),
        citation_url=ENA_FIELDS.format(result="analysis"),
        basis=Basis.DOCUMENTED,
        # result=analysis refuses an experiment, run or submission accession.
        # ENA holds EBI's analyses and no others: SRZ4627089 and DRZ010000 both
        # answer with an empty report, and a search for SRZ* finds nothing.
        inputs=nodes("analysis", "bioproject", "study", "biosample", "sample", archives=_EBI),
        outputs=fields(
            {
                "analysis": "analysis_accession",
                "bioproject": "study_accession",
                "study": "secondary_study_accession",
                "biosample": "sample_accession",
                "sample": "secondary_sample_accession",
                "experiment": "experiment_accession",
                "run": "run_accession",
                "submission": "submission_accession",
            },
            archives=_EBI,
        ),
        probe_with=ANALYSIS_PROBES,
        note="List fields are semicolon-separated: an analysis can draw on several runs.",
    ),
    Mechanism(
        id="ena_filereport_analysis_files",
        name="analysis submitted_ftp",
        api=ENA,
        endpoint=_filereport("submitted_ftp,submitted_md5,submitted_bytes", result="analysis"),
        citation_url=ENA_FIELDS.format(result="analysis"),
        basis=Basis.DOCUMENTED,
        inputs=("analysis_ebi",),
        outputs=each(READ_FORMATS, "submitted_ftp"),
        origin=Origin.SUBMITTED,
        probe_with=ANALYSIS_PROBES,
        note="An analysis' own files: an alignment of its runs, not a run's reads.",
    ),
    Mechanism(
        id="ena_search_study",
        name="search study",
        api=ENA,
        endpoint='GET ena/portal/api/search?result=study&query=<field>="{acc}"',
        citation_url=ENA_FIELDS.format(result="study"),
        basis=Basis.DOCUMENTED,
        inputs=nodes("bioproject", "study", "geo_series"),
        outputs=fields(
            {
                "bioproject": "study_accession",
                "study": "secondary_study_accession",
                "geo_series": "geo_accession",
            }
        ),
        note=(
            "Answers in both directions, because geo_accession is a searchable field: "
            "GSE60450 finds PRJNA258286. ENA answers a burst of these with an HTML "
            "500, so they are paced."
        ),
    ),
    Mechanism(
        id="ena_search_study_parent",
        name="parent project",
        api=ENA,
        endpoint='GET ena/portal/api/search?result=study&query=study_accession="{acc}"'
        "&fields=parent_study_accession",
        citation_url=ENA_FIELDS.format(result="study"),
        basis=Basis.DOCUMENTED,
        inputs=("bioproject",),
        outputs={"bioproject": "parent_study_accession"},
        self_link=True,
        note="The umbrella project: PRJNA258286 sits under PRJNA9559.",
    ),
    Mechanism(
        id="ena_search_sample",
        name="search sample",
        api=ENA,
        endpoint='GET ena/portal/api/search?result=sample&query=<field>="{acc}"',
        citation_url=ENA_FIELDS.format(result="sample"),
        basis=Basis.DOCUMENTED,
        inputs=nodes("biosample", "sample"),
        outputs=fields(
            {
                "biosample": "sample_accession",
                "sample": "secondary_sample_accession",
                "bioproject": "study_accession",
                "submission": "submission_accession",
            }
        ),
    ),
    Mechanism(
        id="ena_links",
        name="links",
        api=ENA,
        endpoint="GET ena/portal/api/links/{study|sample}?accession={acc}&result=…",
        citation_url=ENA_PORTAL_SPEC,
        basis=Basis.DOCUMENTED,
        # A secondary accession (ERP, ERS) answers with an empty table, not an
        # error: only the primary BioProject and BioSample accessions work.
        inputs=("bioproject", "biosample"),
        outputs=fields(
            {
                "run": "result=read_run",
                "experiment": "result=read_experiment",
                "biosample": "result=sample",
            }
        )
        | {"analysis_ebi": "result=analysis"},
        # Analyses need a project that has some; the EBI family's has none.
        probe_with={
            "bioproject>analysis_ebi": ANALYSIS_PROBES["bioproject"],
            "biosample>analysis_ebi": ANALYSIS_PROBES["biosample"],
        },
        note=(
            "Takes only the primary accession: ERP017126 and ERS1348472 return an "
            "empty table where PRJEB15401 and SAMEA4437023 return their records."
        ),
    ),
    Mechanism(
        id="ena_xref_source",
        name="xref",
        api="ENA cross-reference service",
        endpoint="GET ena/xref/rest/json/search?source={GEO|ArrayExpress}&source_accession={acc}",
        citation_url=ENA_XREF_SPEC,
        basis=Basis.DOCUMENTED,
        inputs=("geo_series", "ae_experiment"),
        outputs=fields(
            {"bioproject": "Target Primary Accession", "study": "Target Secondary Accession"}
        ),
        note="Without source= the same accession finds nothing.",
    ),
    Mechanism(
        id="ena_xref_target",
        name="xref",
        api="ENA cross-reference service",
        endpoint="GET ena/xref/rest/json/search?accession={acc}",
        citation_url=ENA_XREF_SPEC,
        basis=Basis.DOCUMENTED,
        inputs=nodes("bioproject", "study"),
        outputs={
            "geo_series": "Source Primary Accession where Source is GEO",
            "ae_experiment": "Source Primary Accession where Source is ArrayExpress",
        },
    ),
    Mechanism(
        id="ena_browser_xml",
        name="browser XML",
        api="ENA Browser API",
        endpoint="GET ena/browser/api/xml/{acc}",
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=("bioproject",),
        outputs={
            "geo_series": 'IDENTIFIERS/EXTERNAL_ID[@namespace="GEO"]',
            "ae_experiment": "PROJECT_LINKS/URL_LINK to arrayexpress",
        },
        note=(
            "Everything else in the XML is in filereport already "
            "(docs/decisions/ena-browser-xml-not-needed.md). On a run record the GEO "
            "identifier was present for 1 of 5 runs checked."
        ),
    ),
    # --- metadata documents ---------------------------------------------------
    Mechanism(
        id="ena_filereport_document",
        name="filereport",
        api=ENA,
        endpoint=_filereport("<metadata fields>"),
        citation_url=ENA_FILE_REPORTS,
        basis=Basis.DOCUMENTED,
        inputs=nodes("experiment", "run"),
        outputs={"metadata_ebi": "the read_run report, as TSV"},
        note="Confirmed when it downloads and parses as a report with a row for the record.",
    ),
    Mechanism(
        id="ena_browser_xml_document",
        name="ENA XML",
        api="ENA Browser API",
        endpoint="GET ena/browser/api/xml/{acc}",
        citation_url=ENA_BROWSER_SPEC,
        basis=Basis.DOCUMENTED,
        inputs=nodes("experiment", "run"),
        outputs={"metadata_ebi": "the record's XML"},
        note="Confirmed when it downloads and parses as XML naming the record.",
    ),
)

# --------------------------------------------------------------------------
# NCBI E-utilities and the SRA backend
# --------------------------------------------------------------------------


def _elink(
    linkname: str,
    sources: tuple[str, ...],
    targets: dict[str, str],
    *,
    route: tuple[tuple[str, str], str] | None = None,
    self_link: bool = False,
    probe_with: dict[str, str] | None = None,
) -> Mechanism:
    dbfrom, _, rest = linkname.partition("_")
    db = rest.split("_")[0]
    return Mechanism(
        id=f"elink_{linkname}",
        name="ELink",
        api=NCBI,
        endpoint=f"esearch db={dbfrom} term={{acc}}, elink linkname={linkname} cmd=neighbor, esummary db={db}",
        citation_url=ENTREZ_LINKS,
        basis=Basis.DOCUMENTED,
        inputs=sources,
        outputs=targets,
        self_link=self_link,
        routes=dict([route]) if route else {},
        probe_with=probe_with or {},
    )


# db=sra is indexed by experiment: ESummary of an sra UID is one experiment
# with its runs nested inside it, and it holds the EBI and DDBJ experiments
# NCBI mirrors as well as its own. db=gds holds series, samples and platforms
# under one UID space, told apart by prefix.
_EXPERIMENTS = fields({"experiment": "Experiment@acc in ExpXml"})

_ELINK: tuple[Mechanism, ...] = (
    _elink(
        "bioproject_biosample_all",
        ("bioproject",),
        {"biosample": "biosample accession"},
        route=(("bioproject", "biosample"), "bioproject->biosample:elink"),
    ),
    _elink("bioproject_sra_all", ("bioproject",), _EXPERIMENTS),
    _elink(
        "bioproject_gds",
        ("bioproject",),
        {"geo_series": "gds accession (GSE)"},
        route=(("bioproject", "geo_series"), "bioproject->geo_series:elink"),
    ),
    # The undirected bioproject_bioproject link answers nothing; the umbrella
    # relation is carried by the directed pair. NCBI records no umbrella for
    # PRJNA258286, where ENA's parent_study_accession names PRJNA9559.
    _elink(
        "bioproject_bioproject_u2d",
        ("bioproject",),
        {"bioproject": "a child project"},
        self_link=True,
        probe_with={"bioproject": "PRJNA988804"},
    ),
    _elink(
        "bioproject_bioproject_d2u",
        ("bioproject",),
        {"bioproject": "the umbrella project"},
        self_link=True,
        probe_with={"bioproject": "PRJNA988806"},
    ),
    _elink("biosample_bioproject", ("biosample",), {"bioproject": "project accession"}),
    _elink("biosample_sra", ("biosample",), _EXPERIMENTS),
    _elink("biosample_gds", ("biosample",), {"geo_sample": "gds accession (GSM)"}),
    _elink("sra_bioproject", nodes("experiment"), {"bioproject": "project accession"}),
    _elink("sra_biosample", nodes("experiment"), {"biosample": "biosample accession"}),
    _elink(
        "sra_gds",
        nodes("experiment"),
        {"geo_sample": "gds accession (GSM)", "geo_series": "gds accession (GSE)"},
    ),
    _elink("gds_bioproject", ("geo_series", "geo_sample"), {"bioproject": "project accession"}),
    _elink("gds_biosample", ("geo_series", "geo_sample"), {"biosample": "biosample accession"}),
    _elink(
        "gds_sra",
        ("geo_series", "geo_sample"),
        fields({"experiment": "Experiment@acc in ExpXml"}, archives=(Archive.NCBI,)),
        route=(("geo_series", "experiment"), "gse->experiment:elink_gds_sra"),
    ),
)

RUNINFO_FIELDS = {
    "run": "Run",
    "experiment": "Experiment",
    "study": "SRAStudy",
    "bioproject": "BioProject",
    "sample": "Sample",
    "biosample": "BioSample",
    "geo_sample": "SampleName",
    "submission": "Submission",
}
RUNINFO_OUTPUTS = fields(RUNINFO_FIELDS)


def _chain(
    id: str,
    name: str,
    endpoint: str,
    route: tuple[tuple[str, str], str],
    via: tuple[str, ...] = (),
    read_from: str = "Run",
    targets: tuple[str, ...] | None = None,
) -> Mechanism:
    """A measured route's own request chain, declaring only the link it measures."""
    (source, target), route_id = route
    return Mechanism(
        id=id,
        name=name,
        api=NCBI,
        endpoint=endpoint,
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=(source,),
        outputs=dict.fromkeys(targets or nodes(target), read_from),
        via=via,
        routes={(source, target): route_id},
    )


# SDL resolves any of these itself, and answers for EBI- and DDBJ-issued runs
# as well as NCBI's: ERR4265294 and DRR188691 return their objects.
SDL_INPUTS = nodes(
    "run", "experiment", "sample", "study", "bioproject", "biosample", "geo_series", "geo_sample"
)

_NCBI: tuple[Mechanism, ...] = (
    *_ELINK,
    _chain(
        "esearch_sra_efetch_runinfo",
        "esearch + runinfo",
        "esearch db=sra term={acc}[GPRJ], efetch rettype=runinfo",
        (("bioproject", "run"), "bioproject->run:efetch_direct"),
    ),
    _chain(
        "esearch_sra_sra_be",
        "esearch + sra-db-be",
        "esearch db=sra term={acc}[GPRJ], sra-db-be.cgi rettype=runinfo by history",
        (("bioproject", "run"), "bioproject->run:sra_be_direct_cgi"),
    ),
    _chain(
        "elink_bioproject_sra_efetch_runinfo",
        "ELink + runinfo",
        "esearch db=bioproject term={acc}, elink bioproject->sra, efetch rettype=runinfo",
        (("bioproject", "run"), "bioproject->run:efetch_elink"),
        via=nodes("experiment"),
    ),
    _chain(
        "elink_bioproject_sra_sra_be",
        "ELink + sra-db-be",
        "esearch db=bioproject term={acc}, elink bioproject->sra, sra-db-be.cgi rettype=runinfo",
        (("bioproject", "run"), "bioproject->run:sra_be_elink"),
        via=nodes("experiment"),
    ),
    # A GEO series links only to NCBI-issued records: GEO brokers its
    # submissions to NCBI's SRA and to no other archive.
    _chain(
        "elink_gds_sra_efetch_runinfo",
        "ELink + runinfo",
        "esearch db=gds term={acc}, elink gds->sra, efetch rettype=runinfo",
        (("geo_series", "run"), "gse->run:elink_gds_sra"),
        via=("experiment_ncbi",),
        targets=("run_ncbi",),
    ),
    _chain(
        "esearch_gds",
        "esearch gds",
        "esearch db=gds term={acc}[ALL], esummary db=gds",
        (("bioproject", "geo_series"), "bioproject->geo_series:gds_direct"),
        read_from="accession of each GSE hit",
    ),
    Mechanism(
        id="sra_be_runinfo",
        name="runinfo",
        api="NCBI SRA backend",
        endpoint="GET trace.ncbi.nlm.nih.gov/Traces/sra-db-be/runinfo?acc={acc}",
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        # No ESearch: the backend resolves the accession itself, from any of
        # these. A submission accession is answered with an empty body.
        inputs=nodes(
            "bioproject",
            "study",
            "biosample",
            "sample",
            "experiment",
            "run",
            "geo_series",
            "geo_sample",
        ),
        outputs=RUNINFO_OUTPUTS,
        note=(
            "One row per run with every INSDC level and, for a GEO submission, the "
            "GSM in SampleName. The same backend the sra_be routes call by history."
        ),
    ),
    Mechanism(
        id="esummary_sra",
        name="ESummary sra",
        api=NCBI,
        endpoint="esearch db=sra term={acc}, esummary db=sra",
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        # ESearch db=sra finds every one of these, GSM included, and not a GSE.
        inputs=nodes(
            "bioproject",
            "study",
            "biosample",
            "sample",
            "experiment",
            "run",
            "submission",
            "geo_sample",
        ),
        outputs=fields(
            {
                "experiment": "ExpXml Experiment@acc",
                "run": "Runs Run@acc",
                "study": "ExpXml Study@acc",
                "sample": "ExpXml Sample@acc",
                "bioproject": "ExpXml Bioproject",
                "biosample": "ExpXml Biosample",
                "submission": "ExpXml Submitter@acc",
            }
        ),
        note="The accessions are inside an XML string inside the JSON document summary.",
    ),
    Mechanism(
        id="sdl",
        name="SDL",
        api="NCBI Storage Data Locator",
        endpoint="POST locate.ncbi.nlm.nih.gov/sdl/2/retrieve acc={acc}",
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=SDL_INPUTS,
        outputs={
            "sra_normalized": "files[type=sra].locations[].link",
            **each(READ_FORMATS, "files[type!=sra].locations[].link, where public"),
        },
        routes={("run", "file"): "run->file:sdl"},
        origin={
            "sra_normalized": Origin.ARCHIVE_FORMAT,
            **dict.fromkeys(READ_FORMATS, Origin.SUBMITTED),
        },
        # The submitter's files come back only where they sit in a public
        # bucket, which none of the main 10x families' do. SDL answers a large
        # project or study with a 504 (PRJNA495040, ERP110462), so those are
        # asked of a family small enough to list.
        probe_with={
            **_into("fastq", _small(TEN_X_ORIGINALS_NCBI)),
            **_into("fastq", {k: v for k, v in TEN_X_FASTQ_EBI.items() if "_ebi" in k}),
            "bioproject>fastq": TEN_X_FASTQ_EBI["bioproject"],
            **_into("cram", _small({k: v for k, v in TEN_X_CRAM_EBI.items() if "_ebi" in k})),
        },
        note=(
            "Undocumented: the service's own root answers 'Documentation is missed'. "
            "It offers the Normalized object, named for its accession, for EBI and "
            "DDBJ runs as well as NCBI's, and the submitter's own files beside it "
            "where they sit in a public bucket: fastqs for 29 of the 3,000 runs its "
            "route surveyed and CRAMs for 4."
        ),
    ),
    Mechanism(
        id="sdl_lite",
        name="SDL sralite",
        api="NCBI Storage Data Locator",
        endpoint="POST locate.ncbi.nlm.nih.gov/sdl/2/retrieve acc={acc}&filetype=sralite",
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=SDL_INPUTS,
        outputs={"sra_lite": "files[type=sra, name *.lite].locations[].link"},
        origin=Origin.ARCHIVE_FORMAT,
        note=(
            "filetype=sralite, or capability ZQA:Z as sra-tools sends it, swaps the "
            "Normalized object for SRA Lite: SRR25056225.lite, 10.3 GB against 13.3 GB."
        ),
    ),
    Mechanism(
        id="sdl_original",
        name="SDL originals",
        api="NCBI Storage Data Locator",
        endpoint=(
            "POST locate.ncbi.nlm.nih.gov/sdl/2/retrieve acc={acc}&accept-alternate-locations=yes"
        ),
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=SDL_INPUTS,
        outputs=each(READ_FORMATS, "files[type!=sra]: the submitter's files"),
        origin=Origin.SUBMITTED,
        note=(
            "Lists the files as submitted -- SRR25056225's TSF2_S35_L002_R1_001.fastq.gz "
            "and R2, barcode read and all -- with an empty link: they sit in cold "
            "storage, reachable through NCBI's Cloud Data Delivery and not by download."
        ),
    ),
    Mechanism(
        id="biosample_record",
        name="BioSample record",
        api=NCBI,
        endpoint="esearch db=biosample term={acc}[accession], efetch db=biosample",
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=("biosample",),
        outputs={
            **fields({"sample": 'Ids/Id[@db="SRA"]'}),
            "geo_sample": 'Ids/Id[@db="GEO"]',
            "bioproject": 'Links/Link[@target="bioproject"]',
            "metadata_biosample": "the BioSample XML, attributes and all",
        },
        note=(
            "Resolved through ESearch, never efetch id=: efetch strips an accession's "
            "prefix and returns a different sample for every SAMEA and SAMD."
        ),
    ),
    Mechanism(
        id="sra_be_runinfo_document",
        name="runinfo",
        api="NCBI SRA backend",
        endpoint="GET trace.ncbi.nlm.nih.gov/Traces/sra-db-be/runinfo?acc={acc}",
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=nodes("experiment", "run"),
        outputs={"metadata_ncbi": "the runinfo table, as CSV"},
        note="Confirmed when it downloads and parses as runinfo with a row for the record.",
    ),
    Mechanism(
        id="efetch_sra_xml",
        name="SRA XML",
        api=NCBI,
        endpoint="esearch db=sra term={acc}, efetch db=sra rettype=full retmode=xml",
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=nodes("experiment", "run"),
        outputs={"metadata_ncbi": "EXPERIMENT_PACKAGE_SET"},
        note="Confirmed when it downloads and parses as XML naming the record.",
    ),
    Mechanism(
        id="sra_be_runinfo_download",
        name="download_path",
        api="NCBI SRA backend",
        endpoint="GET trace.ncbi.nlm.nih.gov/Traces/sra-db-be/runinfo?acc={acc}",
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=SDL_INPUTS,
        outputs={"sra_lite": "download_path"},
        origin=Origin.ARCHIVE_FORMAT,
        note=(
            "runinfo's one file column points at the SRA Lite object (…/SRR1552444.sralite.1), "
            "with no size and no checksum."
        ),
    ),
    Mechanism(
        id="efetch_sra_files",
        name="SRA XML files",
        api=NCBI,
        endpoint="esearch db=sra term={acc}, efetch db=sra rettype=full retmode=xml",
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=nodes("experiment", "run"),
        outputs={
            "sra_normalized": 'RUN/SRAFiles/SRAFile[@semantic_name="SRA Normalized"]',
            "sra_lite": 'RUN/SRAFiles/SRAFile[@semantic_name="SRA Lite"]',
        },
        origin=Origin.ARCHIVE_FORMAT,
        note=(
            "The one place NCBI names the two formats outright, with size and md5 for "
            "each, beside the run's Statistics -- the per-read lengths that say "
            "whether the barcode read was loaded."
        ),
    ),
)

# --------------------------------------------------------------------------
# GEO
# --------------------------------------------------------------------------

SOFT_FAMILY = "GET ftp.ncbi.nlm.nih.gov/geo/series/GSEnnn/{acc}/soft/{acc}_family.soft.gz"
ACC_CGI = "GET ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={acc}&targ=self&form=text&view=brief"
SUPERSERIES = "GSE236087"
GEO_SAMPLE_PROBE = "GSM1480291"

_GEO: tuple[Mechanism, ...] = (
    Mechanism(
        id="geo_soft_family",
        name="SOFT",
        api="GEO FTP",
        endpoint=SOFT_FAMILY,
        citation_url=GEO_SOFT,
        basis=Basis.DOCUMENTED,
        inputs=("geo_series",),
        outputs={
            "geo_sample": "^SAMPLE / !Series_sample_id",
            "geo_platform": "^PLATFORM / !Sample_platform_id",
            "geo_supplementary": "!Series_supplementary_file / !Sample_supplementary_file",
            "metadata_geo": "the SOFT family file",
        },
        routes={("geo_series", "geo_sample"): "gse->geo_sample:soft_family"},
    ),
    Mechanism(
        id="geo_soft_family_relations",
        name="SOFT relations",
        api="GEO FTP",
        endpoint=SOFT_FAMILY,
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=("geo_series",),
        outputs={
            "bioproject": "!Series_relation = BioProject",
            "study_ncbi": "!Series_relation = SRA",
            "geo_series": "!Series_relation = SuperSeries of / SubSeries of",
            "experiment_ncbi": "!Sample_relation = SRA",
            "biosample": "!Sample_relation = BioSample",
        },
        self_link=True,
        routes={
            ("geo_series", "bioproject"): "gse->bioproject:soft_family",
            ("geo_series", "experiment"): "gse->experiment:soft_family",
        },
        probe_with={"geo_series>geo_series": SUPERSERIES},
        note=(
            "GEO's SOFT documentation describes the fields a submitter writes; the "
            "relation lines are added by GEO and are not among them."
        ),
    ),
    Mechanism(
        id="soft_bioproject_ena",
        name="SOFT → filereport",
        api="GEO FTP + ENA Portal API",
        endpoint="!Series_relation = BioProject from {acc}'s SOFT file, then ENA filereport for it",
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=("geo_series",),
        # ENA is asked about the BioProject, so it answers with whatever its
        # runs are; a GEO series' runs are NCBI's.
        outputs=fields({"experiment": "experiment_accession"}),
        via=("bioproject",),
        routes={("geo_series", "experiment"): "gse->experiment:soft_bioproject_ena"},
    ),
    Mechanism(
        id="geo_miniml",
        name="MINiML",
        api="GEO FTP",
        endpoint="GET ftp.ncbi.nlm.nih.gov/geo/series/GSEnnn/{acc}/miniml/{acc}_family.xml.tgz",
        citation_url=GEO_DOWNLOAD,
        basis=Basis.DOCUMENTED,
        inputs=("geo_series",),
        outputs={"metadata_geo": "the MINiML family archive"},
        note="Confirmed when it downloads and its XML parses.",
    ),
    Mechanism(
        id="geo_series_matrix",
        name="series matrix",
        api="GEO FTP",
        endpoint="GET ftp.ncbi.nlm.nih.gov/geo/series/GSEnnn/{acc}/matrix/{acc}_series_matrix.txt.gz",
        citation_url=GEO_DOWNLOAD,
        basis=Basis.DOCUMENTED,
        inputs=("geo_series",),
        outputs={"metadata_geo": "the series matrix"},
        note="Confirmed when it downloads and names the series in !Series_geo_accession.",
    ),
    Mechanism(
        id="geo_acc_series",
        name="acc.cgi",
        api="GEO accession display",
        endpoint=ACC_CGI,
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=("geo_series",),
        outputs={
            "geo_sample": "!Series_sample_id",
            "geo_platform": "!Series_platform_id",
            "bioproject": "!Series_relation = BioProject",
            "study_ncbi": "!Series_relation = SRA",
            "geo_series": "!Series_relation = SuperSeries of / SubSeries of",
            "geo_supplementary": "!Series_supplementary_file",
            "metadata_geo": "the series' SOFT text",
        },
        self_link=True,
        probe_with={"geo_series>geo_series": SUPERSERIES},
        note="acc.cgi's text form and its targ/view parameters are not documented.",
    ),
    Mechanism(
        id="geo_acc_sample",
        name="acc.cgi",
        api="GEO accession display",
        endpoint=ACC_CGI,
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=("geo_sample",),
        outputs={
            "experiment_ncbi": "!Sample_relation = SRA",
            "biosample": "!Sample_relation = BioSample",
            "geo_series": "!Sample_series_id",
            "geo_platform": "!Sample_platform_id",
            "geo_supplementary": "!Sample_supplementary_file",
            "metadata_geo": "the sample's SOFT text",
        },
        routes={
            ("geo_sample", "experiment"): "geo_sample->experiment:acc_cgi",
            ("geo_sample", "biosample"): "geo_sample->biosample:acc_cgi",
            ("geo_sample", "geo_series"): "geo_sample->geo_series:acc_cgi",
        },
    ),
    Mechanism(
        id="geo_acc_platform",
        name="acc.cgi",
        api="GEO accession display",
        endpoint=ACC_CGI,
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=("geo_platform",),
        outputs={"geo_series": "!Platform_series_id", "geo_sample": "!Platform_sample_id"},
        note="Large: GPL13112's record lists 8,834 series and 195,729 samples, 6.9 MB.",
    ),
    Mechanism(
        id="gds_esummary_series",
        name="ESummary gds",
        api=NCBI,
        endpoint="esearch db=gds term={acc}[ACCN] AND GSE[ETYP], esummary db=gds",
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=("geo_series",),
        outputs={
            "bioproject": "bioproject",
            "geo_sample": "samples[].accession",
            "geo_platform": "gpl",
            "study_ncbi": "extrelations[relationtype=SRA].targetobject",
        },
        routes={
            ("geo_series", "bioproject"): "gse->bioproject:gds_summary",
            ("geo_series", "geo_sample"): "gse->geo_sample:gds_summary",
        },
    ),
    Mechanism(
        id="gds_esummary_sample",
        name="ESummary gds",
        api=NCBI,
        # [ACCN] alone also matches the sample's series and platform.
        endpoint="esearch db=gds term={acc}[ACCN] AND GSM[ETYP], esummary db=gds",
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=("geo_sample",),
        outputs={
            "geo_series": "gse",
            "geo_platform": "gpl",
            "experiment_ncbi": "extrelations[relationtype=SRA].targetobject",
        },
    ),
    Mechanism(
        id="gds_esummary_platform",
        name="ESummary gds",
        api=NCBI,
        endpoint="esearch db=gds term={acc}[ACCN] AND GPL[ETYP], esummary db=gds",
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=("geo_platform",),
        outputs={"geo_series": "gse, as semicolon-separated numbers"},
    ),
)

# --------------------------------------------------------------------------
# ArrayExpress and BioStudies
# --------------------------------------------------------------------------

EGEOD = "E-GEOD-60450"
SDRF = "GET biostudies/files/{acc}/{acc}.sdrf.txt"

_BIOSTUDIES: tuple[Mechanism, ...] = (
    Mechanism(
        id="biostudies_idf",
        name="IDF",
        api="BioStudies files",
        endpoint="GET biostudies/files/{acc}/{acc}.idf.txt",
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=("ae_experiment",),
        outputs={
            **fields({"study": "Comment[SecondaryAccession] (ERP/SRP)"}),
            "geo_series": "Comment[SecondaryAccession] (GSE, on an E-GEOD import)",
            "ae_experiment": "Comment[RelatedExperiment]",
            **fields({"run": "Comment[SequenceDataURI], a run range"}),
            "metadata_ae": "the IDF",
        },
        self_link=True,
        routes={("ae_experiment", "study"): "ae_experiment->study:idf_secondary"},
        probe_with={"ae_experiment>geo_series": EGEOD},
        note=(
            "MAGE-TAB allows any Comment[]; which ones ArrayExpress writes is its own "
            "convention. E-MTAB-6505 declares no secondary accession at all."
        ),
    ),
    Mechanism(
        id="biostudies_sdrf",
        name="SDRF",
        api="BioStudies files",
        endpoint=SDRF,
        citation_url=MAGE_TAB,
        basis=Basis.DOCUMENTED,
        inputs=("ae_experiment", "ae_sample"),
        outputs={
            "ae_sample": "Source Name",
            "ae_supplementary": "Derived Array Data File",
            "metadata_ae": "the SDRF",
        },
        note="The SDRF repeats columns; it is read as raw rows, never through a dict.",
    ),
    Mechanism(
        id="biostudies_sdrf_comments",
        name="SDRF comments",
        api="BioStudies files",
        endpoint=SDRF,
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=("ae_experiment", "ae_sample"),
        outputs={
            "biosample": "Comment[BioSD_SAMPLE]",
            **fields({"sample": "Comment[ENA_SAMPLE]"}),
            **fields({"experiment": "Comment[ENA_EXPERIMENT]"}),
            **fields({"run": "Comment[ENA_RUN]"}),
            "geo_sample": "Source Name, on an E-GEOD import ('GSM1480302 1')",
        },
        routes={("ae_experiment", "biosample"): "ae_experiment->biosample:sdrf"},
        probe_with={
            "ae_experiment>geo_sample": EGEOD,
            "ae_sample>geo_sample": f"{EGEOD}:GSM1480302 1",
        },
        note=(
            "ArrayExpress's own columns, not MAGE-TAB's. An E-GEOD import carries no "
            "BioSD_SAMPLE column at all: 0 of 1,550 in the surveyed draw."
        ),
    ),
    Mechanism(
        id="biostudies_sdrf_fastq",
        name="FASTQ_URI",
        api="BioStudies files",
        endpoint=SDRF,
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=("ae_experiment", "ae_sample"),
        outputs={"fastq": "Comment[FASTQ_URI]"},
        routes={("ae_experiment", "file"): "ae_experiment->file:sdrf"},
        origin=Origin.MIXED,
        note=(
            "Whichever copy the curator named. Of the 198,423 URIs the route's survey "
            "returned (sample:3000@arrayexpress-ena), 182,988 in 2,164 studies are "
            "ENA's generated fastqs (…/vol1/fastq/…) and 13,930 in 115 studies ENA's "
            "copy of the submitted files (…/vol1/run/…). E-MTAB-9221 names the "
            "submitter's own R1/R2 on the decommissioned ArrayExpress mirror, which "
            "is believed only where the study still registers the file."
        ),
    ),
    Mechanism(
        id="biostudies_files",
        name="files API",
        api="BioStudies API",
        endpoint="GET biostudies/api/v1/studies/{acc}/files?limit=…&offset=…",
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=("ae_experiment",),
        outputs={"ae_supplementary": "items[].path, where Type is a processed data file"},
        note="Pages silently at 25 without an explicit limit; the list is under items, not files.",
    ),
    Mechanism(
        id="biostudies_study",
        name="study JSON",
        api="BioStudies API",
        endpoint="GET biostudies/api/v1/studies/{acc}",
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=("ae_experiment", "biostudy"),
        outputs={
            **fields({"study": "section.links[].url (Type ENA)"}),
            "bioproject": "section.links[].url, by accession",
            "ae_experiment": "section.links[].url (Type Biostudies)",
            "geo_series": "section.links[].url, by accession",
        },
        self_link=True,
        note=(
            "E-MTAB-5061 links ERP017126 (ENA) and E-MTAB-5060 (Biostudies). A Europe "
            "PMC record's links carry no type at all, so links are read by the "
            "accession's shape."
        ),
    ),
    Mechanism(
        id="biostudies_search",
        name="search",
        api="BioStudies API",
        endpoint="GET biostudies/api/v1/arrayexpress/search?query={acc}",
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=nodes("study", "bioproject", "geo_series"),
        outputs={"ae_experiment": "hits[].accession"},
        routes={("study", "ae_experiment"): "study->ae_experiment:biostudies_search"},
        note=(
            "A full-text search, not a link lookup: a link_type parameter is accepted "
            "and ignored. Serves 20,000 hits and answers the next page with a 500."
        ),
    ),
    Mechanism(
        id="biostudies_search_all",
        name="search",
        api="BioStudies API",
        endpoint="GET biostudies/api/v1/search?query={acc}",
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=nodes("ae_experiment", "study"),
        outputs={"biostudy": "hits[].accession, S- prefix"},
        note=(
            "The same full-text search across every collection. Finds the Europe PMC "
            "records whose papers cite the accession, which are declared links in "
            "the other direction."
        ),
    ),
    # --- EBI BioSamples ---------------------------------------------------
    # The BioSamples API also offers filter=extd:ArrayExpress:<acc>, which
    # reads like an ArrayExpress -> BioSample link and is not one: for
    # E-MTAB-5061 it returns SAMEG318185, a sample *group*.
    Mechanism(
        id="biosamples_record",
        name="BioSamples",
        api="EBI BioSamples API",
        endpoint="GET biosamples/samples/{acc}",
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=("biosample",),
        outputs={
            **fields({"sample": 'characteristics["SRA accession"] / sraAccession'}),
            "metadata_biosample": "the sample's JSON, characteristics and all",
        },
        note="Answers for NCBI-issued samples too: SAMN02991530 carries SRS685538.",
    ),
    Mechanism(
        id="biosamples_filter",
        name="BioSamples",
        api="EBI BioSamples API",
        endpoint="GET biosamples/samples?filter=attr:SRA accession:{acc}",
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=nodes("sample"),
        outputs={"biosample": "_embedded.samples[].accession"},
        note="The filter syntax is documented; that the attribute is called 'SRA accession' is not.",
    ),
)

# --------------------------------------------------------------------------
# DDBJ
# --------------------------------------------------------------------------

# DDBJ Search's record type for each group, as dblink spells it.
DBLINK_TYPES = {
    "bioproject": "bioproject",
    "biosample": "biosample",
    "study": "sra-study",
    "sample": "sra-sample",
    "experiment": "sra-experiment",
    "run": "sra-run",
    "submission": "sra-submission",
    "analysis": "sra-analysis",
    "geo_series": "geo",
}

_DDBJ: tuple[Mechanism, ...] = (
    Mechanism(
        id="ddbj_dblink",
        name="dblink",
        api="DDBJ Search API",
        endpoint="GET ddbj.nig.ac.jp/search/api/dblink/{type}/{acc}",
        citation_url=DDBJ_SEARCH_SPEC,
        basis=Basis.DOCUMENTED,
        inputs=nodes(*DBLINK_TYPES),
        outputs=fields({group: f"dbXrefs[type={kind}]" for group, kind in DBLINK_TYPES.items()}),
        note=(
            "Indexes NCBI and EBI records as well as DDBJ's own: PRJNA258286 and "
            "PRJEB15401 both answer. It backs no route; see "
            "docs/decisions/no-ddbj-provider.md."
        ),
    ),
)

DDBJ_ENTRY = "GET ddbj.nig.ac.jp/search/entry/sra-{run|experiment}/{acc}.json"

_DDBJ_ENTRY: tuple[Mechanism, ...] = (
    Mechanism(
        id="ddbj_entry_sra",
        name="entry SRA",
        api="DDBJ Search",
        endpoint=DDBJ_ENTRY,
        citation_url=DDBJ_SEARCH_SPEC,
        basis=Basis.DOCUMENTED,
        inputs=nodes("run"),
        outputs={
            "sra_normalized": "distribution[encodingFormat=SRA], under dra/sra/",
            "sra_lite": "distribution[encodingFormat=SRA], under dra/sralite/",
        },
        origin=Origin.ARCHIVE_FORMAT,
        note=(
            "DDBJ serves its own runs' Normalized objects and only SRA Lite for the "
            "runs it mirrors: DRR231743.sra under dra/sra/, ERR1630015.sra under "
            "dra/sralite/. No size and no checksum in the entry."
        ),
    ),
    Mechanism(
        id="ddbj_entry_fastq",
        name="entry FASTQ",
        api="DDBJ Search",
        endpoint=DDBJ_ENTRY + ", then the FASTQ directory it names",
        citation_url=DDBJ_SEARCH_SPEC,
        basis=Basis.DOCUMENTED,
        inputs=nodes("run", archives=(Archive.DDBJ,)),
        outputs={"fastq": "distribution[encodingFormat=FASTQ], a directory, listed"},
        origin=Origin.GENERATED,
        note=(
            "Only for DDBJ's own runs. The directory holds one bzip2 file per read, "
            "named for the run (DRR231743_1.fastq.bz2), not the submitter's names; "
            "its listing gives sizes rounded to a megabyte and no checksum."
        ),
    ),
    Mechanism(
        id="ddbj_entry_document",
        name="entry JSON",
        api="DDBJ Search",
        endpoint=DDBJ_ENTRY,
        citation_url=DDBJ_SEARCH_SPEC,
        basis=Basis.DOCUMENTED,
        inputs=nodes("experiment", "run"),
        outputs={"metadata_ddbj": "the entry, as JSON"},
        note="Answers for NCBI's and EBI's records as well as DDBJ's.",
    ),
    Mechanism(
        id="ddbj_dra_xml",
        name="DRA XML",
        api="DDBJ Search",
        endpoint=DDBJ_ENTRY + ", then distribution[encodingFormat=XML]",
        citation_url=DDBJ_SEARCH_SPEC,
        basis=Basis.DOCUMENTED,
        inputs=nodes("experiment", "run"),
        outputs={"metadata_ddbj": "the submission's DRA XML (DRA010287.run.xml)"},
        note="One XML per submission and record type, not per record.",
    ),
)

MECHANISMS: tuple[Mechanism, ...] = (
    *_ENA,
    *_NCBI,
    *_GEO,
    *_BIOSTUDIES,
    *_DDBJ,
    *_DDBJ_ENTRY,
)
