"""Every one-hop link the archives offer, declared by the mechanism that answers it.

A mechanism is one request shape and the fields of its answer. Most of them fan
out -- one ENA ``filereport`` request returns seven INSDC levels from any one of
them -- so links are generated from ``inputs x outputs`` rather than listed, and
a response's fan-out is declared exactly once.

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
  records which link a declared route measures.

Route-specific chains -- an ELink followed by EFetch, a SOFT file followed by an
ENA lookup -- declare only the link their route measures. The general fan-out of
the response at the end of the chain is declared once, on its plainest
mechanism (``sra_be_runinfo`` for a runinfo table), so the same link is not
counted three times under three spellings of the request that reaches it.
"""

from __future__ import annotations

from fetch_series.catalogue.model import Basis, Mechanism
from fetch_series.catalogue.nodes import ANALYSIS_PROBES, EBI_PROBES

# --------------------------------------------------------------------------
# Citations, each checked to load on 2026-10-16.
# --------------------------------------------------------------------------

ENA_FILE_REPORTS = (
    "https://ena-docs.readthedocs.io/en/latest/retrieval/programmatic-access/file-reports.html"
)
ENA_FIELDS = "https://www.ebi.ac.uk/ena/portal/api/returnFields?result={result}"
ENA_PORTAL_SPEC = "https://www.ebi.ac.uk/ena/portal/api/swagger-ui/index.html"
ENA_XREF_SPEC = "https://www.ebi.ac.uk/ena/xref/rest/swagger-ui/index.html"
ENTREZ_LINKS = "https://eutils.ncbi.nlm.nih.gov/entrez/query/static/entrezlinks.html"
GEO_SOFT = "https://www.ncbi.nlm.nih.gov/geo/info/soft.html"
MAGE_TAB = "https://www.fged.org/projects/mage-tab/"
DDBJ_SEARCH_SPEC = "https://github.com/ddbj/ddbj-search-api/blob/main/docs/api-spec.md"

ENA = "ENA Portal API"
NCBI = "NCBI E-utilities"

# The seven INSDC nodes ENA's read_run report accepts as `accession`. Its error
# message lists them: project, study, sample (both forms), experiment, run and
# submission. An analysis or an ArrayExpress accession is refused with a 400.
INSDC_INPUTS = ("bioproject", "study", "biosample", "sample", "experiment", "run", "submission")
INSDC_OUTPUTS = {
    "bioproject": "study_accession",
    "study": "secondary_study_accession",
    "biosample": "sample_accession",
    "sample": "secondary_sample_accession",
    "experiment": "experiment_accession",
    "run": "run_accession",
    "submission": "submission_accession",
}


def _filereport(fields: str) -> str:
    return f"GET ena/portal/api/filereport?accession={{acc}}&result=read_run&fields={fields}"


def _ebi(inputs: tuple[str, ...], target: str | None = None) -> dict[str, str]:
    """Probe ``inputs`` with the EBI half of the family, for one target or all."""
    return {
        (f"{source}>{target}" if target else source): EBI_PROBES[source]
        for source in inputs
        if source in EBI_PROBES
    }


# --------------------------------------------------------------------------
# ENA
# --------------------------------------------------------------------------

_ENA: tuple[Mechanism, ...] = (
    Mechanism(
        id="ena_filereport",
        name="filereport",
        api=ENA,
        endpoint=_filereport(",".join(INSDC_OUTPUTS.values())),
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
            "them. Twelve of its 42 pairs are declared routes."
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
        # An alias names an ArrayExpress study only on a run ArrayExpress
        # brokered: SRR1552444's aliases are GSE60450 and GSM1480291,
        # ERR1630015's are E-MTAB-5061 and E-MTAB-5061:AZ_A11.
        probe_with=_ebi(INSDC_INPUTS, "ae_experiment"),
        note=(
            "ENA documents the aliases as the submitter's names for each record. That "
            "a GEO- or ArrayExpress-brokered submission names its records by their "
            "GEO or ArrayExpress accessions is a convention, not documented."
        ),
    ),
    Mechanism(
        id="ena_filereport_fastq",
        name="fastq",
        api=ENA,
        endpoint=_filereport("fastq_ftp"),
        citation_url=ENA_FILE_REPORTS,
        basis=Basis.DOCUMENTED,
        inputs=INSDC_INPUTS,
        outputs={"ena_files": "fastq_ftp"},
        routes={("run", "ena_files"): "run->file:ena_fastq"},
    ),
    Mechanism(
        id="ena_filereport_submitted",
        name="submitted",
        api=ENA,
        endpoint=_filereport("submitted_ftp"),
        citation_url=ENA_FILE_REPORTS,
        basis=Basis.DOCUMENTED,
        inputs=INSDC_INPUTS,
        outputs={"ena_files": "submitted_ftp"},
        routes={("run", "ena_files"): "run->file:ena_submitted"},
        # GEO's submissions to SRA carry no submitted files at ENA, so the
        # NCBI half of the family would probe empty for a reason that says
        # nothing about the mechanism.
        probe_with=_ebi(INSDC_INPUTS),
    ),
    Mechanism(
        id="ena_filereport_sra",
        name="sra_ftp",
        api=ENA,
        endpoint=_filereport("sra_ftp"),
        citation_url=ENA_FILE_REPORTS,
        basis=Basis.DOCUMENTED,
        inputs=INSDC_INPUTS,
        outputs={"ena_files": "sra_ftp"},
        routes={("run", "ena_files"): "run->file:ena_sra"},
        note="Documented and returnable, and empty for all 3,000 runs its route surveyed.",
    ),
    Mechanism(
        id="ena_filereport_analysis",
        name="filereport analysis",
        api=ENA,
        endpoint="GET ena/portal/api/filereport?accession={acc}&result=analysis&fields=…",
        citation_url=ENA_FIELDS.format(result="analysis"),
        basis=Basis.DOCUMENTED,
        # result=analysis refuses an experiment, run or submission accession.
        inputs=("analysis", "bioproject", "study", "biosample", "sample"),
        outputs={
            "analysis": "analysis_accession",
            "bioproject": "study_accession",
            "study": "secondary_study_accession",
            "biosample": "sample_accession",
            "sample": "secondary_sample_accession",
            "experiment": "experiment_accession",
            "run": "run_accession",
            "submission": "submission_accession",
            "ena_files": "submitted_ftp / generated_ftp",
        },
        probe_with=ANALYSIS_PROBES,
        note="List fields are semicolon-separated: an analysis can draw on several runs.",
    ),
    Mechanism(
        id="ena_search_study",
        name="search study",
        api=ENA,
        endpoint='GET ena/portal/api/search?result=study&query=<field>="{acc}"',
        citation_url=ENA_FIELDS.format(result="study"),
        basis=Basis.DOCUMENTED,
        inputs=("bioproject", "study", "geo_series"),
        outputs={
            "bioproject": "study_accession",
            "study": "secondary_study_accession",
            "geo_series": "geo_accession",
        },
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
        inputs=("biosample", "sample"),
        outputs={
            "biosample": "sample_accession",
            "sample": "secondary_sample_accession",
            "bioproject": "study_accession",
            "submission": "submission_accession",
        },
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
        outputs={
            "run": "result=read_run",
            "experiment": "result=read_experiment",
            "biosample": "result=sample",
            "analysis": "result=analysis",
        },
        probe_with={"bioproject": "PRJEB44468", "biosample": "SAMEA7629265"},
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
        outputs={
            "bioproject": "Target Primary Accession",
            "study": "Target Secondary Accession",
        },
        note="Without source= the same accession finds nothing.",
    ),
    Mechanism(
        id="ena_xref_target",
        name="xref",
        api="ENA cross-reference service",
        endpoint="GET ena/xref/rest/json/search?accession={acc}",
        citation_url=ENA_XREF_SPEC,
        basis=Basis.DOCUMENTED,
        inputs=("bioproject", "study"),
        outputs={
            "geo_series": "Source Primary Accession where Source is GEO",
            "ae_experiment": "Source Primary Accession where Source is ArrayExpress",
        },
        probe_with=_ebi(("bioproject", "study"), "ae_experiment"),
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
        probe_with={"bioproject>ae_experiment": "PRJEB15401"},
        note=(
            "Everything else in the XML is in filereport already "
            "(docs/decisions/ena-browser-xml-not-needed.md). On a run record the GEO "
            "identifier was present for 1 of 5 runs checked."
        ),
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
) -> Mechanism:
    dbfrom, _, db = linkname.partition("_")
    db = db.removesuffix("_all")
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
    )


# db=sra is indexed by experiment: ESummary of an sra UID is one experiment
# with its runs nested inside it. db=gds holds series, samples and platforms
# under one UID space, told apart by prefix.
_ELINK: tuple[Mechanism, ...] = (
    _elink(
        "bioproject_biosample_all",
        ("bioproject",),
        {"biosample": "biosample accession"},
        route=(("bioproject", "biosample"), "bioproject->biosample:elink"),
    ),
    _elink("bioproject_sra_all", ("bioproject",), {"experiment": "Experiment@acc in ExpXml"}),
    _elink(
        "bioproject_gds",
        ("bioproject",),
        {"geo_series": "gds accession (GSE)"},
        route=(("bioproject", "geo_series"), "bioproject->geo_series:elink"),
    ),
    _elink(
        "bioproject_bioproject",
        ("bioproject",),
        {"bioproject": "a parent or child project"},
        self_link=True,
    ),
    _elink("biosample_bioproject", ("biosample",), {"bioproject": "project accession"}),
    _elink("biosample_sra", ("biosample",), {"experiment": "Experiment@acc in ExpXml"}),
    _elink("biosample_gds", ("biosample",), {"geo_sample": "gds accession (GSM)"}),
    _elink("sra_bioproject", ("experiment",), {"bioproject": "project accession"}),
    _elink("sra_biosample", ("experiment",), {"biosample": "biosample accession"}),
    _elink(
        "sra_gds",
        ("experiment",),
        {"geo_sample": "gds accession (GSM)", "geo_series": "gds accession (GSE)"},
    ),
    _elink("gds_bioproject", ("geo_series", "geo_sample"), {"bioproject": "project accession"}),
    _elink("gds_biosample", ("geo_series", "geo_sample"), {"biosample": "biosample accession"}),
    _elink(
        "gds_sra",
        ("geo_series", "geo_sample"),
        {"experiment": "Experiment@acc in ExpXml"},
        route=(("geo_series", "experiment"), "gse->experiment:elink_gds_sra"),
    ),
)

RUNINFO_OUTPUTS = {
    "run": "Run",
    "experiment": "Experiment",
    "study": "SRAStudy",
    "bioproject": "BioProject",
    "sample": "Sample",
    "biosample": "BioSample",
    "geo_sample": "SampleName",
    "submission": "Submission",
}


def _chain(
    id: str,
    name: str,
    endpoint: str,
    route: tuple[tuple[str, str], str],
    via: tuple[str, ...] = (),
    read_from: str = "Run",
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
        outputs={target: read_from},
        via=via,
        routes={(source, target): route_id},
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
        via=("experiment",),
    ),
    _chain(
        "elink_bioproject_sra_sra_be",
        "ELink + sra-db-be",
        "esearch db=bioproject term={acc}, elink bioproject->sra, sra-db-be.cgi rettype=runinfo",
        (("bioproject", "run"), "bioproject->run:sra_be_elink"),
        via=("experiment",),
    ),
    _chain(
        "elink_gds_sra_efetch_runinfo",
        "ELink + runinfo",
        "esearch db=gds term={acc}, elink gds->sra, efetch rettype=runinfo",
        (("geo_series", "run"), "gse->run:elink_gds_sra"),
        via=("experiment",),
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
        # these eight. A submission accession is answered with an empty body.
        inputs=(
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
        inputs=(
            "bioproject",
            "study",
            "biosample",
            "sample",
            "experiment",
            "run",
            "submission",
            "geo_sample",
        ),
        outputs={
            "experiment": "ExpXml Experiment@acc",
            "run": "Runs Run@acc",
            "study": "ExpXml Study@acc",
            "sample": "ExpXml Sample@acc",
            "bioproject": "ExpXml Bioproject",
            "biosample": "ExpXml Biosample",
            "submission": "ExpXml Submitter@acc",
        },
        note="The accessions are inside an XML string inside the JSON document summary.",
    ),
    Mechanism(
        id="sdl",
        name="SDL",
        api="NCBI Storage Data Locator",
        endpoint="POST locate.ncbi.nlm.nih.gov/sdl/2/retrieve acc={acc}",
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=(
            "run",
            "experiment",
            "sample",
            "study",
            "bioproject",
            "biosample",
            "geo_series",
            "geo_sample",
        ),
        outputs={"sra_files": "result[].files[].locations[].link"},
        routes={("run", "sra_files"): "run->file:sdl"},
        note="Undocumented: the service's own root answers 'Documentation is missed'.",
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
            "sample": 'Ids/Id[@db="SRA"]',
            "geo_sample": 'Ids/Id[@db="GEO"]',
            "bioproject": 'Links/Link[@target="bioproject"]',
        },
        note=(
            "Resolved through ESearch, never efetch id=: efetch strips an accession's "
            "prefix and returns a different sample for every SAMEA and SAMD."
        ),
    ),
)

# --------------------------------------------------------------------------
# GEO
# --------------------------------------------------------------------------

SOFT_FAMILY = "GET ftp.ncbi.nlm.nih.gov/geo/series/GSEnnn/{acc}/soft/{acc}_family.soft.gz"
ACC_CGI = "GET ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={acc}&targ=self&form=text&view=brief"
SUPERSERIES = "GSE236087"

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
            "geo_files": "!Series_supplementary_file / !Sample_supplementary_file",
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
            "study": "!Series_relation = SRA",
            "geo_series": "!Series_relation = SuperSeries of / SubSeries of",
            "experiment": "!Sample_relation = SRA",
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
        outputs={"experiment": "experiment_accession"},
        via=("bioproject",),
        routes={("geo_series", "experiment"): "gse->experiment:soft_bioproject_ena"},
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
            "study": "!Series_relation = SRA",
            "geo_series": "!Series_relation = SuperSeries of / SubSeries of",
            "geo_files": "!Series_supplementary_file",
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
            "experiment": "!Sample_relation = SRA",
            "biosample": "!Sample_relation = BioSample",
            "geo_series": "!Sample_series_id",
            "geo_platform": "!Sample_platform_id",
            "geo_files": "!Sample_supplementary_file",
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
            "study": "extrelations[relationtype=SRA].targetobject",
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
            "experiment": "extrelations[relationtype=SRA].targetobject",
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
            "study": "Comment[SecondaryAccession] (ERP/SRP)",
            "geo_series": "Comment[SecondaryAccession] (GSE, on an E-GEOD import)",
            "ae_experiment": "Comment[RelatedExperiment]",
            "run": "Comment[SequenceDataURI], a run range",
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
            "ae_files": "Array Data File / Derived Array Data File",
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
            "sample": "Comment[ENA_SAMPLE]",
            "experiment": "Comment[ENA_EXPERIMENT]",
            "run": "Comment[ENA_RUN]",
            "ae_files": "Comment[FASTQ_URI]",
            "geo_sample": "Source Name, on an E-GEOD import ('GSM1480302 1')",
        },
        routes={
            ("ae_experiment", "biosample"): "ae_experiment->biosample:sdrf",
            ("ae_experiment", "ae_files"): "ae_experiment->file:sdrf",
        },
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
        id="biostudies_files",
        name="files API",
        api="BioStudies API",
        endpoint="GET biostudies/api/v1/studies/{acc}/files?limit=…&offset=…",
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=("ae_experiment",),
        outputs={"ae_files": "items[].path"},
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
            "study": "section.links[].url (Type ENA)",
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
        inputs=("study", "bioproject", "geo_series"),
        outputs={"ae_experiment": "hits[].accession"},
        routes={("study", "ae_experiment"): "study->ae_experiment:biostudies_search"},
        probe_with={"study": "ERP017126", "bioproject": "PRJEB15401"},
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
        inputs=("ae_experiment", "study"),
        outputs={"biostudy": "hits[].accession, S- prefix"},
        probe_with={"study": "ERP017126"},
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
        outputs={"sample": 'characteristics["SRA accession"] / sraAccession'},
        note="Answers for NCBI-issued samples too: SAMN02991530 carries SRS685538.",
    ),
    Mechanism(
        id="biosamples_filter",
        name="BioSamples",
        api="EBI BioSamples API",
        endpoint="GET biosamples/samples?filter=attr:SRA accession:{acc}",
        citation_url=None,
        basis=Basis.LIVE_ONLY,
        inputs=("sample",),
        outputs={"biosample": "_embedded.samples[].accession"},
        probe_with={"sample": "ERS1348472"},
        note="The filter syntax is documented; that the attribute is called 'SRA accession' is not.",
    ),
)

# --------------------------------------------------------------------------
# DDBJ
# --------------------------------------------------------------------------

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
        inputs=tuple(DBLINK_TYPES),
        outputs={node: f"dbXrefs[type={kind}]" for node, kind in DBLINK_TYPES.items()},
        note=(
            "Indexes NCBI and EBI records as well as DDBJ's own: PRJNA258286 and "
            "PRJEB15401 both answer. It backs no route; see "
            "docs/decisions/no-ddbj-provider.md."
        ),
    ),
)

MECHANISMS: tuple[Mechanism, ...] = (*_ENA, *_NCBI, *_GEO, *_BIOSTUDIES, *_DDBJ)
