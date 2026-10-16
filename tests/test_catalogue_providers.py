"""Parsers behind the catalogue's probes, on excerpts of recorded responses.

Every excerpt is cut from a live response saved on 2026-10-16 for the probe
family (GSE60450, E-MTAB-5061 and their records), trimmed to the lines that
carry links.
"""

from __future__ import annotations

import pytest

from fetch_series.providers import biosamples, biostudies, ddbj, ena_portal, eutils, geo
from fetch_series.survey.client import MalformedResponseError


class TestEnaPortal:
    def test_a_short_row_reads_as_empty_not_as_a_crash(self):
        """ENA omits trailing empty cells, and DictReader fills them with None."""
        rows = ena_portal.parse_tsv(
            "sample_accession\tsecondary_sample_accession\tsubmission_accession\n"
            "SAMN02991530\tSRS685538\n"
        )
        assert ena_portal.column(rows, "submission_accession") == []
        assert ena_portal.column(rows, "sample_accession") == ["SAMN02991530"]

    def test_listed_splits_a_semicolon_list(self):
        rows = [{"run_accession": "ERR5753415;ERR5753416;ERR5753417;ERR5753418"}]
        assert ena_portal.listed(rows, "run_accession")[-1] == "ERR5753418"
        assert len(ena_portal.listed(rows, "run_accession")) == 4

    def test_xref_reads_either_side(self):
        entries = [
            {
                "Source": "ArrayExpress",
                "Source Primary Accession": "E-MTAB-5061",
                "Target": "study",
                "Target Primary Accession": "PRJEB15401",
                "Target Secondary Accession": "ERP017126",
            },
            {"Source": "PubMed", "Source Primary Accession": "25730472"},
        ]
        assert ena_portal.xref_sources(entries, "ArrayExpress") == ["E-MTAB-5061"]
        assert ena_portal.xref_sources(entries, "GEO") == []
        assert ena_portal.xref_targets(entries, "Target Secondary Accession") == ["ERP017126"]

    def test_browser_xml_cross_references(self):
        xml = """<PROJECT_SET><PROJECT accession="PRJNA258286">
          <IDENTIFIERS><PRIMARY_ID>PRJNA258286</PRIMARY_ID>
            <EXTERNAL_ID namespace="GEO">GSE60450</EXTERNAL_ID></IDENTIFIERS>
          <PROJECT_LINKS><PROJECT_LINK><URL_LINK>
            <URL>http://www.ebi.ac.uk/arrayexpress/experiments/E-MTAB-5061</URL>
          </URL_LINK></PROJECT_LINK></PROJECT_LINKS></PROJECT></PROJECT_SET>"""
        refs = ena_portal.browser_cross_references(xml)
        assert refs["GEO"] == ["GSE60450"]
        assert refs["url"] == ["http://www.ebi.ac.uk/arrayexpress/experiments/E-MTAB-5061"]

    def test_unparseable_xml_is_a_typed_error(self):
        with pytest.raises(MalformedResponseError):
            ena_portal.browser_cross_references("<html>Service unavailable")


SERIES_RECORD = """^SERIES = GSE236084
!Series_geo_accession = GSE236084
!Series_sample_id = GSM7518065
!Series_supplementary_file = ftp://ftp.ncbi.nlm.nih.gov/geo/series/GSE236nnn/GSE236084/suppl/x.txt.gz
!Series_platform_id = GPL24247
!Series_relation = Reanalysis of: GSM5906363
!Series_relation = SubSeries of: GSE236087
!Series_relation = BioProject: https://www.ncbi.nlm.nih.gov/bioproject/PRJNA988806
"""


class TestGeo:
    def test_record_relations_split_on_the_first_colon_only(self):
        record = geo.parse_record_links("GSE236084", SERIES_RECORD)
        assert ("SubSeries of", "GSE236087") in record.relations
        assert (
            "BioProject",
            "https://www.ncbi.nlm.nih.gov/bioproject/PRJNA988806",
        ) in record.relations
        assert record.samples == ["GSM7518065"]
        assert record.platforms == ["GPL24247"]
        assert len(record.supplementary_files) == 1

    def test_a_sample_record_names_its_series(self):
        text = (
            "^SAMPLE = GSM1480291\n!Sample_series_id = GSE60450\n"
            "!Sample_platform_id = GPL13112\n!Sample_supplementary_file_1 = NONE\n"
            "!Sample_relation = SRA: https://www.ncbi.nlm.nih.gov/sra?term=SRX681985\n"
        )
        record = geo.parse_record_links("GSM1480291", text)
        assert record.series == ["GSE60450"]
        assert record.supplementary_files == []
        assert record.relations == [("SRA", "https://www.ncbi.nlm.nih.gov/sra?term=SRX681985")]

    def test_the_soft_family_now_carries_sra_platforms_and_files(self):
        text = (
            "^SERIES = GSE60450\n"
            "!Series_relation = BioProject: https://www.ncbi.nlm.nih.gov/bioproject/PRJNA258286\n"
            "!Series_relation = SRA: https://www.ncbi.nlm.nih.gov/sra?term=SRP045534\n"
            "!Series_platform_id = GPL13112\n"
            "!Series_supplementary_file = ftp://ftp.ncbi.nlm.nih.gov/geo/series/GSE60nnn/GSE60450/suppl/GSE60450_RAW.tar\n"
            "^PLATFORM = GPL13112\n"
            "^SAMPLE = GSM1480291\n"
            "!Sample_supplementary_file_1 = NONE\n"
        )
        family = geo.parse_soft_family("GSE60450", text)
        assert family.sra_studies == ["SRP045534"]
        assert family.platforms == ["GPL13112"]
        assert family.supplementary_files == [
            "ftp://ftp.ncbi.nlm.nih.gov/geo/series/GSE60nnn/GSE60450/suppl/GSE60450_RAW.tar"
        ]


class TestBioStudies:
    def test_a_repeated_idf_comment_keeps_every_value(self):
        """E-GEOD-60450 declares SecondaryAccession twice: the GSE and the SRP."""
        text = (
            "Comment[ArrayExpressAccession]\tE-GEOD-60450\n"
            "Comment[SecondaryAccession]\tGSE60450\t\t\t\t\n"
            "Comment[SecondaryAccession]\tSRP045534\t\t\t\t\n"
            "Comment[RelatedExperiment]\tE-MTAB-5060\n"
        )
        comments = biostudies.idf_comments(text)
        assert comments["SecondaryAccession"] == ["GSE60450", "SRP045534"]
        assert comments["RelatedExperiment"] == ["E-MTAB-5060"]

    def test_sdrf_values_read_every_copy_of_a_repeated_column(self):
        rows = [
            ["Source Name", "Comment [ENA_RUN]", "Comment[FASTQ_URI]", "Comment[FASTQ_URI]"],
            [
                "AZ_A1",
                "ERR1630013",
                "ftp://x/ERR1630013_1.fastq.gz",
                "ftp://x/ERR1630013_2.fastq.gz",
            ],
            ["AZ_A2", "ERR1630014", "ftp://x/ERR1630014_1.fastq.gz"],
        ]
        values = biostudies.sdrf_values(rows, ("Comment[ENA_RUN]", "Comment[FASTQ_URI]"))
        assert values["Comment[ENA_RUN]"] == ["ERR1630013", "ERR1630014"]
        assert len(values["Comment[FASTQ_URI]"]) == 3

    def test_sdrf_values_for_one_row(self):
        rows = [
            ["Source Name", "Comment[ENA_RUN]"],
            ["AZ_A1", "ERR1630013"],
            ["AZ_A2", "ERR1630014"],
        ]
        values = biostudies.sdrf_values(rows, ("Comment[ENA_RUN]",), source_name="AZ_A2")
        assert values == {"Comment[ENA_RUN]": ["ERR1630014"]}

    def test_study_links_are_found_at_any_depth_and_keep_their_type(self):
        payload = {
            "section": {
                "links": [
                    {"url": "E-MTAB-5060", "attributes": [{"name": "Type", "value": "Biostudies"}]},
                    [{"url": "ERP017126", "attributes": [{"name": "Type", "value": "ENA"}]}],
                ],
                "subsections": [[{"links": [{"url": "PRJEB15401", "attributes": []}]}]],
            }
        }
        assert biostudies.study_links(payload) == [
            ("E-MTAB-5060", "Biostudies"),
            ("ERP017126", "ENA"),
            ("PRJEB15401", None),
        ]


class TestEutils:
    def test_expxml_accessions(self):
        record = {
            "expxml": (
                '<Submitter acc="SRA179382" center_name="GEO"/>'
                '<Experiment acc="SRX681985" ver="1"/><Study acc="SRP045534" name="x"/>'
                '<Sample acc="SRS685538" name=""/><Bioproject>PRJNA258286</Bioproject>'
                "<Biosample>SAMN02991530</Biosample>"
            ),
            "runs": '<Run acc="SRR1552444" total_spots="27919481"/>',
        }
        found = eutils.expxml_accessions(record)
        assert found["Run"] == ["SRR1552444"]
        assert found["Submitter"] == ["SRA179382"]
        assert found["Bioproject"] == ["PRJNA258286"]

    def test_a_biosample_record_names_its_bioproject_by_label(self):
        xml = (
            '<BioSampleSet><BioSample accession="SAMN02991530"><Ids>'
            '<Id db="BioSample" is_primary="1">SAMN02991530</Id><Id db="SRA">SRS685538</Id>'
            "</Ids><Links>"
            '<Link type="entrez" target="bioproject" label="PRJNA258286">258286</Link>'
            "</Links></BioSample></BioSampleSet>"
        )
        (record,) = eutils.parse_biosample_set(xml)
        assert record.bioprojects == ("PRJNA258286",)
        assert record.cross_links == (("SRA", "SRS685538"),)


class TestBioSamples:
    def test_both_spellings_of_the_sra_accession(self):
        ebi = {"sraAccession": "ERS1348472", "characteristics": {}}
        ncbi = {"characteristics": {"SRA accession": [{"text": "SRS685538"}]}}
        assert biosamples.sra_accessions(ebi) == ["ERS1348472"]
        assert biosamples.sra_accessions(ncbi) == ["SRS685538"]

    def test_a_truncated_search_page_is_refused(self):
        page = {
            "_embedded": {"samples": [{"accession": "SAMEA4437023"}]},
            "page": {"totalElements": 2},
        }
        with pytest.raises(MalformedResponseError):
            biosamples.embedded_accessions(page)


class TestDdbj:
    def test_dblink_groups_by_type(self):
        payload = {
            "identifier": "SRR1552444",
            "dbXrefs": [
                {"identifier": "PRJNA258286", "type": "bioproject"},
                {"identifier": "SRX681985", "type": "sra-experiment"},
            ],
        }
        assert ddbj.parse_dblink(payload) == {
            "bioproject": ["PRJNA258286"],
            "sra-experiment": ["SRX681985"],
        }

    def test_an_answer_without_dbxrefs_is_an_error(self):
        with pytest.raises(MalformedResponseError):
            ddbj.parse_dblink({"detail": "Not Found"})
