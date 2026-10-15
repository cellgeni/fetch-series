"""Tier 0: the pure converters, from literals. No network, no mocks."""

from __future__ import annotations

from fetch_series.metadata.fields import Scope
from fetch_series.metadata.harvest import (
    from_biosample,
    from_ena_row,
    from_sdrf,
    from_soft_sample,
)
from fetch_series.providers.biostudies import (
    SdrfCharacteristic,
    sdrf_characteristics,
)
from fetch_series.providers.eutils import parse_biosample_set
from fetch_series.providers.geo import parse_soft_family

# Trimmed from the real SAMN12011225 and SAMN12039425 documents. The typo in
# `sampling enviroment` is the submitter's and is preserved on purpose.
BIOSAMPLE_XML = """<?xml version="1.0" ?>
<BioSampleSet>
  <BioSample accession="SAMN12011225">
    <Ids>
      <Id db="BioSample" is_primary="1">SAMN12011225</Id>
      <Id db_label="Sample name">5864_BA9_10x</Id>
      <Id db="SRA">SRS4937056</Id>
      <Id db="GEO">GSM3880020</Id>
    </Ids>
    <Description>
      <Title>5864_BA9</Title>
      <Organism taxonomy_id="9606" taxonomy_name="Homo sapiens">
        <OrganismName>Homo sapiens</OrganismName>
      </Organism>
    </Description>
    <Package display_name="Human; version 1.0">Human.1.0</Package>
    <Attributes>
      <Attribute attribute_name="isolate" harmonized_name="isolate">5864</Attribute>
      <Attribute attribute_name="tissue" harmonized_name="tissue">Brain, prefrontal cortex</Attribute>
      <Attribute attribute_name="disease" harmonized_name="disease">ASD</Attribute>
      <Attribute attribute_name="sampling enviroment">constant dark</Attribute>
    </Attributes>
  </BioSample>
</BioSampleSet>
"""

SOFT = """^SERIES = GSE1
!Series_title = Single-nucleus RNA-seq of autism cortex
!Series_summary = We profiled prefrontal cortex from ASD patients
!Series_summary = and matched controls.
!Series_overall_design = 16 ASD and 16 control donors
^SAMPLE = GSM1
!Sample_title = 5864_BA9
!Sample_source_name_ch1 = prefrontal cortex
!Sample_organism_ch1 = Homo sapiens
!Sample_characteristics_ch1 = tissue: Brain, prefrontal cortex
!Sample_characteristics_ch1 = tissue: lymph node
!Sample_characteristics_ch1 = Sex: M
!Sample_characteristics_ch1 = a bare value with no colon
^SAMPLE = GSM2
!Sample_characteristics_ch1 = donor: D7
"""


class TestBioSample:
    def test_the_package_is_captured(self) -> None:
        """Without it, `isolate` cannot be resolved without guessing."""
        record = parse_biosample_set(BIOSAMPLE_XML)[0]
        assert record.package == "Human.1.0"
        assert record.taxon == "9606"

    def test_an_unrecognised_key_is_not_harmonised(self) -> None:
        """`sampling enviroment` is the submitter's typo; NCBI offers no harmonised name."""
        record = parse_biosample_set(BIOSAMPLE_XML)[0]
        typo = next(a for a in record.attributes if a.raw_name == "sampling enviroment")
        assert typo.harmonised is False
        tissue = next(a for a in record.attributes if a.raw_name == "tissue")
        assert tissue.harmonised is True

    def test_cross_links_are_captured_but_the_primary_id_is_not_a_link(self) -> None:
        record = parse_biosample_set(BIOSAMPLE_XML)[0]
        assert dict(record.cross_links) == {"SRA": "SRS4937056", "GEO": "GSM3880020"}
        assert record.submitter_name == "5864_BA9_10x"

    def test_the_context_travels_with_every_attribute(self) -> None:
        record = parse_biosample_set(BIOSAMPLE_XML)[0]
        values = from_biosample(record)
        isolate = next(v for v in values if v.key == "isolate")
        assert isolate.context == "Human.1.0"

    def test_sources_are_uniform_so_provenance_parses(self) -> None:
        """`source_of` recovers the declared source by splitting on the bracket."""
        for value in from_biosample(parse_biosample_set(BIOSAMPLE_XML)[0]):
            assert value.source.startswith("biosample:")
            assert value.source.endswith("]")


class TestGeoCharacteristics:
    def test_a_repeated_key_keeps_both_values(self) -> None:
        """A submitter writing `tissue` twice is describing a mixture.

        Reading these through a dict keeps the last only -- the same trap
        csv.DictReader sets for SDRF columns, with the same silent result.
        """
        family = parse_soft_family("GSE1", SOFT)
        assert (
            family.sample_characteristics["GSM1"].count(("tissue", "Brain, prefrontal cortex")) == 1
        )
        assert ("tissue", "lymph node") in family.sample_characteristics["GSM1"]

    def test_a_value_containing_a_colon_survives(self) -> None:
        family = parse_soft_family("GSE1", SOFT)
        assert ("Sex", "M") in family.sample_characteristics["GSM1"]

    def test_a_bare_value_is_dropped_rather_than_given_an_invented_key(self) -> None:
        family = parse_soft_family("GSE1", SOFT)
        assert all(key for key, _ in family.sample_characteristics["GSM1"])

    def test_characteristics_do_not_leak_between_samples(self) -> None:
        family = parse_soft_family("GSE1", SOFT)
        assert family.sample_characteristics["GSM2"] == [("donor", "D7")]

    def test_series_text_is_kept_apart_from_sample_text(self) -> None:
        """!Series_summary is one paragraph shared by every sample."""
        family = parse_soft_family("GSE1", SOFT)
        assert "matched controls" in family.series_text["series_summary"]
        assert "series_summary" not in family.sample_text["GSM1"]


class TestEnaRow:
    def test_study_scoped_columns_are_marked(self) -> None:
        """PRJNA434002's study_title names a disease for its controls too."""
        row = {
            "tissue_type": "Brain, prefrontal cortex",
            "study_title": "Single-nucleus RNA sequencing of post-mortem brain tissue from ASD",
            "sample_title": "5864_BA9",
        }
        values = {v.source: v for v in from_ena_row(row)}
        assert values["ena:filereport[tissue_type]"].scope is Scope.SAMPLE
        assert values["ena:filereport[study_title]"].scope is Scope.STUDY

    def test_identity_columns_are_not_harvested_as_descriptions(self) -> None:
        row = {"run_accession": "SRR1", "sample_accession": "SAMN1", "sex": "male"}
        assert {v.key for v in from_ena_row(row)} == {"sex"}

    def test_prose_columns_carry_no_key(self) -> None:
        values = {v.source: v for v in from_ena_row({"sample_title": "x", "sex": "male"})}
        assert values["ena:filereport[sample_title]"].key == ""
        assert values["ena:filereport[sex]"].key == "sex"


class TestSdrf:
    def test_a_curated_key_is_marked_as_such(self) -> None:
        """EBI's column vocabulary is controlled; GEO's keys are not."""
        values = from_sdrf(
            [SdrfCharacteristic(key="organism part", value="liver", kind="characteristics")]
        )
        assert values[0].curated_key is True
        assert values[0].source == "ae:characteristics[organism part]"

    def test_term_columns_bind_to_the_preceding_qualifiable_column(self) -> None:
        """E-MTAB-10553: UO_0000036 is the *unit* `year`, not the age.

        Header runs `Characteristics[age]  Unit[time unit]  Term Source REF
        Term Accession Number`. Binding the term to the nearest
        `Characteristics[...]` would record every such age as ontology-grounded,
        and grounded to a unit.
        """
        rows = [
            [
                "Source Name",
                "Comment[BioSD_SAMPLE]",
                "Characteristics[age]",
                "Unit[time unit]",
                "Term Source REF",
                "Term Accession Number",
                "Characteristics[organism part]",
                "Term Source REF",
                "Term Accession Number",
            ],
            ["s1", "SAMEA1", "84", "year", "EFO", "UO_0000036", "liver", "EFO", "UBERON_0002107"],
        ]
        found = {c.key: c for c in sdrf_characteristics(rows)["SAMEA1"]}
        assert found["age"].curie is None
        assert found["organism part"].curie == "UBERON:0002107"

    def test_a_term_accession_that_is_not_curie_shaped_is_not_invented(self) -> None:
        characteristic = SdrfCharacteristic(
            key="organism part", value="liver", kind="characteristics", term_accession="see note"
        )
        assert characteristic.curie is None

    def test_a_geo_import_is_keyed_by_its_gsm(self) -> None:
        """E-GEOD SDRFs have no BioSD column; Source Name is `GSM677627 1`.

        Left as written it joins to nothing, which is the whole purpose of the
        key.
        """
        rows = [
            ["Source Name", "Characteristics[organism]"],
            ["GSM677627 1", "Caenorhabditis elegans"],
        ]
        assert list(sdrf_characteristics(rows)) == ["GSM677627"]

    def test_a_submitter_name_with_no_accession_is_kept(self) -> None:
        rows = [["Source Name", "Characteristics[organism]"], ["donor1_S1", "Homo sapiens"]]
        assert list(sdrf_characteristics(rows)) == ["donor1_S1"]


class TestSoftProseFeedsTheAssayLayerToo:
    def test_the_assay_layer_field_names_are_unchanged(self) -> None:
        """Widening _TEXT_FIELDS must not rename what the assay layer scans."""
        family = parse_soft_family("GSE1", SOFT)
        text = family.sample_text["GSM1"]
        assert "sample_title" in text
        assert text["sample_source_name"] == "prefrontal cortex"

    def test_source_name_and_organism_reach_the_harvest(self) -> None:
        family = parse_soft_family("GSE1", SOFT)
        sources = {v.source for v in from_soft_sample(family, "GSM1")}
        assert "geo:soft[sample_source_name]" in sources
        assert "geo:characteristics[tissue]" in sources
