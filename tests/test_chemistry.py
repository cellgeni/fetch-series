"""10x chemistry named in metadata.

Complementary to `rename10xrun`'s empirical inference, not a replacement. The
case that justifies having both is the one the incumbent warns about itself:
3' v2 and 5' v1/v2 share a barcode length, a UMI length and a whitelist, so
FASTQ geometry cannot separate them and metadata can.
"""

from __future__ import annotations

import pytest

from fetch_series.assays.chemistry import (
    CHEMISTRY_LABELS,
    CHEMISTRY_PHRASES,
    GEOMETRY_AMBIGUOUS,
    chemistry_of,
)


def protocol(text: str) -> dict[str, str]:
    return {"library_construction_protocol": text}


class TestNamedChemistries:
    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("Single Cell 3' Reagent Kit v3 libraries", "10x_3p_v3_family"),
            ("Single Cell 3' v3.1 dual index", "10x_3p_v3_family"),
            ("10x Genomics Single Cell 3' v2 chemistry", "10x_3p_v2"),
            ("Chromium Next GEM Single Cell 5' Library & Gel Bead Kit v2", "10x_5p_v1_v2"),
            ("GEM-X Universal 3' v4 Gene Expression", "10x_3p_v4_gemx"),
            ("GEM-X Universal 5' v3", "10x_5p_v3_gemx"),
            ("Single Cell Multiome ATAC + Gene Expression", "10x_multiome_arc_gex"),
            ("Chromium Single Cell ATAC v2", "10x_atac_v1_v1.1_v2"),
            ("Single Cell 3' v1 chemistry", "10x_3p_v1"),
        ],
    )
    def test_the_metadata_chemistry_is_read(self, text, expected):
        assert chemistry_of(protocol(text)).chemistry == expected

    @pytest.mark.parametrize(
        "text",
        [
            "10x Genomics scRNA-seq libraries were prepared",
            "Cells were loaded on a Chromium controller",
            "Smart-seq2 protocol with SMARTScribe",
            "",
        ],
    )
    def test_a_protocol_that_names_no_chemistry_yields_none(self, text):
        """Silence is the honest answer far more often than not. A recognised
        assay does not imply a known chemistry."""
        call = chemistry_of(protocol(text))
        assert not call.named
        assert "no chemistry named" in call.explain()


class TestSpecificBeforeGeneral:
    def test_gemx_is_not_read_as_plain_three_prime(self):
        """A GEM-X protocol also says 3'; the coarser answer would be wrong."""
        assert chemistry_of(protocol("GEM-X Universal 3' v4")).chemistry == "10x_3p_v4_gemx"

    def test_v3_1_is_not_read_as_v3_only_by_accident(self):
        """`v3.1` contains `v3`; both land in the same family here, so the test
        is that the family is reached at all rather than falling through."""
        assert chemistry_of(protocol("Single Cell 3' v3.1")).chemistry == "10x_3p_v3_family"

    def test_multiome_is_not_read_as_atac(self):
        """A Multiome protocol names ATAC. Reading it as plain scATAC would send
        the gene expression half of the library down an ATAC pipeline."""
        call = chemistry_of(protocol("Chromium Single Cell Multiome ATAC + Gene Expression"))
        assert call.chemistry == "10x_multiome_arc_gex"

    def test_a_version_number_does_not_fire_on_a_longer_one(self):
        assert chemistry_of(protocol("Single Cell 3' v20 prototype")).chemistry != "10x_3p_v2"


class TestComplementarityWithGeometry:
    """The reason to read metadata at all."""

    @pytest.mark.parametrize("expected", ["10x_3p_v2", "10x_5p_v1_v2"])
    def test_the_two_that_share_a_whitelist_are_marked_as_finer_than_geometry(self, expected):
        assert expected in GEOMETRY_AMBIGUOUS["10x_3p_v2_or_5p_v1_v2"]

    def test_a_five_prime_call_reports_what_it_splits(self):
        call = chemistry_of(protocol("Chromium Next GEM Single Cell 5' v2"))
        assert call.splits == "10x_3p_v2_or_5p_v1_v2"
        assert "FASTQ geometry cannot split" in call.explain()

    def test_a_chemistry_geometry_can_reach_alone_reports_no_split(self):
        assert chemistry_of(protocol("Single Cell 3' v3")).splits is None


class TestAmbiguityAndSubsumption:
    """The protocol field describes a whole study, not one library."""

    def test_a_study_that_ran_both_kits_asserts_neither(self):
        """GSE111360's protocol names a Chromium 3' kit and a 5' kit, because
        the study used both, in one field shared by every run."""
        call = chemistry_of(protocol("Single Cell 3' v3 and Single Cell 5' v2 kits were used"))
        assert call.ambiguous
        assert call.candidates == ("10x_3p_v3_family", "10x_5p_v1_v2")
        assert call.chemistry is None
        assert "ambiguous" in call.explain()

    def test_a_gemx_kit_is_not_ambiguous_with_the_plain_family_it_implies(self):
        """A GEM-X 3' protocol also matches the plain 3' phrases. That is one
        library described at two levels, not two libraries."""
        call = chemistry_of(protocol("GEM-X Universal 3' v4 Gene Expression"))
        assert not call.ambiguous
        assert call.chemistry == "10x_3p_v4_gemx"

    def test_multiome_is_not_ambiguous_with_atac(self):
        call = chemistry_of(protocol("Chromium Single Cell Multiome ATAC + Gene Expression"))
        assert not call.ambiguous
        assert call.chemistry == "10x_multiome_arc_gex"

    def test_the_v3_family_subsumes_the_older_three_prime_versions(self):
        """`Single Cell 3' v3` also contains the substring the v2 phrase needs
        only when a submitter writes both; where it does, v3 is the answer."""
        call = chemistry_of(protocol("upgraded from Single Cell 3' v2 to Single Cell 3' v3"))
        assert call.chemistry == "10x_3p_v3_family"

    def test_nothing_named_is_not_ambiguous(self):
        call = chemistry_of(protocol("10x Genomics libraries"))
        assert not call.ambiguous and not call.named


class TestVocabulary:
    """The ids are rename10xrun's, so the two answers compare without
    translation. A label drifting out of that vocabulary silently breaks the
    comparison this module exists for."""

    def test_every_matcher_has_a_label(self):
        for identifier, _ in CHEMISTRY_PHRASES:
            assert identifier in CHEMISTRY_LABELS, identifier

    def test_every_label_has_a_matcher(self):
        matched = {identifier for identifier, _ in CHEMISTRY_PHRASES}
        assert set(CHEMISTRY_LABELS) == matched

    def test_the_ambiguous_classes_name_only_known_chemistries(self):
        for covered in GEOMETRY_AMBIGUOUS.values():
            for identifier in covered:
                assert identifier in CHEMISTRY_LABELS, identifier

    def test_an_unnamed_call_has_a_usable_label(self):
        assert chemistry_of({}).label == "unknown"
