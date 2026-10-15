"""Assay recognition from submitter free text.

The screening question is asked before anything is downloaded: GSE109816 is 880
runs of Smart-seq2 that the reprocessing pipeline fetched in full before
rejecting them at the chemistry step.
"""

from __future__ import annotations

from fetch_series.assays import Assay
from fetch_series.assays.registry import ASSAYS, SMARTSEQ, TENX, call_all, is_10x

TENX_PROTOCOL = (
    "Cells were loaded onto a 10x Genomics Chromium controller. "
    "Single cell 3' gene expression libraries were prepared."
)
SMARTSEQ_PROTOCOL = (
    "Selected cells were reversed to get cDNA using 5M Betaine and "
    "SMARTScribe reverse transcriptase, following the Smart-seq2 protocol."
)


class TestScreening:
    def test_a_10x_protocol_is_recognised(self):
        call = is_10x({"library_construction_protocol": TENX_PROTOCOL})
        assert call.recognised and call.assay == "10x"

    def test_a_smartseq_protocol_is_not_10x(self):
        """The GSE109816 case: 880 runs downloaded in full before rejection."""
        call = is_10x({"library_construction_protocol": SMARTSEQ_PROTOCOL})
        assert not call.recognised

    def test_empty_metadata_is_unknown_not_known_negative(self):
        """Screening on absence would discard every submission that left the
        protocol field blank."""
        call = is_10x({})
        assert not call.recognised and not call.had_metadata
        assert "no assay metadata" in call.explain()

    def test_metadata_that_simply_is_not_10x_says_so_differently(self):
        """A Smart-seq2 run has plenty of metadata and is not 10x. Reporting it
        as 'no metadata' would blame the archive for a correct answer."""
        call = is_10x({"library_construction_protocol": SMARTSEQ_PROTOCOL})
        assert not call.recognised and call.had_metadata
        assert "no assay signature" in call.explain()

    def test_the_grounds_are_reported_with_the_call(self):
        call = is_10x({"library_construction_protocol": TENX_PROTOCOL})
        assert {m.phrase for m in call.matches} >= {"10x", "Chromium"}
        assert all(m.field == "library_construction_protocol" for m in call.matches)

    def test_10x_is_not_matched_inside_a_longer_number(self):
        """`110x magnification` is not a 10x library."""
        assert not is_10x({"experiment_title": "imaged at 110x magnification"}).recognised

    def test_every_scanned_field_is_searched(self):
        assert is_10x({"experiment_title": "10x Genomics scRNA-seq of liver"}).recognised
        assert is_10x({"sample_extract_protocol": "Chromium Next GEM"}).recognised


class TestLibraryTypes:
    def test_gene_expression_is_gex(self):
        assert is_10x({"library_construction_protocol": TENX_PROTOCOL}).library_type == "GEX"

    def test_a_protocol_naming_two_families_asserts_neither(self):
        """ENA's protocol field is per-experiment in the schema and per-*study*
        in practice: submitters paste the whole methods section into every run.
        GSE111360's names both a Chromium 5' kit and TCR V(D)J, so every run
        matches two families and the text cannot say which library this is."""
        call = is_10x(
            {
                "library_construction_protocol": (
                    "10x Chromium gene expression and TotalSeq-C antibody capture libraries"
                )
            }
        )
        assert call.ambiguous_library_type
        assert call.library_types == ("ADT", "GEX")
        assert call.library_type is None
        assert "ambiguous" in call.explain()

    def test_an_unambiguous_family_is_asserted(self):
        call = is_10x({"library_construction_protocol": "10x Chromium TCR V(D)J immune profiling"})
        assert call.library_type == "VDJ"

    def test_the_specific_families_are_reported_before_gene_expression(self):
        call = is_10x(
            {"library_construction_protocol": "10x Chromium 5' gene expression and TCR V(D)J"}
        )
        assert call.library_types[0] == "VDJ"

    def test_an_unlabelled_10x_run_has_no_library_type(self):
        call = is_10x({"experiment_title": "10x Genomics library"})
        assert call.recognised and call.library_type is None
        assert call.library_types == ()


class TestTheInterfaceIsAnInterface:
    """An interface proven against one implementation is not proven."""

    def test_a_second_assay_is_registered_and_works(self):
        call = SMARTSEQ.call({"library_construction_protocol": SMARTSEQ_PROTOCOL})
        assert call.assay == "smart-seq"

    def test_an_assay_with_no_library_types_is_valid(self):
        """Smart-seq is one library per cell: there is no feature-barcode family
        to distinguish, and the interface must not require one."""
        assert SMARTSEQ.library_types == {}
        call = SMARTSEQ.call({"library_construction_protocol": SMARTSEQ_PROTOCOL})
        assert call.library_type is None and call.library_types == ()

    def test_a_new_assay_needs_no_change_to_the_core(self):
        parse = Assay(name="parse-seq", phrases=("Parse Biosciences", "Evercode"))
        call = parse.call({"experiment_title": "Evercode WT v2 whole transcriptome"})
        assert call.assay == "parse-seq"

    def test_exclusions_veto_a_match(self):
        vetoed = Assay(name="x", phrases=("single cell",), excludes=("bulk",))
        assert not vetoed.call({"experiment_title": "bulk single cell comparison"}).recognised
        assert "ruled out by" in vetoed.call({"experiment_title": "bulk single cell"}).explain()


class TestAmbiguity:
    def test_a_study_naming_both_assays_reports_both(self):
        """Comparing 10x against Smart-seq2 is a common study design. A caller
        screening for 10x should see the ambiguity, not a tie-break it cannot
        inspect."""
        both = {"library_construction_protocol": TENX_PROTOCOL + " " + SMARTSEQ_PROTOCOL}
        calls = call_all(both)
        assert {c.assay for c in calls} == {"10x", "smart-seq"}

    def test_calls_are_ordered_by_weight_of_evidence(self):
        both = {"library_construction_protocol": TENX_PROTOCOL + " " + SMARTSEQ_PROTOCOL}
        calls = call_all(both)
        assert len(calls[0].matches) >= len(calls[1].matches)

    def test_nothing_recognised_returns_nothing(self):
        assert call_all({"experiment_title": "whole genome sequencing of soil"}) == []

    def test_every_registered_assay_has_a_distinct_name(self):
        names = [a.name for a in ASSAYS]
        assert len(names) == len(set(names))
        assert TENX in ASSAYS and SMARTSEQ in ASSAYS
