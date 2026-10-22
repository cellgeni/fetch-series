"""Basic QC over the sample, and the library's finer type: a VDJ chain, a multiplexing tag set."""

from __future__ import annotations

import random

import pytest

from fetch_series.inspect.model import FileSample, Record
from fetch_series.inspect.qc import library, qc
from fetch_series.inspect.references import build_index
from fetch_series.inspect.tags import TagSet, parse

RNG = random.Random(11)


def _random(n: int) -> str:
    return "".join(RNG.choice("ACGT") for _ in range(n))


def reads(seqs: list[str]) -> FileSample:
    return FileSample(
        "x", "fastq", records=[Record(f"r{i}", s, "F" * len(s)) for i, s in enumerate(seqs)]
    )


def v3_call(**extra) -> dict:
    call = {
        "layout": "10x_3p_v3_family",
        "candidates": ["10x_3p_v3_family"],
        "level": "HIGH",
        "barcode_list": "tenx_v3",
        "barcode_read": 0,
        "cdna_read": 1,
        "library_type": "GEX",
        "evidence": {
            "hits": [
                {
                    "barcode_list": "tenx_v3",
                    "read": 0,
                    "offset": 0,
                    "exact": 0.91,
                    "near": 0.96,
                    "sampled": 400,
                    "background": 0.0016,
                }
            ],
            "umi_entropy": 1.98,
            "tail": {"poly_t": 0.8, "tso": 0.0, "reads": 400},
        },
    }
    call.update(extra)
    return call


class TestQC:
    def test_the_listed_rates_are_the_calls(self):
        barcode = reads([_random(28) for _ in range(400)])
        made = qc([barcode, reads([_random(91)] * 400)], v3_call())
        assert made is not None
        assert made["barcodes"]["exact"] == 0.91 and made["barcodes"]["within_one"] == 0.96
        assert made["umi"]["entropy"] == 1.98 and made["tail"]["poly_t"] == 0.8

    def test_repeated_molecules_lower_the_distinct_share(self):
        molecules = [_random(28) for _ in range(100)]
        barcode = reads([RNG.choice(molecules) for _ in range(400)])
        made = qc([barcode, reads([_random(91)] * 400)], v3_call())
        assert made is not None and made["cb_umi"]["distinct_share"] <= 100 / 400
        assert made["cb_umi"]["reads"] == 400

    def test_no_barcode_read_no_qc(self):
        assert qc([reads([_random(91)] * 200)], v3_call(barcode_read=None)) is None


TCR = _random(600)
BCR = _random(600)
REFERENCES = {
    "tcr_constant_human": build_index("tcr_constant_human", [TCR]),
    "bcr_constant_human": build_index("bcr_constant_human", [BCR]),
}


class TestLibrary:
    def test_a_t_cell_vdj_library(self):
        cdna = reads([TCR[i : i + 90] for i in range(0, 500, 5)])
        made = library(
            [reads([_random(26)] * 100), cdna], v3_call(library_type="VDJ"), REFERENCES, {}
        )
        assert made["chain"] == "T" and made["provisional"] is True

    def test_a_b_cell_vdj_library(self):
        cdna = reads([BCR[i : i + 90] for i in range(0, 500, 5)])
        made = library(
            [reads([_random(26)] * 100), cdna], v3_call(library_type="VDJ"), REFERENCES, {}
        )
        assert made["chain"] == "B"

    def test_without_the_references_the_chain_is_not_said(self):
        cdna = reads([TCR[i : i + 90] for i in range(0, 500, 5)])
        made = library([reads([_random(26)] * 100), cdna], v3_call(library_type="VDJ"), {}, {})
        assert made["chain"] is None and "not fetched" in made["why"]

    HTO = TagSet("hto", "HTO", (0, 10), 2, "0" * 64, "s", "t")
    TAGS = frozenset({"GTCAACTCTTTAGCG", "TGATGGCCTATTGGG"})

    def test_hashtags_after_ten_bases(self):
        feature = reads(
            [_random(10) + RNG.choice(sorted(self.TAGS)) + _random(65) for _ in range(300)]
        )
        made = library(
            [reads([_random(28)] * 300), feature],
            v3_call(library_type="feature barcode"),
            {},
            {"hto": (self.HTO, self.TAGS)},
        )
        assert made["multiplexing"] == "HTO" and made["tag_shares"]["hto"] == 1.0

    def test_unlisted_tags_stay_a_feature_barcode_library(self):
        feature = reads([_random(10) + "A" * 15 + _random(65) for _ in range(300)])
        made = library(
            [reads([_random(28)] * 300), feature],
            v3_call(library_type="feature barcode"),
            {},
            {"hto": (self.HTO, self.TAGS)},
        )
        assert made["multiplexing"] is None

    def test_totalseq_a_puts_the_hashtag_first(self):
        feature = reads([RNG.choice(sorted(self.TAGS)) + _random(75) for _ in range(300)])
        made = library(
            [reads([_random(28)] * 300), feature],
            v3_call(library_type="feature barcode"),
            {},
            {"hto": (self.HTO, self.TAGS)},
        )
        assert made["multiplexing"] == "HTO"

    def test_tso_artefacts_are_flagged(self):
        """SRR30069691: R2 starts AAGCAGTGGTATCAACGCAGAGTAC in a third of reads."""
        artefact = "AAGCAGTGGTATCAACGCAGAGTACATGGG"
        feature = reads(
            [artefact + _random(70) if i % 3 == 0 else _random(100) for i in range(300)]
        )
        made = library(
            [reads([_random(28)] * 300), feature],
            v3_call(library_type="feature barcode"),
            {},
            {"hto": (self.HTO, self.TAGS)},
        )
        assert "template-switch" in made["suspect"] and made["multiplexing"] is None

    def test_no_tag_set_fetched(self):
        made = library(
            [reads([_random(28)] * 10), reads([_random(90)] * 10)],
            v3_call(library_type="feature barcode"),
            {},
            {},
        )
        assert made["multiplexing"] is None and made["why"] == "no tag set fetched"


class TestTagSets:
    TEXT = "id,name,read,pattern,sequence,feature_type\nA,A,R2,5P(BC),ACGT,Multiplexing Capture\n"

    def test_parse_checks_the_count(self):
        assert parse(self.TEXT, TagSet("x", "CMO", (0,), 1, "0", "s", "t")) == frozenset({"ACGT"})
        with pytest.raises(ValueError, match="the register says 2"):
            parse(self.TEXT, TagSet("x", "CMO", (0,), 2, "0", "s", "t"))
