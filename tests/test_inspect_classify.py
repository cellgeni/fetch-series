"""The classifier on synthetic reads, one layout at a time, with small stand-in lists."""

from __future__ import annotations

import gzip
import hashlib
import random

import httpx
import pytest

from fetch_series.assays.chemistry import ChemistryCall
from fetch_series.inspect import lists as lists_module
from fetch_series.inspect.agree import across_sources, with_metadata
from fetch_series.inspect.classify import (
    SCAFFOLD,
    Call,
    classify,
    instrument,
    umi_entropy,
)
from fetch_series.inspect.classify.layouts import BY_ID, LAYOUTS, TSO_PREFIX, list_class
from fetch_series.inspect.lists import BarcodeList, Entry, ListMismatch, encode, pack
from fetch_series.inspect.model import FileSample, Record
from fetch_series.inspect.references import build_index, revcomp

RNG = random.Random(20261019)
N = 1_500


def _random(n: int) -> str:
    return "".join(RNG.choice("ACGT") for _ in range(n))


def _list(list_id: str, length: int = 16, count: int = 500) -> tuple[BarcodeList, list[str]]:
    barcodes = sorted({_random(length) for _ in range(count)})
    entry = Entry(list_id, length, len(barcodes), "0" * 64, "test", "test")
    return BarcodeList(entry, pack(barcodes, entry)), barcodes


TRANSCRIPTS = [_random(2_000) for _ in range(20)]
CHRM_SEQ = _random(16_000)
HOUSEKEEPING = build_index("housekeeping_human", TRANSCRIPTS)
CHRM = build_index("human_chrM", [CHRM_SEQ])
# A library's reads: mostly transcripts, a fifth mitochondrial, as real ones are.
EXPRESSED = TRANSCRIPTS + [CHRM_SEQ] * 5
CONSTANT = build_index("tcr_bcr_constant_human", [_random(1_500) for _ in range(5)])
REFERENCES = {
    "housekeeping_human": HOUSEKEEPING,
    "human_chrM": CHRM,
    "tcr_bcr_constant_human": CONSTANT,
}


def _cdna(antisense: bool, *, source: list[str] = EXPRESSED, length: int = 91) -> str:
    transcript = RNG.choice(source)
    at = RNG.randrange(0, len(transcript) - length)
    read = transcript[at : at + length]
    if not antisense and RNG.random() < 0.3:
        read = read[:70] + "A" * 21  # 3' reads reach the poly-A tail
    return revcomp(read) if antisense else read


def _sample(seqs: list[str], source: str, role: str | None) -> FileSample:
    return FileSample(
        source=source,
        format="fastq",
        role_hint=role,
        records=[Record(f"SRR1.{i}", s, "F" * len(s)) for i, s in enumerate(seqs)],
    )


def library(
    barcodes: list[str],
    *,
    umi: int,
    r1_extra: str = "",
    antisense: bool = False,
    offset: int = 0,
    constant_umi: bool = False,
    cdna=None,
) -> list[FileSample]:
    r1, r2 = [], []
    for _ in range(N):
        bc = RNG.choice(barcodes)
        u = "A" * umi if constant_umi else _random(umi)
        r1.append(_random(offset) + bc + u + r1_extra)
        r2.append(cdna() if cdna else _cdna(antisense))
    return [_sample(r1, "x_R1_001.fastq.gz", "R1"), _sample(r2, "x_R2_001.fastq.gz", "R2")]


V2, V2_CODES = _list("tenx_v2")
V3, V3_CODES = _list("tenx_v3")
ATAC, ATAC_CODES = _list("tenx_arc_atac")
LISTS = {"tenx_v2": V2, "tenx_v3": V3, "tenx_arc_atac": ATAC}


class TestLayouts:
    def test_every_layout_draws_from_a_registered_list(self):
        assert {layout.barcode_list for layout in LAYOUTS} <= set(lists_module.REGISTER)

    def test_ids_are_the_metadata_layers(self):
        from fetch_series.assays.chemistry import CHEMISTRY_LABELS

        gex = {layout.id for layout in LAYOUTS if layout.modality == "gex"}
        assert gex <= set(CHEMISTRY_LABELS)

    def test_3p_v2_and_5p_share_a_list_and_nothing_else_does_by_accident(self):
        assert list_class("10x_3p_v2") == list_class("10x_5p_v1_v2") == "tenx_v2"
        assert BY_ID["10x_3p_v2"].strand != BY_ID["10x_5p_v1_v2"].strand

    def test_flex_is_absent_because_its_list_cannot_be_fetched(self):
        assert not any("flex" in layout.id for layout in LAYOUTS)


class TestClassify:
    def test_3p_v3_sense_is_high(self):
        call = classify(library(V3_CODES, umi=12), LISTS, REFERENCES)
        assert (call.layout, call.level, call.library_type) == ("10x_3p_v3_family", "HIGH", "GEX")
        assert call.evidence["strand"]["strand"] == "sense"

    def test_strand_separates_3p_v2_from_5p(self):
        """The case metadata alone cannot settle: same list, same lengths."""
        three = classify(library(V2_CODES, umi=10), LISTS, REFERENCES)
        five = classify(library(V2_CODES, umi=10, antisense=True), LISTS, REFERENCES)
        assert three.layout == "10x_3p_v2" and five.layout == "10x_5p_v1_v2"

    def test_a_barcode_with_no_room_for_a_umi_is_not_gene_expression(self):
        """SRR31955810: a 16-base ATAC i5 read matched the ARC GEX list."""
        call = classify(library(V3_CODES, umi=0), LISTS, REFERENCES)
        assert call.layout != "10x_3p_v3_family"

    def test_one_strand_vote_does_not_settle_a_shared_list(self):
        """SRR19242687: poly-A alone said 3' v2 against a 5' label; tchem said ambiguous."""
        call = classify(library(V2_CODES, umi=10, cdna=lambda: _random(70) + "A" * 21), LISTS, {})
        assert call.level == "ambiguous"
        assert call.evidence["strand"]["votes"] == {"poly_a": "sense"}

    def test_without_strand_the_v2_list_is_ambiguous(self):
        # Random cDNA: no reference to orient it, and no poly-A run to vote.
        call = classify(library(V2_CODES, umi=10, cdna=lambda: _random(91)), LISTS, {})
        assert call.level == "ambiguous" and set(call.candidates) == {"10x_3p_v2", "10x_5p_v1_v2"}

    @pytest.mark.parametrize(
        ("tail", "expected"),
        [(TSO_PREFIX + "AAAAAAAAAAA", "10x_5p_v1_v2"), ("T" * 24, "10x_3p_v2")],
    )
    def test_a_long_barcode_read_shows_its_tail(self, tail, expected):
        call = classify(library(V2_CODES, umi=10, r1_extra=tail), LISTS, {})
        assert call.layout == expected

    def test_an_atac_barcode_after_a_spacer(self):
        samples = library(ATAC_CODES, umi=0, offset=8)
        call = classify(samples, LISTS, REFERENCES)
        assert call.layout == "10x_multiome_arc_atac"

    def test_a_constant_umi_is_a_contradiction(self):
        call = classify(library(V3_CODES, umi=12, constant_umi=True), LISTS, REFERENCES)
        assert any("UMI entropy" in c for c in call.contradictions) and call.level != "HIGH"

    def test_swapped_files_are_noticed(self):
        r1, r2 = library(V3_CODES, umi=12)
        r1.role_hint, r2.role_hint = "R2", "R1"
        call = classify([r2, r1], LISTS, REFERENCES)
        assert call.layout == "10x_3p_v3_family"
        assert any("swapped" in c for c in call.contradictions)

    def test_a_lone_cdna_read_gets_the_hint(self):
        """SRR29534765: the ENA fastq that dropped the barcode read."""
        call = classify(
            [_sample([_cdna(False) for _ in range(500)], "x.fastq.gz", None)], LISTS, REFERENCES
        )
        assert call.layout is None and call.level == "none"
        assert "barcode read missing" in (call.hint or "")

    def test_no_lists_says_so(self):
        call = classify(library(V3_CODES, umi=12), {}, REFERENCES)
        assert "lists --fetch" in (call.hint or "")

    def test_vdj_from_constant_regions(self):
        constant = [_random(1_500) for _ in range(5)]
        refs = {
            **REFERENCES,
            "tcr_bcr_constant_human": build_index("tcr_bcr_constant_human", constant),
        }
        call = classify(
            library(V2_CODES, umi=10, cdna=lambda: _cdna(True, source=constant)), LISTS, refs
        )
        assert call.library_type == "VDJ"

    def test_crispr_from_the_scaffold(self):
        call = classify(
            library(V3_CODES, umi=12, cdna=lambda: _random(30) + SCAFFOLD + _random(36)),
            LISTS,
            REFERENCES,
        )
        assert call.library_type == "CRISPR"

    def test_a_feature_barcode_library_is_a_handful_of_tags(self):
        tags = [_random(15) for _ in range(20)]
        call = classify(
            library(V3_CODES, umi=12, cdna=lambda: _random(10) + RNG.choice(tags) + _random(66)),
            LISTS,
            REFERENCES,
        )
        assert call.library_type == "feature barcode"

    def test_nothing_readable(self):
        assert classify([], LISTS).hint == "nothing could be read"

    def test_a_call_records_its_version(self):
        from fetch_series.inspect.classify import VERSION

        assert classify(library(V3_CODES, umi=12), LISTS, REFERENCES).version == VERSION


class TestSignals:
    def test_umi_entropy(self):
        assert umi_entropy([_random(28) for _ in range(500)], 16, 12) > 1.9
        assert umi_entropy(["A" * 28 for _ in range(500)], 16, 12) == 0.0

    @pytest.mark.parametrize(
        ("header", "name"),
        [
            ("A01044:65:HVVJ5DMXX:2:1101:1217:1000", "NovaSeq 6000"),
            ("K00337:202:HWNYLBBXX:6:1101:2747:1297", "HiSeq 3000/4000"),
            ("NB501234:1:HXXX:1:11101:1:1", "NextSeq 500/550"),
            ("LH00123:4:22X:1:1101:1:1", "NovaSeq X"),
        ],
    )
    def test_instrument_from_the_read_name(self, header, name):
        sample = FileSample("x", "fastq", facts={"instrument_ids": [header]})
        assert instrument(sample) == name


class TestLists:
    def test_encoding_is_two_bits_a_base(self):
        assert encode("ACGT") == 0b00011011 and encode("ACGN") is None

    def test_encoding_matches_a_base_by_base_loop(self):
        """The base-4 parse is an optimisation, so it must agree everywhere."""

        def loop(seq: str) -> int | None:
            value = 0
            for base in seq:
                if base not in "ACGT":
                    return None
                value = (value << 2) | "ACGT".index(base)
            return value

        rng = random.Random(11)
        cases = ["", "A", "TTTT", "acgt", "AC GT", "AC_GT", "A0C", "+ACG", "ACGT\n"]
        cases += [
            "".join(rng.choice("ACGTN") for _ in range(rng.randint(1, 24))) for _ in range(500)
        ]
        assert [encode(s) for s in cases] == [loop(s) for s in cases]

    def test_one_mismatch_and_an_n_wildcard(self):
        listed = V3_CODES[0]
        flipped = listed[:5] + ("A" if listed[5] != "A" else "C") + listed[6:]
        assert V3.near(flipped) and not V3.exact(flipped)
        assert V3.near(listed[:3] + "N" + listed[4:])
        assert not V3.near(listed[:3] + "NN" + listed[5:])

    def test_a_list_must_match_its_register(self):
        entry = Entry("x", 4, 3, "0" * 64, "s", "t")
        with pytest.raises(ListMismatch):
            pack(["ACGT", "ACGA"], entry)
        with pytest.raises(ListMismatch):
            pack(["ACGT", "ACG", "TTTT"], entry)

    def test_a_download_whose_sha256_differs_is_refused(self, tmp_path):
        body = gzip.compress(b"ACGT\nACGA\nTTTT\n")
        good = Entry("x", 4, 3, hashlib.sha256(body).hexdigest(), "https://h/x.txt.gz", "t")
        bad = Entry("y", 4, 3, "0" * 64, "https://h/x.txt.gz", "t")
        client = httpx.Client(
            transport=httpx.MockTransport(lambda r: httpx.Response(200, content=body))
        )
        path = lists_module.fetch(good, tmp_path, client=client)
        assert len(lists_module.load(good, tmp_path)) == 3 and path.exists()
        with pytest.raises(ListMismatch, match="sha256"):
            lists_module.fetch(bad, tmp_path, client=client)

    def test_the_register_ships_no_list(self):
        """10x's lists are under 10x's terms: the repository carries sources and digests only."""
        assert all("never redistributed" in entry.terms for entry in lists_module.REGISTER.values())


class TestAgreement:
    def _call(self, layout, candidates=None):
        return Call(
            layout,
            candidates or ([layout] if layout else []),
            "HIGH",
            barcode_list=list_class(layout or ""),
        )

    def test_exact_and_class(self):
        agreement = with_metadata(self._call("10x_3p_v2"), ChemistryCall(("10x_5p_v1_v2",)))
        assert (agreement.exact, agreement.same_class) == (False, True)
        assert agreement.note == "same list, different kit"

    def test_a_study_naming_two_kits_agrees_with_either(self):
        """GSE111360's protocol names a 3' and a 5' kit."""
        both = ChemistryCall(("10x_3p_v3_family", "10x_5p_v1_v2"))
        assert with_metadata(self._call("10x_5p_v1_v2"), both).same_class is True

    def test_no_call_is_not_a_disagreement(self):
        assert with_metadata(self._call(None), ChemistryCall(("10x_3p_v2",))).exact is None

    def test_across_sources_sets_an_unfit_source_apart(self):
        result = across_sources(
            {
                "ena": self._call(None),
                "sra": self._call("10x_3p_v2"),
                "bam": self._call("10x_3p_v2"),
            }
        )
        assert result["agree"] is True and result["uncalled"] == ["ena"]


class TestPool:
    """The worker pool is an optimisation: what it returns, inline returns."""

    async def test_a_call_in_a_worker_is_the_call_inline(self):
        from fetch_series.inspect.pool import Pool, judge, parse
        from fetch_series.inspect.readers.fastq import read_fastq

        samples = library(V3_CODES, umi=12, r1_extra="T" * 10)
        body = "".join(
            f"@SRR1.{i}\n{r.seq}\n+\n{r.qual}\n" for i, r in enumerate(samples[0].records)
        )
        with Pool(LISTS, REFERENCES, 2) as pool:
            assert await pool.judge(samples) == judge(samples, LISTS, REFERENCES)
            parsed = await parse(pool, read_fastq, body.encode(), "x.fastq", max_reads=100)
        assert parsed == read_fastq(body.encode(), "x.fastq", max_reads=100)
