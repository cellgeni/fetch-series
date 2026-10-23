"""What a copy is ready for: geometry, the mapping block, conversion, and the whitelist file."""

from __future__ import annotations

import hashlib
from typing import Any

import pytest

from fetch_series.inspect.lists import BarcodeList, Entry, ListMismatch, export, pack, unpack
from fetch_series.inspect.readiness import convertible, geometry, mapping, readiness, roles


def facts(mode: int, *, source: str = "x", fmt: str = "fastq", hint=None, lo=None, hi=None):
    return {
        "source": source,
        "format": fmt,
        "role_hint": hint,
        "length": {
            "mode": mode,
            "min": lo if lo is not None else mode,
            "max": hi if hi is not None else mode,
            "at_mode": 1.0,
        },
    }


def a_call(
    layout: str | None,
    *,
    barcode: int | None = 0,
    cdna: int | None = 1,
    barcode_list: str = "tenx_v3",
    level: str = "HIGH",
    strand: str | None = "sense",
    library: str | None = "GEX",
    offset: int = 0,
) -> dict[str, Any]:
    hits = (
        [
            {
                "barcode_list": barcode_list,
                "read": barcode,
                "offset": offset,
                "exact": 0.9,
                "near": 0.95,
                "background": 0.0016,
            }
        ]
        if barcode is not None
        else []
    )
    return {
        "layout": layout,
        "level": level,
        "barcode_list": barcode_list,
        "barcode_read": barcode,
        "cdna_read": cdna,
        "library_type": library,
        "evidence": {"hits": hits, "strand": {"strand": strand} if strand else None},
    }


V3 = a_call("10x_3p_v3_family")


class TestRoles:
    def test_index_barcode_and_cdna(self):
        """SRR29534765's SRA object: 8 + 26 + 101."""
        files = [facts(8), facts(26), facts(101)]
        call = a_call("10x_3p_v2", barcode=1, cdna=2, barcode_list="tenx_v2")
        assert roles(files, call) == ["I1", "R1", "R2"]

    def test_two_lanes_of_one_library(self):
        files = [
            facts(28, source="L1_R1", hint="R1"),
            facts(91, source="L1_R2", hint="R2"),
            facts(28, source="L2_R1", hint="R1"),
            facts(91, source="L2_R2", hint="R2"),
        ]
        assert roles(files, V3) == ["R1", "R2", "R1", "R2"]

    def test_atac_reads_its_barcode_as_the_i5(self):
        files = [facts(50), facts(16), facts(49)]
        call = a_call(
            "10x_atac_v1_v1.1_v2", barcode=1, cdna=0, barcode_list="tenx_atac", strand=None
        )
        assert roles(files, call) == ["R1", "R2", "R3"]

    def test_what_is_left_is_not_guessed(self):
        assert roles([facts(28), facts(91), facts(20)], V3) == ["R1", "R2", None]


class TestGeometry:
    def test_positions_are_one_based(self):
        shape = geometry({"files": [facts(28), facts(91)]}, V3, run="SRR1")
        assert shape["barcode"] == {
            "read": 0,
            "list": "tenx_v3",
            "cb_start": 1,
            "cb_len": 16,
            "umi_start": 17,
            "umi_len": 12,
            "umi_short": False,
        }
        assert shape["cdna"] == {"read": 1} and shape["trimmed"] is False

    def test_a_short_barcode_read_shortens_the_umi(self):
        shape = geometry({"files": [facts(26), facts(91)]}, V3)
        assert shape["barcode"]["umi_len"] == 10 and shape["barcode"]["umi_short"]

    def test_varying_lengths_are_reported_not_explained(self):
        shape = geometry({"files": [facts(28), facts(91, lo=35, hi=91)]}, V3)
        assert shape["trimmed"] is True and shape["reads"][1]["length"]["min"] == 35

    def test_fastq_names(self):
        files = [
            facts(28, source="https://h/a_1.fastq.gz"),
            facts(91, source="https://h/a_2.fastq.gz"),
        ]
        names = [r["name"] for r in geometry({"files": files}, V3, run="SRR1")["reads"]]
        assert names == [
            {"listed": "a_1.fastq.gz", "renamed": "SRR1_S1_L001_R1_001.fastq.gz"},
            {"listed": "a_2.fastq.gz", "renamed": "SRR1_S1_L001_R2_001.fastq.gz"},
        ]

    def test_an_sra_objects_reads_are_named_as_fastq_dump_splits_them(self):
        files = [
            facts(28, source="https://sra/o", fmt="sra"),
            facts(91, source="https://sra/o", fmt="sra"),
        ]
        names = [r["name"]["listed"] for r in geometry({"files": files}, V3, run="SRR1")["reads"]]
        assert names == ["SRR1_1.fastq", "SRR1_2.fastq"]


def block(files, call, fit=True):
    return mapping(geometry({"files": files}, call, run="SRR1"), call, fit)


class TestMapping:
    def test_a_3p_v3_block(self):
        made = block([facts(28), facts(91)], V3)
        assert made is not None
        assert made["starsolo"] == {
            "soloType": "CB_UMI_Simple",
            "soloCBwhitelist": "3M-february-2018.txt",
            "soloCBstart": 1,
            "soloCBlen": 16,
            "soloUMIstart": 17,
            "soloUMIlen": 12,
            "soloStrand": "Forward",
            "readFilesIn": ["x", "x"],
        }
        assert made["cellranger_chemistry"] == "SC3Pv3" and made["validated"] is True
        assert made["validation"]["same_list"] == made["validation"]["blocks"]
        assert made["starsolo_args"].startswith("--readFilesIn x x --soloType CB_UMI_Simple")

    def test_a_bams_reads_go_by_the_names_its_conversion_gives(self):
        made = block([facts(28, fmt="bam"), facts(91, fmt="bam")], V3)
        assert made is not None
        assert made["starsolo"]["readFilesIn"] == [
            "SRR1_S1_L001_R2_001.fastq.gz",
            "SRR1_S1_L001_R1_001.fastq.gz",
        ]

    def test_antisense_is_reverse(self):
        """SRR11181954: 5' v1/v2, 26 + 100, the cDNA read antisense."""
        call = a_call("10x_5p_v1_v2", barcode_list="tenx_v2", strand="antisense")
        made = block([facts(26), facts(100)], call)
        assert made is not None
        assert made["starsolo"]["soloStrand"] == "Reverse"
        assert made["cellranger_chemistry"] == "SC5P-R2"
        assert made["starsolo"]["soloCBwhitelist"] == "737K-august-2016.txt"

    @pytest.mark.parametrize("strand", ["unstranded", None])
    def test_an_unmeasured_strand_is_omitted_not_guessed(self, strand):
        made = block([facts(28), facts(91)], a_call("10x_3p_v3_family", strand=strand))
        assert made is not None and "soloStrand" not in made["starsolo"]
        assert any("soloStrand" in why for why in made["omitted"])

    def test_a_longer_barcode_read_turns_off_the_length_check(self):
        """SRR22434015's barcode read is 30 bases."""
        made = block([facts(30), facts(151)], V3)
        assert made is not None and made["starsolo"]["soloBarcodeReadLength"] == 0

    def test_an_exact_barcode_read_keeps_it(self):
        made = block([facts(28), facts(91)], V3)
        assert made is not None and "soloBarcodeReadLength" not in made["starsolo"]

    @pytest.mark.parametrize(
        ("call", "fit"),
        [
            (a_call("10x_3p_v3_family", level="LOW"), None),
            (a_call("10x_3p_v3_family", library="VDJ"), None),
            (V3, False),
            (a_call(None), None),
            (a_call("10x_atac_v1_v1.1_v2", barcode_list="tenx_atac"), None),
        ],
    )
    def test_no_block_for_anything_but_a_firm_gex_call(self, call, fit):
        assert block([facts(28), facts(91)], call, fit) is None


def test_a_layout_never_compared_says_it_is_unvalidated():
    call = a_call("10x_3p_v4_gemx", barcode_list="tenx_v4_3p")
    made = block([facts(28), facts(91)], call)
    assert made is not None and made["validated"] is False and made["validation"] is None


class TestConvertible:
    def bam(self, **facts_: Any) -> dict[str, Any]:
        return {"files": [facts(28, fmt="bam"), facts(91, fmt="bam")], "bam": facts_}

    def test_fastq_is_not_converted(self):
        assert convertible("fastq", {}, V3, True, "")["reason"] == "already FASTQ"

    def test_a_10x_header_says_bamtofastq(self):
        checks = self.bam(bamtofastq=["R1", "R2"], tags={"CR": 10}, records=10)
        out = convertible("bam", checks, V3, True, "")
        assert out["method"] == "bamtofastq" and out["convertible"] is True

    def test_tags_without_a_header(self):
        """ERR4265292: CR/CY and RX/QX, no 10x_bam_to_fastq line."""
        checks = self.bam(bamtofastq=[], tags={"CR": 10, "RX": 10}, records=10)
        out = convertible("bam", checks, V3, True, "")
        assert out["method"] == "tags" and "RX/QX" in out["reason"]

    def test_mates_in_the_records_split_by_flag(self):
        """ERR13855116's lane CRAM: 26 + 96 as 0x40/0x80 mates, no barcode tags."""
        checks = self.bam(
            bamtofastq=[], tags={"CR": 0}, records=10, paired={"first": 5, "second": 5}
        )
        out = convertible("cram", checks, V3, True, "")
        assert out["method"] == "samtools fastq" and out["convertible"] is True

    def test_no_barcode_tag_on_any_record(self):
        checks = self.bam(bamtofastq=[], tags={"CR": 0, "CB": 0}, records=500)
        call = a_call(None, barcode=None, cdna=None)
        assert convertible("bam", checks, call, None, "")["convertible"] is False

    def test_an_sra_object_split_into_the_called_reads(self):
        checks = {"files": [facts(28, fmt="sra"), facts(91, fmt="sra")], "quality": ["real"]}
        out = convertible("sra", checks, V3, True, "x", run_spot=119.0)
        assert out["convertible"] is True and out["method"] == "fastq-dump --split-files"

    def test_a_lite_object_is_flagged(self):
        checks = {"files": [facts(28, fmt="sra"), facts(91, fmt="sra")], "quality": ["substituted"]}
        assert "substituted" in convertible("sra", checks, V3, True, "x", run_spot=119.0)["note"]

    def test_a_prefix_short_of_the_run_is_undetermined(self):
        """DRR878211: its first spots are 26 + 8, its mean 66."""
        checks = {"files": [facts(26, fmt="sra"), facts(8, fmt="sra")], "quality": ["real"]}
        out = convertible("sra", checks, a_call(None), None, "x", run_spot=66.0)
        assert out["convertible"] is None and "never reached" in out["reason"]


def test_readiness_takes_a_report_source():
    source = {
        "kind": "fastq",
        "checks": {"files": [facts(28), facts(91)]},
        "call": V3,
        "fit": True,
        "reason": "10x_3p_v3_family at HIGH",
    }
    made = readiness(source, run="SRR1")
    assert set(made) == {"geometry", "mapping", "convertible"} and made["mapping"]


class TestWhitelistExport:
    CODES = ("AAAACCCCGGGGTTTT", "ACGTACGTACGTACGT", "TTTTTTTTTTTTTTTA")

    def entry(self, export_sha256: str) -> Entry:
        return Entry(
            "tenx_v3", 16, 3, "0" * 64, "https://h/3M-february-2018.txt.gz", "t", export_sha256
        )

    def test_unpack_reverses_pack(self):
        assert list(unpack(pack(self.CODES, self.entry("")), 16)) == sorted(self.CODES)

    def test_a_fourteen_base_list(self):
        codes = ["ACGTACGTACGTAC", "TTTTGGGGCCCCAA"]
        entry = Entry("tenx_v1", 14, 2, "0" * 64, "s", "t")
        assert list(unpack(pack(codes, entry), 14)) == sorted(codes)

    def text(self) -> str:
        return "".join(f"{c}\n" for c in sorted(self.CODES))

    def cached(self, tmp_path, entry: Entry) -> None:
        (tmp_path / "cache").mkdir()
        with (tmp_path / "cache" / "tenx_v3.u32").open("wb") as handle:
            pack(self.CODES, entry).tofile(handle)

    def test_the_export_is_checked_against_the_register(self, tmp_path):
        digest = hashlib.sha256(self.text().encode()).hexdigest()
        entry = self.entry(digest)
        self.cached(tmp_path, entry)
        path, written = export(entry, tmp_path / "out", tmp_path / "cache")
        assert path.name == "3M-february-2018.txt" and path.read_text() == self.text()
        assert written == digest

    def test_an_export_that_does_not_match_is_refused(self, tmp_path):
        entry = self.entry("f" * 64)
        self.cached(tmp_path, entry)
        with pytest.raises(ListMismatch):
            export(entry, tmp_path / "out", tmp_path / "cache")
        assert not list((tmp_path / "out").iterdir())

    def test_barcodes_of_a_loaded_list(self):
        entry = self.entry("")
        assert list(BarcodeList(entry, pack(self.CODES, entry)).barcodes()) == sorted(self.CODES)


@pytest.mark.integration
class TestConvertibleLive:
    """Each reader's copy, read live, says how it converts. Needs the lists fetched."""

    async def _source(self, record, *, run_spot=None):
        from fetch_series.inspect import lists as barcode_lists
        from fetch_series.inspect import references
        from fetch_series.inspect.fetch import sample_file
        from fetch_series.inspect.report import _source
        from fetch_series.survey.client import Limits, SurveyClient

        held = barcode_lists.available()
        if not held:
            pytest.skip("barcode lists not fetched")
        async with SurveyClient(Limits(rps=2, base_timeout=120)) as client:
            samples = await sample_file(record, client, reads=20_000)
        refs = {k: v for k in references.REGISTER if (v := references.index(k)) is not None}
        entry, _ = _source(
            record.source, str(record.kind), [record.url], samples, [record], held, refs, run_spot
        )
        return entry

    async def test_err4265292_bam_rebuilds_r1_from_tags(self):
        from fetch_series.files import FileKind, FileRecord

        url = "https://ftp.sra.ebi.ac.uk/vol1/run/ERR426/ERR4265292/UPN2_S2_L001.bam"
        record = FileRecord("ERR4265292", url, "UPN2_S2_L001.bam", FileKind.BAM, "ena")
        entry = await self._source(record)
        assert entry["convertible"]["method"] == "tags"

    async def test_err13855116_cram_splits_by_flag(self):
        from fetch_series.files import FileKind, FileRecord
        from fetch_series.inspect.readers import cram

        if not cram.available():
            pytest.skip("pysam not installed")
        url = "https://ftp.sra.ebi.ac.uk/vol1/run/ERR138/ERR13855116/27865_1#3.cram"
        record = FileRecord("ERR13855116", url, "27865_1#3.cram", FileKind.CRAM, "ena")
        entry = await self._source(record)
        assert entry["convertible"]["method"] == "samtools fastq"

    async def test_srr25056225_normalized_object_splits(self):
        from fetch_series.files import FileKind, FileRecord
        from fetch_series.inspect.readers import sra

        if not sra.available():
            pytest.skip("sra-tools not installed")
        url = "https://sra-pub-run-odp.s3.amazonaws.com/sra/SRR25056225/SRR25056225"
        record = FileRecord("SRR25056225", url, "SRR25056225", FileKind.SRA, "sdl")
        entry = await self._source(record, run_spot=119.0)
        assert entry["convertible"]["convertible"] is True
        assert entry["mapping"]["cellranger_chemistry"] == "SC5P-R2"
