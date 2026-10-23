"""Offline tests for the read-level readers and checks, on synthetic files."""

from __future__ import annotations

import bz2
import gzip
import struct

import httpx
import pytest

from fetch_series.files import FileKind, FileRecord
from fetch_series.inspect.checks import check_set, duplicates, quality_verdict
from fetch_series.inspect.decompress import gunzip_prefix, inflate
from fetch_series.inspect.model import FileSample, Record
from fetch_series.inspect.readers.bam import parse_bam, read_bam
from fetch_series.inspect.readers.fastq import read_fastq, role_hint
from fetch_series.survey.client import Limits, SurveyClient


def fastq(n: int, length: int, *, prefix: str = "SRR1", suffix: str = "", qual: str = "F") -> str:
    return "".join(
        f"@{prefix}.{i} A00:1:X:1:{i}{suffix}\n{'ACGT' * (length // 4)}{'A' * (length % 4)}\n"
        f"+\n{qual * length}\n"
        for i in range(1, n + 1)
    )


# ------------------------------------------------------------------ FASTQ


class TestFastq:
    def test_plain_gzip_and_bzip2_read_alike(self):
        text = fastq(10, 28).encode()
        for data in (text, gzip.compress(text), bz2.compress(text)):
            sample = read_fastq(data, "x_1.fastq.gz", max_reads=100, complete=True)
            assert len(sample.records) == 10 and set(sample.lengths) == {28}
            assert sample.reached_end

    def test_multi_member_gzip_is_read_through(self):
        data = gzip.compress(fastq(5, 28).encode()) + gzip.compress(
            fastq(5, 28, prefix="SRR2").encode()
        )
        sample = read_fastq(data, "x.fastq.gz", max_reads=100, complete=True)
        assert len(sample.records) == 10

    def test_a_record_cut_by_the_prefix_is_dropped_not_kept_short(self):
        """A truncated read would look like a short one."""
        data = gzip.compress(fastq(200, 91).encode())
        prefix = data[: len(data) // 2]
        sample = read_fastq(prefix, "x.fastq.gz", max_reads=1_000)
        assert sample.records and set(sample.lengths) == {91}
        assert not sample.reached_end

    def test_a_plain_prefix_cut_mid_quality_line(self):
        text = fastq(3, 91)
        sample = read_fastq(text[: len(text) - 20].encode(), "x.fastq", max_reads=10)
        assert len(sample.records) == 2

    def test_max_reads_stops_the_read(self):
        sample = read_fastq(fastq(50, 28).encode(), "x.fastq", max_reads=7, complete=True)
        assert len(sample.records) == 7 and not sample.reached_end

    def test_the_suffix_that_says_which_read_survived(self):
        """SRR29534765's ENA fastq names its one read /3."""
        sample = read_fastq(fastq(4, 91, suffix="/3").encode(), "x.fastq", max_reads=10)
        assert sample.suffixes == {"3": 4}

    def test_malformed_records_are_an_error_not_a_sample(self):
        with pytest.raises(ValueError):
            read_fastq(
                b"@a\nACGT\n+\nFF\n@b\nACGT\n+\nFFFF\n", "x.fastq", max_reads=10, complete=True
            )

    @pytest.mark.parametrize(
        ("name", "role"),
        [
            ("TSF2_S35_L002_R1_001.fastq.gz", "R1"),
            ("TSF2_S35_L002_I1_001.fastq.gz", "I1"),
            ("SRR1_2.fastq.gz", "R2"),
            ("SRR1.fastq.gz", None),
        ],
    )
    def test_a_name_is_a_hint(self, name, role):
        assert role_hint(name) == role


# ----------------------------------------------------------------- BAM


def _bgzf(raw: bytes, block: int = 4_000) -> bytes:
    return b"".join(
        gzip.compress(raw[i : i + block]) for i in range(0, len(raw), block)
    ) + gzip.compress(b"")


def _tag(name: str, value: str) -> bytes:
    return name.encode() + b"Z" + value.encode() + b"\x00"


def _record(
    name: str, seq: str, *, flag: int = 4, tags: bytes = b"", qual: str | None = None
) -> bytes:
    table = {c: i for i, c in enumerate("=ACMGRSVTWYHKDBN")}
    packed = bytes(
        (table[seq[i]] << 4) | (table[seq[i + 1]] if i + 1 < len(seq) else 0)
        for i in range(0, len(seq), 2)
    )
    quals = bytes(ord(c) - 33 for c in qual) if qual else b"\xff" * len(seq)
    read_name = name.encode() + b"\x00"
    body = (
        struct.pack("<iiBBHHHiiii", -1, -1, len(read_name), 255, 4680, 0, flag, len(seq), -1, -1, 0)
        + read_name
        + packed
        + quals
        + tags
    )
    return struct.pack("<i", len(body)) + body


def bam(header: str, records: list[bytes]) -> bytes:
    text = header.encode()
    raw = (
        b"BAM\x01" + struct.pack("<i", len(text)) + text + struct.pack("<i", 0) + b"".join(records)
    )
    return _bgzf(raw)


class TestBam:
    def test_a_cell_ranger_bam_is_rebuilt_as_bamtofastq_would(self):
        header = (
            "@HD\tVN:1.4\tSO:coordinate\n"
            "@PG\tID:cellranger\tPN:cellranger\n"
            "@CO\t10x_bam_to_fastq:R1(CR:CY,UR:UY)\n"
            "@CO\t10x_bam_to_fastq:R2(SEQ:QUAL)\n"
        )
        tags = (
            _tag("CR", "A" * 16)
            + _tag("CY", "F" * 16)
            + _tag("UR", "C" * 12)
            + _tag("UY", "F" * 12)
        )
        records = [
            _record(f"r{i}", "ACGT" * 22 + "A", flag=0, tags=tags, qual="F" * 89) for i in range(20)
        ]
        r1, r2 = read_bam(bam(header, records), "x.bam", max_reads=100, complete=True)
        assert (r1.role_hint, r1.derived, set(r1.lengths)) == ("R1", True, {28})
        assert (r2.role_hint, r2.derived, set(r2.lengths)) == ("R2", False, {89})
        assert r1.facts["sort_order"] == "coordinate" and r1.facts["first_reads_are_chr1"]
        assert r1.facts["bamtofastq"] == ["R1", "R2"]

    def test_a_trimmed_tag_named_by_the_header_may_be_absent(self):
        """SRR6459192: Cell Ranger 2 names TR:TQ in R1's spec and writes it only on trimmed reads."""
        header = (
            "@HD\tVN:1.4\tSO:coordinate\n"
            "@CO\t10x_bam_to_fastq:I1(BC:QT)\n"
            "@CO\t10x_bam_to_fastq:R1(CR:CY,UR:UY,TR:TQ)\n"
            "@CO\t10x_bam_to_fastq:R2(SEQ:QUAL)\n"
        )
        tags = (
            _tag("BC", "G" * 8)
            + _tag("QT", "F" * 8)
            + _tag("CR", "A" * 16)
            + _tag("CY", "F" * 16)
            + _tag("UR", "C" * 10)
            + _tag("UY", "F" * 10)
        )
        trimmed = tags + _tag("TR", "T" * 4) + _tag("TQ", "F" * 4)
        records = [
            _record(f"r{i}", "ACGT" * 25, flag=0, tags=trimmed if i % 4 == 0 else tags)
            for i in range(20)
        ]
        i1, r1, r2 = read_bam(bam(header, records), "x.bam", max_reads=100, complete=True)
        assert (i1.role_hint, set(i1.lengths)) == ("I1", {8})
        assert (r1.role_hint, len(r1.records), set(r1.lengths)) == ("R1", 20, {26, 30})
        assert set(r2.lengths) == {100}

    def test_a_barcode_tag_named_by_the_header_is_never_optional(self):
        header = "@CO\t10x_bam_to_fastq:R1(CR:CY,UR:UY)\n@CO\t10x_bam_to_fastq:R2(SEQ:QUAL)\n"
        records = [_record("r", "ACGT" * 25, flag=0, tags=_tag("UR", "C" * 10))]
        r1, _ = read_bam(bam(header, records), "x.bam", max_reads=10, complete=True)
        assert r1.records == []

    def test_a_bam_without_the_header_is_rebuilt_from_its_tags(self):
        """ERR4265292: fastq_pre_barcodes wrote CR/CY and RX/QX, no 10x_bam_to_fastq."""
        tags = (
            _tag("CR", "A" * 16)
            + _tag("CY", "F" * 16)
            + _tag("RX", "C" * 12)
            + _tag("QX", "F" * 12)
        )
        records = [
            _record(f"r{i}", "ACGT" * 22 + "ACG", tags=tags, qual="F" * 91) for i in range(10)
        ]
        r1, r2 = read_bam(
            bam("@HD\tVN:1.5\n@PG\tID:1 PN:fastq_pre_barcodes\n", records), "x.bam", max_reads=100
        )
        assert set(r1.lengths) == {28} and r1.derived and set(r2.lengths) == {91}
        assert r1.facts["programs"] == ["fastq_pre_barcodes"] and r1.facts["rebuilt_from_tags"]

    def test_a_raw_barcode_is_used_never_a_corrected_one(self):
        """CB is what a pipeline decided; checking it against a whitelist passes by construction."""
        tags = _tag("CB", "A" * 16 + "-1")
        records = [_record("r", "ACGT" * 23, tags=tags)]
        (only,) = read_bam(bam("@HD\tVN:1.5\n", records), "x.bam", max_reads=10)
        assert set(only.lengths) == {92} and not only.derived

    def test_secondary_and_supplementary_records_are_not_reads(self):
        records = [
            _record("a", "ACGT" * 10, flag=0x40 | 0x1),
            _record("a", "ACGT" * 10, flag=0x100),
            _record("b", "ACGT" * 10, flag=0x800),
        ]
        parsed = parse_bam(bam("@HD\tVN:1.5\n", records), max_records=10)
        assert [r.name for r in parsed.records] == ["a"]

    def test_paired_records_split_by_mate(self):
        records = [_record(f"s{i}", "ACGT" * 7, flag=0x41) for i in range(3)] + [
            _record(f"s{i}", "ACGT" * 25, flag=0x81) for i in range(3)
        ]
        r1, r2 = read_bam(bam("@HD\tVN:1.5\n", records), "x.cram.bam", max_reads=10)
        assert (r1.role_hint, set(r1.lengths), r2.role_hint, set(r2.lengths)) == (
            "R1",
            {28},
            "R2",
            {100},
        )

    def test_a_record_cut_by_the_prefix_is_dropped(self):
        records = [_record(f"r{i}", "ACGT" * 25) for i in range(200)]
        data = bam("@HD\tVN:1.5\n", records)
        parsed = parse_bam(data[: len(data) // 2], max_records=1_000)
        assert 0 < len(parsed.records) < 200
        assert all(len(r.seq) == 100 for r in parsed.records)

    def test_not_a_bam_is_an_error(self):
        with pytest.raises(ValueError):
            parse_bam(gzip.compress(b"@SQ not a bam"), max_records=1)


class TestBgzfPrefix:
    def test_a_prefix_inside_a_member_is_not_clean(self):
        data = _bgzf(b"x" * 20_000)
        out, clean = gunzip_prefix(data[:-30])
        assert not clean and len(out) < 20_000

    def test_plain_data_passes_through(self):
        assert inflate(b"@r\nA\n+\nF\n") == (b"@r\nA\n+\nF\n", True)


# ----------------------------------------------------------------- checks


def sample(
    lengths: int, n: int = 50, *, qual: str = "F:,", source: str = "a", derived: bool = False
) -> FileSample:
    return FileSample(
        source=source,
        format="fastq",
        records=[Record(f"SRR1.{i}", "A" * lengths, (qual * lengths)[:lengths]) for i in range(n)],
        derived=derived,
    )


class TestChecks:
    def test_a_paired_10x_set_has_both_reads(self):
        checks = check_set([sample(28, source="r1"), sample(91, source="r2")])
        assert checks["barcode_read"] and checks["cdna_read"]
        assert checks["spot_names_agree"] == 1.0

    def test_a_lone_cdna_read_has_lost_the_barcode_read(self):
        """SRR29534765's ENA fastq; ERR4265292's SRA object."""
        checks = check_set([sample(91)])
        assert not checks["barcode_read"] and checks["cdna_read"]

    def test_a_28_base_read_alone_holds_no_cdna(self):
        """SRR30132199: 28-base reads only, unrecoverable everywhere."""
        checks = check_set([sample(28)])
        assert not checks["barcode_read"] and not checks["cdna_read"]

    def test_an_index_read_is_not_a_barcode_read(self):
        checks = check_set([sample(8, source="i1"), sample(91, source="r2")])
        assert not checks["barcode_read"]

    def test_lite_quality_is_substituted(self):
        assert quality_verdict(sample(28, qual="?")) == "substituted"
        assert quality_verdict(sample(28)) == "real"
        empty = FileSample("x", "bam", records=[Record("r", "ACGT", None)])
        assert quality_verdict(empty) == "absent"

    def test_nothing_read_is_not_readable(self):
        assert check_set([FileSample("x", "fastq")])["readable"] is False

    def test_the_checks_never_carry_a_sequence(self):
        checks = check_set([sample(28, source="r1"), sample(91, source="r2")])
        assert "AAAAAAAAAAAAAAAAAAAAAAAA" not in repr(checks)

    def test_identical_files_are_found_by_md5_and_size(self):
        def rec(url, md5):
            return FileRecord("SRR1", url, url, FileKind.FASTQ, "t", size=10, md5=md5)

        assert duplicates([rec("a", "x"), rec("b", "x"), rec("c", "y")]) == [["a", "b"]]


# ---------------------------------------------------------- stream_prefix


class TestStreamPrefix:
    @staticmethod
    async def _fetch(handler, max_bytes: int):
        async with (
            httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http,
            SurveyClient(Limits(rps=0), client=http) as client,
        ):
            return await client.stream_prefix("https://h/27865_1#3.cram", max_bytes)

    async def test_a_ranged_answer(self):
        seen = []

        def handler(request):
            seen.append((request.url.raw_path, request.url.fragment, request.headers["range"]))
            return httpx.Response(
                206, content=b"x" * 10, headers={"content-range": "bytes 0-9/500"}
            )

        prefix = await self._fetch(handler, 10)
        assert prefix.ranged and prefix.total == 500 and not prefix.complete
        # The # is part of the name, never a fragment.
        assert seen == [(b"/27865_1%233.cram", "", "bytes=0-9")]

    async def test_a_server_that_ignores_range_is_cut_off(self):
        def handler(request):
            return httpx.Response(200, content=b"y" * 100_000, headers={"content-length": "100000"})

        prefix = await self._fetch(handler, 1_000)
        assert len(prefix.data) == 1_000 and not prefix.ranged and not prefix.complete

    async def test_a_small_file_is_complete(self):
        def handler(request):
            return httpx.Response(206, content=b"z" * 5, headers={"content-range": "bytes 0-4/5"})

        assert (await self._fetch(handler, 1_000)).complete

    async def test_errors_are_redacted(self):
        def handler(request):
            raise httpx.ConnectError(
                f"no route to {request.url}?api_key=abcdefghijklmnopqrstuvwxyz01"
            )

        with pytest.raises(httpx.ConnectError) as caught:
            await self._fetch(handler, 10)
        assert "abcdefghijklmnop" not in str(caught.value)
