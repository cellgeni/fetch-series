"""The file layer: classification, mate detection, and disagreement.

The file layer exists because a run accession is not a deliverable. These tests
cover the three ways a file set looks fine and is not: an index file counted as
data, a checksum attached to the wrong file, and one provider's answer taken for
the whole truth.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pytest

from fetch_series.files import (
    FileKind,
    FileRecord,
    FileSet,
    classify,
    from_ena_row,
    from_sdl_files,
    mate_of,
)


class TestClassification:
    @pytest.mark.parametrize(
        ("name", "kind"),
        [
            ("SRR25056225_1.fastq.gz", FileKind.FASTQ),
            ("x.fq.gz", FileKind.FASTQ),
            ("Day2_r1.bam", FileKind.BAM),
            ("x.cram", FileKind.CRAM),
            ("SRR1.sra", FileKind.SRA),
            ("something.txt", FileKind.OTHER),
        ],
    )
    def test_classifies_by_suffix(self, name, kind):
        assert classify(name) == kind

    @pytest.mark.parametrize("name", ["Day2_r1.bam.bai", "x.cram.crai", "y.vcf.gz.tbi"])
    def test_an_index_is_not_its_data_file(self, name):
        """`x.bam.bai` contains `.bam`. Testing for the data suffix first
        misclassifies every index in the archive as the thing it indexes --
        and ERR2861957's .bai is 16 bytes, so the size does not give it away
        unless something already knows to look."""
        assert classify(name) is FileKind.INDEX


class TestMateDetection:
    @pytest.mark.parametrize(
        ("name", "mate"),
        [
            ("SRR1_1.fastq.gz", 1),
            ("SRR1_2.fastq.gz", 2),
            ("SRR1_3.fastq.gz", 3),
            ("SRR1.fastq.gz", None),
            ("Day2_r1.bam", None),
        ],
    )
    def test_reads_the_mate_from_the_name(self, name, mate):
        assert mate_of(name) == mate

    def test_an_index_read_is_not_a_data_mate(self):
        """I1 carries the 10x barcode: a real read, and not mate 1 or 2. A run
        offering _1 and _I1 has two files and half a pair."""
        assert mate_of("SRR1_I1.fastq.gz") == 0
        assert (
            FileSet("SRR1", (_fastq("SRR1_1.fastq.gz", 1), _fastq("SRR1_I1.fastq.gz", 0))).is_paired
            is False
        )

    def test_a_full_pair_is_paired(self):
        pair = FileSet("SRR1", (_fastq("SRR1_1.fastq.gz", 1), _fastq("SRR1_2.fastq.gz", 2)))
        assert pair.is_paired


def _fastq(name: str, mate: int | None, md5: str | None = "d41d8") -> FileRecord:
    return FileRecord(
        run="SRR1",
        url=f"https://example/{name}",
        name=name,
        kind=FileKind.FASTQ,
        source="test",
        mate=mate,
        md5=md5,
    )


class TestEnaColumns:
    """ENA packs parallel `;`-joined lists that are meaningful only positionally."""

    ROW: ClassVar[dict[str, str]] = {
        "run_accession": "SRR25056225",
        "fastq_ftp": "ftp.sra.ebi.ac.uk/a/SRR25056225_1.fastq.gz;ftp.sra.ebi.ac.uk/a/SRR25056225_2.fastq.gz",
        "fastq_md5": "aaa;bbb",
        "fastq_bytes": "10;20",
    }

    def test_pairs_url_md5_and_size_by_position(self):
        records = from_ena_row(self.ROW, "fastq", "r")
        assert [r.name for r in records] == ["SRR25056225_1.fastq.gz", "SRR25056225_2.fastq.gz"]
        assert [r.md5 for r in records] == ["aaa", "bbb"]
        assert [r.size for r in records] == [10, 20]
        assert [r.mate for r in records] == [1, 2]

    def test_adds_the_scheme_ena_omits(self):
        assert from_ena_row(self.ROW, "fastq", "r")[0].url.startswith("https://")

    def test_a_mismatched_checksum_list_is_dropped_not_misaligned(self):
        """Pairing by index anyway attaches one file's checksum to another,
        which is worse than no checksum: it fails a download that was fine."""
        row = {**self.ROW, "fastq_md5": "aaa"}
        records = from_ena_row(row, "fastq", "r")
        assert len(records) == 2
        assert all(r.md5 is None for r in records)
        # The sizes still line up, so they survive.
        assert [r.size for r in records] == [10, 20]

    def test_an_absent_column_family_yields_nothing(self):
        """A run with no submitted_ftp has no original deposit registered.
        That is an answer, not an error."""
        assert from_ena_row(self.ROW, "submitted", "r") == []


class TestSdlBundles:
    BUNDLE: ClassVar[list[dict[str, Any]]] = [
        {
            "accession": "ERR2861957",
            "type": "bam",
            "name": "Day2_r1.bam",
            "size": 14278674831,
            "md5": "b6baf5",
            "locations": [{"service": "ebi", "link": "http://ftp.sra.ebi.ac.uk/a/Day2_r1.bam"}],
        },
        {
            "accession": "ERR2861957",
            "type": "bam",
            "name": "Day2_r1.bam.bai",
            "size": 16,
            "md5": "f35cc0",
            "locations": [{"service": "ebi", "link": "http://ftp.sra.ebi.ac.uk/a/Day2_r1.bam.bai"}],
        },
    ]

    def test_the_index_is_carried_but_is_not_data(self):
        records = from_sdl_files("ERR2861957", self.BUNDLE, "sdl")
        kinds = {r.name: r.kind for r in records}
        assert kinds["Day2_r1.bam"] is FileKind.BAM
        assert kinds["Day2_r1.bam.bai"] is FileKind.INDEX
        assert len(FileSet("ERR2861957", tuple(records)).data_files) == 1

    def test_an_sra_object_named_for_its_accession_is_still_an_sra_file(self):
        """SDL names the .sra object after the run, with no suffix at all, so
        the name alone classifies it OTHER. SDL's own type field decides."""
        records = from_sdl_files(
            "SRR1",
            [
                {
                    "type": "sra",
                    "name": "SRR1",
                    "locations": [{"link": "https://s3/sra/SRR1"}],
                }
            ],
            "sdl",
        )
        assert records[0].kind is FileKind.SRA

    def test_each_service_is_a_separate_record(self):
        """S3 and EBI are real alternatives, and merging them would hide that
        one of the two is the one that costs money to retrieve."""
        records = from_sdl_files(
            "SRR1",
            [
                {
                    "type": "sra",
                    "name": "SRR1",
                    "locations": [
                        {"service": "s3", "link": "https://s3/x", "payRequired": True},
                        {"service": "ncbi", "link": "https://ncbi/x"},
                    ],
                }
            ],
            "sdl",
        )
        assert len(records) == 2
        assert [r.is_free for r in records] == [False, True]


class TestFileSet:
    """The set keeps every provider's answer, disagreements included."""

    def test_sources_are_reported_in_order_seen(self):
        records = (
            _fastq("a_1.fastq.gz", 1),
            FileRecord("SRR1", "u", "a.bam", FileKind.BAM, "sdl"),
            _fastq("a_2.fastq.gz", 2),
        )
        assert FileSet("SRR1", records).sources == ("test", "sdl")

    def test_a_set_with_no_checksums_is_not_fully_verifiable(self):
        unverifiable = FileSet("SRR1", (_fastq("a_1.fastq.gz", 1, md5=None),))
        assert not unverifiable.fully_verifiable
        assert FileSet("SRR1", (_fastq("a_1.fastq.gz", 1),)).fully_verifiable

    def test_an_empty_set_is_not_fully_verifiable(self):
        """Vacuous truth here would report 'all files verified' for a run with
        no files at all."""
        assert not FileSet("SRR1", ()).fully_verifiable

    def test_an_index_alone_does_not_make_a_verifiable_set(self):
        index_only = FileSet(
            "SRR1", (FileRecord("SRR1", "u", "a.bam.bai", FileKind.INDEX, "sdl", md5="x"),)
        )
        assert not index_only.fully_verifiable
