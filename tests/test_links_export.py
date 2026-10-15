"""The links.tsv parity contract with fetch10xmeta.

The incumbent's nine-step priority chain, reproduced deliberately -- including
the places where this project's own file layer would choose differently. Parity
first; the improvements are in `fetch_series.files.recommend` and are reached
through `fetch files`, not through this exporter.
"""

from __future__ import annotations

from fetch_series.export.links import links_for, offer_for
from fetch_series.files import FileKind, FileRecord, FileSet

ENA_FASTQ = "run->file:ena_fastq"
ENA_SUBMITTED = "run->file:ena_submitted"
ENA_SRA = "run->file:ena_sra"
SDL = "run->file:sdl"
AE_SDRF = "ae_experiment->file:sdrf"


def rec(source: str, url: str, kind: FileKind, **kw) -> FileRecord:
    return FileRecord(
        run="SRR1", url=url, name=url.rsplit("/", 1)[-1], kind=kind, source=source, **kw
    )


def fs(*records: FileRecord) -> FileSet:
    return FileSet("SRR1", records)


class TestPriorityChain:
    def test_1_the_sdrf_fastq_outranks_everything(self):
        offer = fs(
            rec(AE_SDRF, "https://bs/a_R1.fastq.gz", FileKind.FASTQ),
            rec(ENA_FASTQ, "https://ena/SRR1_1.fastq.gz", FileKind.FASTQ),
            rec(ENA_FASTQ, "https://ena/SRR1_2.fastq.gz", FileKind.FASTQ),
        )
        assert offer_for(offer)[0] == "ORIFQ"

    def test_2_a_complete_ena_pair_is_enafq(self):
        kind, urls = offer_for(
            fs(
                rec(ENA_FASTQ, "https://ena/SRR1_1.fastq.gz", FileKind.FASTQ),
                rec(ENA_FASTQ, "https://ena/SRR1_2.fastq.gz", FileKind.FASTQ),
            )
        )
        assert kind == "ENAFQ" and len(urls) == 2

    def test_2_an_unpaired_ena_fastq_is_not_enafq(self):
        """ENA names the reads it serves itself _1/_2 and the incumbent requires
        both. ERP129702 publishes one file per run for a library it declares
        PAIRED; taking it would fetch half the reads."""
        offer = fs(
            rec(ENA_FASTQ, "https://ena/SRR1.fastq.gz", FileKind.FASTQ),
            rec(ENA_SRA, "https://ena/SRR1.sra", FileKind.SRA),
        )
        assert offer_for(offer)[0] == "SRA"

    def test_3_a_submitted_fastq_with_no_bam_is_orifq(self):
        kind, _ = offer_for(fs(rec(ENA_SUBMITTED, "https://ena/mine.fastq.gz", FileKind.FASTQ)))
        assert kind == "ORIFQ"

    def test_4_a_submitted_bam_outranks_a_submitted_fastq_beside_it(self):
        """A 10x BAM keeps the original reads including the barcode read, which
        a submitter fastq beside it may not."""
        kind, urls = offer_for(
            fs(
                rec(ENA_SUBMITTED, "https://ena/mine.fastq.gz", FileKind.FASTQ),
                rec(ENA_SUBMITTED, "https://ena/mine.bam", FileKind.BAM),
            )
        )
        assert kind == "BAM" and urls == ("https://ena/mine.bam",)

    def test_4_a_bam_index_is_never_offered(self):
        _, urls = offer_for(
            fs(
                rec(ENA_SUBMITTED, "https://ena/mine.bam", FileKind.BAM),
                rec(ENA_SUBMITTED, "https://ena/mine.bam.bai", FileKind.INDEX),
            )
        )
        assert urls == ("https://ena/mine.bam",)

    def test_5_an_sdl_bam_is_taken_only_when_it_is_free(self):
        paid = fs(rec(SDL, "https://s3/x.bam", FileKind.BAM, pay_required=True))
        assert offer_for(paid) == ("", ())
        cold = fs(rec(SDL, "https://s3/x.bam", FileKind.BAM, rehydration_required=True))
        assert offer_for(cold) == ("", ())
        free = fs(rec(SDL, "https://s3/x.bam", FileKind.BAM))
        assert offer_for(free)[0] == "BAM"

    def test_6_ena_sra_comes_before_sdl_sra(self):
        offer = fs(
            rec(ENA_SRA, "https://ena/SRR1.sra", FileKind.SRA),
            rec(SDL, "https://s3/sra/SRR1", FileKind.SRA),
        )
        assert offer_for(offer)[1] == ("https://ena/SRR1.sra",)

    def test_7_a_paid_sdl_sra_is_not_offered(self):
        assert offer_for(fs(rec(SDL, "https://s3/x", FileKind.SRA, pay_required=True))) == ("", ())

    def test_a_run_nothing_answered_for_yields_an_empty_offer(self):
        assert offer_for(fs()) == ("", ())


class TestRows:
    def test_columns_are_the_incumbents_and_placeholders_are_dashes(self):
        rows = links_for({"SRR1": fs()}, species={}, samples={})
        assert rows[0].as_row() == ("SRR1", "UNKNOWN", "-", "-", "NA")

    def test_urls_are_semicolon_joined(self):
        offer = fs(
            rec(ENA_FASTQ, "https://ena/SRR1_1.fastq.gz", FileKind.FASTQ),
            rec(ENA_FASTQ, "https://ena/SRR1_2.fastq.gz", FileKind.FASTQ),
        )
        row = links_for({"SRR1": offer}, {"SRR1": "Homo sapiens"}, {"SRR1": "GSM1"})[0]
        assert row.as_row() == (
            "SRR1",
            "Homo sapiens",
            "https://ena/SRR1_1.fastq.gz;https://ena/SRR1_2.fastq.gz",
            "ENAFQ",
            "GSM1",
        )

    def test_a_run_with_no_offer_still_gets_a_row(self):
        """Dropping it would make a series look smaller than it is, which hides
        missing data instead of reporting it."""
        rows = links_for({"SRR1": fs(), "SRR2": fs()}, {}, {})
        assert [r.run for r in rows] == ["SRR1", "SRR2"]

    def test_rows_are_sorted_by_run(self):
        rows = links_for({"SRR9": fs(), "SRR1": fs()}, {}, {})
        assert [r.run for r in rows] == ["SRR1", "SRR9"]
