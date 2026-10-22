"""The fit rule, the content survey's fan-out, and the readers that may be absent."""

from __future__ import annotations

import gzip
from datetime import UTC, datetime

import httpx
import pytest

from fetch_series.files import FileKind, FileRecord
from fetch_series.inspect.checks import check_set
from fetch_series.inspect.classify import NO_LISTS, Call
from fetch_series.inspect.fetch import sample_file
from fetch_series.inspect.fit import RULE, MissingInput, fit, fit_from_detail
from fetch_series.inspect.model import FileSample, Record
from fetch_series.inspect.readers import cram, sra
from fetch_series.inspect.survey import INSPECT_GATE, rejudge, screen_links
from fetch_series.results import Outcome, RouteResult
from fetch_series.survey import corpora
from fetch_series.survey.client import Limits, SurveyClient


def sample(length: int, n: int = 50) -> FileSample:
    return FileSample(
        "x", "fastq", records=[Record(f"r{i}", "A" * length, "F" * length) for i in range(n)]
    )


def hit(read: int = 0, near: float = 0.9, exact: float = 0.85) -> dict[str, object]:
    return {
        "barcode_list": "tenx_v3",
        "read": read,
        "offset": 0,
        "exact": exact,
        "near": near,
        "sampled": 50,
        "background": 0.0016,
    }


def call(layout: str | None, level: str, *, hits=(), hint=None, library_type="GEX") -> Call:
    return Call(
        layout,
        [layout] if layout else [],
        level,
        evidence={"hits": list(hits)},
        hint=hint,
        library_type=library_type,
    )


class TestFit:
    def test_a_called_10x_set_is_fit(self):
        verdict = fit([sample(28), sample(91)], call("10x_3p_v3_family", "HIGH", hits=[hit()]))
        assert verdict == {
            "fit": True,
            "rule": RULE,
            "reason": "10x_3p_v3_family at HIGH",
            "run_spot": None,
            "mates": 1.0,
        }

    def test_no_cdna_read_is_proven_unusable(self):
        """SRR30132199: 28-base reads only."""
        assert fit([sample(28)], call(None, "ambiguous", hits=[hit()]))["fit"] is False

    def test_a_lone_cdna_read_with_no_listed_barcode_is_proven_unusable(self):
        """SRR29534765's ENA fastq."""
        verdict = fit([sample(101)], call(None, "none", hint="barcode read missing: ..."))
        assert verdict["fit"] is False

    def test_no_list_to_check_against_is_undetermined_not_unfit(self):
        assert fit([sample(28), sample(91)], call(None, "none", hint=NO_LISTS))["fit"] is None

    def test_nothing_read_is_undetermined(self):
        assert fit([FileSample("x", "cram")], call(None, "none"))["fit"] is None

    def test_a_library_that_is_not_gene_expression_is_none(self):
        verdict = fit(
            [sample(28), sample(91)], call("10x_5p_v1_v2", "HIGH", hits=[hit()], library_type="VDJ")
        )
        assert verdict["fit"] is None and "VDJ" in verdict["reason"]

    def test_a_weak_call_proves_nothing_either_way(self):
        verdict = fit(
            [sample(28), sample(91)], call("10x_3p_v2", "LOW", hits=[hit(near=0.3, exact=0.25)])
        )
        assert verdict["fit"] is None

    def test_a_listed_barcode_on_a_short_read_is_not_a_barcode_read(self):
        """A hit on an 8-base index read cannot be a 16-base barcode."""
        verdict = fit(
            [sample(8), sample(91)],
            call(None, "none", hits=[hit(read=0)], hint="no listed 10x barcode"),
        )
        assert verdict["fit"] is False


def sra_object(*lengths: int, n: int = 50) -> list[FileSample]:
    """One SRA object's reads, split by fastq-dump: every read shares the object's URL."""
    return [
        FileSample(
            "https://sra/SRR1",
            "sra",
            records=[Record(f"SRR1.{i}", "A" * length, "F" * length) for i in range(n)],
        )
        for length in lengths
    ]


class TestFit2:
    """The three conditions the 2026-10-20 content survey added (fit-2)."""

    def test_an_sra_prefix_short_of_the_run_refuses_nothing(self):
        """DRR878211: 26 + 8-base spots first, 98-base spots later; a mean of 66."""
        verdict = fit(sra_object(26, 8), call(None, "none", hits=[hit()]), run_spot=66.0)
        assert verdict["fit"] is None and "never reached" in verdict["reason"]

    def test_an_sra_prefix_that_is_the_run_still_refuses(self):
        """SRR30132199: 28 bases a spot, all the way through."""
        verdict = fit(sra_object(28), call(None, "none", hits=[hit()]), run_spot=28.0)
        assert verdict["fit"] is False

    def test_an_sra_object_without_the_runs_mean_refuses_nothing(self):
        assert fit(sra_object(28), call(None, "none", hits=[hit()]))["fit"] is None

    def test_a_derived_fastq_short_of_the_run_is_still_refused(self):
        """SRR29534765: ENA's lone fastq is short of the run because it omits a read."""
        verdict = fit([sample(101)], call(None, "none", hint="barcode read missing: ..."))
        assert verdict["fit"] is False

    def test_a_barcode_shaped_read_no_list_holds_refuses_nothing(self):
        """SRR9105023: 10x 3' v1 -- 8 + 98 + 14 + 10, its barcode in the 14-base read."""
        verdict = fit(
            sra_object(8, 98, 14, 10),
            call(None, "none", hint="no listed 10x barcode"),
            run_spot=130.0,
        )
        assert verdict["fit"] is None and "14-base" in verdict["reason"]

    def test_a_cdna_read_from_other_spots_is_not_fit(self):
        """DRR878211's DDBJ fastqs: the 98-base reads are unpaired spots of their own."""
        barcode = FileSample(
            "x_1.fastq",
            "fastq",
            records=[Record(f"DRR1.{i}", "A" * 26, "F" * 26) for i in range(50)],
        )
        orphans = FileSample(
            "x.fastq",
            "fastq",
            records=[Record(f"DRR1.{i + 1000}", "A" * 98, "F" * 98) for i in range(50)],
        )
        verdict = fit([orphans, barcode], call("10x_3p_v3_family", "HIGH", hits=[hit(read=1)]))
        assert verdict["fit"] is None and verdict["mates"] == 0.0


class TestFit3:
    """A cDNA read is any read of 30 bases or more that is not the barcode read."""

    def test_a_47_base_cdna_read_is_usable(self):
        """SRR34778689: 28 + 47, refused by fit-2 on every copy."""
        verdict = fit([sample(28), sample(47)], call("10x_3p_v3_family", "MEDIUM", hits=[hit()]))
        assert verdict["fit"] is True

    def test_a_28_base_set_is_still_unfit(self):
        """SRR30132199."""
        verdict = fit([sample(28)], call(None, "none", hits=[hit()]))
        assert verdict["fit"] is False and verdict["reason"] == "no read of 30 bases or more"

    def test_a_30_base_barcode_read_is_not_its_own_cdna_read(self):
        """SRR22434015's barcode read is 30 bases; alone, it is no set at all."""
        verdict = fit([sample(30)], call("10x_3p_v3_family", "MEDIUM", hits=[hit()]))
        assert verdict["fit"] is False and "besides the barcode read" in verdict["reason"]

    def test_a_30_base_barcode_read_beside_a_cdna_read_is_fit(self):
        verdict = fit([sample(30), sample(98)], call("10x_3p_v3_family", "HIGH", hits=[hit()]))
        assert verdict["fit"] is True

    def test_a_long_barcode_read_alone_is_never_fit(self):
        """DRR259182: one 150-base fastq whose reads start with listed barcodes; fit-2 passed it."""
        verdict = fit([sample(150)], call("10x_3p_v3_family", "MEDIUM", hits=[hit()]))
        assert verdict["fit"] is None and "room for one" in verdict["reason"]

    def test_every_verdict_records_its_inputs(self):
        verdict = fit([sample(28)], call(None, "none", hits=[hit()]), run_spot=28.0)
        assert verdict["run_spot"] == 28.0 and "mates" in verdict


def detail_of(samples, made: Call, run_spot=None):
    """What the content survey records for a set: checks, call, and verdict."""
    return {
        "checks": check_set(samples),
        "call": made.as_detail(),
        **fit(samples, made, run_spot=run_spot),
    }


def unpaired() -> list[FileSample]:
    """DRR878211's DDBJ fastqs: the 98-base reads are spots of their own."""
    return [
        FileSample(
            "x.fastq",
            "fastq",
            records=[Record(f"D.{i + 1000}", "A" * 98, "F" * 98) for i in range(50)],
        ),
        FileSample(
            "x_1.fastq", "fastq", records=[Record(f"D.{i}", "A" * 26, "F" * 26) for i in range(50)]
        ),
    ]


class TestFitFromDetail:
    """A recorded verdict judged again without its reads gives what fit gives with them."""

    @pytest.mark.parametrize(
        ("samples", "made", "run_spot"),
        [
            ([sample(28), sample(91)], call("10x_3p_v3_family", "HIGH", hits=[hit()]), None),
            ([sample(28), sample(47)], call("10x_3p_v3_family", "MEDIUM", hits=[hit()]), None),
            ([sample(28)], call(None, "ambiguous", hits=[hit()]), None),
            ([sample(150)], call("10x_3p_v3_family", "MEDIUM", hits=[hit()]), None),
            ([sample(101)], call(None, "none", hint="barcode read missing: ..."), None),
            ([sample(28), sample(91)], call(None, "none", hint=NO_LISTS), None),
            (sra_object(26, 8), call(None, "none", hits=[hit()]), 66.0),
            (sra_object(28), call(None, "none", hits=[hit()]), 28.0),
            (sra_object(8, 98, 14, 10), call(None, "none", hint="no listed 10x barcode"), 130.0),
            (unpaired(), call("10x_3p_v3_family", "HIGH", hits=[hit(read=1)]), None),
            ([FileSample("x", "cram")], call(None, "none"), None),
        ],
    )
    def test_the_same_verdict(self, samples, made, run_spot):
        assert fit_from_detail(detail_of(samples, made, run_spot)) == fit(
            samples, made, run_spot=run_spot
        )

    def test_every_pair_of_sources_is_recorded(self):
        checks = check_set(unpaired())
        assert checks["concordance"] == [[0, 1, 0.0]]

    def test_an_older_record_is_judged_again_when_its_mates_share_a_source(self):
        detail = detail_of([sample(28), sample(47)], call("10x_3p_v3", "MEDIUM", hits=[hit()]))
        del detail["checks"]["concordance"]
        assert fit_from_detail(detail)["fit"] is True

    def test_an_older_record_whose_mates_were_compared_needs_its_reads(self):
        detail = detail_of(unpaired(), call("10x_3p_v3_family", "HIGH", hits=[hit(read=1)]))
        del detail["checks"]["concordance"]
        with pytest.raises(MissingInput):
            fit_from_detail(detail)


class TestRejudge:
    def result(self, accession: str, detail) -> RouteResult:
        return RouteResult(
            accession,
            "screen:100000:run_ncbi->fastq:x",
            "c",
            Outcome.RESOLVED,
            ("u",),
            fetched_at=datetime(2026, 10, 20, tzinfo=UTC),
            detail=detail,
        )

    def test_judged_again_without_a_read(self):
        fit2 = detail_of([sample(28), sample(47)], call("10x_3p_v3", "MEDIUM", hits=[hit()]))
        del fit2["checks"]["concordance"]
        fit2.update(fit=False, rule="fit-2", reason="no read of 50 bases or more")
        old = detail_of(unpaired(), call("10x_3p_v3_family", "HIGH", hits=[hit(read=1)]))
        del old["checks"]["concordance"]
        old["rule"] = "fit-2"
        changed, lacking = rejudge(
            [
                self.result("SRR34778689", fit2),
                self.result("DRR878211", old),
                self.result("SRR1", None),
            ]
        )
        (again,) = changed
        assert again.detail["fit"] is True and again.detail["rule"] == RULE
        assert again.fetched_at == datetime(2026, 10, 20, tzinfo=UTC), "nothing was read"
        assert lacking == {"DRR878211"}

    def test_a_verdict_the_rule_does_not_change_is_left(self):
        current = detail_of([sample(28), sample(91)], call("10x_3p_v3", "HIGH", hits=[hit()]))
        assert rejudge([self.result("SRR1", current)]) == ([], set())


class TestScreenLinks:
    def test_every_run_to_reads_link_measured_or_not(self):
        links = screen_links()
        assert links and all(link.source.startswith("run_") for link in links)
        assert any(link.route_id for link in links), "route-measured links are screened too"

    def test_the_gate_is_a_gate(self):
        gate = corpora.load("inspect-gate")
        assert set(gate.values) == set(INSPECT_GATE) and corpora.is_gate("inspect-gate")


class TestUnavailableReaders:
    async def test_without_sra_tools_an_object_is_undetermined(self, monkeypatch):
        monkeypatch.setattr(sra, "available", lambda: None)
        record = FileRecord("SRR1", "https://h/SRR1", "SRR1", FileKind.SRA, "sdl")
        (only,) = await sample_file(record, object())  # type: ignore[arg-type]
        assert only.facts["reader"] == "unavailable" and "sra-tools" in only.facts["why"]

    async def test_without_pysam_a_cram_is_undetermined_and_not_fetched(self, monkeypatch):
        monkeypatch.setattr(cram, "available", lambda: False)
        record = FileRecord(
            "ERR1", "https://h/27865_1#3.cram", "27865_1#3.cram", FileKind.CRAM, "ena"
        )
        (only,) = await sample_file(record, object())  # type: ignore[arg-type]
        assert "pysam" in only.facts["why"]

    async def test_a_full_disk_fails_the_run_instead_of_the_file(self, monkeypatch):
        """2026-10-20: a full disk recorded 196 SRA objects as unreadable."""
        import errno

        async def full(target, *, max_reads):
            raise OSError(errno.ENOSPC, "No space left on device")

        monkeypatch.setattr(sra, "available", lambda: "fastq-dump")
        monkeypatch.setattr(sra, "read_sra", full)
        record = FileRecord("SRR1", "https://h/SRR1", "SRR1", FileKind.SRA, "sdl")
        with pytest.raises(OSError, match="No space"):
            await sample_file(record, object())  # type: ignore[arg-type]

    async def test_a_stalled_cram_read_is_killed(self, monkeypatch, tmp_path):
        """2026-10-20: htslib's reference fetch stalled a worker, and the survey with it."""
        # A child that never finishes, as a reference fetch with no deadline does.
        stall = tmp_path / "stall"
        stall.write_text("#!/bin/sh\nexec sleep 60\n")
        stall.chmod(0o755)
        monkeypatch.setattr(cram, "available", lambda: True)
        monkeypatch.setattr(cram.sys, "executable", str(stall))
        with pytest.raises(TimeoutError, match="reference fetch stalled"):
            await cram.read_cram_bounded(b"", "x.cram", max_reads=10, timeout=0.5)

    async def test_a_versioned_name_is_read_by_its_format(self):
        """SRR10377488's submitted BAM is possorted_genome_bam1.bam.1."""
        body = gzip.compress(
            b"@r1\nACGTACGTACGTACGTACGTACGTACGT\n+\nFFFFFFFFFFFFFFFFFFFFFFFFFFFF\n"
        )

        def handler(request):
            return httpx.Response(
                206, content=body, headers={"content-range": f"bytes 0-{len(body) - 1}/{len(body)}"}
            )

        record = FileRecord(
            "SRR1", "https://h/x_R1_001.fastq.gz.1", "x_R1_001.fastq.gz.1", FileKind.OTHER, "t"
        )
        async with (
            httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http,
            SurveyClient(Limits(rps=0), client=http) as client,
        ):
            (only,) = await sample_file(record, client)
        assert only.format == "fastq" and len(only.records) == 1


@pytest.mark.integration
class TestTheGate:
    """The content survey's gate runs, live. Each asserts what the run is there for."""

    async def _sra(self, target: str):
        if not sra.available():
            pytest.skip("sra-tools not installed")
        return await sra.read_sra(target, max_reads=20_000)

    async def test_srr29534765_sra_object_holds_the_read_ena_dropped(self):
        samples = await self._sra("SRR29534765")
        assert sorted(max(set(s.lengths), key=s.lengths.count) for s in samples) == [8, 26, 101]

    async def test_err4265292_sra_object_is_the_cdna_read_alone(self):
        samples = await self._sra("ERR4265292")
        assert [max(set(s.lengths), key=s.lengths.count) for s in samples] == [91]

    async def test_lite_quality_is_substituted(self):
        from fetch_series.inspect.checks import quality_verdict

        lite = "https://sra-downloadb.be-md.ncbi.nlm.nih.gov/sos13/sra-pub-zq-1307/SRR025/25056/SRR25056225/SRR25056225.lite.1"
        samples = await self._sra(lite)
        assert {quality_verdict(s) for s in samples} == {"substituted"}

    async def test_the_cram_with_a_hash_in_its_name(self):
        if not cram.available():
            pytest.skip("pysam not installed")
        record = FileRecord(
            "ERR13855116",
            "https://ftp.sra.ebi.ac.uk/vol1/run/ERR138/ERR13855116/27865_1#3.cram",
            "27865_1#3.cram",
            FileKind.CRAM,
            "ena",
        )
        async with SurveyClient(Limits(rps=2, base_timeout=120)) as client:
            samples = await sample_file(record, client, reads=10_000)
        assert [(s.role_hint, max(set(s.lengths), key=s.lengths.count)) for s in samples] == [
            ("R1", 26),
            ("R2", 96),
        ]
