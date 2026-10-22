"""Other technologies, detected and named from their constants -- and never given a 10x verdict."""

from __future__ import annotations

import random

from fetch_series.inspect.classify import classify
from fetch_series.inspect.classify.technologies import BY_ID, TECHNOLOGIES, detect
from fetch_series.inspect.fit import fit
from fetch_series.inspect.model import FileSample, Record
from fetch_series.inspect.readiness import readiness
from fetch_series.inspect.report import technology_of

RNG = random.Random(3)
L1 = "GTGGCCGATGTTTCGCATCGGCGTACGACT"


def _random(n: int) -> str:
    return "".join(RNG.choice("ACGT") for _ in range(n))


def reads(seqs: list[str]) -> FileSample:
    return FileSample(
        "x", "fastq", records=[Record(f"r{i}", s, "F" * len(s)) for i, s in enumerate(seqs)]
    )


def parse_r2(n: int = 400) -> FileSample:
    """SRR24749105's R2: UMI 10, BC3 8, linker 1, BC2 8, microSPLiT's linker 2, BC1 8."""
    return reads(
        [_random(18) + L1 + _random(8) + "ATCCACGTGCTTGAGACTGTGG" + _random(8) for _ in range(n)]
    )


def bd_r1(n: int = 400) -> FileSample:
    """DRR346905's R1: CLS1 9, linker, CLS2 9, linker, CLS3 9, UMI 8, poly-T."""
    return reads(
        [
            _random(9) + "ACTGGCCTGCGA" + _random(9) + "GGTAGCGGTGACA" + _random(17) + "T" * 15
            for _ in range(n)
        ]
    )


class TestDetect:
    def test_parse_reads_as_microsplit(self):
        found = detect([reads([_random(74)] * 400), parse_r2()])
        assert found is not None and found["id"] == "microsplit_parse" and found["read"] == 1

    def test_bd_rhapsody(self):
        found = detect([bd_r1(), reads([_random(225)] * 400)])
        assert found is not None and found["id"] == "bd_rhapsody"

    def test_a_short_read_falls_back_to_the_family(self):
        found = detect([reads([_random(18) + L1 + _random(4) for _ in range(300)])])
        assert found is not None and found["id"] == "splitseq_family"

    def test_random_reads_are_no_technology(self):
        assert (
            detect([reads([_random(28) for _ in range(2000)]), reads([_random(91)] * 200)]) is None
        )

    def test_a_minority_is_not_enough(self):
        mixed = [s.seq for s in parse_r2(100).records] + [_random(86) for _ in range(300)]
        assert detect([reads(mixed)]) is None

    def test_a_long_constant_tolerates_one_substitution_in_ten(self):
        from fetch_series.inspect.classify.technologies import Constant

        linker = Constant(L1, (18,))
        two = L1[:5] + "A" + L1[6:20] + "C" + L1[21:]
        assert linker.at(_random(18) + two)
        flip = str.maketrans("ACGT", "CATG")
        four = "".join(b.translate(flip) if i in (1, 9, 17, 25) else b for i, b in enumerate(L1))
        assert sum(a != b for a, b in zip(four, L1, strict=True)) == 4
        assert not linker.at(_random(18) + four)
        assert not Constant("CAGAGC", (9,)).at(_random(9) + "CAGAGA")

    def test_parse_by_its_second_linker_alone(self):
        """SRR32391187: the first linker varies; the second sits at 56."""
        garbled = reads([_random(56) + "ATCCACGTGCTTGAGACTGTGG" + _random(8) for _ in range(400)])
        found = detect([garbled])
        assert found is not None and found["id"] == "microsplit_parse"

    def test_every_row_cites_its_source(self):
        assert TECHNOLOGIES and all(t.source.startswith("https://") for t in TECHNOLOGIES)
        assert all(len(c.seq) >= 4 for t in TECHNOLOGIES for c in t.constants)


class TestNeverTenX:
    def test_the_call_has_no_layout(self):
        call = classify([reads([_random(74)] * 400), parse_r2()], {"tenx_v3": object()})  # type: ignore[dict-item]
        assert call.layout is None and call.technology == "microsplit_parse"
        assert "not 10x" in (call.hint or "")

    def test_fit_is_undetermined_not_unfit(self):
        samples = [reads([_random(74)] * 400), parse_r2()]
        call = classify(samples, {"tenx_v3": object()})  # type: ignore[dict-item]
        verdict = fit(samples, call)
        assert verdict["fit"] is None and BY_ID["microsplit_parse"].name in verdict["reason"]

    def test_no_mapping_block(self):
        samples = [reads([_random(74)] * 400), parse_r2()]
        call = classify(samples, {"tenx_v3": object()})  # type: ignore[dict-item]
        verdict = fit(samples, call)
        source = {
            "kind": "fastq",
            "checks": {"files": []},
            "call": call.as_detail(),
            "fit": verdict["fit"],
            "reason": verdict["reason"],
        }
        assert readiness(source)["mapping"] is None
        assert technology_of(source["call"])["id"] == "microsplit_parse"
