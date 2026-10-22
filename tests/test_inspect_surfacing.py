"""S5: where read verdicts surface -- the refusal path, the report, links.tsv parity."""

from __future__ import annotations

import ast
import gzip
import json
import random
from pathlib import Path

from typer.testing import CliRunner

from fetch_series.files import FileKind, FileRecord, FileSet, recommend
from fetch_series.inspect.lists import BarcodeList, Entry, pack
from fetch_series.inspect.report import SCHEMA_VERSION, inspect_local, refusals

RNG = random.Random(5)


def _record(source: str, name: str, kind: FileKind, *, md5: str = "x") -> FileRecord:
    return FileRecord("SRR1", f"https://h/{name}", name, kind, source, size=10, md5=md5)


FILESET = FileSet(
    run="SRR1",
    records=(
        _record("ena", "SRR1_1.fastq.gz", FileKind.FASTQ),
        _record("ena", "SRR1_2.fastq.gz", FileKind.FASTQ),
        _record("sdl", "SRR1", FileKind.SRA),
    ),
)


class TestRefusalPath:
    def test_without_verdicts_nothing_changes(self):
        assert recommend(FILESET).chosen.source == "ena"  # type: ignore[union-attr]

    def test_a_refused_set_loses_to_any_other(self):
        """ERR4265292: the SRA object holds the cDNA read alone."""
        chosen = recommend(FILESET, {("ena", FileKind.FASTQ): "no barcode read"}).chosen
        assert chosen is not None and chosen.source == "sdl"

    def test_every_offer_refused_chooses_nothing_and_shows_everything(self):
        rec = recommend(
            FILESET,
            {("ena", FileKind.FASTQ): "no barcode read", ("sdl", FileKind.SRA): "no cDNA read"},
        )
        assert rec.chosen is None and len(rec.alternatives) == 2
        assert "REFUSED by its reads" in rec.explain()

    def test_an_undetermined_verdict_is_not_a_refusal(self):
        report = {
            "sources": [
                {"source": "ena", "kind": "fastq", "fit": None, "reason": "reader unavailable"},
                {"source": "sdl", "kind": "sra", "fit": False, "reason": "no cDNA read"},
                {"source": "x", "kind": "bam", "fit": True, "reason": "fit"},
            ]
        }
        assert refusals(report) == {("sdl", FileKind.SRA): "no cDNA read"}


def _calls_of(path: Path, function: str | None = None) -> set[str]:
    tree = ast.parse(path.read_text())
    if function is not None:
        tree = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef) and node.name == function
        )
    return {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }


class TestLinksParity:
    def test_links_tsv_never_goes_through_recommend(self):
        """links.tsv reproduces the incumbent's choice; a refusal must not move it."""
        assert "recommend" not in _calls_of(Path("src/fetch_series/filesets.py"), "links_table")
        assert "recommend" not in _calls_of(Path("src/fetch_series/export/links.py"))


def _list(codes: list[str]) -> dict[str, BarcodeList]:
    entry = Entry("tenx_v3", 16, len(codes), "0" * 64, "t", "t")
    return {"tenx_v3": BarcodeList(entry, pack(codes, entry))}


def _write(path: Path, seqs: list[str]) -> Path:
    body = "".join(f"@SRR1.{i}\n{s}\n+\n{'F' * len(s)}\n" for i, s in enumerate(seqs))
    path.write_bytes(gzip.compress(body.encode()))
    return path


def _random(n: int) -> str:
    return "".join(RNG.choice("ACGT") for _ in range(n))


class TestLocalReport:
    CODES = sorted({_random(16) for _ in range(300)})

    async def test_a_local_pair_is_called_and_fit(self, tmp_path):
        r1 = _write(
            tmp_path / "x_R1_001.fastq.gz",
            [RNG.choice(self.CODES) + _random(12) for _ in range(800)],
        )
        r2 = _write(tmp_path / "x_R2_001.fastq.gz", [_random(70) + "A" * 21 for _ in range(800)])
        report = await inspect_local([r1, r2], _list(self.CODES), {})
        assert report["schema_version"] == SCHEMA_VERSION and report["run"] is None
        (source,) = report["sources"]
        assert source["call"]["barcode_list"] == "tenx_v3"
        assert "AAAA" not in repr(source["checks"])  # counts, never sequences
        assert source["geometry"]["barcode"]["cb_len"] == 16
        assert source["convertible"]["reason"] == "already FASTQ"

    async def test_a_lone_cdna_file_is_proven_unusable(self, tmp_path):
        r2 = _write(tmp_path / "x.fastq.gz", [_random(91) for _ in range(500)])
        (source,) = (await inspect_local([r2], _list(self.CODES), {}))["sources"]
        assert source["fit"] is False


def test_inspect_run_on_local_files(tmp_path, monkeypatch):
    from fetch_series.cli import app
    from fetch_series.inspect import lists as lists_module

    codes = TestLocalReport.CODES
    monkeypatch.setattr(lists_module, "available", lambda directory=None: _list(codes))
    r1 = _write(
        tmp_path / "x_R1_001.fastq.gz", [RNG.choice(codes) + _random(12) for _ in range(500)]
    )
    r2 = _write(tmp_path / "x_R2_001.fastq.gz", [_random(91) for _ in range(500)])
    out = tmp_path / "report.json"
    result = CliRunner().invoke(app, ["inspect", "run", str(r1), str(r2), "--json", str(out)])
    assert result.exit_code == 0, result.output
    assert "local files" in result.output and out.exists()


def test_inspect_run_params_on_local_files(tmp_path, monkeypatch):
    from fetch_series.cli import app
    from fetch_series.inspect import lists as lists_module

    codes = TestLocalReport.CODES
    monkeypatch.setattr(lists_module, "available", lambda directory=None: _list(codes))
    whitelist = tmp_path / "3M-february-2018.txt"
    monkeypatch.setattr(lists_module, "exported", lambda entry, out=None: whitelist)
    r1 = _write(
        tmp_path / "x_R1_001.fastq.gz", [RNG.choice(codes) + _random(12) for _ in range(800)]
    )
    r2 = _write(tmp_path / "x_R2_001.fastq.gz", [_random(70) + "A" * 21 for _ in range(800)])
    out = tmp_path / "report.json"
    result = CliRunner().invoke(
        app, ["inspect", "run", str(r1), str(r2), "--params", "--json", str(out)]
    )
    assert result.exit_code == 0, result.output
    report = json.loads(out.read_text())
    (source,) = report["sources"]
    assert source["mapping"]["whitelist"]["path"] == str(whitelist)
    assert f"--soloCBwhitelist {whitelist}" in result.output
    assert f"--readFilesIn {r2} {r1}" in result.output
