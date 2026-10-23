"""The pipeline's own STARsolo runs, imported as a per-sample record."""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from fetch_series.inspect import truth

HEADER = (
    "Sample\tRd_all\tRd_in_cells\tFrc_in_cells\tUMI_in_cells\tCells\tMed_nFeature\tGood_BC\tWL\t"
    "Species\tPaired\tStrand\tall_u+m\tall_u\texon_u+m\texon_u\tfull_u+m\tfull_u\n"
)
ROW = (
    "GSM3439401\t181394489\t98212061\t0.541\t45522910\t3515\t2362\t0.980288\tv2\tMouse\tSingle\t"
    "Forward\t0.871743\t0.743883\t0.687877\t0.664253\t0.776653\t0.718618\n"
)
SUMMARY = "Number of Reads,181394489\nReads With Valid Barcodes,0.980237\nSequencing Saturation,0.530116\nQ30 Bases in CB+UMI,0.962439\n"


def bundle(tmp_path: Path) -> Path:
    root = tmp_path / "bundle" / "irods"
    dataset = root / "GSE102935"
    (dataset / "GSM3439401" / "output" / "GeneFull").mkdir(parents=True)
    (dataset / "GSM3439401" / "output" / "GeneFull" / "Summary.csv").write_text(SUMMARY)
    (dataset / "GSE102935.solo_qc.tsv").write_text(HEADER + ROW)
    # A repeated table (solo_qc1) must not repeat the sample.
    (dataset / "GSE102935.solo_qc1.tsv").write_text(HEADER + ROW)
    controlled = root / "EGAD00001006049"
    controlled.mkdir()
    (controlled / "solo_qc.tsv").write_text(HEADER + ROW.replace("GSM3439401", "10X210_3"))
    return tmp_path / "bundle"


def test_one_row_per_sample(tmp_path):
    samples = list(truth.read_bundle(bundle(tmp_path)))
    assert [(s.dataset, s.sample) for s in samples] == [
        ("EGAD00001006049", "10X210_3"),
        ("GSE102935", "GSM3439401"),
    ]
    gsm = samples[1]
    assert gsm.barcode_list == "tenx_v2" and gsm.strand == "Forward" and gsm.public
    assert gsm.valid_barcodes == 0.980237 and gsm.saturation == 0.530116
    assert not samples[0].public and samples[0].saturation is None


def test_the_split_is_by_dataset_and_stable():
    assert truth.split_of("GSE102935") == truth.split_of("GSE102935")
    halves = {truth.split_of(f"GSE{n}") for n in range(100000, 100050)}
    assert halves == {"design", "heldout"}


def test_import_command(tmp_path):
    from fetch_series.cli import app

    out = tmp_path / "out"
    result = CliRunner().invoke(
        app, ["inspect", "truth", "import", str(bundle(tmp_path)), "--out", str(out)]
    )
    assert result.exit_code == 0, result.output
    assert [r["sample"] for r in truth.load(out / "truth.csv")] == ["GSM3439401"]
