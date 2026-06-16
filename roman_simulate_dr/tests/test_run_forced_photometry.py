import subprocess
from unittest.mock import call, patch

import pytest

from roman_simulate_dr.scripts import run_forced_photometry


def test_main_runs_for_multiple_subset_coadds(monkeypatch, tmp_path):
    """Purpose: Verify each subset coadd is processed with the derived full-stack segmentation."""
    coadd_1 = tmp_path / "r00001_r0_s02002_270p65x67y51_f146_coadd.asdf"
    coadd_2 = tmp_path / "r00001_r0_s02002_270p65x67y51_f158_coadd.asdf"
    segm = tmp_path / "r00001_r0_full_270p65x67y51_segm.asdf"
    coadd_1.touch()
    coadd_2.touch()
    segm.touch()

    monkeypatch.setattr(
        run_forced_photometry.sys, "argv", ["prog", str(coadd_1), str(coadd_2)]
    )

    with patch(
        "roman_simulate_dr.scripts.run_forced_photometry.subprocess.run"
    ) as mock_run:
        run_forced_photometry.main()

    output_dir = tmp_path / "FORCED"
    assert mock_run.call_args_list == [
        call(
            [
                "strun",
                "romancal.step.SourceCatalogStep",
                str(coadd_1),
                "--forced_segmentation",
                str(segm),
                "--output_dir",
                str(output_dir),
                "--output_file",
                "r00001_r0_s02002_270p65x67y51_f146_cat.parquet",
            ],
            check=True,
        ),
        call(
            [
                "strun",
                "romancal.step.SourceCatalogStep",
                str(coadd_2),
                "--forced_segmentation",
                str(segm),
                "--output_dir",
                str(output_dir),
                "--output_file",
                "r00001_r0_s02002_270p65x67y51_f158_cat.parquet",
            ],
            check=True,
        ),
    ]


def test_main_expands_glob_inputs(monkeypatch, tmp_path):
    """Purpose: Ensure wildcard input patterns are expanded into subset coadd files."""
    coadd_file = tmp_path / "r00001_r0_s02002_270p65x67y51_f146_coadd.asdf"
    segm_file = tmp_path / "r00001_r0_full_270p65x67y51_segm.asdf"
    coadd_file.touch()
    segm_file.touch()

    pattern = str(tmp_path / "*_s02002_*_f146_coadd.asdf")
    monkeypatch.setattr(run_forced_photometry.sys, "argv", ["prog", pattern])

    with patch(
        "roman_simulate_dr.scripts.run_forced_photometry.subprocess.run"
    ) as mock_run:
        run_forced_photometry.main()

    assert mock_run.call_count == 1


def test_main_exits_if_coadd_is_missing(monkeypatch, tmp_path, capsys):
    """Purpose: Confirm the script exits with an error for missing coadd inputs."""
    missing_coadd = tmp_path / "r00001_r0_s02002_270p65x67y51_f146_coadd.asdf"
    monkeypatch.setattr(
        run_forced_photometry.sys, "argv", ["prog", str(missing_coadd)]
    )

    with pytest.raises(SystemExit) as exc_info:
        run_forced_photometry.main()

    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert f"Error: Coadd file not found: {missing_coadd}" in captured.err


def test_main_exits_if_derived_segm_is_missing(monkeypatch, tmp_path, capsys):
    """Purpose: Confirm derived full-stack segmentation files are required."""
    coadd_file = tmp_path / "r00001_r0_s02002_270p65x67y51_f146_coadd.asdf"
    coadd_file.touch()
    monkeypatch.setattr(run_forced_photometry.sys, "argv", ["prog", str(coadd_file)])

    with pytest.raises(SystemExit) as exc_info:
        run_forced_photometry.main()

    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "Error: Multiband segmentation file not found:" in captured.err

def test_main_accepts_pass_grouping_token(monkeypatch, tmp_path):
    """Purpose: Confirm p-prefixed pass grouping tokens are accepted."""
    coadd_file = tmp_path / "r00001_r0_p10010_270p65x67y51_f146_coadd.asdf"
    segm_file = tmp_path / "r00001_r0_full_270p65x67y51_segm.asdf"
    coadd_file.touch()
    segm_file.touch()

    monkeypatch.setattr(run_forced_photometry.sys, "argv", ["prog", str(coadd_file)])

    with patch(
        "roman_simulate_dr.scripts.run_forced_photometry.subprocess.run"
    ) as mock_run:
        run_forced_photometry.main()

    output_dir = tmp_path / "FORCED"
    assert mock_run.call_args_list == [
        call(
            [
                "strun",
                "romancal.step.SourceCatalogStep",
                str(coadd_file),
                "--forced_segmentation",
                str(segm_file),
                "--output_dir",
                str(output_dir),
                "--output_file",
                "r00001_r0_p10010_270p65x67y51_f146_cat.parquet",
            ],
            check=True,
        )
    ]


def test_main_exits_on_non_subset_grouping(monkeypatch, tmp_path, capsys):
    """Purpose: Confirm full-grouping coadds are rejected for forced-photometry input."""
    coadd_file = tmp_path / "r00001_r0_full_270p65x67y51_f146_coadd.asdf"
    coadd_file.touch()
    monkeypatch.setattr(run_forced_photometry.sys, "argv", ["prog", str(coadd_file)])

    with pytest.raises(SystemExit) as exc_info:
        run_forced_photometry.main()

    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "Error: expected a pass/subset-level coadd filename" in captured.err


def test_main_exits_when_strun_fails(monkeypatch, tmp_path, capsys):
    """Purpose: Ensure underlying strun failures are surfaced as exit code 1."""
    coadd_file = tmp_path / "r00001_r0_s02002_270p65x67y51_f146_coadd.asdf"
    segm_file = tmp_path / "r00001_r0_full_270p65x67y51_segm.asdf"
    coadd_file.touch()
    segm_file.touch()
    monkeypatch.setattr(run_forced_photometry.sys, "argv", ["prog", str(coadd_file)])

    with patch(
        "roman_simulate_dr.scripts.run_forced_photometry.subprocess.run",
        side_effect=subprocess.CalledProcessError(1, ["strun"]),
    ):
        with pytest.raises(SystemExit) as exc_info:
            run_forced_photometry.main()

    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert f"Error running forced photometry for {coadd_file.name}" in captured.err