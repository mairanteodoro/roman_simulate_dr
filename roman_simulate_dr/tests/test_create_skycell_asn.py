import subprocess
from unittest.mock import patch

import pytest

from roman_simulate_dr.scripts import create_skycell_asn


def test_main_runs_skycell_asn_once_for_all_matches(monkeypatch):
    """Purpose: Verify the command runs in standard and DR modes."""
    monkeypatch.setattr(create_skycell_asn.sys, "argv", ["prog"])

    matching_files = ["r00001_test_cal.asdf", "r00002_test_cal.asdf"]
    with patch(
        "roman_simulate_dr.scripts.create_skycell_asn.glob.glob",
        return_value=matching_files,
    ), patch("roman_simulate_dr.scripts.create_skycell_asn.subprocess.run") as mock_run:
        create_skycell_asn.main()
    assert mock_run.call_count == 2
    assert mock_run.call_args_list[0].args[0] == [
        "skycell_asn",
        *matching_files,
        "-o",
        "r00001",
        "--product-type",
        "full",
    ]
    assert mock_run.call_args_list[1].args[0] == [
        "skycell_asn",
        *matching_files,
        "-o",
        "r00001",
        "--product-type",
        "full",
        "--data-release-id",
        "r0",
    ]
    assert mock_run.call_args_list[0].kwargs == {"check": True}
    assert mock_run.call_args_list[1].kwargs == {"check": True}


def test_main_handles_multiple_filters(monkeypatch):
    """Purpose: Confirm each mode can receive a distinct file set."""
    monkeypatch.setattr(create_skycell_asn.sys, "argv", ["prog"])

    with patch(
        "roman_simulate_dr.scripts.create_skycell_asn.glob.glob",
        side_effect=[
            ["r00001_a_cal.asdf"],
            ["r00002_b_cal.asdf"],
        ],
    ), patch("roman_simulate_dr.scripts.create_skycell_asn.subprocess.run") as mock_run:
        create_skycell_asn.main()

    assert mock_run.call_count == 2
    assert mock_run.call_args_list[0].args[0] == [
        "skycell_asn",
        "r00001_a_cal.asdf",
        "-o",
        "r00001",
        "--product-type",
        "full",
    ]
    assert mock_run.call_args_list[1].args[0] == [
        "skycell_asn",
        "r00002_b_cal.asdf",
        "-o",
        "r00001",
        "--product-type",
        "full",
        "--data-release-id",
        "r0",
    ]


def test_main_handles_custom_output_and_product_type(monkeypatch):
    """Purpose: Confirm optional args are forwarded to skycell_asn."""
    monkeypatch.setattr(
        create_skycell_asn.sys,
        "argv",
        ["prog", "-o", "custom_root", "--product-type", "visit"],
    )
    matching_files = ["r00001_test_cal.asdf"]
    with patch(
        "roman_simulate_dr.scripts.create_skycell_asn.glob.glob",
        return_value=matching_files,
    ), patch("roman_simulate_dr.scripts.create_skycell_asn.subprocess.run") as mock_run:
        create_skycell_asn.main()

    assert mock_run.call_args_list[0].args[0] == [
        "skycell_asn",
        *matching_files,
        "-o",
        "custom_root",
        "--product-type",
        "visit",
    ]


def test_main_exits_if_no_inputs_match(monkeypatch, capsys):
    """Purpose: Ensure skycell_asn is called even when no files are found."""
    monkeypatch.setattr(create_skycell_asn.sys, "argv", ["prog"])

    with patch(
        "roman_simulate_dr.scripts.create_skycell_asn.glob.glob",
        return_value=[],
    ), patch("roman_simulate_dr.scripts.create_skycell_asn.subprocess.run") as mock_run:
        create_skycell_asn.main()

    assert mock_run.call_count == 2
    assert mock_run.call_args_list[0].args[0] == [
        "skycell_asn",
        "-o",
        "r00001",
        "--product-type",
        "full",
    ]
    assert mock_run.call_args_list[1].args[0] == [
        "skycell_asn",
        "-o",
        "r00001",
        "--product-type",
        "full",
        "--data-release-id",
        "r0",
    ]
    captured = capsys.readouterr()
    assert captured.err == ""


def test_main_exits_on_skycell_asn_failure(monkeypatch, capsys):
    """Purpose: Ensure subprocess failures are surfaced as exit code 1."""
    monkeypatch.setattr(create_skycell_asn.sys, "argv", ["prog"])

    with patch(
        "roman_simulate_dr.scripts.create_skycell_asn.glob.glob",
        return_value=["r00001_test_cal.asdf"],
    ), patch(
        "roman_simulate_dr.scripts.create_skycell_asn.subprocess.run",
        side_effect=subprocess.CalledProcessError(1, ["skycell_asn"]),
    ):
        with pytest.raises(SystemExit) as exc_info:
            create_skycell_asn.main()

    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "Error executing skycell_asn" in captured.err