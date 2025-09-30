from unittest.mock import patch

from roman_simulate_dr.scripts import utils


def test_generate_roman_filename_basic():
    fname = utils.generate_roman_filename(
        program=1,
        plan=2,
        passno=3,
        segment=4,
        observation=5,
        visit=6,
        exposure=7,
        sca=8,
        bandpass="F062",
        suffix="uncal",
    )
    assert fname == "r102003004005006_0007_wfi08_f062_uncal.asdf"


@patch("roman_simulate_dr.scripts.utils.Table")
def test_read_obs_plan_returns_expected_dict(mock_table):
    # Setup a fake table with required columns
    row = {
        "PLAN": 1,
        "PASS": 2,
        "PA": 0.0,
        "VISIT": 3,
        "RA": 10.0,
        "DEC": 20.0,
        "BANDPASS": "F062",
        "DURATION": 100,
        "MA_TABLE_NUMBER": 109,
        "SEGMENT": 1,
        "OBSERVATION": 1,
        "EXPOSURE": 1,
    }
    mock_table.read.return_value = [row]
    result = utils.read_obs_plan("test.ecsv")
    assert "passes" in result
    assert result["romanisim_input_catalog_name"] == "test_cat.ecsv"
    assert len(result["passes"]) == 1
    assert result["passes"][0]["visits"][0]["lon"] == 10.0
    assert result["passes"][0]["visits"][0]["lat"] == 20.0
    assert result["passes"][0]["visits"][0]["exposures"][0]["filter_names"] == ["F062"]


def test_parallelize_jobs_sequential(monkeypatch):
    calls = []

    def dummy_method(a, b):
        calls.append((a, b))

    jobs = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    utils.parallelize_jobs(dummy_method, jobs, max_workers=1)
    assert calls == [(1, 2), (3, 4)]
