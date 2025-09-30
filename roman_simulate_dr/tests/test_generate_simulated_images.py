from unittest.mock import MagicMock, patch

from roman_simulate_dr.scripts.generate_simulated_images import RomanisimImages


@patch("roman_simulate_dr.scripts.generate_simulated_images.read_obs_plan")
def test_init_input_filename_from_plan(mock_read_obs_plan, mock_plan):
    catalog_name = "input_cat.ecsv"
    mock_read_obs_plan.return_value = mock_plan(catalog_name)
    obj = RomanisimImages(catalog_name)
    assert obj.input_filename == "input_cat.ecsv"


@patch("roman_simulate_dr.scripts.generate_simulated_images.read_obs_plan")
def test_init_input_filename_override(mock_read_obs_plan, mock_plan):
    mock_read_obs_plan.return_value = mock_plan()
    obj = RomanisimImages("dummy.toml", input_filename="override.ecsv")
    assert obj.input_filename == "override.ecsv"


@patch("subprocess.run")
def test_generate_simulated_images_calls_subprocess(mock_run):
    mock_run.return_value = MagicMock(stdout="out", stderr="err", returncode=0)
    obj = RomanisimImages.__new__(RomanisimImages)
    result = obj._generate_simulated_images(output_filename="test.asdf")
    assert result == ("test.asdf", 0)
    mock_run.assert_called_once()


@patch("roman_simulate_dr.scripts.generate_simulated_images.read_obs_plan")
@patch(
    "roman_simulate_dr.scripts.generate_simulated_images.generate_roman_filename",
    return_value="sim.asdf",
)
@patch("roman_simulate_dr.scripts.generate_simulated_images.parallelize_jobs")
def test_run_builds_jobs_and_parallelizes(
    mock_parallelize, mock_gen_filename, mock_read_obs_plan, mock_plan
):
    mock_read_obs_plan.return_value = mock_plan()
    obj = RomanisimImages("dummy.toml")
    obj.run()
    assert mock_parallelize.called
    jobs = mock_parallelize.call_args[0][1]
    assert len(jobs) == 4  # 2 sca_ids * 2 filter_names
    assert jobs[0]["output_filename"] == "sim.asdf"
