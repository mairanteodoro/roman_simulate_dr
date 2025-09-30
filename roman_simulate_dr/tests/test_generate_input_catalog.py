from unittest.mock import patch

import pytest

from roman_simulate_dr.scripts.generate_input_catalog import InputCatalog


@pytest.fixture
def mock_plan():
    return {
        "passes": [
            {"visits": [{"lon": 10.0, "lat": 20.0}, {"lon": 12.0, "lat": 22.0}]}
        ],
        "romanisim_input_catalog_name": "mock_output.ecsv",
    }


@patch("roman_simulate_dr.scripts.generate_input_catalog.read_obs_plan")
def test_init_with_master_values(mock_read_obs_plan, mock_plan):
    mock_read_obs_plan.return_value = mock_plan
    cat = InputCatalog(
        obs_plan_filename="dummy.ecsv",
        output_catalog_filename="output.ecsv",
        chunk_size=100,
        master_ra=10.0,
        master_dec=20.0,
        master_radius=1.0,
    )
    assert cat.master_ra == 10.0
    assert cat.master_dec == 20.0
    assert cat.master_radius == 1.0
    assert cat.catalog_filename == "output.ecsv"
    assert cat.chunk_size == 100


@patch("roman_simulate_dr.scripts.generate_input_catalog.read_obs_plan")
def test_init_computes_master_values(mock_read_obs_plan, mock_plan):
    mock_read_obs_plan.return_value = mock_plan
    cat = InputCatalog(
        obs_plan_filename="dummy.ecsv",
        output_catalog_filename=None,
        chunk_size=None,
        master_ra=None,
        master_dec=None,
        master_radius=None,
    )
    assert cat.master_ra == pytest.approx(11.0)
    assert cat.master_dec == pytest.approx(21.0)
    assert cat.catalog_filename == "mock_output.ecsv"


@patch("roman_simulate_dr.scripts.generate_input_catalog.Path.unlink")
def test_janitor_removes_files(mock_unlink):
    cat = InputCatalog.__new__(InputCatalog)
    cat.cat_component_filenames = ["file1.ecsv", "file2.ecsv"]
    with patch(
        "roman_simulate_dr.scripts.generate_input_catalog.Path.exists",
        return_value=True,
    ):
        cat._janitor()
    assert mock_unlink.call_count == 2
