from unittest.mock import patch

import pytest

from roman_simulate_dr.scripts.generate_input_catalog import InputCatalog


@patch("roman_simulate_dr.scripts.generate_input_catalog.read_obs_plan")
def test_init_with_master_values(mock_read_obs_plan, mock_plan):
    mock_read_obs_plan.return_value = mock_plan()
    cat = InputCatalog(
        obs_plan_filename="dummy.ecsv",
        output_catalog_filename="output.ecsv",
        master_ra=10.0,
        master_dec=20.0,
        master_radius=1.0,
    )
    assert cat.master_ra == 10.0
    assert cat.master_dec == 20.0
    assert cat.master_radius == 1.0
    assert cat.catalog_filename == "output.ecsv"


@patch("roman_simulate_dr.scripts.generate_input_catalog.read_obs_plan")
def test_init_computes_master_values(mock_read_obs_plan, mock_plan):
    catalog_name = "mock_output.ecsv"
    mock_read_obs_plan.return_value = mock_plan(catalog_name)
    cat = InputCatalog(
        obs_plan_filename="dummy.ecsv",
        output_catalog_filename=None,
        master_ra=None,
        master_dec=None,
        master_radius=None,
    )
    assert cat.master_ra == pytest.approx(10.0)
    assert cat.master_dec == pytest.approx(20.0)
    assert cat.catalog_filename == catalog_name
