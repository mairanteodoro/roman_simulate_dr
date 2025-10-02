from unittest.mock import MagicMock, patch

import pytest

from roman_simulate_dr.scripts.generate_input_catalog import InputCatalog


@pytest.fixture
def mock_plan():
    # Minimal plan with RA and DEC columns
    return {"RA": [10.0, 20.0], "DEC": [30.0, 40.0]}


@patch("roman_simulate_dr.scripts.generate_input_catalog.read_obs_plan")
def test_init_sets_attributes(mock_read_obs_plan, mock_plan):
    """
    Purpose: Verify that InputCatalog initializes its attributes correctly
    when provided with explicit arguments and a mocked observation plan.
    """
    mock_read_obs_plan.return_value = mock_plan
    obj = InputCatalog(
        "plan.ecsv",
        output_catalog_filename="out.ecsv",
        ra=1.0,
        dec=2.0,
        radius=0.5,
    )
    assert obj.plan == mock_plan
    assert obj.catalog_filename == "out.ecsv"
    assert obj.ra == 1.0
    assert obj.dec == 2.0
    assert obj.radius == 0.5


@patch("roman_simulate_dr.scripts.generate_input_catalog.vstack")
@patch("roman_simulate_dr.scripts.generate_input_catalog.make_stars")
@patch("roman_simulate_dr.scripts.generate_input_catalog.make_gaia_stars")
@patch("roman_simulate_dr.scripts.generate_input_catalog.make_cosmos_galaxies")
@patch("roman_simulate_dr.scripts.generate_input_catalog.read_obs_plan")
def test_generate_catalog_calls_components(
    mock_read_obs_plan,
    mock_make_cosmos_galaxies,
    mock_make_gaia_stars,
    mock_make_stars,
    mock_vstack,
    mock_plan,
):
    """
    Purpose: Ensure that _generate_catalog calls all required component
    functions and writes the catalog using the correct filename and format.
    """
    mock_read_obs_plan.return_value = mock_plan
    mock_make_cosmos_galaxies.return_value = MagicMock()
    mock_make_gaia_stars.return_value = MagicMock()
    mock_make_stars.return_value = MagicMock()
    mock_catalog = MagicMock()
    mock_vstack.return_value = mock_catalog
    obj = InputCatalog("plan.ecsv", output_catalog_filename="out.ecsv")
    obj._generate_catalog(filter_list=["f062"])
    mock_make_cosmos_galaxies.assert_called_once()
    mock_make_gaia_stars.assert_called_once()
    mock_make_stars.assert_called_once()
    mock_vstack.assert_called_once()
    mock_catalog.write.assert_called_once_with(
        "out.ecsv", format="parquet", overwrite=True
    )


@patch.object(InputCatalog, "_generate_catalog")
@patch("roman_simulate_dr.scripts.generate_input_catalog.read_obs_plan")
def test_run_calls_generate_catalog(
    mock_read_obs_plan, mock_generate_catalog, mock_plan
):
    """
    Purpose: Verify that the run() method of InputCatalog triggers
    _generate_catalog exactly once.
    """
    mock_read_obs_plan.return_value = mock_plan
    obj = InputCatalog("plan.ecsv", output_catalog_filename="out.ecsv")
    obj.run()
    mock_generate_catalog.assert_called_once()
