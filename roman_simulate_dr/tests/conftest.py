import pytest


@pytest.fixture
def mock_plan():
    def _factory(catalog_name: str = "mock_output.ecsv"):
        return {
            "passes": [
                {
                    "name": "pass1",
                    "roll": 0.0,
                    "visits": [
                        {
                            "name": "visit1",
                            "lon": 10.0,
                            "lat": 20.0,
                            "exposures": [
                                {"sca_ids": [1, 2], "filter_names": ["f062", "f087"]}
                            ],
                        }
                    ],
                }
            ],
            "romanisim_input_catalog_name": catalog_name,
        }
    return _factory
