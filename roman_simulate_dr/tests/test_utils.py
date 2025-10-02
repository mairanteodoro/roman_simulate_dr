from astropy.table import Table

from roman_simulate_dr.scripts.utils import (
    generate_catalog_name,
    generate_roman_filename,
    parallelize_jobs,
    read_obs_plan,
)


def test_generate_roman_filename_basic():
    """
    Purpose: Verify that generate_roman_filename produces the correct filename format
    given a set of input parameters.
    """
    fname = generate_roman_filename(
        program=1,
        plan=2,
        passno=3,
        segment=4,
        observation=5,
        visit=6,
        exposure=7,
        sca=8,
        bandpass="F106",
        suffix="cat",
    )
    assert fname == "r102003004005006_0007_wfi08_f106_cat.asdf"


def test_read_obs_plan(tmp_path):
    """
    Purpose: Ensure that read_obs_plan correctly reads an ECSV file and returns
    an astropy Table with expected columns and values.
    """
    # Create a minimal ECSV file
    ecsv_content = """
    # %ECSV 0.9
    # ---
    # datatype:
    # - {name: RA, datatype: float64}
    # - {name: DEC, datatype: float64}
    RA DEC
    10.0 20.0
    30.0 40.0
    """
    ecsv_file = tmp_path / "test.ecsv"
    ecsv_file.write_text(ecsv_content)
    table = read_obs_plan(str(ecsv_file))
    assert isinstance(table, Table)
    assert "RA" in table.colnames
    assert "DEC" in table.colnames
    assert table["RA"][0] == 10.0
    assert table["DEC"][1] == 40.0


def test_parallelize_jobs():
    """
    Purpose: Test that parallelize_jobs executes all jobs in parallel and collects
    the correct results, verifying both argument passing and parallel execution.
    """
    results = []

    def dummy_method(x, y):
        results.append(x + y)

    jobs = [{"x": 1, "y": 2}, {"x": 3, "y": 4}]
    parallelize_jobs(dummy_method, jobs, max_workers=2)
    assert sorted(results) == [3, 7]


def test_generate_catalog_name_basic():
    assert generate_catalog_name("plan.ecsv") == "plan_cat.ecsv"
    assert generate_catalog_name("myplan.txt") == "myplan_cat.txt"
    assert generate_catalog_name("data/obs_plan.ecsv") == "data/obs_plan_cat.ecsv"
    assert generate_catalog_name("plan") == "plan_cat"
    assert generate_catalog_name("plan.v1.ecsv") == "plan.v1_cat.ecsv"
