import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import groupby

from astropy.table import Table

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_obs_plan(filename: str):
    """
    Reads an observation plan from an ECSV file and returns a dictionary
    structured with passes and visits for compatibility with InputCatalog.
    """
    table = Table.read(filename, format="ascii.ecsv")
    # Group by PLAN and PASS
    passes = []
    for (plan, pass_num), pass_rows in groupby(
        sorted(table, key=lambda row: (row["PLAN"], row["PASS"])),
        key=lambda row: (row["PLAN"], row["PASS"]),
    ):
        pass_rows = list(pass_rows)
        pass_dict = {
            "name": f"plan_{plan}_pass_{pass_num}",
            "plan": plan,
            "pass": pass_num,
            "roll": float(pass_rows[0]["PA"]),
            "visits": [],
        }
        # Group by VISIT within each pass
        for visit_num, visit_rows in groupby(
            sorted(pass_rows, key=lambda row: row["VISIT"]),
            key=lambda row: row["VISIT"],
        ):
            visit_rows = list(visit_rows)
            visit_dict = {
                "name": f"visit_{visit_num}",
                "lon": float(visit_rows[0]["RA"]),
                "lat": float(visit_rows[0]["DEC"]),
                "exposures": [
                    {
                        "filter_names": [row["BANDPASS"]],
                        "sca_ids": [1],
                        "duration": int(row["DURATION"]),
                        "ma_table_number": int(row["MA_TABLE_NUMBER"]),
                        "segment": int(row["SEGMENT"]),
                        "observation": int(row["OBSERVATION"]),
                        "exposure": int(row["EXPOSURE"]),
                    }
                    for row in visit_rows
                ],
            }
            pass_dict["visits"].append(visit_dict)
        passes.append(pass_dict)
    # Extract romanisim_input_catalog_name (i.e., the output catalog filename) from filename
    plan_dict = {
        "passes": passes,
        "romanisim_input_catalog_name": filename.replace(".ecsv", "_cat.ecsv"),
    }
    return plan_dict


def parallelize_jobs(method, jobs, max_workers: int | None = None):
    """
    Run jobs in parallel using ThreadPoolExecutor.

    Parameters
    ----------
    method : callable
        The function or method to execute.
    jobs : list of dict
        Each dict contains the keyword arguments for one call to `method`.
    max_workers : int
        Number of parallel workers.

    Returns
    -------
    None
    """
    if max_workers and max_workers > 1:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(method, **job) for job in jobs]
            for future, job in zip(as_completed(futures), jobs, strict=False):
                future.result()
    else:
        for job in jobs:
            method(**job)


def generate_roman_filename(
    program: int,
    plan: int,
    passno: int,
    segment: int,
    observation: int,
    visit: int,
    exposure: int,
    sca: int,
    bandpass: str,
    suffix: str,
) -> str:
    filename = (
        f"r{program}{plan:02d}{passno:03d}{segment:03d}"
        f"{observation:03d}{visit:03d}_{exposure:04d}"
        f"_wfi{sca:02d}_{bandpass.lower()}_{suffix}.asdf"
    )
    return filename
