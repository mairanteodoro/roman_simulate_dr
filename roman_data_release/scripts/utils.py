import logging
import tomllib
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_obs_plan(filename: str):
    """
    Read the observation plan from a TOML file.

    Parameters
    ----------
    filename : str
        Path to the TOML observation plan file.

    Returns
    -------
    dict
        Parsed observation plan data.
    """
    with open(filename, "rb") as f:
        data = tomllib.load(f)

    logger.info(f"Loaded observation plan from {filename}")
    return data


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
                logger.info(
                    f" -> Saved temporary catalog to {job.get('output_catalog_filename', '<unknown>')}."
                )
    else:
        for job in jobs:
            method(**job)
            logger.info(
                f" -> Saved temporary catalog to {job.get('output_catalog_filename', '<unknown>')}."
            )
