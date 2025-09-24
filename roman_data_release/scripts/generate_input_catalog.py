import argparse
import logging
import tomllib
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
from astropy.coordinates import SkyCoord
from astropy.table import Table, vstack
from romanisim.catalog import make_cosmos_galaxies, make_gaia_stars, make_stars

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(processName)s %(message)s",
)
logger = logging.getLogger(__name__)


class InputCatalog:
    """
    Class to generate Romanisim input catalogs based on an observation plan.

    Parameters
    ----------
    obs_plan_filename : str
        Path to the observation plan TOML file.
    output_catalog_filename : str or None, optional
        Filename for the final output catalog.
    chunk_size : int or None, optional
        Number of rows per chunk when writing the final catalog. If None or <= 0, disables chunking.
    max_workers : int or None, optional
        Number of parallel workers for exposure catalog generation. If None or <= 1, disables parallelization.
    """

    def __init__(
        self,
        obs_plan_filename: str,
        output_catalog_filename: str | None = None,
        chunk_size: int | None = None,
        max_workers: int | None = None,
    ):
        self.plan = self._read_obs_plan(obs_plan_filename)
        self.cat_component_filenames = []
        self.catalog_filename = output_catalog_filename
        self.chunk_size = chunk_size
        self.max_workers = max_workers

    def _janitor(self):
        """
        Delete all intermediate catalog component files listed in self.cat_component_filenames.

        Returns
        -------
        None
        """
        for f in self.cat_component_filenames:
            try:
                path = Path(f)
                if path.exists():
                    path.unlink()
            except Exception as e:
                logger.warning(f"Could not delete file {f}: {e}")

    def _read_obs_plan(self, filename: str):
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

    def _generate_exposure_catalog(
        self,
        ra_ref: float = 270.0,
        dec_ref: float = 66.0,
        search_radius: float = 1,
        filter_list: list[str] | None = None,
        output_catalog_filename: str = "output_cat.ecsv",
    ) -> None:
        """
        Generate a catalog for a single exposure, including galaxies and stars.

        Parameters
        ----------
        ra_ref : float, optional
            Reference right ascension in degrees.
        dec_ref : float, optional
            Reference declination in degrees.
        search_radius : float, optional
            Search radius in degrees.
        filter_list : list of str or None, optional
            List of filter names to use. If None, uses default filters.
        output_catalog_filename : str, optional
            Filename for the output exposure catalog.

        Returns
        -------
        None
        """
        logger.info(f"Search radius to be used (in degrees): {search_radius}")

        prefix = Path(output_catalog_filename).stem
        cosmos_gal_output_filename = f"{prefix}_cosmos_galaxies.ecsv"
        gaia_stars_output_filename = f"{prefix}_gaia_stars.ecsv"
        stars_output_filename = f"{prefix}_additional_stars.ecsv"
        n_additional_stars = 1000

        output_catalog_format = "ascii.ecsv"
        if filter_list is None:
            filter_list = ["f062", "f087", "f106", "f129", "f158", "f184", "f213"]
        bandpasses = [bp.upper() for bp in filter_list]
        coords = SkyCoord(ra=ra_ref, dec=dec_ref, unit="deg", frame="icrs")

        gal_cat = make_cosmos_galaxies(
            coord=coords, bandpasses=bandpasses, seed=42, radius=search_radius
        )
        gaia_star_cat = make_gaia_stars(
            coord=coords, bandpasses=bandpasses, seed=42, radius=search_radius
        )
        star_cat = make_stars(
            coord=coords,
            n=n_additional_stars,
            bandpasses=bandpasses,
            seed=42,
            radius=search_radius,
        )

        gal_cat.write(
            cosmos_gal_output_filename, format=output_catalog_format, overwrite=True
        )
        gaia_star_cat.write(
            gaia_stars_output_filename, format=output_catalog_format, overwrite=True
        )
        star_cat.write(
            stars_output_filename, format=output_catalog_format, overwrite=True
        )

        self.cat_component_filenames.extend(
            [
                cosmos_gal_output_filename,
                gaia_stars_output_filename,
                stars_output_filename,
            ]
        )

    def _generate_final_catalog(
        self, output_catalog_filename: str = "final_romanisim_input_catalog.ecsv"
    ):
        """
        Concatenate all component catalogs, remove duplicates, and write the final catalog.
        Supports chunked writing if chunk_size is set.

        Parameters
        ----------
        output_catalog_filename : str, optional
            Filename for the final output catalog.

        Returns
        -------
        None
        """
        if self.catalog_filename is None:
            self.catalog_filename = output_catalog_filename

        tables = [
            Table.read(fname, format="ascii.ecsv")
            for fname in self.cat_component_filenames
        ]
        catalog = vstack(tables)

        coords = SkyCoord(
            ra=catalog["ra"], dec=catalog["dec"], unit="deg", frame="icrs"
        )
        _, unique_indices = np.unique(coords.to_string("decimal"), return_index=True)
        unique_catalog = catalog[unique_indices]

        # Write in chunks only if chunk_size is provided and > 0
        if self.chunk_size is not None and self.chunk_size > 0:
            n_rows = len(unique_catalog)
            first_chunk = True
            for start in range(0, n_rows, self.chunk_size):
                chunk = unique_catalog[start : start + self.chunk_size]
                chunk.write(
                    output_catalog_filename,
                    format="ascii.ecsv",
                    overwrite=first_chunk,
                    append=not first_chunk,
                )
                first_chunk = False
        else:
            unique_catalog.write(
                output_catalog_filename, format="ascii.ecsv", overwrite=True
            )
        logger.info(
            f"""
              Final concatenated catalog saved to '{output_catalog_filename}'.
              Total unique sources: {len(unique_catalog)}.
              """
        )

    def run(self, output_catalog_filename: str | None = None):
        """
        Run the Romanisim input catalog generation workflow.

        This method processes all passes, visits, and exposures described in the observation plan.
        Each exposure job is executed independently, and parallelization is applied globally across all jobs
        if `max_workers` is set to a value greater than 1. After all exposure catalogs are generated,
        the final catalog is assembled and intermediate files are cleaned up.

        Parameters
        ----------
        output_catalog_filename : str or None, optional
            Filename for the final output catalog. If None, uses the value provided at initialization.

        Returns
        -------
        None
        """
        jobs = []
        for p in self.plan["passes"]:
            pass_name = p.get("name", "pass")
            for v in p.get("visits", []):
                visit_name = v.get("name", "visit")
                ra_ref = v.get("lon")
                dec_ref = v.get("lat")
                logger.info(f"pass: {pass_name}, visit: {visit_name}")
                logger.info(f"  ra_ref: {ra_ref}, dec_ref: {dec_ref}")

                for eidx, e in enumerate(v.get("exposures", [])):
                    filter_list = e.get(
                        "filter_names",
                        ["f062", "f087", "f106", "f129", "f158", "f184", "f213"],
                    )
                    exposure_catalog_filename = (
                        f"{pass_name}_{visit_name}_exp{eidx}_cat.ecsv"
                    )
                    jobs.append(
                        {
                            "ra_ref": ra_ref,
                            "dec_ref": dec_ref,
                            "search_radius": 0.2,
                            "filter_list": filter_list,
                            "output_catalog_filename": exposure_catalog_filename,
                        }
                    )

        # Parallelize all jobs if max_workers > 1
        if self.max_workers and self.max_workers > 1:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [
                    executor.submit(
                        self._generate_exposure_catalog,
                        job["ra_ref"],
                        job["dec_ref"],
                        job["search_radius"],
                        job["filter_list"],
                        job["output_catalog_filename"],
                    )
                    for job in jobs
                ]
                for future, job in zip(
                    as_completed(futures), jobs, strict=False
                ):
                    future.result()
                    logger.info(
                        f" -> Saved temporary catalog to {job['output_catalog_filename']}."
                    )
        else:
            for job in jobs:
                self._generate_exposure_catalog(
                    job["ra_ref"],
                    job["dec_ref"],
                    job["search_radius"],
                    job["filter_list"],
                    job["output_catalog_filename"],
                )
                logger.info(
                    f" -> Saved temporary catalog to {job['output_catalog_filename']}."
                )

        self._generate_final_catalog(
            self.catalog_filename
            or output_catalog_filename
            or "romanisim_input_catalog.ecsv"
        )
        self._janitor()


def _cli():
    parser = argparse.ArgumentParser(
        description="Generate Romanisim input catalogs based on an observation plan."
    )
    parser.add_argument(
        "--obs-plan",
        type=str,
        default="obs_plan.toml",
        help="Observation plan filename (default: obs_plan.toml)",
    )
    parser.add_argument(
        "--output-filename",
        type=str,
        default="romanisim_input_catalog.ecsv",
        help="Output catalog filename (default: romanisim_input_catalog.ecsv)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=0,
        help="Chunk size for writing the final catalog (default: 0, disables chunking)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=1,
        help="Number of parallel workers for exposure generation (default: 1, disables parallelization)",
    )
    args = parser.parse_args()

    chunk_size = args.chunk_size if args.chunk_size > 0 else None
    max_workers = args.max_workers if args.max_workers > 1 else None

    input_catalog = InputCatalog(
        obs_plan_filename=args.obs_plan,
        output_catalog_filename=args.output_filename,
        chunk_size=chunk_size,
        max_workers=max_workers,
    )
    input_catalog.run(output_catalog_filename=args.output_filename)

    logger.info("Done.")


if __name__ == "__main__":
    _cli()
