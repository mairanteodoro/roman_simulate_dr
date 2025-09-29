import argparse
import logging
from pathlib import Path

import numpy as np
from astropy.coordinates import SkyCoord
from astropy.table import Table, vstack
from romanisim.catalog import make_cosmos_galaxies, make_gaia_stars, make_stars

from roman_simulate_dr.scripts.utils import parallelize_jobs, read_obs_plan

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
    master_ra : float or None, optional
        Override for the master RA (deg) of the whole survey/catalog.
    master_dec : float or None, optional
        Override for the master Dec (deg) of the whole survey/catalog.
    master_radius : float or None, optional
        Override for the master radius (deg) of the whole survey/catalog.
    """

    def __init__(
        self,
        obs_plan_filename: str,
        output_catalog_filename: str | None = None,
        chunk_size: int | None = None,
        max_workers: int | None = None,
        master_ra: float | None = None,
        master_dec: float | None = None,
        master_radius: float | None = None,
    ):
        self.plan = read_obs_plan(obs_plan_filename)
        self.cat_component_filenames = []
        if output_catalog_filename is not None:
            self.catalog_filename = output_catalog_filename
        else:
            self.catalog_filename = self.plan.get("romanisim_input_catalog_name")
        self.chunk_size = chunk_size
        self.max_workers = max_workers

        # Determine the master catalog center/size
        if master_ra is not None and master_dec is not None and master_radius is not None:
            self.master_ra = master_ra
            self.master_dec = master_dec
            self.master_radius = master_radius
        else:
            # Compute bounding circle for all visits if not provided
            visits = [
                (v.get("lon"), v.get("lat"))
                for p in self.plan["passes"]
                for v in p.get("visits", [])
                if v.get("lon") is not None and v.get("lat") is not None
            ]
            if not visits:
                raise RuntimeError("No visits with lon/lat found in the observation plan.")
            # crude average for center
            ra_vals = np.array([v[0] for v in visits])
            dec_vals = np.array([v[1] for v in visits])
            self.master_ra = float(np.mean(ra_vals))
            self.master_dec = float(np.mean(dec_vals))
            # crude max separation for radius
            coords = SkyCoord(ra=ra_vals, dec=dec_vals, unit="deg", frame="icrs")
            max_sep = np.max(coords.separation(SkyCoord(self.master_ra, self.master_dec, unit="deg")).deg)
            self.master_radius = float(max_sep + 0.3)  # add margin to cover all visits

    def _janitor(self):
        """
        Delete all intermediate catalog component files listed in self.cat_component_filenames.
        """
        for f in self.cat_component_filenames:
            try:
                path = Path(f)
                if path.exists():
                    path.unlink()
            except Exception as e:
                logger.warning(f"Could not delete file {f}: {e}")

    def _generate_master_catalog(self, filter_list=None):
        """
        Generate a single catalog covering the full area.
        """
        logger.info(
            f"Generating master catalog at RA={self.master_ra} Dec={self.master_dec} radius={self.master_radius} deg"
        )
        if filter_list is None:
            filter_list = ["f062", "f087", "f106", "f129", "f158", "f184", "f213"]
        bandpasses = [bp.upper() for bp in filter_list]
        coords = SkyCoord(ra=self.master_ra, dec=self.master_dec, unit="deg", frame="icrs")
        output_catalog_format = "ascii.ecsv"
        prefix = Path(self.catalog_filename).stem

        gal_cat = make_cosmos_galaxies(
            coord=coords, bandpasses=bandpasses, seed=42, radius=self.master_radius
        )
        gaia_star_cat = make_gaia_stars(
            coord=coords, bandpasses=bandpasses, seed=42, radius=self.master_radius
        )
        star_cat = make_stars(
            coord=coords, n=1000, bandpasses=bandpasses, seed=42, radius=self.master_radius
        )
        gal_cat.write(f"{prefix}_cosmos_galaxies.ecsv", format=output_catalog_format, overwrite=True)
        gaia_star_cat.write(f"{prefix}_gaia_stars.ecsv", format=output_catalog_format, overwrite=True)
        star_cat.write(f"{prefix}_additional_stars.ecsv", format=output_catalog_format, overwrite=True)
        self.cat_component_filenames.extend([
            f"{prefix}_cosmos_galaxies.ecsv",
            f"{prefix}_gaia_stars.ecsv",
            f"{prefix}_additional_stars.ecsv",
        ])

    def _generate_final_catalog(self):
        """
        Concatenate all component catalogs, remove duplicates, and write the final catalog.
        Supports chunked writing if chunk_size is set.
        """
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
                    self.catalog_filename,
                    format="ascii.ecsv",
                    overwrite=first_chunk,
                    append=not first_chunk,
                )
                first_chunk = False
        else:
            unique_catalog.write(
                self.catalog_filename, format="ascii.ecsv", overwrite=True
            )
        logger.info(
            f"""
              Final concatenated catalog saved to '{self.catalog_filename}'.
              Total unique sources: {len(unique_catalog)}.
              """
        )

    def run(self) -> None:
        """
        Run the Romanisim input catalog generation workflow.
        This method creates a single master catalog for all exposures.
        """
        self._generate_master_catalog()
        self._generate_final_catalog()
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
        default=None,
        required=False,
        help="Output catalog filename (default: determined from the observation plan)",
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
    parser.add_argument(
        "--master-ra",
        type=float,
        default=None,
        help="Override: RA center of master catalog (deg)",
    )
    parser.add_argument(
        "--master-dec",
        type=float,
        default=None,
        help="Override: Dec center of master catalog (deg)",
    )
    parser.add_argument(
        "--master-radius",
        type=float,
        default=None,
        help="Override: Radius of master catalog (deg)",
    )
    args = parser.parse_args()

    chunk_size = args.chunk_size if args.chunk_size > 0 else None
    max_workers = args.max_workers if args.max_workers > 1 else None

    input_catalog = InputCatalog(
        obs_plan_filename=args.obs_plan,
        output_catalog_filename=args.output_filename,
        chunk_size=chunk_size,
        max_workers=max_workers,
        master_ra=args.master_ra,
        master_dec=args.master_dec,
        master_radius=args.master_radius,
    )
    input_catalog.run()

    logger.info("Done.")


if __name__ == "__main__":
    _cli()
