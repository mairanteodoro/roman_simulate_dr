import argparse
import logging

import numpy as np
from astropy.coordinates import SkyCoord
from astropy.table import vstack
from romanisim.catalog import make_cosmos_galaxies, make_gaia_stars, make_stars

from roman_simulate_dr.scripts.utils import read_obs_plan, generate_catalog_name

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
        Path to the observation plan file.
    output_catalog_filename : str or None, optional
        Filename for the final output catalog. If None, the filename will be derived
        from obs_plan_filename by appending '_cat' before the file extension.
    master_ra : float or None, optional
        Override for the master RA (deg) of the whole catalog.
    master_dec : float or None, optional
        Override for the master Dec (deg) of the whole catalog.
    master_radius : float or None, optional
        Override for the master radius (deg) of the whole catalog.
    """

    def __init__(
        self,
        obs_plan_filename: str,
        output_catalog_filename: str | None = None,
        master_ra: float | None = None,
        master_dec: float | None = None,
        master_radius: float | None = None,
    ):
        self.plan = read_obs_plan(obs_plan_filename)
        if output_catalog_filename is not None:
            self.catalog_filename = output_catalog_filename
        else:
            self.catalog_filename = generate_catalog_name(obs_plan_filename)

        # set reference coordinates and radius to simulate
        self.master_ra = (
            master_ra
            if master_ra is not None
            else float(np.mean(np.array(self.plan["RA"])))
        )
        self.master_dec = (
            master_dec
            if master_dec is not None
            else float(np.mean(np.array(self.plan["DEC"])))
        )
        self.master_radius = master_radius if master_radius is not None else 0.3

    def _generate_master_catalog(self, filter_list=None):
        """
        Generate a single catalog covering the full area and keep components in memory.
        """
        logger.info(
            f"Generating master catalog at RA={self.master_ra} Dec={self.master_dec} radius={self.master_radius} deg"
        )
        if filter_list is None:
            filter_list = ["f062", "f087", "f106", "f129", "f158", "f184", "f213"]
        bandpasses = [bp.upper() for bp in filter_list]

        coords = SkyCoord(
            ra=self.master_ra, dec=self.master_dec, unit="deg", frame="icrs"
        )

        # compute components
        gal_cat = make_cosmos_galaxies(
            coord=coords, bandpasses=bandpasses, seed=42, radius=self.master_radius
        )
        gaia_star_cat = make_gaia_stars(
            coord=coords, bandpasses=bandpasses, seed=42, radius=self.master_radius
        )
        star_cat = make_stars(
            coord=coords,
            n=1000,
            bandpasses=bandpasses,
            seed=42,
            radius=self.master_radius,
        )

        # concatenate and save
        catalog = vstack([gal_cat, gaia_star_cat, star_cat])
        catalog.write(self.catalog_filename, format="ascii.ecsv", overwrite=True)

        logger.info(
            f"""
              Final concatenated catalog saved to '{self.catalog_filename}'.
              Total sources: {len(catalog)}.
              """
        )

    def run(self) -> None:
        """
        Run the Romanisim input catalog generation workflow.
        This method creates a single master catalog for all exposures.
        """
        self._generate_master_catalog()


def _cli():
    parser = argparse.ArgumentParser(
        description="Generate Romanisim input catalogs based on an observation plan."
    )
    parser.add_argument(
        "--obs-plan",
        type=str,
        default="obs_plan.ecsv",
        help="Observation plan filename (default: obs_plan.ecsv)",
    )
    parser.add_argument(
        "--output-filename",
        type=str,
        default=None,
        required=False,
        help="Output catalog filename (default: append '_cat' to the observation plan filename)",
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
        default=0.3,
        help="Override: Radius of master catalog (deg; default 0.3)",
    )
    args = parser.parse_args()

    input_catalog = InputCatalog(
        obs_plan_filename=args.obs_plan,
        output_catalog_filename=args.output_filename,
        master_ra=args.master_ra,
        master_dec=args.master_dec,
        master_radius=args.master_radius,
    )
    input_catalog.run()

    logger.info("Done.")


if __name__ == "__main__":
    _cli()
