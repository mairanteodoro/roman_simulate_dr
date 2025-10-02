import argparse

import numpy as np
from astropy.coordinates import SkyCoord
from astropy.table import vstack
from romanisim.catalog import make_cosmos_galaxies, make_gaia_stars, make_stars

from roman_simulate_dr.scripts.logger import logger
from roman_simulate_dr.scripts.utils import generate_catalog_name, read_obs_plan


class InputCatalog:
    """
    Class to generate Romanisim input catalogs based on an observation plan.
    """

    def __init__(
        self,
        obs_plan_filename: str,
        output_catalog_filename: str | None = None,
        ra: float | None = None,
        dec: float | None = None,
        radius: float | None = None,
    ):
        """
        Initialize the InputCatalog object.

        Parameters
        ----------
        obs_plan_filename : str
            Path to the observation plan file.
        output_catalog_filename : str or None, optional
            Filename for the final output catalog.
        ra : float or None, optional
            Override for the RA (deg) of the whole catalog.
        dec : float or None, optional
            Override for the Dec (deg) of the whole catalog.
        radius : float or None, optional
            Override for the radius (deg) of the whole catalog.
        """
        self.plan = read_obs_plan(obs_plan_filename)
        if output_catalog_filename is not None:
            self.catalog_filename = output_catalog_filename
        else:
            self.catalog_filename = generate_catalog_name(obs_plan_filename)

        # set reference coordinates and radius to simulate
        self.ra = (
            ra
            if ra is not None
            else float(np.mean(np.array(self.plan["RA"])))
        )
        self.dec = (
            dec
            if dec is not None
            else float(np.mean(np.array(self.plan["DEC"])))
        )
        self.radius = radius if radius is not None else 0.3

    def _generate_catalog(self, filter_list=None):
        """
        Generate a single catalog covering the full area and keep components in memory.

        Parameters
        ----------
        filter_list : list of str or None, optional
            List of filter names to use for bandpasses. If None, uses default filters.
        """
        logger.info(
            f"Generating catalog at RA={self.ra} Dec={self.dec} radius={self.radius} deg"
        )
        if filter_list is None:
            filter_list = ["f062", "f087", "f106", "f129", "f158", "f184", "f213"]
        bandpasses = [bp.upper() for bp in filter_list]

        coords = SkyCoord(
            ra=self.ra, dec=self.dec, unit="deg", frame="icrs"
        )

        # compute components
        gal_cat = make_cosmos_galaxies(
            coord=coords, bandpasses=bandpasses, seed=42, radius=self.radius
        )
        gaia_star_cat = make_gaia_stars(
            coord=coords, bandpasses=bandpasses, seed=42, radius=self.radius
        )
        star_cat = make_stars(
            coord=coords,
            n=1000,
            bandpasses=bandpasses,
            seed=42,
            radius=self.radius,
        )

        # concatenate and save
        catalog = vstack([gal_cat, gaia_star_cat, star_cat])
        catalog.write(self.catalog_filename, format="parquet", overwrite=True)

        logger.info(
            f"""
              Final concatenated catalog saved to '{self.catalog_filename}'.
              Total sources: {len(catalog)}.
              """
        )

    def run(self) -> None:
        """
        Run the Romanisim input catalog generation workflow.

        This method creates a single catalog for all exposures.
        """
        self._generate_catalog()


def _cli():
    """
    Command-line interface for generating Romanisim input catalogs.

    Parses arguments and runs the catalog generation workflow.
    """
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
        "--ra",
        type=float,
        default=None,
        help="Override: RA center of catalog (deg)",
    )
    parser.add_argument(
        "--dec",
        type=float,
        default=None,
        help="Override: Dec center of catalog (deg)",
    )
    parser.add_argument(
        "--radius",
        type=float,
        default=0.3,
        help="Override: Radius of catalog (deg; default 0.3)",
    )
    args = parser.parse_args()

    input_catalog = InputCatalog(
        obs_plan_filename=args.obs_plan,
        output_catalog_filename=args.output_filename,
        ra=args.ra,
        dec=args.dec,
        radius=args.radius,
    )
    input_catalog.run()

    logger.info("Done.")


if __name__ == "__main__":
    """
    Entry point for the script when run as a standalone program.
    """
    _cli()
