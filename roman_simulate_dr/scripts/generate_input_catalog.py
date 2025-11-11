import argparse

import numpy as np
from astropy.coordinates import SkyCoord
from astropy.table import Table, vstack
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
        flux_catalog_filename: str | None = None,
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
        flux_catalog_filename : str or None, optional
            Path to a flux_catalog file previously produced by roman_photoz (e.g. parquet).
            If provided, the generated catalog will be updated using this flux catalog.
        """
        self.plan = read_obs_plan(obs_plan_filename)
        if output_catalog_filename is not None:
            self.catalog_filename = output_catalog_filename
        else:
            self.catalog_filename = generate_catalog_name(obs_plan_filename)

        # set reference coordinates and radius to simulate
        self.ra = ra if ra is not None else float(np.mean(np.array(self.plan["RA"])))
        self.dec = (
            dec if dec is not None else float(np.mean(np.array(self.plan["DEC"])))
        )
        self.radius = radius if radius is not None else 0.3

        # new: externally-provided flux catalog file (produced by roman_photoz)
        self.flux_catalog_filename = flux_catalog_filename

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

        coords = SkyCoord(ra=self.ra, dec=self.dec, unit="deg", frame="icrs")

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

        # If the user supplied a flux_catalog filename, update the generated catalog
        # using that file (user must run roman_photoz separately and provide the flux_catalog output).
        if self.flux_catalog_filename is not None:
            logger.info(
                f"Updating generated catalog using flux catalog: {self.flux_catalog_filename}"
            )
            updated = self.update_catalog_fluxes(catalog)
            # If update_catalog_fluxes succeeds it already writes the updated catalog.
            logger.info(
                f"Updated catalog with roman_photoz fluxes saved to '{self.catalog_filename}'. Total sources: {len(updated)}"
            )

    def update_catalog_fluxes(self, catalog: Table) -> Table:
        """
        Update the provided catalog's fluxes using an externally provided flux catalog file.

        This method isolates the logic of reading the flux catalog file, importing the
        update helper from roman_photoz, performing the update, and writing the result.

        Parameters
        ----------
        catalog : astropy.table.Table
            The generated romanisim catalog to be updated.

        Returns
        -------
        updated : astropy.table.Table
            The updated catalog (also written to self.catalog_filename).
        """
        if self.flux_catalog_filename is None:
            raise RuntimeError(
                "No flux_catalog_filename provided to update_catalog_fluxes."
            )

        try:
            flux_catalog = Table.read(self.flux_catalog_filename, format="parquet")
        except Exception as exc:
            logger.error(
                f"Could not read flux catalog '{self.flux_catalog_filename}': {exc}"
            )
            raise

        # import the helper that performs the update (keep import local to avoid
        # forcing roman_photoz to be installed when users just want to generate catalogs).
        try:
            from roman_photoz.update_romanisim_catalog_fluxes import update_fluxes
        except Exception:
            logger.error(
                "Failed to import 'update_fluxes' from roman_photoz. "
                "If you want to update fluxes using a roman_photoz output file, "
                "please ensure the 'roman_photoz' package is installed and importable."
            )
            raise

        try:
            updated = update_fluxes(target_catalog=catalog, flux_catalog=flux_catalog)
        except Exception as exc:
            logger.error(f"Failed to update catalog fluxes: {exc}")
            raise

        # Overwrite the previously written catalog with the updated version
        updated.write(self.catalog_filename, format="parquet", overwrite=True)
        return updated

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
    parser.add_argument(
        "--flux-catalog",
        type=str,
        default=None,
        required=False,
        help="Path to a flux_catalog file produced by roman_photoz (parquet). If provided, the generated catalog will be updated using this file.",
    )
    args = parser.parse_args()

    input_catalog = InputCatalog(
        obs_plan_filename=args.obs_plan,
        output_catalog_filename=args.output_filename,
        ra=args.ra,
        dec=args.dec,
        radius=args.radius,
        flux_catalog_filename=args.flux_catalog,
    )
    input_catalog.run()

    logger.info("Done.")


if __name__ == "__main__":
    """
    Entry point for the script when run as a standalone program.
    """
    _cli()
