import argparse
import subprocess
import sys
import tempfile
from pathlib import Path

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
        self.plan = read_obs_plan(obs_plan_filename)
        if output_catalog_filename is not None:
            self.catalog_filename = output_catalog_filename
        else:
            self.catalog_filename = generate_catalog_name(obs_plan_filename)
        self.ra = ra if ra is not None else float(np.mean(np.array(self.plan["RA"])))
        self.dec = (
            dec if dec is not None else float(np.mean(np.array(self.plan["DEC"])))
        )
        self.radius = radius if radius is not None else 0.3

    @staticmethod
    def run_simulated_catalog_and_update_fluxes(target_catalog):
        """
        Run the roman_photoz SimulatedCatalog and update_fluxes in a subprocess to fully isolate multiprocessing,
        and return the updated Astropy Table result using a temporary Parquet file.
        """
        import pickle

        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tf_flux:
            flux_path = Path(tf_flux.name)
        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tf_target:
            target_path = Path(tf_target.name)
            # Save target_catalog to a pickle file for the subprocess
            pickle.dump(target_catalog, tf_target)

        script = (
            "import pickle, re\n"
            "from roman_photoz.create_simulated_catalog import SimulatedCatalog\n"
            "from roman_photoz.update_romanisim_catalog_fluxes import update_fluxes\n"
            f"with open({str(target_path)!r}, 'rb') as f:\n"
            "    target_catalog = pickle.load(f)\n"
            "try:\n"
            "    rpz_catalog = SimulatedCatalog(len(target_catalog))\n"
            "    flux_catalog = rpz_catalog.process(return_catalog=True)\n"
            "except ValueError as e:\n"
            "    msg = str(e)\n"
            "    match = re.search(r'only (\\d+) lines are available', msg)\n"
            "    if match:\n"
            "        available = int(match.group(1))\n"
            "        rpz_catalog = SimulatedCatalog(available)\n"
            "        flux_catalog = rpz_catalog.process(return_catalog=True)\n"
            "        target_catalog = target_catalog[:available]\n"
            "    else:\n"
            "        raise\n"
            f"updated = update_fluxes(target_catalog=target_catalog, flux_catalog=flux_catalog)\n"
            f"with open({str(flux_path)!r}, 'wb') as f:\n"
            "    pickle.dump(updated, f)\n"
        )
        subprocess.run([sys.executable, "-c", script], check=True)

        with open(flux_path, "rb") as f:
            updated_catalog = pickle.load(f)
        flux_path.unlink()
        target_path.unlink()
        return updated_catalog

    def _generate_catalog(self, filter_list=None, use_photoz=False):
        """
        Generate a single catalog covering the full area and keep components in memory.

        Parameters
        ----------
        filter_list : list of str or None, optional
            List of filter names to use for bandpasses. If None, uses default filters.
        use_photoz : bool, optional
            Whether to use roman_photoz SimulatedCatalog for galaxies.
        """
        logger.info(
            f"Generating catalog at RA={self.ra} Dec={self.dec} radius={self.radius} deg"
        )
        if filter_list is None:
            filter_list = ["f062", "f087", "f106", "f129", "f158", "f184", "f213"]
        bandpasses = [bp.upper() for bp in filter_list]

        coords = SkyCoord(ra=self.ra, dec=self.dec, unit="deg", frame="icrs")

        gal_cat = make_cosmos_galaxies(
            coord=coords, bandpasses=bandpasses, seed=42, radius=self.radius
        )

        if use_photoz:
            gal_cat = self.run_simulated_catalog_and_update_fluxes(
                target_catalog=gal_cat,
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

        catalog = vstack([gal_cat, gaia_star_cat, star_cat])
        catalog.write(self.catalog_filename, format="parquet", overwrite=True)

        logger.info(
            f"""
              Final concatenated catalog saved to '{self.catalog_filename}'.
              Total sources: {len(catalog)}.
              """
        )

    def run(self, use_photoz=False) -> None:
        self._generate_catalog(use_photoz=use_photoz)


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
        "--use-photoz",
        action="store_true",
        help="Use roman_photoz SimulatedCatalog for galaxies",
    )

    args = parser.parse_args()
    input_catalog = InputCatalog(
        obs_plan_filename=args.obs_plan,
        output_catalog_filename=args.output_filename,
        ra=args.ra,
        dec=args.dec,
        radius=args.radius,
    )
    input_catalog.run(use_photoz=args.use_photoz)
    logger.info("Done.")


def main():
    _cli()


if __name__ == "__main__":
    main()
