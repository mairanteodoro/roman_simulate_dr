import argparse
from pathlib import Path
import logging
import tomllib

from astropy.coordinates import SkyCoord
from astropy.table import vstack
from romanisim.catalog import make_cosmos_galaxies, make_gaia_stars, make_stars

# Setup logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(processName)s %(message)s",
)
logger = logging.getLogger(__name__)


def read_obs_plan(filename: str):
    """
    Read the observation plan file from a TOML file.
    """
    with open(filename, "rb") as f:
        data = tomllib.load(f)
    logger.info(f"Loaded observation plan from {filename}")
    return data


def generate_romanisim_input_catalog(
    ra_ref: float = 270.0,
    dec_ref: float = 66.0,
    search_radius: float = 1,
    filter_list: list[str] | None = None,
    output_catalog_filename: str = "output_cat.ecsv",
) -> None:
    """
    Generate a romanisim input catalog containing galaxies and stars.
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
    star_cat.write(stars_output_filename, format=output_catalog_format, overwrite=True)

    catalog = vstack([gal_cat, gaia_star_cat, star_cat])
    catalog.write(output_catalog_filename, format="ascii.ecsv", overwrite=True)
    logger.info(
        f"Final concatenated catalog saved to '{output_catalog_filename}'. Total sources: {len(catalog)}."
    )


def run(plan: dict, output_catalog_filename: str | None = None):
    """
    Run the romanisim simulation workflow for all passes and visits in the plan.
    """
    for p in plan["pass"]["details"]:
        for v in plan["pass"]["visit"]["details"]:
            logger.info(f"pass: {p['name']}, visit: {v['name']}")
            logger.info(f"  ra_ref: {v['lon']}, dec_ref: {v['lat']}")
            if output_catalog_filename is None:
                output_catalog_filename = f"{p['name']}_{v['name']}_cat.ecsv"

            # Generate catalog ONCE per pass/visit (not per exposure/SCA/filter)
            generate_romanisim_input_catalog(
                ra_ref=v["lon"],
                dec_ref=v["lat"],
                search_radius=0.2,
                filter_list=[
                    filt
                    for e in plan["pass"]["visit"]["exposure"]["details"]
                    for filt in e["filter_names"]
                ],
                output_catalog_filename=output_catalog_filename,
            )
            logger.info(f" -> Saved catalog to {output_catalog_filename}.")


def main():
    # last run (09/19/25) took 1h 47min with max_workers=16
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
    args = parser.parse_args()

    plan = read_obs_plan(args.obs_plan)

    run(plan, output_catalog_filename=args.output_filename)

    logger.info("Done.")


if __name__ == "__main__":
    main()
