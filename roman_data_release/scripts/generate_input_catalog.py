import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import tomllib
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
    def __init__(
        self,
        obs_plan_filename: str,
        output_catalog_filename: str | None = None,
        chunk_size: int | None = None,
        max_workers: int | None = None,
    ):
        self.plan = self.read_obs_plan(obs_plan_filename)
        self.cat_component_filenames = []
        self.catalog_filename = output_catalog_filename
        self.chunk_size = chunk_size
        self.max_workers = max_workers

    def read_obs_plan(self, filename: str):
        with open(filename, "rb") as f:
            data = tomllib.load(f)
        logger.info(f"Loaded observation plan from {filename}")
        return data

    def generate_exposure_catalog(
        self,
        ra_ref: float = 270.0,
        dec_ref: float = 66.0,
        search_radius: float = 1,
        filter_list: list[str] | None = None,
        output_catalog_filename: str = "output_cat.ecsv",
    ) -> None:
        logger.info(f"Search radius to be used (in degrees): {search_radius}")

        logger.info(f"Started exposure catalog generation in thread {threading.current_thread().name}")

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

    def generate_final_catalog(
        self, output_catalog_filename: str = "final_romanisim_input_catalog.ecsv"
    ):
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
            f"""Final concatenated catalog saved to '{output_catalog_filename}'.
              Total sources: {len(catalog)}.
              Total unique sources: {len(unique_catalog)}.
              """
        )

    def run(self, output_catalog_filename: str | None = None):
        for p in self.plan["passes"]:
            pass_name = p.get("name", "pass")
            for v in p.get("visits", []):
                visit_name = v.get("name", "visit")
                ra_ref = v.get("lon")
                dec_ref = v.get("lat")
                logger.info(f"pass: {pass_name}, visit: {visit_name}")
                logger.info(f"  ra_ref: {ra_ref}, dec_ref: {dec_ref}")

                exposures = []
                for eidx, e in enumerate(v.get("exposures", [])):
                    filter_list = e.get(
                        "filter_names",
                        ["F062", "F087", "F106", "F129", "F158", "F184", "F213"],
                    )
                    exposure_catalog_filename = (
                        f"{pass_name}_{visit_name}_exp{eidx}_cat.ecsv"
                    )
                    exposures.append(
                        {
                            "ra_ref": ra_ref,
                            "dec_ref": dec_ref,
                            "search_radius": 0.2,
                            "filter_list": filter_list,
                            "output_catalog_filename": exposure_catalog_filename,
                        }
                    )

                # Parallelize only if max_workers > 1
                if (
                    hasattr(self, "max_workers")
                    and self.max_workers
                    and self.max_workers > 1
                ):
                    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                        futures = [
                            executor.submit(
                                self.generate_exposure_catalog,
                                exp["ra_ref"],
                                exp["dec_ref"],
                                exp["search_radius"],
                                exp["filter_list"],
                                exp["output_catalog_filename"],
                            )
                            for exp in exposures
                        ]
                        for future, exp in zip(
                            as_completed(futures), exposures, strict=False
                        ):
                            future.result()
                            logger.info(
                                f" -> Saved catalog to {exp['output_catalog_filename']}."
                            )
                else:
                    for exp in exposures:
                        self.generate_exposure_catalog(
                            exp["ra_ref"],
                            exp["dec_ref"],
                            exp["search_radius"],
                            exp["filter_list"],
                            exp["output_catalog_filename"],
                        )
                        logger.info(
                            f" -> Saved catalog to {exp['output_catalog_filename']}."
                        )

        self.generate_final_catalog(
            self.catalog_filename
            or output_catalog_filename
            or "romanisim_input_catalog.ecsv"
        )


def main():
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
    main()
