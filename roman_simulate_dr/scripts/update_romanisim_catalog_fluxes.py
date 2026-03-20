import numpy as np
from astropy.table import Table

from roman_photoz.update_romanisim_catalog_fluxes import (
    create_random_catalog,
    njy_to_mgy,
    update_fluxes,
)

if __name__ == "__main__":

    def parse_args():
        """
        Parse command-line arguments for the script.

        Returns
        -------
        argparse.Namespace
            Parsed arguments.
        """
        import argparse

        parser = argparse.ArgumentParser(
            description="Update fluxes in a Romanisim catalog using a reference catalog."
        )

        parser.add_argument(
            "--target-catalog",
            type=str,
            default="romanisim_input_catalog.ecsv",
            help="Target catalog to update fluxes in.",
        )

        parser.add_argument(
            "--flux-catalog",
            type=str,
            default="roman_simulated_catalog.parquet",
            help="Reference catalog to use for updating fluxes.",
        )

        parser.add_argument(
            "--output-filename",
            type=str,
            default="romanisim_input_catalog_fluxes_updated.ecsv",
            help="Output filename for the updated catalog.",
        )

        parser.add_argument(
            "--nobj",
            type=int,
            default=None,
            help=(
                "Number of sources to subselect from target catalog.  Must "
                "be smaller than the number of rows in the target catalog."
            ),
        )

        return parser.parse_args()

    args = parse_args()

    romanisim_catalog_filename = args.target_catalog
    roman_photoz_catalog_filename = args.flux_catalog
    output_filename = args.output_filename

    romanisim_cat = Table.read(romanisim_catalog_filename, format="ascii.ecsv")
    if args.nobj is not None:
        if args.nobj > len(romanisim_cat):
            raise ValueError(
                "number of objects must be smaller than the number of objects"
                "in the target catalog."
            )
        rng = np.random.default_rng()
        romanisim_cat = romanisim_cat[
            rng.choice(len(romanisim_cat), args.nobj, replace=False)
        ]
    # roman_photoz_catalog fluxes are in nJy
    rpz_cat = Table.read(roman_photoz_catalog_filename, format="parquet")
    # create an array of minimum magnitudes across all selected flux columns
    # (this will be used as a boolean mask to filter out invalid objects)
    minmag = np.min(
        [
            -2.5 * np.log10(njy_to_mgy(rpz_cat[x]).value)
            for x in rpz_cat.dtype.names
            if x.endswith("_flux") and x.startswith("segment")
        ],
        axis=0,
    )
    # Create a filter mask to filter out objects outside the valid magnitude range
    rpz_cat = rpz_cat[(minmag > 0) & (minmag < 33)]
    # create a random catalog from rpz_cat with the same number of rows as romanisim_cat
    rpz_cat = create_random_catalog(table=rpz_cat, n=len(romanisim_cat))

    update_fluxes_cat = update_fluxes(
        target_catalog=romanisim_cat,
        flux_catalog=rpz_cat,
    )
    update_fluxes_cat.write(output_filename, format="ascii.ecsv", overwrite=True)

    print("done")
