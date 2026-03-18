import argparse
import glob
import concurrent.futures
from typing import List

import asdf
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import roman_datamodels as rdm
from astropy.visualization import simple_norm
from reproject import reproject_interp
from reproject.mosaicking import find_optimal_celestial_wcs, reproject_and_coadd
from tqdm import tqdm

# Standard Roman WFI Filters
ROMAN_FILTERS = ["f062", "f087", "f106", "f129", "f146", "f158", "f184", "f213"]


def create_mosaic_plot(mosaic_array, target_wcs, filter_id, filename):
    """Generates a clean, white-background mosaic with Viridis and Degree coordinates."""

    fig = plt.figure(figsize=(12, 12), dpi=300)
    ax = plt.subplot(111, projection=target_wcs)

    # 1. Background and Figure Setup
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    # 2. Handle the "empty" space (Gaps/Background)
    # Masking zeros/NaNs so they render as white
    display_data = np.where(
        (mosaic_array <= 0) | np.isnan(mosaic_array), np.nan, mosaic_array
    )

    current_cmap = cm.get_cmap("viridis").copy()
    current_cmap.set_bad(color="white")  # Set NaN areas to white

    norm = simple_norm(display_data, "asinh", percent=99.5)

    ax.imshow(
        display_data,
        origin="lower",
        norm=norm,
        cmap=current_cmap,
        interpolation="nearest",
    )

    ax.set_title(
        f"Roman WFI Mosaic - {filter_id.upper()}", fontsize=18, pad=30, color="black"
    )

    # 3. Coordinate Formatting (Decimal Degrees)
    lon = ax.coords[0]
    lat = ax.coords[1]

    ax.coords.grid(color="gray", alpha=0.2, linestyle="--")

    lon.set_axislabel("Right Ascension (degrees)", fontsize=14, color="black")
    lat.set_axislabel("Declination (degrees)", fontsize=14, color="black")

    # Use decimal degrees (d.dd)
    lon.set_major_formatter("d.dd")
    lat.set_major_formatter("d.dd")

    ax.tick_params(axis="both", colors="black", labelsize=10)

    plt.savefig(filename, dpi=300, facecolor="white", bbox_inches="tight")
    plt.close(fig)


def process_single_filter(
    filter_id: str, file_pattern: str, output_prefix: str, save_asdf: bool
):
    search_pattern = f"{file_pattern.rstrip('_')}_{filter_id.lower()}_coadd.asdf"
    file_list = sorted(glob.glob(search_pattern))

    if not file_list:
        return f"Skipped {filter_id}: No files found."

    try:
        input_data_wcs = []
        wcs_shape_pairs = []

        for filename in file_list:
            with rdm.open(filename) as model:
                input_data_wcs.append((model.data.copy(), model.meta.wcs))
                wcs_shape_pairs.append((model.data.shape, model.meta.wcs))

        target_wcs, target_shape = find_optimal_celestial_wcs(wcs_shape_pairs)

        mosaic_array, footprint = reproject_and_coadd(
            input_data_wcs,
            output_projection=target_wcs,
            shape_out=target_shape,
            reproject_function=reproject_interp,
            combine_function="mean",
            match_background=True,
        )

        del input_data_wcs, wcs_shape_pairs

        # Save ASDF only if parameter is passed
        if save_asdf:
            asdf_out = f"{output_prefix}_{filter_id}.asdf"
            output_tree = {
                "data": mosaic_array,
                "footprint": footprint,
                "meta": {"wcs": target_wcs, "filter": filter_id.upper()},
            }
            with asdf.AsdfFile(output_tree) as af:
                af.write_to(asdf_out)

        # Plotting
        png_out = f"{output_prefix}_{filter_id}.png"
        create_mosaic_plot(mosaic_array, target_wcs, filter_id, png_out)

        del mosaic_array
        return f"Success: {filter_id.upper()}"

    except Exception as e:
        return f"Error: {filter_id.upper()} - {str(e)}"


def main():
    parser = argparse.ArgumentParser(description="Parallel Roman Mosaic Generator")
    parser.add_argument("pattern", help="Base file pattern")
    parser.add_argument("--filter", help="Specific filter only")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument(
        "--save-asdf", action="store_true", help="Save the coadded ASDF file"
    )

    args = parser.parse_args()
    output_prefix = args.pattern.strip("*").strip("_")
    filters_to_run = [args.filter.lower()] if args.filter else ROMAN_FILTERS

    print(f"Starting mosaic production on {len(filters_to_run)} filter(s)...")

    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(
                process_single_filter, f, args.pattern, output_prefix, args.save_asdf
            ): f
            for f in filters_to_run
        }

        # Progress Bar Implementation
        for future in tqdm(
            concurrent.futures.as_completed(futures),
            total=len(futures),
            desc="Mosaicking Filters",
        ):
            result = future.result()
            # Optionally print the result if it's an error
            if "Error" in result:
                print(f"\n{result}")


if __name__ == "__main__":
    main()
