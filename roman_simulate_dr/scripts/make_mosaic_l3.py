import glob

import asdf
import matplotlib.pyplot as plt
import numpy as np
import roman_datamodels as rdm
from astropy.visualization import ImageNormalize, LogStretch, ZScaleInterval
from reproject import reproject_interp
from reproject.mosaicking import find_optimal_celestial_wcs, reproject_and_coadd


def create_l3_mosaic_with_grid(
    file_pattern="*_coadd.asdf", output_base="l3_mosaic_output"
):
    # 1. Gather L3 Coadd Files
    files = sorted(glob.glob(file_pattern))
    if not files:
        print(f"No files found matching {file_pattern}")
        return

    data_info = []
    wcs_info = []

    print(f"Loading and validating {len(files)} L3 coadd tiles...")
    for f in files:
        try:
            # L3 products typically use the MosaicModel
            with rdm.open(f) as model:
                if model.meta.wcs is None:
                    print(f"  [Skip] {f}: Missing WCS.")
                    continue

                # Extract data and strip units (L3 often has units like MJy/sr)
                data_arr = np.asanyarray(
                    model.data.value if hasattr(model.data, "value") else model.data
                )

                # Pre-clean NaNs for the coadding process
                data_arr = np.nan_to_num(data_arr, nan=0.0)

                wcs_info.append((data_arr.shape, model.meta.wcs))
                data_info.append((data_arr, model.meta.wcs))
        except Exception as e:
            print(f"  [Error] {f}: {e}")

    if not data_info:
        print("No valid L3 data found.")
        return

    # 2. Determine Mosaic Geometry
    print(f"Calculating optimal WCS for {len(data_info)} tiles...")
    target_wcs, target_shape = find_optimal_celestial_wcs(wcs_info)

    # 3. Reproject and Coadd
    print(f"Stitching tiles into final shape {target_shape}...")
    mosaic_array, footprint = reproject_and_coadd(
        data_info,
        target_wcs,
        shape_out=target_shape,
        reproject_function=reproject_interp,
        combine_function="mean",
    )

    # 4. Save as ASDF (preserving L3 Model metadata)
    # print(f"Saving {output_base}.asdf...")
    # tree = {
    #     "roman": {
    #         "data": mosaic_array,
    #         "meta": {
    #             "wcs": target_wcs,
    #             "model_type": "MosaicModel",
    #             "software_note": "L3 Mosaic created via reproject",
    #         },
    #     }
    # }
    # with asdf.AsdfFile(tree) as af:
    #     af.write_to(f"{output_base}.asdf")

    # 5. Save PNG with Coordinate Grid
    print(f"Generating {output_base}.png with RA/Dec grid...")

    norm = ImageNormalize(mosaic_array, interval=ZScaleInterval(), stretch=LogStretch())

    # Create the figure with WCS projection
    fig = plt.figure(figsize=(12, 10))
    ax = fig.add_subplot(1, 1, 1, projection=target_wcs)

    # Show the image
    im = ax.imshow(mosaic_array, origin="lower", cmap="magma", norm=norm)

    # Configure the Grid
    ax.coords.grid(color="white", alpha=0.3, linestyle="solid")

    # Set labels to Decimal Degrees
    ra = ax.coords[0]
    dec = ax.coords[1]

    ra.set_format_unit("deg")
    dec.set_format_unit("deg")

    ra.set_axislabel("Right Ascension (degrees)", fontsize=12)
    dec.set_axislabel("Declination (degrees)", fontsize=12)

    plt.title(f"Roman L3 Mosaic: {len(files)} Tiles", pad=20)
    plt.colorbar(im, ax=ax, label="Surface Brightness", fraction=0.046, pad=0.04)

    # Save the figure
    plt.savefig(f"{output_base}.png", bbox_inches="tight", dpi=300)
    plt.close()

    print("Success! L3 mosaic complete.")


if __name__ == "__main__":
    filter_list = ["f062", "f087", "f106", "f129", "f158", "f184", "f213"]
    for filter_name in filter_list:
        print(f"Creating mosaic for filter: {filter_name}")
        output_name = f"mosaic_{filter_name}_l3"
        file_pattern = f"r00001_r0_full_*_{filter_name}_coadd.asdf"
        create_l3_mosaic_with_grid(file_pattern=file_pattern, output_base=output_name)
