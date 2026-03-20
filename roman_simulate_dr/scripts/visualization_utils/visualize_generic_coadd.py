#!/usr/bin/env python
"""
Nancy Grace Roman Space Telescope WFI Visualization Tool.

This module provides a command-line utility to visualize ASDF files from the
Roman Wide Field Instrument (WFI). It generates high-contrast, side-by-side
comparisons of calibrated images against both 'truth' catalogs (Gaia) and
locally detected sources.

Key Features
------------
- Support for Association files (.json) and Pipeline logs (.log).
- Cross-matching with Parquet-based source catalogs.
"""

import argparse
import glob
import re
from pathlib import Path

import astropy.units as u
import matplotlib.pyplot as plt
import pandas as pd
from astropy.coordinates import SkyCoord
from astropy.visualization import (
    AsinhStretch,
    ImageNormalize,
    ZScaleInterval,
)
from roman_datamodels import datamodels as rdm
from romancal.associations import load_asn


def plot_asdf_file(filepath, global_catalog=None, show_sources=False):
    """
    Read ASDF data and plot side-by-side comparison images.

    Parameters
    ----------
    filepath : pathlib.Path
        The path to the input .asdf file to be visualized.
    global_catalog : pandas.DataFrame, optional
        A catalog containing 'ra' and 'dec' columns used for truth
        overlays (red markers).
    show_sources : bool, optional
        If True, searches for a companion '_cat.parquet' file to
        overlay detected centroids (cyan markers). Default is False.

    Returns
    -------
    bool
        True if the file was processed and saved successfully, False otherwise.
    """
    print(f"Processing: {filepath.name}")
    fig = None

    try:
        with rdm.open(filepath) as model:
            data = model.data
            ny, nx = data.shape
            wcs = model.meta.wcs

            # High contrast normalization for faint source detection
            norm = ImageNormalize(
                data, interval=ZScaleInterval(contrast=0.5), stretch=AsinhStretch()
            )

            fig, (ax1, ax2) = plt.subplots(
                1,
                2,
                figsize=(22, 10),
                subplot_kw={"projection": wcs},
                sharex=True,
                sharey=True,
            )

            # --- Axis Formatting ---
            for i, ax in enumerate([ax1, ax2]):
                lon_axis = ax.coords[0]
                lat_axis = ax.coords[1]

                lon_axis.set_format_unit(u.deg)
                lat_axis.set_format_unit(u.deg)

                # Force decimal formatting (d.dddd)
                lon_axis.set_major_formatter("d.dddd")
                lat_axis.set_major_formatter("d.dddd")

                ax.set_xlabel("Right Ascension", fontsize=12)
                if i == 0:
                    ax.set_ylabel("Declination", fontsize=12)
                else:
                    lat_axis.ticklabels.set_visible(False)

            # --- Left Pane: Original Data ---
            ax1.imshow(
                data, origin="lower", norm=norm, cmap="viridis", interpolation="nearest"
            )
            ax1.set_title("Original Image (No Markers)", fontsize=14, pad=15)

            # --- Right Pane: Data + Markers ---
            im = ax2.imshow(
                data, origin="lower", norm=norm, cmap="viridis", interpolation="nearest"
            )
            ax2.set_title("Source Comparison", fontsize=14, pad=15)

            # Layer 1: Global Catalog (Gaia Truth -> RED)
            if global_catalog is not None:
                coords = SkyCoord(
                    ra=global_catalog["ra"].values * u.deg,
                    dec=global_catalog["dec"].values * u.deg,
                    frame="icrs",
                )
                px, py = wcs.world_to_pixel(coords)
                mask = (px >= 0) & (px < nx) & (py >= 0) & (py < ny)
                if mask.any():
                    ax2.scatter(
                        global_catalog["ra"][mask],
                        global_catalog["dec"][mask],
                        transform=ax2.get_transform("icrs"),
                        marker="+",
                        color="red",
                        s=12,
                        linewidths=0.7,
                        alpha=0.6,
                        label=f"Global Gaia (n={mask.sum()})",
                    )

            # Layer 2: Local Centroid Catalog (Detected -> CYAN)
            if show_sources:
                suffix = (
                    "_coadd.asdf"
                    if filepath.name.endswith("_coadd.asdf")
                    else "_cal.asdf"
                )
                local_cat_path = filepath.parent / filepath.name.replace(
                    suffix, "_cat.parquet"
                )
                if local_cat_path.exists():
                    local_df = pd.read_parquet(local_cat_path)
                    lx, ly = (
                        local_df["x_centroid"].values,
                        local_df["y_centroid"].values,
                    )

                    ax2.scatter(
                        lx,
                        ly,
                        marker="x",
                        color="cyan",
                        s=25,
                        linewidths=0.9,
                        alpha=0.9,
                        label=f"Local Centroids (n={len(lx)})",
                    )
                    print(f"  -> Found local catalog: {local_cat_path.name}")

            # Formatting and Output
            ax2.legend(
                loc="upper right", frameon=True, facecolor="black", labelcolor="white"
            )
            cbar = fig.colorbar(im, ax=[ax1, ax2], fraction=0.046, pad=0.04)
            cbar.set_label("Flux (Asinh ZScale)", fontsize=12)
            fig.suptitle(
                f"File: {filepath.name}", fontsize=16, fontweight="bold", y=0.96
            )

            output_filename = filepath.with_name(f"{filepath.stem}_comparison.png")
            plt.savefig(output_filename, dpi=150, bbox_inches="tight")
            return True

    except Exception as e:
        print(f"Error processing {filepath.name}: {e}")
        return False
    finally:
        if fig:
            plt.close(fig)


def parse_log_file(log_filename):
    """
    Parse a pipeline log file for output file paths.

    Parameters
    ----------
    log_filename : str
        The path to the text-based log file.

    Returns
    -------
    list of str
        A list of extracted file paths found matching the 'output_file' pattern.
    """
    pattern = re.compile(r"output_file='([^']+)'")
    extracted_values = []
    log_path = Path(log_filename)
    if not log_path.exists():
        return extracted_values
    with log_path.open() as f:
        for line in f:
            match = pattern.search(line)
            if match:
                extracted_values.append(match.group(1))
    return extracted_values


def main():
    """
    CLI interface for Roman WFI file visualization.

    Handles argument parsing and identifies files from direct inputs,
    wildcard globs, association files, or log files.
    """
    parser = argparse.ArgumentParser(description="Visualize Roman WFI files.")
    parser.add_argument(
        "inputs", nargs="+", help="ASDF files, wildcards, or a .log file."
    )
    parser.add_argument(
        "--show-sources",
        action="store_true",
        help="Overlay local x_centroid/y_centroid sources.",
    )
    args = parser.parse_args()

    # Load truth catalog if available
    catalog_path = Path("romanisim_input_catalog.parquet")
    global_df = pd.read_parquet(catalog_path) if catalog_path.exists() else None

    files_to_process = []
    for item in args.inputs:
        if item.endswith(".log"):
            files_to_process.extend([Path(f) for f in parse_log_file(item)])
        elif "*" in item or "?" in item:
            files_to_process.extend([Path(f) for f in glob.glob(item)])
        elif item.endswith(".json"):
            with open(item) as jf:
                asn = load_asn(jf, format="json")
            for member in asn["products"][0]["members"]:
                files_to_process.append(Path(member["expname"]))
        else:
            files_to_process.append(Path(item))

    # Iterate through unique, sorted paths
    for filepath in sorted(set(files_to_process)):
        if filepath.suffix == ".asdf":
            plot_asdf_file(
                filepath, global_catalog=global_df, show_sources=args.show_sources
            )


if __name__ == "__main__":
    main()
