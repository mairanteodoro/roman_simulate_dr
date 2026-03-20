# /// script
# dependencies = [
#    "pandas",
#    "pyarrow",
#    "matplotlib",
#    "numpy",
# ]
# ///

"""
Roman Space Telescope Photometry and SED Visualization Tool.

This module provides utilities to extract multi-band photometry from Parquet
catalogs and generate Spectral Energy Distribution (SED) plots. It specifically
targets the Nancy Grace Roman Space Telescope WFI filter set (F062 through F213).

The tool includes automatic flux scaling to maintain readable y-axis units and
overlays photometric redshift (photo-z) metadata, including confidence
intervals and best-fit templates.
"""

import argparse

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def plot_source_photometry(
    df, source_label, flux_type="psf", auto_scale=True, save_filename=None
):
    """
    Extract and plot the multi-band photometry for a specific source.

    Parameters
    ----------
    df : pandas.DataFrame
        The input catalog containing photometry and photo-z columns.
        Expected columns follow the pattern `{flux_type}_{filter}_flux`.
    source_label : int or str
        The unique identifier for the source in the 'label' column.
    flux_type : str, optional
        The type of photometry to plot (e.g., 'psf', 'kron', 'aper01').
        Default is 'psf'.
    auto_scale : bool, optional
        Whether to automatically scale the y-axis flux values by powers
        of 10 for better readability. Default is True.
    save_filename : str, optional
        Path to save the resulting plot. If None, the plot is only
        displayed. Default is None.

    Notes
    -----
    The function assumes the existence of several photo-z metadata columns
    to populate the info box: 'photoz', 'photoz_sed', 'photoz_low68',
    'photoz_high68', etc.
    """
    # 1. Filter for the specific source
    source = df[df["label"] == source_label]
    if source.empty:
        print(f"Error: Source label '{source_label}' not found.")
        return
    source = source.iloc[0]

    # 2. Filter definitions (Roman Space Telescope filters in microns)
    filters = {
        "f062": 0.620,
        "f087": 0.869,
        "f106": 1.060,
        "f129": 1.293,
        "f146": 1.464,
        "f158": 1.577,
        "f184": 1.842,
        "f213": 2.125,
    }

    wavelengths, raw_fluxes, raw_errors, filter_labels = [], [], [], []

    # 3. Extract data
    for f_name, wave in filters.items():
        f_col = f"{flux_type}_{f_name}_flux"
        e_col = f"{flux_type}_{f_name}_flux_err"

        if f_col in df.columns:
            wavelengths.append(wave)
            raw_fluxes.append(source[f_col])
            raw_errors.append(source[e_col])
            filter_labels.append(f_name.upper())

    if not raw_fluxes:
        print(f"Error: No columns found for flux type '{flux_type}'.")
        return

    # 4. Auto-Scaling Logic
    scale_factor = 1.0
    units_label = "Flux [Arbitrary Units]"
    if auto_scale:
        # Use the maximum absolute flux to determine the scale
        max_f = np.nanmax(np.abs(raw_fluxes))
        if max_f > 0 and not np.isnan(max_f):
            exponent = int(np.floor(np.log10(max_f)))
            scale_factor = 10 ** (-exponent)
            units_label = f"Flux [$\\times 10^{{{exponent}}}$ units]"

    flux_vals = np.array(raw_fluxes) * scale_factor
    err_vals = np.array(raw_errors) * scale_factor

    # 5. Plotting
    plt.figure(figsize=(10, 6))
    plt.errorbar(
        wavelengths,
        flux_vals,
        yerr=err_vals,
        # Dark slate for points, soft red for error bars
        fmt="o-",
        capsize=4,
        markersize=8,
        color="#2c3e50",
        ecolor="#e74c3c",
        linewidth=1.5,
        label=f"{flux_type.upper()} Photometry",
    )

    plt.xticks(wavelengths, filter_labels)
    plt.xlabel("Wavelength ($\\mu m$)", fontsize=12)
    plt.ylabel(units_label, fontsize=12)
    plt.title(
        f"Photometry for Source Label: {source_label}", fontsize=14, fontweight="bold"
    )
    plt.grid(True, linestyle=":", alpha=0.6)

    # 6. Photo-z Metadata Text Box (FIXED COLUMN NAMES)
    p_z = source.get("photoz", np.nan)
    sed = source.get("photoz_sed", "N/A")

    photoz_info = (
        f"Source ID: {source_label}\n"
        f"Photo-$z$: {p_z:.3f}\n"
        f"68% CI: [{source.get('photoz_low68', 0):.2f}, {source.get('photoz_high68', 0):.2f}]\n"
        f"90% CI: [{source.get('photoz_low90', 0):.2f}, {source.get('photoz_high90', 0):.2f}]\n"
        f"Template: {sed}"
    )

    plt.gca().text(
        0.03,
        0.97,
        photoz_info,
        transform=plt.gca().transAxes,
        fontsize=10,
        verticalalignment="top",
        family="monospace",
        bbox=dict(boxstyle="round", facecolor="white", alpha=0.9, edgecolor="gray"),
    )

    plt.legend(loc="lower right")
    plt.tight_layout()

    if save_filename:
        plt.savefig(save_filename, dpi=300, bbox_inches="tight")
        print(f"Saved: {save_filename}")

    plt.show()


def main():
    """
    Command-line interface for generating source SED plots.
    """
    parser = argparse.ArgumentParser(description="Plot SED from Parquet.")
    parser.add_argument("file", help="Catalog file (.parquet)")
    parser.add_argument("label", type=int, help="Source ID")
    parser.add_argument("--type", default="psf", help="Flux type (psf, kron, aper01)")
    parser.add_argument("--save", help="Filename to save plot")

    args = parser.parse_args()
    try:
        df = pd.read_parquet(args.file)
        plot_source_photometry(
            df, args.label, flux_type=args.type, save_filename=args.save
        )
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
