import argparse
import glob
import warnings

import astropy.units as u
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from astropy.coordinates import SkyCoord
from astropy.table import Table, vstack
from astropy.utils.metadata import MergeConflictWarning

warnings.filterwarnings("ignore", category=MergeConflictWarning)


def label_outlier_regions(z_true, z_phot):
    regions = np.zeros(len(z_true), dtype=int)
    regions[np.abs(z_phot - z_true) < 0.1] = 0
    # Outlier Regions 1-6
    regions[(z_phot > 5.0) & (z_true < 2.5)] = 1
    regions[(z_phot > 3.6) & (z_phot < 5.0) & (z_true < 3.0)] = 2
    regions[(z_phot > 1.9) & (z_phot < 2.3) & (z_true < 1.5)] = 3
    regions[(z_phot > 4.2) & (z_phot < 4.7) & (z_true > 5.5)] = 4
    regions[(z_phot > 1.8) & (z_phot < 2.2) & (z_true > 3.0)] = 5
    regions[(z_phot < 1.7) & (z_true > 3.0)] = 6
    return regions


def plot_outlier_diagnosis(df, flux_type="segment", save_filename="sed_diagnosis.png"):
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

    region_list = [0, 1, 2, 3, 4, 5, 6]
    fig, axes = plt.subplots(7, 2, figsize=(16, 32))
    plt.subplots_adjust(hspace=0.6, wspace=0.2)

    for idx, region_id in enumerate(region_list):
        region_subset = df[df["outlier_region"] == region_id]
        samples = region_subset.head(2)

        for col_idx in range(2):
            ax = axes[idx, col_idx]
            if col_idx >= len(samples):
                ax.axis("off")
                ax.text(0.5, 0.5, f"Region {region_id}\nNo sources found", ha="center")
                continue

            source = samples.iloc[col_idx]
            wavelengths, fluxes, errors = [], [], []

            for f_name, wave in filters.items():
                f_col = f"{flux_type}_{f_name}_flux"
                e_col = f"{flux_type}_{f_name}_flux_err"

                if f_col in df.columns:
                    wavelengths.append(wave)
                    fluxes.append(source[f_col])
                    errors.append(source.get(e_col, 0))

            if not fluxes:
                continue

            # Scaling and Stats
            flux_arr, err_arr = np.array(fluxes), np.array(errors)
            snr = np.nanmax(flux_arr / err_arr) if np.any(err_arr > 0) else 0
            max_f = np.nanmax(np.abs(flux_arr))
            exponent = int(np.floor(np.log10(max_f))) if max_f > 0 else 0
            scale = 10 ** (-exponent)

            sed_template = source.get("photoz_sed", "N/A")

            ax.errorbar(
                wavelengths,
                flux_arr * scale,
                yerr=err_arr * scale,
                fmt="o-",
                capsize=4,
                color="#2c3e50",
                ecolor="#e74c3c",
                label=f"Flux: {flux_type}\nTemplate: {sed_template}",
            )

            # Physics Indicators (Lyman and Balmer breaks)
            z_t = source["matched_redshift_true"]
            ax.axvline(
                0.1216 * (1 + z_t),
                color="blue",
                ls="--",
                alpha=0.6,
                label="Lyman Break",
            )
            ax.axvline(
                0.4000 * (1 + z_t),
                color="green",
                ls="-.",
                alpha=0.6,
                label="Balmer Break",
            )

            ax.set_xticks(list(filters.values()))
            ax.set_xticklabels([f.upper() for f in filters.keys()], fontsize=8)

            title_text = (
                "SUCCESS (1:1)" if region_id == 0 else f"OUTLIER Region {region_id}"
            )
            ax.set_title(
                f"{title_text}\nTrue z: {z_t:.2f}", fontsize=11, fontweight="bold"
            )
            ax.set_ylabel(f"Flux [$\\times 10^{{{exponent}}}$]")
            ax.grid(True, linestyle=":", alpha=0.6)

            ax.text(
                0.05,
                0.95,
                f"ID: {int(source['matched_label'])}\nPhoto-z: {source['photoz']:.2f}\nSNR: {snr:.1f}\nχ²: {source['photoz_gof']:.2f}",
                transform=ax.transAxes,
                verticalalignment="top",
                bbox=dict(boxstyle="round", facecolor="white", alpha=0.9),
                fontsize=9,
            )

            ax.legend(loc="lower right", fontsize=8)

    plt.tight_layout()
    plt.savefig(save_filename, dpi=200)
    print(f"Saved: {save_filename}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--flux", default="segment", help="Photometry type (segment, psf, kron)"
    )
    parser.add_argument(
        "--ab-cutoff",
        type=float,
        default=26.0,
        help="AB magnitude limit (fainter ignored)",
    )
    args = parser.parse_args()

    # Load and stack catalogs
    cat_files = sorted(glob.glob("r00001_r0_full_270p65x71y51_cat.parquet"))
    all_targets = vstack(
        [Table.read(f, format="parquet") for f in cat_files],
        metadata_conflicts="silent",
    )

    # Matching with truth catalog
    truth = Table.read("romanisim_input_catalog.parquet", format="parquet")
    valid_truth = truth[~np.isnan(truth["ra"]) & (truth["type"] != "PSF")]

    t_coords = SkyCoord(ra=all_targets["ra"], dec=all_targets["dec"])
    c_coords = SkyCoord(ra=valid_truth["ra"] * u.deg, dec=valid_truth["dec"] * u.deg)
    idx, sep, _ = t_coords.match_to_catalog_sky(c_coords)

    all_targets["matched_label"] = valid_truth["label"][idx]
    all_targets["matched_redshift_true"] = valid_truth["redshift_true"][idx]

    matched_df = all_targets[sep.arcsec <= 0.1].to_pandas()

    # AB Magnitude Filter (assuming nanomaggies; ZP = 22.5)
    # nanomaggies to AB: mag = 22.5 - 2.5*log10(flux)
    zp = 22.5
    flux_limit = 10 ** ((zp - args.ab_cutoff) / 2.5)

    # Filter based on the chosen flux type across any band
    f_cols = [
        c
        for c in matched_df.columns
        if args.flux in c and "flux" in c and "err" not in c
    ]
    matched_df = matched_df[matched_df[f_cols].max(axis=1) > flux_limit]

    print(f"Post-filter source count: {len(matched_df)} (Limit: {args.ab_cutoff} AB)")

    matched_df["outlier_region"] = label_outlier_regions(
        matched_df["matched_redshift_true"], matched_df["photoz"]
    )
    plot_outlier_diagnosis(matched_df, flux_type=args.flux)


if __name__ == "__main__":
    main()
