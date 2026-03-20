import logging
import os

import matplotlib.pyplot as plt
import numpy as np
import psutil
import roman_datamodels as rdm
from astropy.visualization import ImageNormalize, LogStretch, ZScaleInterval
from reproject import reproject_interp
from reproject.mosaicking import find_optimal_celestial_wcs, reproject_and_coadd


def setup_logging(log_file=None):
    """Configures logging to both console and optionally a file."""
    handlers = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
    )


logger = logging.getLogger(__name__)


def log_memory(stage=""):
    """Log current RSS memory usage of the process."""
    process = psutil.Process(os.getpid())
    mem_mb = process.memory_info().rss / 1024 / 1024
    logger.info(f"Memory Usage {stage}: {mem_mb:.2f} MB")


def print_usage_help():
    """Prints a formatted guide on how to run this module from the terminal."""

    help_text = """
    Roman Mosaic Utility - Usage Guide
    ---------------------------------------
    You can run this script in two ways:

    1. Direct Path (Recommended for local dev):
       uv run python utils/create_mosaic_with_grid.py <files> [options]

    2. Module Syntax:
       uv run python -m utils.create_mosaic_with_grid <files> [options]

    Common Examples:
    ----------------
    # Basic L2 Mosaic (F062 Filter)
    uv run python utils/create_mosaic_with_grid.py r00001_*_f062_cal.asdf --output l2_mosaic_f062

    # Dry Run (Verify shell expansion & memory)
    uv run python utils/create_mosaic_with_grid.py *.asdf --dry-run

    # L3 Mosaic with logging
    uv run python utils/create_mosaic_with_grid.py *_coadd.asdf --l3 --log run.log

    Flags:
    ------
    --output    Base name for the .png file (default: mosaic_output)
    --log       Save memory & process logs to a file
    --dry-run   List files and estimate memory without processing
    """
    print(help_text)


def save_mosaic_plot(mosaic_array, target_wcs, output_path, title, cmap="viridis"):
    """
    Internal helper to generate a PNG with a decimal degree coordinate grid.
    """
    logger.info(f"Creating visualization: {output_path}")

    # Prepare the colormap to handle NaNs as transparent
    current_cmap = plt.get_cmap(cmap).copy()
    current_cmap.set_bad(alpha=0)  # This makes NaNs 100% transparent

    norm = ImageNormalize(mosaic_array, interval=ZScaleInterval(), stretch=LogStretch())

    fig = plt.figure(figsize=(12, 10))

    # Set the figure background to transparent as well
    fig.patch.set_alpha(0)

    ax = fig.add_subplot(1, 1, 1, projection=target_wcs)
    im = ax.imshow(mosaic_array, origin="lower", cmap=current_cmap, norm=norm)

    ax.coords.grid(color="black", alpha=0.3, linestyle="solid")
    ra, dec = ax.coords[0], ax.coords[1]
    ra.set_format_unit("deg")
    dec.set_format_unit("deg")

    ra.set_axislabel("Right Ascension", fontsize=12)
    dec.set_axislabel("Declination", fontsize=12)

    plt.title(title, pad=20)
    plt.colorbar(im, ax=ax, label="Intensity", fraction=0.046, pad=0.04)
    plt.savefig(output_path, bbox_inches="tight", dpi=300, transparent=True)
    plt.close()


def create_mosaic(files, output="mosaic_output", dry_run=False):
    """
    Core logic to coadd Roman ASDF files with an optional dry-run mode.

    Parameters
    ----------
    files : list of str
        List of paths to ASDF files (expanded by shell).
    output : str
        Base name for output files.
    dry_run : bool
        If True, lists files and calculates expected memory without processing.
    """

    # FALLBACK: If no logging handlers exist, set up a basic one
    # so the user actually sees the INFO messages.
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%H:%M:%S",
        )

    if not files:
        logger.warning("No files provided for processing.")
        return

    logger.info(f"--- {'DRY RUN' if dry_run else 'STARTING MOSAIC'} ---")
    logger.info(f"Files identified by shell: {len(files)}")

    for i, f in enumerate(sorted(files)):
        if dry_run:
            # In dry run, we just check existence and print names
            exists = "EXISTS" if os.path.exists(f) else "MISSING"
            logger.info(f"  [{i + 1:03d}] {f} ({exists})")

    if dry_run:
        # Estimate memory: roughly 32MB per WFI detector at float64
        est_mem = len(files) * 32
        logger.info(f"Estimated raw data volume: ~{est_mem} MB")
        logger.info("Dry run complete. No files were processed.")
        return

    log_memory("Initial")
    data_info = []
    wcs_info = []

    logger.info(f"Loading {len(files)} files...")
    for f in sorted(files):
        try:
            with rdm.open(f) as model:
                if model.meta.wcs is None:
                    continue

                data_arr = np.asanyarray(
                    model.data.value if hasattr(model.data, "value") else model.data
                )

                wcs_info.append((data_arr.shape, model.meta.wcs))
                data_info.append((data_arr, model.meta.wcs))
        except Exception as e:
            logger.error(f"Failed to load {f}: {e}")

    log_memory("After Data Load")

    if not data_info:
        logger.error("No valid data/WCS found.")
        return

    logger.info("Reprojecting and coadding...")
    target_wcs, target_shape = find_optimal_celestial_wcs(wcs_info)
    mosaic_array, _ = reproject_and_coadd(
        data_info,
        target_wcs,
        shape_out=target_shape,
        reproject_function=reproject_interp,
        combine_function="mean",
    )

    log_memory("After Coaddition")

    # Set zero values to NaN for better visualization (they will be transparent)
    mosaic_array[mosaic_array == 0] = np.nan

    # Save PNG
    png_path = f"{output}.png"
    cmap = "viridis"
    save_mosaic_plot(
        mosaic_array,
        target_wcs,
        png_path,
        f"Roman Mosaic ({len(files)} files)",
        cmap=cmap,
    )

    # Cleanup memory before finishing
    del data_info
    del wcs_info
    log_memory("Final Cleanup")


def main():
    import argparse
    import sys

    # If no arguments provided, show the custom help and exit
    if len(sys.argv) == 1:
        print_usage_help()
        sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Create Roman Mosaics with memory logging."
    )
    parser.add_argument("files", nargs="+", help="Input ASDF files.")
    parser.add_argument("--output", default="mosaic_output", help="Output base name.")
    parser.add_argument(
        "--log",
        type=str,
        default="mosaic_utils.log",
        help="Path to a log file (e.g., pipeline.log)",
    )
    parser.add_argument("--dry-run", action="store_true", help="List files and exit.")

    args = parser.parse_args()

    # Initialize logging with the optional file path
    setup_logging(args.log)

    create_mosaic(args.files, output=args.output, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
