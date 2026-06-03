"""Run forced photometry catalogs from a segmentation map across filters."""
import argparse
import subprocess
import sys
from pathlib import Path


def main():
    """Parse arguments and execute SourceCatalogStep forced photometry runs."""
    parser = argparse.ArgumentParser(
        description="Run SourceCatalogStep forced photometry on a segmentation file across multiple bands."
    )
    parser.add_argument(
        "segm_file", type=str, help="Path to the target *_segm.asdf file."
    )
    parser.add_argument(
        "filters",
        type=str,
        nargs="*",
        help="Optional list of specific filters to process (e.g., f062 f146).",
    )

    args = parser.parse_args()
    segm_path = Path(args.segm_file)

    if not segm_path.exists():
        print(f"Error: Segmentation file not found: {segm_path}", file=sys.stderr)
        sys.exit(1)

    input_dir = segm_path.parent
    output_dir = input_dir / "FORCED"

    # Extract base filename (e.g., r00001_r0_full_270p65x67y51)
    base_filename = segm_path.name.replace("_segm.asdf", "")

    # Use all filters if none are provided
    filter_list = (
        args.filters
        if args.filters
        else ["f062", "f087", "f106", "f129", "f146", "f158", "f184", "f213"]
    )

    print(f"Processing: {segm_path.name}")
    print(f"Filters to run: {' '.join(filter_list)}")
    print("-" * 40)

    for band in filter_list:
        coadd_file = input_dir / f"{base_filename}_{band}_coadd.asdf"
        output_file = f"{base_filename}_{band}_cat.parquet"

        print(f" -> Band: {band}")
        print(f"    Looking for coadd: {coadd_file}")
        print(f"    Output target:     {output_dir / output_file}")

        cmd = [
            "strun",
            "romancal.step.SourceCatalogStep",
            str(coadd_file),
            "--forced_segmentation",
            str(segm_path),
            "--output_dir",
            str(output_dir),
            "--output_file",
            output_file,
        ]

        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as e:
            print(
                f"Error processing band {band} for {segm_path.name}: {e}",
                file=sys.stderr,
            )
            sys.exit(1)

        print("    Done.")
        print("-" * 20)


if __name__ == "__main__":
    main()
