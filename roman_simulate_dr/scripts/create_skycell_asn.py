#!/usr/bin/env python3
"""Create skycell association files for Roman coadds by filter.

The script looks for `r*_<filter>_cal.asdf` files in the current directory and
invokes `skycell_asn` for each requested filter.
"""
import argparse
import glob
import subprocess
import sys


def main():
    """Parse CLI filters and execute `skycell_asn` for matching inputs."""
    parser = argparse.ArgumentParser(
        description="Generate skycell association files for Roman data releases."
    )
    parser.add_argument(
        "filters",
        type=str,
        nargs="+",
        help="One or more space-separated filter names to process (e.g., f158 f146).",
    )

    args = parser.parse_args()

    # Configuration mapped directly from the original script
    # types = ["visit", "pass", "full", "NO_TYPE"]
    types = ["full"]
    data_releases = ["", "_DR"]

    # Loop over product types
    for base in types:
        # Loop over data releases
        for dr in data_releases:
            # Set arguments based on type and data release
            cmd_args = []
            if dr == "_DR":
                cmd_args.extend(["--data-release-id", "r0"])
            if base != "NO_TYPE":
                cmd_args.extend(["--product-type", base])

            # Loop over filters passed from the command line
            for x in args.filters:
                # Find files matching the wildcard pattern r*"_filter"_cal.asdf
                pattern = f"r*_{x}_cal.asdf"
                matching_files = glob.glob(pattern)

                if not matching_files:
                    print(
                        f"Warning: No files found matching pattern: {pattern}"
                    )
                    continue

                print(f"Processing all files for filter {x}")

                # Build the complete skycell_asn command execution array
                # equivalent to: skycell_asn r*_"filter"_cal.asdf -o r00001 $pt_arg $dr_arg
                cmd = ["skycell_asn"] + matching_files + ["-o", "r00001"] + cmd_args

                try:
                    subprocess.run(cmd, check=True)
                except subprocess.CalledProcessError as e:
                    print(
                        f"Error executing skycell_asn for filter {x}: {e}",
                        file=sys.stderr,
                    )
                    sys.exit(1)


if __name__ == "__main__":
    main()
