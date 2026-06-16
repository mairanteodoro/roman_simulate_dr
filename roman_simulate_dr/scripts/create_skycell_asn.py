"""Create skycell association files from Roman calibrated products.

This runs one pass without ``--data-release-id`` and
one pass with ``--data-release-id r0`` for each
requested filter. Product type (e.g. full, pass) can
be specified with the ``--product-type`` argument.
"""

import argparse
import glob
import subprocess
import sys


def main():
    """Parse CLI inputs and execute `skycell_asn`."""
    parser = argparse.ArgumentParser(
        description=(
            "Generate skycell association files from calibrated Roman products."
        )
    )

    parser.add_argument(
        "-o",
        "--output-root",
        default="r00001",
        help="Output root passed to skycell_asn (default: r00001).",
    )
    parser.add_argument(
        "--product-type",
        default="full",
        help="Product type passed to skycell_asn (default: full).",
    )

    args = parser.parse_args()

    for data_release_id in (None, "r0"):
        matching_files = sorted(glob.glob("r*_cal.asdf"))
        cmd = [
            "skycell_asn",
            *matching_files,
            "-o",
            args.output_root,
            "--product-type",
            args.product_type,
        ]
        if data_release_id is not None:
            cmd.extend(["--data-release-id", data_release_id])

        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as err:
            print(f"Error executing skycell_asn: {err}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
