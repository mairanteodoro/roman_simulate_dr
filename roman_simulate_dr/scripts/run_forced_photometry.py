"""Run forced photometry on L3 pass/subset coadds using full-stack multiband segmentation."""
import argparse
import glob
import re
import subprocess
import sys
from pathlib import Path

L3_COADD_PATTERN = re.compile(
    r"^(?P<proposal>r\d{5})_"
    r"(?P<product_id>[^_]+)_"
    r"(?P<grouping>[^_]+)_"
    r"(?P<skycell>[^_]+)_"
    r"(?P<optical_element>[^_]+)_coadd\.asdf$"
)


def _expand_inputs(inputs: list[str]) -> list[Path]:
    """Expand input files/globs while preserving order and removing duplicates."""
    expanded: list[Path] = []
    for input_item in inputs:
        if any(char in input_item for char in "*?["):
            expanded.extend(Path(path) for path in sorted(glob.glob(input_item)))
        else:
            expanded.append(Path(input_item))

    unique_files: list[Path] = []
    seen: set[Path] = set()
    for path in expanded:
        if path not in seen:
            seen.add(path)
            unique_files.append(path)
    return unique_files


def _multiband_segm_path_from_subset_coadd(coadd_path: Path) -> Path:
    """Derive the full-stack multiband segmentation filename from an L3 pass/subset coadd."""
    match = L3_COADD_PATTERN.match(coadd_path.name)
    if not match:
        msg = (
            "Expected an L3 coadd filename with pattern "
            "'r<ppppp>_<l3-product-id>_<grouping>_<skycell>_<optical>_coadd.asdf', "
            f"got: {coadd_path.name}"
        )
        raise ValueError(msg)

    fields = match.groupdict()
    if not fields["product_id"].startswith("r"):
        msg = (
            "Expected a data-release L3 product identifier when deriving multiband "
            f"segmentation, got: {fields['product_id']}"
        )
        raise ValueError(msg)
    if fields["grouping"] == "full":
        msg = f"expected a pass/subset-level coadd filename, got: {coadd_path.name}"
        raise ValueError(msg)
    if not fields["grouping"].startswith(("s", "p")):
        msg = (
            "Expected an L3 pass/subset grouping token (starts with 's' or 'p') "
            "in filename, "
            f"got grouping: {fields['grouping']}"
        )
        raise ValueError(msg)

    segm_name = (
        f"{fields['proposal']}_{fields['product_id']}_full_{fields['skycell']}_segm.asdf"
    )
    return coadd_path.with_name(segm_name)


def main():
    """Parse arguments and execute SourceCatalogStep forced photometry runs."""
    parser = argparse.ArgumentParser(
        description=(
            "Run SourceCatalogStep forced photometry on L3 pass/subset coadds "
            "using multiband segmentation files."
        )
    )
    parser.add_argument(
        "coadd_inputs",
        type=str,
        nargs="+",
        help=(
            "L3 pass/subset coadd files or glob patterns "
            "(e.g., 'r00001_r1_p02002_10m6x2y50_f146_coadd.asdf')."
        ),
    )

    args = parser.parse_args()
    coadd_files = _expand_inputs(args.coadd_inputs)
    if not coadd_files:
        print(
            f"Error: no files matched the provided inputs: {' '.join(args.coadd_inputs)}",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Processing {len(coadd_files)} L3 pass/subset coadd file(s).")
    print("-" * 40)

    for coadd_path in coadd_files:
        if not coadd_path.exists():
            print(f"Error: Coadd file not found: {coadd_path}", file=sys.stderr)
            sys.exit(1)

        try:
            segm_path = _multiband_segm_path_from_subset_coadd(coadd_path)
        except ValueError as err:
            print(f"Error: {err}", file=sys.stderr)
            sys.exit(1)

        if not segm_path.exists():
            print(
                f"Error: Multiband segmentation file not found: {segm_path}",
                file=sys.stderr,
            )
            sys.exit(1)

        output_dir = coadd_path.parent / "FORCED"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = coadd_path.name.replace("_coadd.asdf", "_cat.parquet")

        print(f" -> Coadd:        {coadd_path}")
        print(f"    Segmentation: {segm_path}")
        print(f"    Output target:{output_dir / output_file}")

        cmd = [
            "strun",
            "romancal.step.SourceCatalogStep",
            str(coadd_path),
            "--forced_segmentation",
            str(segm_path),
            "--output_dir",
            str(output_dir),
            "--output_file",
            output_file,
        ]

        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as err:
            print(
                f"Error running forced photometry for {coadd_path.name}: {err}",
                file=sys.stderr,
            )
            sys.exit(1)

        print("    Done.")
        print("-" * 20)


if __name__ == "__main__":
    main()
