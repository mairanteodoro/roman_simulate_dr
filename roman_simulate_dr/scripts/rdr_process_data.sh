#!/bin/bash
# Script to automate the Roman data release processing pipeline.
#
# Steps performed:
#   1. Runs roman_elp on all *_uncal.asdf files in parallel;
#   2. Generates skycell association files from calibrated products;
#   3. Runs roman_mos on all skycell association JSON files in parallel;
#   4. Creates multiband association files from *_coadd.asdf files;
#   5. Runs MultibandCatalogStep on all relevant JSON files.
#   6. Runs SourceCatalogStep via a standalone worker script for forced photometry.
#
# Output logs for each step are saved to dr_logs_*.log files.
#
# Usage: ./rdr_process_data.sh

# stop on error
set -e
set -o pipefail

# 1. Resolve target directory: Argument $1, then Env Var, then Fail
TARGET_DIR="${1:-$RDR_OUTPUT_PATH}"

if [ -z "$TARGET_DIR" ]; then
  echo "Error: No output path provided. Use: rdr-process-data /path/to/data"
  echo "Or set RDR_OUTPUT_PATH in your .env file."
  exit 1
fi

# Ensure the path exists and change to it
if [ ! -d "$TARGET_DIR" ]; then
  echo "Error: Directory does not exist: $TARGET_DIR"
  # create the folder and the subfolder for
  # the prompt and forced photometry outputs
  mkdir -p "$TARGET_DIR/FORCED" "$TARGET_DIR/PROMPT"
fi

cd "$TARGET_DIR"
echo "Processing Roman data in: $TARGET_DIR"

# --- Pipeline Steps ---

# 1 - roman_elp
find . -maxdepth 1 -name '*_uncal.asdf' | xargs -I{} -P4 -n1 strun roman_elp {} \
  2>&1 | tee dr_logs_elp.log


# --- FULL-LEVEL PROCESSING ---
# 2 - skycell association (full)
skycell_asn r*_cal.asdf -o r00001 --product-type full \
  2>&1 | tee dr_logs_create_skycells_asn.log
skycell_asn r*_cal.asdf -o r00001 --product-type full --data-release-id r0 \
  2>&1 | tee -a dr_logs_create_skycells_asn.log

# 3 - roman_mos
find . -maxdepth 1 -type f -name 'r00001_*_*_*x*y*_asn.json' |
  xargs -I{} -P4 -n1 strun roman_mos {} \
    2>&1 | tee dr_logs_mos.log

# 4 - multiband association
multiband_asn *_coadd.asdf \
  2>&1 | tee dr_logs_multiband_asn.log

# 5 - MultibandCatalogStep
find . -maxdepth 1 -type f -name "*r0_full*.json" -not -name '*_f[0-9][0-9][0-9]_*' |
  xargs -I{} -P4 -n1 strun romancal.step.MultibandCatalogStep {} \
    2>&1 | tee dr_logs_multiband_catalog_step.log


# --- PASS-LEVEL PROCESSING ---
# 2 - skycell association (pass)
skycell_asn r*_cal.asdf -o r00001 --product-type pass \
  2>&1 | tee dr_logs_create_skycells_asn_pass.log
skycell_asn r*_cal.asdf -o r00001 --product-type pass --data-release-id r0 \
  2>&1 | tee -a dr_logs_create_skycells_asn_pass.log

# 3 - build pass-level coadds
find . -maxdepth 1 -type f -name 'r*_r0_p*_*_f[0-9][0-9][0-9]_asn.json' \
  | xargs -I{} -P4 -n1 uv run strun roman_mos {} \
  2>&1 | tee dr_logs_mos_pass.log

# 4 - forced photometry on pass-level coadds
rdr-run-forced-photometry 'r*_r0_p*_*_f[0-9][0-9][0-9]_coadd.asdf' \
  2>&1 | tee dr_logs_forced_photometry.log

