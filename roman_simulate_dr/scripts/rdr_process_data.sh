#!/bin/bash
# Script to automate the Roman data release processing pipeline.
#
# Steps performed:
#   1. Runs roman_elp on all *_uncal.asdf files in parallel;
#   2. Generates skycell association files for a predefined filter list using create_skycell_asn.sh;
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

filter_list=(f062 f087 f106 f129 f146 f158 f184 f213)

# 1 - roman_elp
find . -maxdepth 1 -name '*_uncal.asdf' | xargs -I{} -P4 -n1 strun roman_elp {} \
  2>&1 | tee dr_logs_elp.log

# 2 - skycell association
rdr-create-skycell-asn "${filter_list[@]}" \
  2>&1 | tee dr_logs_create_skycells_asn.log

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

# 6 - forced photometry
find . -maxdepth 1 -type f -name "*r0_full*_segm.asdf" -not -name '*_f[0-9][0-9][0-9]_segm.asdf' | \
  xargs -I{} -P4 -n1 rdr-run-forced-photometry {} \
    2>&1 | tee dr_logs_forced_photometry.log
