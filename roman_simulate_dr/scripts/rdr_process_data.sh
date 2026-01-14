#!/bin/bash
# Script to automate the Roman data release processing pipeline.
#
# Steps performed:
#   1. Runs roman_elp on all *_uncal.asdf files in parallel;
#   2. Generates skycell association files for a predefined filter list using create_skycell_asn.sh;
#   3. Runs roman_mos on all skycell association JSON files in parallel;
#   4. Creates multiband association files from *_coadd.asdf files;
#   5. Runs MultibandCatalogStep on all relevant JSON files.
#
# Output logs for each step are saved to dr_logs_*.log files.
#
# Usage: ./rdr_process_data.sh

# stop on error
set -e

# filter list
filter_list="f062 f087 f106 f129 f146 f158 f184 f213"

# 1 - create association files for ELP and run roman_elp
find *_uncal.asdf | xargs -I{} -P8 -n1 strun roman_elp {} \
  &> dr_logs_elp.log &&

# 2 - create the association files for skycells
./create_skycell_asn.sh ${filter_list} \
  &> dr_logs_create_skycells_asn.log &&

# 3 - create association files for MOS and run roman_mos
find . -maxdepth 1 -type f -name 'r00001_*_*_*x*y*_asn.json' | \
  xargs -I{} -P8 -n1 strun roman_mos {} \
  &> dr_logs_mos.log &&

# 4 - create association files for multiband catalog
multiband_asn *_coadd.asdf \
  &> dr_logs_multiband_asn.log &&

# 5 - run MultibandCatalogStep
find . -maxdepth 1 -type f -name "*.json" -not -name '*_f[0-9][0-9][0-9]_*' | \
  xargs -I{} -P8 -n1 strun romancal.step.MultibandCatalogStep {} --snr_threshold 5 \
  &> dr_logs_multiband_catalog_step.log &&

# 6 - add mock unique catalog source identifier to each multiband catalog
# (find all multiband catalog parquet files and add a unique source_id column using astropy Table.
# The length of the unique identifier should be 64 bits with signed integer type. Do not use pandas.)
# find . -maxdepth 1 -type f -name "*_cat.parquet" | \
#   xargs -I{} -P8 -n1 python -c "
# import sys
# from astropy.table import Table
# import numpy as np
# # Load the existing catalog
# cat = Table.read('{}', format='parquet')
# # Generate a unique source_id for each entry with 64-bit signed integers
# cat['source_id'] = np.arange(1, len(cat) + 1, dtype=np.int64)
# # Save the updated catalog back to parquet
# cat.write('{}', format='parquet', overwrite=True)
# " \
#   &> dr_logs_add_source_id.log
