#!/bin/bash
# Script to simulate Roman L1 data products for a predefined filter list.
#
# Steps performed:
#   1. Generates an input catalog for romanisim using specified filters and a flux catalog.
#      Output is saved to romanisim_input_catalog.parquet and logs to dr_logs_generate_input_catalog.log.
#   2. Simulates L1 image files using the generated input catalog and observation plan.
#      Output logs are saved to dr_logs_generate_simulated_l1_images.log.
#
# Usage: ./rdr_simulate_data.sh

# stop on error
set -e

# filter list
filter_list="f062 f087 f106 f129 f146 f158 f184 f213"

# L1 SIMULATION STEPS (NOT NEEDED FOR DATA RELEASE)
# 1 - create romanisim input catalog
# (the fluxes will be scaled by the flux in the COSMOS-213 band)
python -m generate_input_catalog \
  --output-filename romanisim_input_catalog.parquet \
  --filter-list ${filter_list} \
  --flux-catalog roman_photoz_simulated_catalog_v2.parquet \
  --radius 0.3
&>dr_logs_generate_input_catalog.log

# 2 - generate L1 image files
python -m generate_simulated_l1_images \
  --obs-plan obs_plan.ecsv \
  --input-filename romanisim_input_catalog.parquet \
  --sca-ids 1 2 10 11 \
  --max-workers 4 \
  &>dr_logs_generate_simulated_l1_images.log
