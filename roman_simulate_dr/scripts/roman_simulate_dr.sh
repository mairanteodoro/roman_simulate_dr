#!/bin/bash

# requires that PYTHONPATH and PATH contain the roman_photoz/scripts directory.

# stop on error
set -e

# filter list
filter_list="f062 f087 f106 f129 f146 f158 f184 f213"

# 1 - create romanisim input catalog
# (the fluxes will be scaled by the flux in the COSMOS-213 band)
python -m generate_input_catalog \
  --output-filename romanisim_input_catalog.parquet \
  &> dr_logs_generate_input_catalog.log &&

# 2 - generate L1 image files
python -m generate_simulated_l1_images \
  --obs-plan obs_plan.ecsv \
  --input-filename romanisim_input_catalog.parquet \
  --max-workers 16 \
  &> dr_logs_generate_simulated_l1_images.log &&

# 2 - create association files for ELP and run roman_elp
find *_uncal.asdf | xargs -I{} -P8 -n1 strun roman_elp {} \
  &> dr_logs_elp.log &&

# 3 - create the association files for skycells
./create_skycell_asn.sh ${filter_list} \
  &> dr_logs_create_skycells_asn.log &&

# 4 - create association files for MOS and run roman_mos
find . -maxdepth 1 -type f -name 'r00001_*_*_*x*y*_asn.json' | \
  xargs -I{} -P8 -n1 strun roman_mos {} \
  &> dr_logs_mos.log &&

# 5 - create association files for multiband catalog
multiband_asn *_coadd.asdf \
  &> dr_logs_multiband_asn.log &&

# 6 - run MultibandCatalogStep
find . -maxdepth 1 -type f -name "*.json" -not -name '*_f[0-9][0-9][0-9]_*' | \
  xargs -I{} -P8 -n1 strun romancal.step.MultibandCatalogStep {} --snr_threshold 5 \
  &> dr_logs_multiband_catalog_step.log &&

# 7 - run roman_photoz
# # can't run in parallel because the files overwrite themselves.
find 270*_cat.parquet | xargs -I{} -P8 -n1 roman-photoz --input-filename {} \
  &> dr_logs_roman_photoz.log




