#!/bin/bash

# requires that PYTHONPATH and PATH contain the roman_photoz/scripts directory.

# stop on error
set -e

# filter list
filter_list="f062 f087 f106 f129 f146 f158 f184 f213"

# L1 SIMULATION STEPS (NOT NEEDED FOR DATA RELEASE)
# 1 - create romanisim input catalog
# (the fluxes will be scaled by the flux in the COSMOS-213 band)
# python -m generate_input_catalog \
#   --output-filename romanisim_input_catalog.parquet \
#   --filter-list ${filter_list} \
#   --flux-catalog roman_photoz_simulated_catalog_v2.parquet \
#   --radius 1.0 #\
#   &> dr_logs_generate_input_catalog.log &&

# 2 - generate L1 image files
# python -m generate_simulated_l1_images \
#   --obs-plan obs_plan.ecsv \
#   --input-filename romanisim_input_catalog.parquet \
#   --sca-ids 1 \
#   --max-workers 16 \
#   &> dr_logs_generate_simulated_l1_images.log &&

# PROCESSING STEPS FOR DATA RELEASE
# (L1 -> L3 + MULTIBAND CATALOGS + PHOTOZ)
# 1 - create association files for ELP and run roman_elp
# find *_uncal.asdf | xargs -I{} -P8 -n1 strun roman_elp {} \
#   &> dr_logs_elp.log &&

# 2 - create the association files for skycells
# ./create_skycell_asn.sh ${filter_list} \
#   &> dr_logs_create_skycells_asn.log &&

# 3 - create association files for MOS and run roman_mos
# find . -maxdepth 1 -type f -name 'r00001_*_*_*x*y*_asn.json' | \
#   xargs -I{} -P8 -n1 strun roman_mos {} \
#   &> dr_logs_mos.log &&

# 4 - create association files for multiband catalog
# multiband_asn *_coadd.asdf \
#   &> dr_logs_multiband_asn.log &&

# 5 - run MultibandCatalogStep
# find . -maxdepth 1 -type f -name "*.json" -not -name '*_f[0-9][0-9][0-9]_*' | \
#   xargs -I{} -P8 -n1 strun romancal.step.MultibandCatalogStep {} --snr_threshold 5 \
#   &> dr_logs_multiband_catalog_step.log #&&

# 6 - run roman_photoz
# # can't run in parallel because the files overwrite themselves.
find 270*_cat.parquet | xargs -I{} -P8 -n1 roman-photoz --input-filename {} \
  &> dr_logs_roman_photoz.log




# find RCAL-1168 -type f -name '*.json' | awk -F/ '{
#   dir = "";
#   for (i=1; i<NF; i++) dir = dir $i "/";
#   if (!seen[dir]++) print
# }'
# RCAL-1168/FULL       r00001_p_full_270p65x71y51_f062_asn.json
# RCAL-1168/PASS       r00001_p_p10010_270p65x70y49_f087_asn.json
# RCAL-1168/VISIT      r00001_p_v1001001001_270p65x69y49_f158_asn.json
#
# RCAL-1168/NO_TYPE    r00001_p_v1001001001_270p65x69y49_f158_asn.json
#
#
# RCAL-1168/FULL_DR    r00001_r0_full_270p65x70y48_f158_asn.json
# RCAL-1168/PASS_DR    r00001_r0_p10010_270p65x71y49_f213_asn.json
# RCAL-1168/VISIT_DR   r00001_r0_v1001001001_270p65x70y49_f213_asn.json
#
# RCAL-1168/NO_TYPE_DR r00001_r0_v1001001001_270p65x70y49_f213_asn.json
#
