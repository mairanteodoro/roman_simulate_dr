#!/bin/bash
# Script to generate skycell association files for different product types and data releases.
#
# Usage: ./create_skycell_asn.sh filter1 [filter2 ...]
#
# For each product type (visit, pass, full) and for both standard and data-release modes,
# this script processes all files matching r*_"filter"_cal.asdf for each provided filter argument.
# It calls the 'skycell_asn' command with appropriate --product-type and --data-release-id options.
#
# Arguments:
#   FILTER   One or more filter names to process (e.g., f158, f146).
#
# Example:
#   ./create_skycell_asn.sh f158 f146

# Set product types
# (using array to avoid issues with word splitting)
types=(visit pass full NO_TYPE)

# Loop over product types
for base in "${types[@]}"; do

  # Loop over data releases
  for dr in "" "_DR"; do

    # Set arguments based on type and data release
    dr_arg=""
    [[ $dr == "_DR" ]] && dr_arg="--data-release-id r0"
    pt_arg=""
    [[ $base != "NO_TYPE" ]] && pt_arg="--product-type $base"

    # Loop over filters
    for x in "$@"; do
      echo "Processing all files for filter $x"
      skycell_asn r*_"${x}"_cal.asdf -o r00001 $pt_arg $dr_arg
    done

  done

done
