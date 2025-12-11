#!/bin/bash

# Set output directory
# output_dir="RCAL-1168"

# Set product types
# (using array to avoid issues with word splitting)
types=(visit pass full)

# Loop over product types
for base in "${types[@]}"; do

  # Loop over data releases
  for dr in "" "_DR"; do

    # Set target directory name
    # t="${base}${dr}"
    # # Create output directory
    # dir=$(echo "${output_dir}/${t}" | tr '[:lower:]' '[:upper:]')
    # mkdir -p "$dir"

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
