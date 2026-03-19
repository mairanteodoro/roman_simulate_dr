from pathlib import Path

from utils.create_mosaic_with_grid import create_mosaic

# If you want to create a mosaic from the L2 files, you can use the following code:
my_l2_files = list(Path().glob("r*_f062_cal.asdf"))

create_mosaic(my_l2_files, output="l2_mosaic_f062", log_file="l2_mosaic_f062.log")


# To create a mosaic from the L3 coadd files, you can use the following code:
my_l3_files = list(Path().glob("r00001_r0_full*_f062_coadd.asdf"))

create_mosaic(my_l3_files, output="l3_mosaic_f062", log_file="l3_mosaic_f062.log")
