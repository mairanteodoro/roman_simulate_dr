# roman_simulate_dr

Roman Data Release
This repository contains tools for the data release process for the Roman Space Telescope.

# Installation

Clone this repository:

```shell
git clone git@github.com:mairanteodoro/roman_simulate_dr.git
```

Install the tools locally:

```shell
pip install .
```

Generate the input catalog for romanisim containing COSMOS galaxies,
Gaia stars, and additional random stellar sources:

```shell
rdr_generate_input_catalog \
  --obs-plan observation_plan.ecsv \
  --output-filename romanisim_input_catalog.ecsv
```

# Observation Plan

The observation plan file (`observation_plan.ecsv` in the example above) should be in ECSV format and
contain the following columns:

```
- RA (float64)
- DEC (float64)
- PA (float64)
- BANDPASS (string)
- MA_TABLE_NUMBER (int64)
- DURATION (int64)
- PLAN (int64)
- PASS (int64)
- SEGMENT (int64)
- OBSERVATION (int64)
- VISIT (int64)
- EXPOSURE (int64)
```
