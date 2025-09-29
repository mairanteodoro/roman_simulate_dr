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
│ # ---
│ # datatype:
│ # - {name: RA, datatype: float64}
│ # - {name: DEC, datatype: float64}
│ # - {name: PA, datatype: float64}
│ # - {name: BANDPASS, datatype: string}
│ # - {name: MA_TABLE_NUMBER, datatype: int64}
│ # - {name: DURATION, datatype: int64}
│ # - {name: PLAN, datatype: int64}
│ # - {name: PASS, datatype: int64}
│ # - {name: SEGMENT, datatype: int64}
│ # - {name: OBSERVATION, datatype: int64}
│ # - {name: VISIT, datatype: int64}
│ # - {name: EXPOSURE, datatype: int64}
```
