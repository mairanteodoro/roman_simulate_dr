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

Generate the input catalog for romanisim containing COSMOS galaxies, Gaia stars, and additional random stellar sources:

```shell
rdr_generate_input_catalog \
  --obs-plan observation_plan.toml \
  --output-filename romanisim_input_catalog.ecsv
```

N.B.: Currently, four files will be created:
  - one containing only the COSMOS galaxies;
  - one containing only the Gaia stars;
  - one containing only the additional random stars;
  - one containing all the above sources.
