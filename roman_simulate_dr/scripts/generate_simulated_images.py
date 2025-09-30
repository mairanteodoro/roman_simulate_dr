import argparse
import logging

from roman_simulate_dr.scripts.utils import (
    generate_roman_filename,
    parallelize_jobs,
    read_obs_plan,
)

# Setup logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(processName)s %(message)s",
)
logger = logging.getLogger(__name__)


class RomanisimImages:
    def __init__(
        self,
        obs_plan_filename: str,
        input_filename: str | None = None,
        max_workers: int | None = None,
    ):
        self.plan = read_obs_plan(obs_plan_filename)
        # Get the output catalog filename either
        # from the CLI or from the observation plan file
        if input_filename is not None:
            self.input_filename = input_filename
        else:
            self.input_filename = self.plan.get("romanisim_input_catalog_name")

        self.max_workers = max_workers

    def _generate_simulated_images(
        self,
        radec: tuple = (270.0, 66.0),
        level: int = 1,
        sca: int = 1,
        bandpass: str = "F062",
        roll: float = 0.0,
        catalog: str = "romanisim_input_cat.ecsv",
        stpsf: bool = True,
        ma_table_number: int = 109,
        date: str = "2027-06-01T00:00:00",
        drop_extra_dq: bool = True,
        output_filename: str = "romanisim_simulated_image.asdf",
    ):
        """
        Run the romanisim simulation with the given parameters.
        """
        import subprocess

        cmd = [
            "romanisim-make-image",
            "--radec",
            str(radec[0]),
            str(radec[1]),
            "--level",
            str(level),
            "--sca",
            str(sca),
            "--bandpass",
            str(bandpass),
            "--roll",
            str(roll),
            "--catalog",
            str(catalog),
            *(["--stpsf"] if stpsf else []),
            "--ma_table_number",
            str(ma_table_number),
            "--date",
            date,
            "--rng_seed",
            "1",
            "--usecrds",
            *(["--drop-extra-dq"] if drop_extra_dq else []),
            output_filename,
        ]

        result = subprocess.run(
            cmd, capture_output=True, text=True, shell=False, check=False
        )
        logger.info(f"[{output_filename}] STDOUT:\n{result.stdout}")
        logger.info(f"[{output_filename}] STDERR:\n{result.stderr}")

        return output_filename, result.returncode

    def run(self):
        """
        Run the romanisim simulation workflow for all passes and visits in the plan.
        """
        jobs = []
        for pidx, p in enumerate(self.plan["passes"]):
            for vidx, v in enumerate(p.get("visits", [])):
                ra_ref = v.get("lon")
                dec_ref = v.get("lat")
                logger.info(f"pass: {p['name']}, visit: {v['name']}")
                logger.info(f"  ra_ref: {v['lon']}, dec_ref: {v['lat']}")

                for eidx, e in enumerate(v.get("exposures", [])):
                    for sca in e.get("sca_ids"):
                        for filt in e.get("filter_names"):
                            bandpass = filt.upper()
                            output_filename = generate_roman_filename(
                                program=1,
                                plan=1,
                                passno=int(pidx),
                                segment=1,
                                observation=1,
                                visit=int(vidx),
                                exposure=int(eidx),
                                sca=int(sca),
                                bandpass=bandpass,
                                suffix="uncal",
                            )
                            jobs.append(
                                dict(
                                    radec=(ra_ref, dec_ref),
                                    sca=sca,
                                    bandpass=bandpass,
                                    roll=p.get("roll"),
                                    catalog=self.input_filename,
                                    output_filename=output_filename,
                                )
                            )
        parallelize_jobs(
            self._generate_simulated_images,
            jobs,
            max_workers=self.max_workers,
        )


def _cli():
    parser = argparse.ArgumentParser(
        description="Generate Romanisim input catalogs based on an observation plan."
    )
    parser.add_argument(
        "--obs-plan",
        type=str,
        default="obs_plan.toml",
        help="Observation plan filename (default: obs_plan.toml)",
    )
    parser.add_argument(
        "--input-filename",
        type=str,
        default=None,
        required=False,
        help="Input catalog filename (default: determined from the observation plan)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=1,
        help="Number of parallel workers for exposure generation (default: 1, disables parallelization)",
    )
    args = parser.parse_args()

    max_workers = args.max_workers if args.max_workers > 1 else None

    input_catalog = RomanisimImages(
        obs_plan_filename=args.obs_plan,
        input_filename=args.input_filename,
        max_workers=max_workers,
    )
    input_catalog.run()

    logger.info("Done.")


if __name__ == "__main__":
    _cli()
