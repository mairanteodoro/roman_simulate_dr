import argparse
import logging

from roman_simulate_dr.scripts.utils import (
    generate_roman_filename,
    parallelize_jobs,
    read_obs_plan,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(processName)s %(message)s",
)
logger = logging.getLogger(__name__)


class RomanisimImages:
    """
    Handles generation of simulated Roman L1 images based on an observation plan and input catalog.
    """

    def __init__(
        self,
        obs_plan_filename: str,
        input_filename: str,
        max_workers: int | None = None,
        sca_ids: list[int] | None = None,
    ):
        if not obs_plan_filename:
            raise ValueError("An observation plan filename must be provided.")
        if not input_filename:
            raise ValueError("An input catalog filename must be provided.")
        self.plan = read_obs_plan(obs_plan_filename)
        self.input_filename = input_filename
        self.max_workers = max_workers
        self.sca_ids = sca_ids if sca_ids is not None else [1]

    def _generate_simulated_images(
        self,
        radec: tuple = (270.0, 66.0),
        level: int = 1,
        sca: int = 1,
        bandpass: str = "F062",
        roll: float = 0.0,
        catalog: str = "",
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
        if result.returncode != 0:
            logger.error(f"[{output_filename}] STDERR:\n{result.stderr}")
        else:
            logger.info(f"[{output_filename}] STDERR:\n{result.stderr}")

        return output_filename, result.returncode

    def run(self):
        """
        Run the romanisim simulation workflow for all passes and visits in the plan.
        """
        jobs = []
        for (
            ra_ref,
            dec_ref,
            pa,
            bandpass,
            ma_table_number,
            _,  # duration, not used here
            plan,
            pidx,
            segment,
            observation,
            vidx,
            eidx,
        ) in self.plan:
            for sca in self.sca_ids:
                bandpass = bandpass.upper()
                output_filename = generate_roman_filename(
                    program=1,
                    plan=plan,
                    passno=int(pidx),
                    segment=segment,
                    observation=observation,
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
                        roll=pa,
                        ma_table_number=ma_table_number,
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
    """
    Command-line interface for generating Romanisim simulated images.
    """
    parser = argparse.ArgumentParser(
        description="Generate Romanisim input catalogs based on an observation plan."
    )
    parser.add_argument(
        "--obs-plan",
        type=str,
        default="obs_plan.ecsv",
        help="Observation plan filename (default: obs_plan.ecsv)",
    )
    parser.add_argument(
        "--input-filename",
        type=str,
        default="romanisim_input_catalog.ecsv",
        required=False,
        help="Input catalog filename (default: romanisim_input_catalog.ecsv)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=1,
        help="Number of parallel workers for exposure generation (default: 1, disables parallelization)",
    )
    parser.add_argument(
        "--sca-ids",
        type=int,
        nargs="+",
        default=[1],
        help="List of SCA IDs to simulate (default: [1])",
    )
    args = parser.parse_args()
    input_catalog = RomanisimImages(
        obs_plan_filename=args.obs_plan,
        input_filename=args.input_filename,
        max_workers=args.max_workers,
        sca_ids=args.sca_ids,
    )
    input_catalog.run()
    logger.info("Done.")


if __name__ == "__main__":
    _cli()
