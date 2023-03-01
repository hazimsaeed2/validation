import argparse
import logging
import os

import pe_memberdna.etl.lib.validations_ETL as validations
from pe_memberdna.lib.job_manager import JobManager
from pe_memberdna.etl.lib.utility import *


def main(job, data_paths, config_validation):

    logging.info("Starting processing table skeleton")

    header_fiscal = job.spark.read.parquet(
        data_paths["intermediate"]["header_fiscal"]
    )
    fwe_min = header_fiscal.agg({"FISCAL_WEEK_END": "min"}).collect()[0][0]
    fwe_max = header_fiscal.agg({"FISCAL_WEEK_END": "max"}).collect()[0][0]

    members = header_fiscal.select("MBRSHP_SID").distinct()

    fiscal_days = job.spark.read.parquet(
        data_paths["intermediate"]["fiscal_days"]
    )
    fiscal_weeks = fiscal_days.drop("FISCAL_DAY").distinct()
    fiscal_weeks_inscope = fiscal_weeks.filter(
        'FISCAL_WEEK_END between "{}" and "{}"'.format(fwe_min, fwe_max)
    )

    skeleton = members.crossJoin(fiscal_weeks_inscope)

    validations.validate_table(
        job.spark,
        "intermediate",
        "skeleton",
        config_validation,
        skeleton,
        [validations.TestColNames, validations.TestDuplicates],
    )

    logging.info(
        "Saving the intermediate file "
        + data_paths["intermediate"]["skeleton"]
    )

    skeleton.repartition("FISCAL_WEEK_END").write.parquet(
        data_paths["intermediate"]["skeleton"],
        partitionBy="FISCAL_WEEK_END",
        mode="overwrite",
    )


job = JobManager("skeleton")
parser = argparse.ArgumentParser()

parser.add_argument(
    "--config_path",
    type=str,
    default=os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../configs/config.yaml",
    ),
    help=(
        """
        path to the config file
        """
    ),
)
args = parser.parse_args()
config = job.load_config(args)
data_paths, club_square_config, config_validation = job.split_config(config)
main(job, data_paths, config_validation)
