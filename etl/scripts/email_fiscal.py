import argparse
import logging
import os

import pe_memberdna.etl.lib.validations_ETL as validations
from pe_memberdna.lib.job_manager import JobManager
from pe_memberdna.etl.lib.utility import *


def main(job, data_paths, config_validation):

    logging.info("Starting processing table email_fiscal")

    email = job.spark.read.parquet(data_paths["intermediate"]["email"])
    email.registerTempTable("email")

    fiscal_days = job.spark.read.parquet(
        data_paths["intermediate"]["fiscal_days"]
    )
    fiscal_days.registerTempTable("fiscal_days")

    sql = """
    SELECT e.*, f.FISCAL_WEEK_START, f.FISCAL_WEEK_END
    FROM email e
    JOIN fiscal_days f
    ON e.FIRST_OPEN_DATE = f.FISCAL_DAY
    """

    email_fiscal = job.spark.sql(sql)

    validations.validate_table(
        job.spark,
        "intermediate",
        "email_fiscal",
        config_validation,
        email_fiscal,
    )

    logging.info(
        "Saving the intermediate file "
        + data_paths["intermediate"]["email_fiscal"]
    )
    email_fiscal.repartition("FISCAL_WEEK_END").write.parquet(
        data_paths["intermediate"]["email_fiscal"],
        partitionBy="FISCAL_WEEK_END",
        mode="overwrite",
    )


job = JobManager("email_fiscal")
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
