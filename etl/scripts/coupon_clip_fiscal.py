import argparse
import logging
import os

import pe_memberdna.etl.utils.validations_ETL as validations
from pe_memberdna.lib.job_manager import JobManager
from pe_memberdna.etl.utils.utility import *


def main(job, data_paths, config_validation):

    logging.info("Starting processing table coupon_clip_fiscal")

    coupon_clip = job.spark.read.parquet(
        data_paths["intermediate"]["coupon_clip"]
    )
    coupon_clip.registerTempTable("coupon_clip")

    fiscal_days = job.spark.read.parquet(
        data_paths["intermediate"]["fiscal_days"]
    )
    fiscal_days.registerTempTable("fiscal_days")

    sql = """
    SELECT c.*, f.FISCAL_WEEK_START, f.FISCAL_WEEK_END
    FROM coupon_clip c
    JOIN fiscal_days f
    ON c.EVENTDATETIME = f.FISCAL_DAY
    """

    coupon_clip_fiscal = job.spark.sql(sql)

    validations.validate_table(
        job.spark,
        "intermediate",
        "coupon_clip_fiscal",
        config_validation,
        coupon_clip_fiscal,
        # coupon_clip_fiscal.py is last job called in run.py
        # We want to archive the final stats, so set to True for this last job
        archive=True,
    )

    logging.info(
        "Saving the intermediate file "
        + data_paths["intermediate"]["coupon_clip_fiscal"]
    )

    coupon_clip_fiscal.repartition("FISCAL_WEEK_END").write.parquet(
        data_paths["intermediate"]["coupon_clip_fiscal"],
        partitionBy="FISCAL_WEEK_END",
        mode="overwrite",
    )


job = JobManager("Cupon_clip_fiscal")
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
