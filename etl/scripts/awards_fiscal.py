import argparse
import logging
import os

from pe_memberdna.lib.job_manager import JobManager
from pe_memberdna.etl.lib.s3 import etl_input_data_validator
from pe_memberdna.etl.lib.utility import *


def main(job, data_paths):
    """
    Reads the intermediate awards table and converts and partitions it with
    respect to fiscal week

    Args:
        job.spark - spark session object
        data_paths - dict structure containing the source and intermediate paths
    Returns:
    """

    awards_intermediate_path = data_paths["intermediate"].get("awards")

    recency_lookback_duration = data_paths.get("recency_lookback_duration", {})
    if not awards_intermediate_path:
        return

    logging.info("Starting processing table awards")

    etl_input_data_validator(
        "intermediate",
        recency_lookback_duration,
        data_paths,
        [
            "fiscal_days",
            "awards",
        ],
    )

    fiscal_days = job.spark.read.parquet(
        data_paths["intermediate"]["fiscal_days"]
    )
    fiscal_days.registerTempTable("fiscal_days")

    awards = job.spark.read.parquet(awards_intermediate_path)
    awards.registerTempTable("awards")

    sql = """
    select
        aw.*,
        f.FISCAL_WEEK_END as FISCAL_WEEK_END
    from
        awards as aw
    inner join
        fiscal_days f
    on
        aw.AWRD_CERT_ISSUE_DT = f.FISCAL_DAY
    """
    awards_fiscal = job.spark.sql(sql)

    awards_fiscal.repartition("FISCAL_WEEK_END").write.parquet(
        data_paths["intermediate"]["awards_fiscal"],
        partitionBy="FISCAL_WEEK_END",
        mode="overwrite",
    )


job = JobManager("Awards_fiscal")
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
main(job, data_paths)
