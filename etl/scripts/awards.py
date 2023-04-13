import argparse
import logging
import os

from pe_memberdna.lib.job_manager import JobManager
from pe_memberdna.etl.lib.s3 import etl_input_data_validator
from pe_memberdna.etl.lib.utility import *


def main(job, data_paths, config_validation):
    """
    Reads the source csv awards table and converts it to the parquet format

    Args:
        spark - spark session object
        data_paths - dict structure containing the source and intermediate paths
        config_validation - dict structure containing the DQ_check configuration
    Returns:
    """

    recency_lookback_duration = data_paths.get("recency_lookback_duration", {})
    etl_input_data_validator(
        "source", recency_lookback_duration, data_paths, ["awards"]
    )
    source_path = data_paths["source"].get("awards")

    if not source_path:
        return

    logging.info("Starting processing table awards")

    cast_sql = """
    select
        CAST(MBRSHP_SID AS long) as MBRSHP_SID,
        AWRD_CERT_NBR,
        CAST(AWRD_CERT_AMT as double) as AWRD_CERT_AMT,
        CAST(AWRD_CERT_ISSUE_DT as date) as AWRD_CERT_ISSUE_DT,
        CAST(AWRD_CERT_EXP_DT as date) as AWRD_CERT_EXP_DT,
        AWRD_CERT_RDMPTN_CD,
        AWRD_PROMO_ID
    from df
    """

    dest_path = data_paths["intermediate"]["awards"]
    repartition_val = "AWRD_CERT_ISSUE_DT"

    createSchemaParquet(
        job.spark,
        source_path,
        config_validation,
        "awards",
        cast_sql,
        dest_path,
        repartition_val,
        sep=",",
        header=True,
    )


job = JobManager("awards")
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
