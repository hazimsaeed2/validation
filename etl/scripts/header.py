import argparse
import logging
import os

from pe_memberdna.lib.job_manager import JobManager
from pe_memberdna.etl.lib.s3 import etl_input_data_validator
from pe_memberdna.etl.lib.utility import *


def main(job, data_paths, config_validation):
    logging.info("Starting processing table header")

    recency_lookback_duration = data_paths.get("recency_lookback_duration", {})
    source_path = data_paths["source"]["header"]
    etl_input_data_validator(
        "source",
        recency_lookback_duration,
        data_paths,
        ["header"],
    )

    cast_sql = """
    select
         cast(PURCH_HDR_ID as long) as PURCH_HDR_ID
        ,cast(MBRSHP_SID as long) as MBRSHP_SID
        ,cast(SITE_NBR as int) as SITE_NBR
        ,cast(PURCH_DT as date) as PURCH_DT
        ,SALES_CHANNEL_ID
        ,cast(TOT_SALES_AMT as double) as TOT_SALES_AMT
        ,cast(TAX_AMT as double) as TAX_AMT
        ,PURCHASE_TM
    from df
    """
    dest_path = data_paths["intermediate"]["header"]
    repartition_val = "PURCH_DT"
    filter_cond = 'PURCH_DT >= "2012-09-01"'

    createSchemaParquet(
        job.spark,
        source_path,
        config_validation,
        "header",
        cast_sql,
        dest_path,
        repartition_val,
        filter_cond,
    )


job = JobManager("header")
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
