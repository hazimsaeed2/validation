import argparse
import logging
import os

from pe_memberdna.lib.job_manager import JobManager
from pe_memberdna.etl.lib.s3 import etl_input_data_validator
from pe_memberdna.etl.lib.utility import *


def main(job, data_paths, config_validation):

    logging.info("Starting processing table payment")

    recency_lookback_duration = data_paths.get("recency_lookback_duration", {})
    source_path = data_paths["source"]["payment"]
    etl_input_data_validator(
        "source",
        recency_lookback_duration,
        data_paths,
        ["payment"],
    )

    cast_sql = """
    select
         cast(MBRSHP_SID as long) as MBRSHP_SID
        ,cast(PURCH_HDR_ID as long) as PURCH_HDR_ID
        ,cast(PURCH_PYMT_SEQ_ID as int) as PURCH_PYMT_SEQ_ID
        ,TENDER_TYPE_CD
        ,CPN_NBR
        ,PYMT_SCANNED_OR_KEYED_IND
        ,cast(SALES_PYMT_AMT as decimal(9,2)) as SALES_PYMT_AMT
        ,TENDER_ID
    from df
    """
    dest_path = data_paths["intermediate"]["payment"]
    repartition_val = 10

    createSchemaParquet(
        job.spark,
        source_path,
        config_validation,
        "payment",
        cast_sql,
        dest_path,
        repartition_val,
        del_dup=True,
    )


job = JobManager("payment")
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
