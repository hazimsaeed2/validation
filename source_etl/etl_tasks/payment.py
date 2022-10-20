import argparse
import logging
import os

from memberdna.lib.job_manager import JobManager
from memberdna.source_etl.utils.utility import *


def main(job, data_paths, config_validation):

    logging.info("Starting processing table payment")

    source_path = data_paths["source"]["payment"]

    cast_sql = """
    select
         cast(MBRSHP_SID as int) as MBRSHP_SID
        ,cast(PURCH_HDR_ID as int) as PURCH_HDR_ID
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

parser.add_argument("--prod-mode", dest="prod_mode", action="store_true")
parser.add_argument(
    "config_path",
    nargs="?",
    default=os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../configs/config.yaml",
    ),
)
parser.set_defaults(prod_mode=True)
args = parser.parse_args()
config = job.load_config(args)
data_paths, club_square_config, config_validation = job.split_config(config)
main(job, data_paths, config_validation)
