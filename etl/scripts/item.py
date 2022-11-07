import argparse
import logging
import os

from pe_memberdna.lib.job_manager import JobManager
from pe_memberdna.etl.lib.utility import *


def main(job, data_paths, config_validation):

    logging.info("Starting processing table item")

    source_path = data_paths["source"]["item"]

    cast_sql = """
    select
        GTIN_CD
        ,ARTICLE_DESC
        ,ARTICLE_NBR
        ,MCH4_CD
        ,MCH4_DESC
        ,MCH3_CD
        ,MCH3_DESC
        ,MCH2_CD
        ,MCH2_DESC
        ,MCH1_CD
        ,MCH1_DESC
        ,MC_CD
        ,MC_DESC
        ,AH1_CD
        ,AH1_DESC
        ,AH2_CD
        ,AH2_DESC
        ,AH3_CD
        ,AH3_DESC
        ,AH4_CD
        ,AH4_DESC
        ,AH5_CD
        ,AH5_DESC
        ,AH6_CD
        ,AH6_DESC
        ,BRAND_TYPE
        ,cast(EFF_DT as date) as EFF_DT
        ,cast(EXP_DT as date) as EXP_DT
        ,ORDR_AS as REPLACEMENT_ARTICLE
    from df
    """
    dest_path = data_paths["intermediate"]["item"]
    repartition_val = 1

    createSchemaParquet(
        job.spark,
        source_path,
        config_validation,
        "item",
        cast_sql,
        dest_path,
        repartition_val,
        header=True,
    )


job = JobManager("item")
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
