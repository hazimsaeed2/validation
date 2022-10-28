import argparse
import logging
import os

import pe_memberdna.etl.utils.validations_ETL as validations
from pe_memberdna.lib.job_manager import JobManager
from pe_memberdna.etl.utils.utility import *


def main(job, data_paths, config_validation):

    logging.info("Starting processing table detail_fiscal")

    detail = job.spark.read.parquet(data_paths["intermediate"]["detail"])

    detail.registerTempTable("detail")

    header_fiscal = job.spark.read.parquet(
        data_paths["intermediate"]["header_fiscal"]
    )
    header_fiscal.registerTempTable("header_fiscal")

    item = job.spark.read.parquet(data_paths["intermediate"]["item"])
    item.registerTempTable("item")

    sql = """
    select
         d.*
        ,h.MBRSHP_SID
        ,h.SITE_NBR
        ,h.FISCAL_WEEK_START
        ,h.FISCAL_WEEK_END
        ,i.ARTICLE_DESC
        ,i.MCH4_CD
        ,i.MCH4_DESC
        ,i.MCH3_CD
        ,i.MCH3_DESC
        ,i.MCH2_CD
        ,i.MCH2_DESC
        ,i.MCH1_CD
        ,i.MCH1_DESC
        ,i.MC_DESC
        ,i.AH1_CD
        ,i.AH1_DESC
        ,i.AH2_CD
        ,i.AH2_DESC
        ,i.AH3_CD
        ,i.AH3_DESC
        ,i.AH4_CD
        ,i.AH4_DESC
        ,i.AH5_CD
        ,i.AH5_DESC
        ,i.AH6_CD
        ,i.AH6_DESC
        ,i.BRAND_TYPE
        ,i.EFF_DT
        ,i.EXP_DT
    from detail d
    join header_fiscal h
        on  d.PURCH_DT = h.PURCH_DT and
            d.PURCH_HDR_ID = h.PURCH_HDR_ID
    left join item i
        on  d.GTIN_CD = i.GTIN_CD and
            d.ARTICLE_NBR = i.ARTICLE_NBR
    """
    detail_fiscal = job.spark.sql(sql)

    validations.validate_table(
        job.spark,
        "intermediate",
        "detail_fiscal",
        config_validation,
        detail_fiscal,
    )

    logging.info(
        "Saving the intermediate file "
        + data_paths["intermediate"]["detail_fiscal"]
    )

    detail_fiscal.repartition("FISCAL_WEEK_END").write.parquet(
        data_paths["intermediate"]["detail_fiscal"],
        partitionBy="FISCAL_WEEK_END",
        mode="overwrite",
    )


job = JobManager("detail_fiscal")
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
