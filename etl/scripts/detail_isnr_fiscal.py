import argparse
import logging
import os

import pe_memberdna.etl.lib.validations_ETL as validations
from pe_memberdna.lib.job_manager import JobManager
from pe_memberdna.etl.lib.s3 import input_data_validator
from pe_memberdna.etl.lib.utility import *


def main(job, data_paths, config_validation):

    logging.info("Starting processing table detail_isnr_fiscal")

    detail_fiscal = job.spark.read.parquet(
        data_paths["intermediate"]["detail_fiscal"]
    )

    filter_in_store = detail_fiscal["SALES_CTGRY_CD"] == "03"
    filter_no_returns = detail_fiscal["SALES_QTY"] > 0
    # still saw returns after prior filter
    filter_no_returns_deli = detail_fiscal["QTY_IN_UNITS"] > 0

    filters = filter_in_store & filter_no_returns & filter_no_returns_deli

    detail_isnr_fiscal = detail_fiscal.filter(filters)

    validations.validate_table(
        job.spark,
        "intermediate",
        "detail_isnr_fiscal",
        config_validation,
        detail_isnr_fiscal,
    )

    logging.info(
        "Saving the intermediate file "
        + data_paths["intermediate"]["detail_isnr_fiscal"]
    )
    detail_isnr_fiscal.repartition("FISCAL_WEEK_END").write.parquet(
        data_paths["intermediate"]["detail_isnr_fiscal"],
        partitionBy="FISCAL_WEEK_END",
        mode="overwrite",
    )


job = JobManager("detail_isnr_fiscal")
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
