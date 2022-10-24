import argparse
import logging
import os

import memberdna.source_etl.utils.validations_ETL as validations
from memberdna.lib.job_manager import JobManager
from memberdna.source_etl.utils.utility import *


def main(job, data_paths, config_validation):
    logging.info("Starting processing table header_fiscal")

    header = job.spark.read.parquet(data_paths["intermediate"]["header"])
    header.registerTempTable("header")

    fiscal_days = job.spark.read.parquet(
        data_paths["intermediate"]["fiscal_days"]
    )
    fiscal_days.registerTempTable("fiscal_days")

    sql = """
    select h.*, f.FISCAL_WEEK_START, f.FISCAL_WEEK_END
    from header h
    join fiscal_days f
        on  h.PURCH_DT = f.FISCAL_DAY
    """
    header_fiscal = job.spark.sql(sql)

    validations.validate_table(
        job.spark,
        "intermediate",
        "header_fiscal",
        config_validation,
        header_fiscal,
    )

    logging.info(
        "Saving the intermediate file "
        + data_paths["intermediate"]["header_fiscal"]
    )

    header_fiscal.repartition("FISCAL_WEEK_END").write.parquet(
        data_paths["intermediate"]["header_fiscal"],
        partitionBy="FISCAL_WEEK_END",
        mode="overwrite",
    )


job = JobManager("header_fiscal")
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
