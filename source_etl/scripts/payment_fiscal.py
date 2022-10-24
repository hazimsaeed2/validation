import argparse
import logging
import os

import memberdna.source_etl.utils.validations_ETL as validations
from memberdna.lib.job_manager import JobManager
from memberdna.source_etl.utils.utility import *


def main(job, data_paths, config_validation):

    logging.info("Starting processing table payment fiscal")

    payment = job.spark.read.parquet(data_paths["intermediate"]["payment"])
    payment.registerTempTable("payment")

    header_fiscal = job.spark.read.parquet(
        data_paths["intermediate"]["header_fiscal"]
    )
    header_fiscal.registerTempTable("header_fiscal")

    header_fields = [
        "PURCH_HDR_ID",
        "PURCH_DT",
        "FISCAL_WEEK_START",
        "FISCAL_WEEK_END",
    ]
    payment_fiscal = payment.join(header_fiscal[header_fields], "PURCH_HDR_ID")

    validations.validate_table(
        job.spark,
        "intermediate",
        "payment_fiscal",
        config_validation,
        payment_fiscal,
        [
            validations.CompareColAggregatesPrior,
            validations.TestColNames,
            validations.TestDuplicates,
            validations.TestOutlierDays,
        ],
    )

    logging.info(
        "Saving the intermediate file "
        + data_paths["intermediate"]["payment_fiscal"]
    )

    payment_fiscal.repartition("FISCAL_WEEK_END").write.parquet(
        data_paths["intermediate"]["payment_fiscal"],
        partitionBy="FISCAL_WEEK_END",
        mode="overwrite",
    )


job = JobManager("payment_fiscal")
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
