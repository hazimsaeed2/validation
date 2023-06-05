import argparse
import logging
import os

import pe_memberdna.etl.lib.validations_ETL as validations
from pe_memberdna.etl.lib.s3 import etl_input_data_validator
from pe_memberdna.lib.job_manager import JobManager


def filter_gas_nr(detail_fiscal):
    """
    Filters the detail table for only positive sales (no returns).
    And filters out transactions at BJs cafe and other minor exclusions
    (established by the client).
    """
    mc_cds = ["402030190", "402030191", "203010098"]

    return detail_fiscal.filter(
        (detail_fiscal.EXTENDED_PRC_AMT > 0)
        & (detail_fiscal.QTY_IN_UNITS > 0)
        & (~detail_fiscal.MC_CD.isin(mc_cds))
    )


def main(job, data_paths, config_validation):

    logging.info("Starting processing table detail_gas_nr_fiscal")

    recency_lookback_duration = data_paths.get("recency_lookback_duration", {})
    etl_input_data_validator(
        "intermediate",
        recency_lookback_duration,
        data_paths,
        [
            "detail_fiscal",
        ],
    )

    detail_fiscal = job.spark.read.parquet(
        data_paths["intermediate"]["detail_fiscal"]
    )

    detail_gas_nr_fiscal = filter_gas_nr(detail_fiscal)

    validations.validate_table(
        job.spark,
        "intermediate",
        "detail_gas_nr_fiscal",
        config_validation,
        detail_gas_nr_fiscal,
    )

    logging.info(
        "Saving the intermediate file "
        + data_paths["intermediate"]["detail_gas_nr_fiscal"]
    )

    detail_gas_nr_fiscal.repartition("FISCAL_WEEK_END").write.partitionBy(
        "FISCAL_WEEK_END"
    ).parquet(
        data_paths["intermediate"]["detail_gas_nr_fiscal"], mode="overwrite"
    )


job = JobManager("detail_gas_nr_fiscal")
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
