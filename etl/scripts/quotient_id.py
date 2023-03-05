import argparse
import logging
import os

import pe_memberdna.etl.lib.validations_ETL as validations
import pyspark.sql.functions as sqlf
from pe_memberdna.lib.job_manager import JobManager
from pe_memberdna.etl.lib.s3 import input_data_validator
from pe_memberdna.etl.lib.utility import *
from pyspark.sql.types import *

quotient_schema = StructType(
    [
        StructField("LOYALTYNUMBER", StringType(), True),
        StructField("USERCODE", StringType(), True),
        StructField("SIGNUP", DateType(), True),
    ]
)


quotient_out = {"MBRSHP_SID", "MBRSHP_NBR", "USERCODE", "SIGNUP"}


def main(job, data_paths, config_validation):
    """
    INFO: We're not using "createSchemaParquet" because we need to merge data from two sources
    and that only supports querying a single one
    """
    logging.info("Starting processing table quotient_id")

    logging.info(
        "Reading the input file " + data_paths["source"]["quotient_id"]
    )

    df = (
        job.spark.read.schema(quotient_schema)
        .option("header", "true")
        .option("dateFormat", "yyyymmdd")
        .csv(data_paths["source"]["quotient_id"])
    )

    # validations.validate_table(
    #     job.spark, "source", "quotient_id", config_validation, df
    # )

    # Join with member extend to add MBRSHP_SID to the table
    member_extended = job.spark.read.parquet(
        data_paths["intermediate"]["member_extended"]
    )
    member_extended = member_extended.select(["MBRSHP_NBR", "MBRSHP_SID"])
    df = df.withColumnRenamed("LOYALTYNUMBER", "MBRSHP_NBR").join(
        member_extended, "MBRSHP_NBR", "inner"
    )

    df = df.select(*quotient_out).withColumn("HAS_QUOTIENT_ID", sqlf.lit(1))

    dest_path = data_paths["intermediate"]["quotient_id"]

    # validations.validate_table(
    #     job.spark, "intermediate", "quotient_id", config_validation, df
    # )

    logging.info("Saving the intermediate file " + dest_path)

    df.repartition(32).write.parquet(dest_path, mode="overwrite")


job = JobManager("quotient_id")
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
