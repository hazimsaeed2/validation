from pyspark.sql.types import *
import pyspark.sql.functions as sqlf

import memberdna.source_etl.utils.validations_ETL as validations
from memberdna.source_etl.utils.utility import *


quotient_schema = StructType(
    [
        StructField("LOYALTYNUMBER", StringType(), True),
        StructField("USERCODE", StringType(), True),
        StructField("SIGNUP", DateType(), True),
    ]
)


quotient_out = {"MBRSHP_SID", "MBRSHP_NBR", "USERCODE", "SIGNUP"}


def main(spark, data_paths, config_validation):
    """
    INFO: We're not using "createSchemaParquet" because we need to merge data from two sources
    and that only supports querying a single one
    """
    logging.info("Starting processing table quotient_id")

    logging.info(
        "Reading the input file " + data_paths["source"]["quotient_id"]
    )

    df = (
        spark.read.schema(quotient_schema)
        .option("header", "true")
        .option("dateFormat", "yyyymmdd")
        .csv(data_paths["source"]["quotient_id"])
    )

    # validations.validate_table(
    #     spark, "source", "quotient_id", config_validation, df
    # )

    # Join with member extend to add MBRSHP_SID to the table
    member_extended = spark.read.parquet(
        data_paths["intermediate"]["member_extended"]
    )
    member_extended = member_extended.select(["MBRSHP_NBR", "MBRSHP_SID"])
    df = df.withColumnRenamed("LOYALTYNUMBER", "MBRSHP_NBR").join(
        member_extended, "MBRSHP_NBR", "inner"
    )

    df = df.select(*quotient_out).withColumn("HAS_QUOTIENT_ID", sqlf.lit(1))

    dest_path = data_paths["intermediate"]["quotient_id"]

    # validations.validate_table(
    #     spark, "intermediate", "quotient_id", config_validation, df
    # )

    logging.info("Saving the intermediate file " + dest_path)

    df.repartition(32).write.parquet(dest_path, mode="overwrite")
