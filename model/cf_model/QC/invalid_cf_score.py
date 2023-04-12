from datetime import datetime
import os

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as sqlf

from pe_memberdna.model.cf_model.lib.cf_io import (
    load_config,
    calculate_filepaths,
)
from memberdna.lib.iotools import (
    write_local_to_s3,
)

# (1) ---- ARGUMENTS ---- #

CNF, CFG_PATH = load_config()
PARAMS = dict(list(CNF["shared"].items()) + list(CNF["combine"].items()))
PATHS = CNF["paths"]
OUTPUT_PATH = PATHS["MODEL"]
RUN_NAME = PARAMS["run_name"]
PARAMS, PATHS = calculate_filepaths(PARAMS, PATHS)

now = datetime.now().strftime("%Y%m%d")
input_path = os.path.join(
    PATHS["COMBINE_PREDICTION"],
    PARAMS["subtype"] + "-" + now + "-" + "combined",
    "PARQUET/",
)

conf = SparkConf().setAppName("cf_combine")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")


columns = ["MBRSHP_SID", "CATEGORY_NAME", "CATEGORY_ID", "prediction"]
cf_data = spark.read.parquet(input_path)


valid_cf = (
    cf_data.select(*columns)
    .withColumn("valid", sqlf.when(sqlf.col("prediction") > 0, 1).otherwise(0))
    .groupBy("MBRSHP_SID")
    .agg(sqlf.sum(sqlf.col("valid")).alias("count_valid"))
)
invalid_cf = valid_cf.filter(sqlf.col("count_valid") == 0)

CUBE = (
    spark.read.parquet(PATHS["CUBE"])
    .filter(
        sqlf.col("FISCAL_WEEK_END").between(PARAMS["start"], PARAMS["end"])
    )
    .join(invalid_cf, on="MBRSHP_SID", how="inner")
)

max_fiscal_week_end = (
    CUBE.groupBy().agg(sqlf.max("FISCAL_WEEK_END")).toPandas().iloc[0, 0]
)

CUBE_sub = (
    CUBE.filter(sqlf.col("FISCAL_WEEK_END") == max_fiscal_week_end)
    .select(
        "MBRSHP_SID",
        "TENURE",
        "LAST_EIGHT_WEEK_SPEND",
        "LAST_TWELVE_WEEK_SPEND",
        "LAST_TWENTY-SIX_WEEK_SPEND",
        "LAST_FIFTY-TWO_WEEK_SPEND",
        "WEEK_TRIPS",
        "LAST_FOUR_WEEK_TRIPS",
        "LAST_EIGHT_WEEK_TRIPS",
        "LAST_TWELVE_WEEK_TRIPS",
        "LAST_TWENTY-SIX_WEEK_TRIPS",
        "LAST_FIFTY-TWO_WEEK_TRIPS",
        "LAST_FISCAL_WEEK_TRIP",
    )
    .toPandas()
)

CUBE_sub["MBRSHP_SID"] = CUBE_sub["MBRSHP_SID"].astype("str")
invalid_sum = CUBE_sub.describe().reset_index()

current_output_path = os.path.join(OUTPUT_PATH, "QC", RUN_NAME)
write_local_to_s3(
    invalid_sum,
    os.path.join(current_output_path, "invalid_cf", "invalid_cf.csv"),
)
