import argparse
import logging
import os

import pyspark.sql.functions as F
from pyspark.sql.types import *

import memberdna.source_etl.utils.validations_ETL as validations
from memberdna.lib.job_manager import JobManager

coupon_clip_schema = StructType(
    [
        StructField("usercode", StringType(), True),
        StructField("EVENTDATETIME", DateType(), True),
        StructField("EVENTTYPE", StringType(), True),
        StructField("OFFERID", StringType(), True),
        StructField("STORENAME", StringType(), True),
        StructField("OFFERCODE", StringType(), True),
        StructField("OFFERACTIVEDATE", DateType(), True),
        StructField("OFFERSHUTOFFDATE", DateType(), True),
        StructField("OFFEREXPIRYDATE", DateType(), True),
        StructField("BRAND", StringType(), True),
        StructField("OFFERVALUE", DoubleType(), True),
        StructField("OFFERTYPE", StringType(), True),
        StructField("DISCOUNTTYPE", StringType(), True),
        StructField("CATEGORY", StringType(), True),
        StructField("SOURCE", StringType(), True),
        StructField("APPID", StringType(), True),
        StructField("APPCODE", StringType(), True),
    ]
)

coupon_clip_out = {
    "MBRSHP_SID",
    "MBRSHP_NBR",
    "EVENTTYPE",
    "EVENTDATETIME",
    "OFFERACTIVEDATE",
    "OFFERSHUTOFFDATE",
    "OFFEREXPIRYDATE",
}


def main(job, data_paths, config_validation):

    logging.info("Starting processing table coupon_clip")

    coupon_clip_path = data_paths["source"]["coupon_clip"]
    coupon_clip_usercode_path = data_paths["source"]["coupon_clip_usercode"]

    logging.info("Reading the input file " + coupon_clip_path)
    coupon_clip = (
        job.spark.read.schema(coupon_clip_schema)
        .option("header", "true")
        .csv(coupon_clip_path)
    )

    # validations.validate_table(
    #     job.spark, "source", "coupon_clip", config_validation, coupon_clip
    # )

    coupon_clip_usercodes = job.spark.read.option("header", "true").csv(
        coupon_clip_usercode_path
    )

    coupon_clip_usercodes = coupon_clip_usercodes.dropDuplicates()

    coupon_clip = coupon_clip.join(coupon_clip_usercodes, "usercode", "left")
    coupon_clip = coupon_clip.drop("usercode")
    coupon_clip = coupon_clip.withColumnRenamed("identifier", "MBRSHP_NBR")

    # Join with member extend to add MBRSHP_SID to the table
    member_extended = job.spark.read.parquet(
        data_paths["intermediate"]["member_extended"]
    )
    member_extended = member_extended.select(["MBRSHP_NBR", "MBRSHP_SID"])
    coupon_clip = coupon_clip.join(member_extended, "MBRSHP_NBR", "inner")

    coupon_clip = coupon_clip.select(*coupon_clip_out)

    validations.validate_table(
        job.spark,
        "intermediate",
        "coupon_clip",
        config_validation,
        coupon_clip,
    )

    logging.info(
        "Saving the intermediate file "
        + data_paths["intermediate"]["coupon_clip"]
    )

    coupon_clip.repartition("OFFERACTIVEDATE").write.parquet(
        data_paths["intermediate"]["coupon_clip"],
        partitionBy="OFFERACTIVEDATE",
        mode="overwrite",
    )


job = JobManager("coupon_clip")
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
