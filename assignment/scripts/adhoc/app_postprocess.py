"""
Post-process the assignment output to meet the required Quotient format.

This script is not being maintained as digital is on hold and provider is
changing.

To run, you must include the following the in the config:
shared:
      leading_8_Offer_Code: '81729301'  # Quotient BJ's identification code
      offassociationcode_date: '181022' # Date of the assignment, used for leading quotient unique offer association code
"""
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

# ---- Initiate Spark Context --- #
name = "app_assignment_output"
conf = SparkConf().setAppName(name)
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ---- Function Definitions --- #
def format_app_output(
    path_assign, f_member, f_member_extended, f_out, offcode_prefix
):

    """
    Process the app assignment file to output the Quotient format.

    input:
        path_assign: s3 path for the app assignment
        f_member: s3 path for raw member file (.csv)
                    Assumes file structure of at least:
                        'identifier'
                        'usercode'
        offcode_prefix: the first six digits to assign to each
                        OfferAssociationCode (e.g. today's date as yymmdd)
        f_out: s3 path to write the final output to
    output:
        f_out has file structure of:
            ''
    """
    app_assignment = spark.read.csv(path_assign, header=True).withColumn(
        "MBRSHP_SID", col("MBRSHP_SID").cast("int")
    )

    member_lookup = spark.read.csv(f_member, header=True)
    member_lookup = member_lookup.dropDuplicates(
        ["identifier", "usercode"]
    ).withColumnRenamed("identifier", "MBRSHP_NBR")

    app_assignment = app_assignment.join(member_lookup, "MBRSHP_SID", "left")

    # Misc blank columns required by Quotient
    app_assignment = (
        app_assignment.withColumn("TargetCode", lit(""))
        .withColumn("TargetName", lit(""))
        .withColumn("ChannelCode", lit(""))
        .withColumn("Offer Association Score", lit(""))
        .withColumn("Chain Code", lit(""))
    )

    ## create the quotient requested OfferAssociationCode column which is a 15
    ## digit unique number, starting with offcode_prefix and followed by a
    ## unique row number starting with 000000001
    app_assignment.createOrReplaceTempView("df")
    app_assignment = spark.sql(
        'select row_number() over (order by "TargetName") as OfferAssociationCode, * from df'
    )
    app_assignment = app_assignment.withColumn(
        "OfferAssociationCode",
        concat(lit("000000000"), col("OfferAssociationCode")),
    )
    ## get only last 9 digits of OfferAssociationCode
    app_assignment = app_assignment.withColumn(
        "OfferAssociationCode",
        F.regexp_extract(col("OfferAssociationCode"), r"(\d)(.{8}$)", 0),
    ).withColumn(
        "OfferAssociationCode",
        concat(lit(offcode_prefix), col("OfferAssociationCode")),
    )

    ## select only Quotient required columns
    app_assignment = app_assignment.select(
        [
            "Partner Code",
            "identifier",
            "Offer Code",
            "OfferAssociationCode",
            "TargetCode",
            "TargetName",
            "ChannelCode",
            "Offer Association Score",
            "Chain Code",
        ]
    )
    return app_assignment


def write_app_output(assignment, output_path):

    assignment.coalesce(1).write.mode("append").csv(output_path, header=False)


# ---- Function Call --- #
app_assignment = format_app_output(
    path_assign="s3://memberanalytics-data-out/xxxx",
    f_member="s3://memberanalytics-data-out/xxxx",
    f_member_extended="s3://memberanalytics-data-out/xxxx",
    offcode_prefix="181022",
)
write_app_output(
    assignment=app_assignment, output_path="s3://memberanalytics-data-out/xxxx"
)
