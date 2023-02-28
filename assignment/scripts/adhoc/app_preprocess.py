"""
Pre-process coupon and member files to meet the required format for
assignment pipeline and prepare lookup files for final output post-processing.

This script is not being maintained as digital is on hold and provider is
changing.

To run, you must include the following the in the config:
shared:
      leading_8_Offer_Code: '81729301'  # Quotient BJ's identification code
      offassociationcode_date: '181022' # Date of the assignment, used for leading quotient unique offer association code
"""
import boto3
import findspark

findspark.init()
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark import StorageLevel
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
import numpy as np

# ---- Initiate Spark Context --- #
name = "Convert_app_cpn_members"
conf = SparkConf().setAppName(name)
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ---- Function Definitions --- #
def process_coupon(f_raw, f_pipe, f_lkup, lead_cd="81729301"):
    """
    Process f_raw (raw app coupon file) to meet the required structure for the
    assignment pipeline (f_pipe).

    Additionally, create a coupon lookup file (f_lkup) to be used during
    post-processing. Quotient requires the coupon numbers in a different format
    than the pipeline, so each app coupon will have two unique identifiers,
    one for pipeline and one for quotient.

    input:
        f_raw: s3 path to retrieve the raw coupon file (.csv)
                Assumes file structure of:
                    'PMR Offer ID'
                    'Coupon Article'
                    'Bonus Buy'
                    'offer_id'
                    'cpn_class_id'
                    'cpn_ah4_cd' # assumes one of ah4 / ah5 is empty
                    'cpn_ah5_cd'
                    'cpn_desc'
                    'cpn_dollar_off'
                    'cpn_dollar_threshold'
                    'cpn_start'
                    'cpn_end'
        f_pipe: s3 path to place the pipeline formatted coupon file (.csv)
        f_lkup: s3 path to place the coupon processing lookup file (.csv)
        lead_cd: The lead_code is provided by Quotient and is used by them to
                 identify the offer as belonging to BJs.

    output:
        f_pipe has file structure of:
            'cpn_nbr', StringType()
            'offer_id', LongType()
            'cpn_class_id', LongType()
            'cpn_ah4_cd', LongType()
            'cpn_ah5_cd', LongType()
            'cpn_desc', StringType()
            'cpn_dollar_off', DoubleType()
            'cpn_dollar_threshold', DoubleType()
        f_lkup has file structure of:
            'true_coupon_nbr' (Quotient coupon number)
            'cpn_nbr' (pipeline coupon number)
    """
    offer_code = spark.read.csv(f_raw, header=True)

    offer_code_component = [
        "leading_8_Offer_Code",
        "part2_offer_code",
        "part3_offer_code",
        "part4_offer_code",
    ]

    offer_code = (
        offer_code.withColumn("leading_8_Offer_Code", F.lit(lead_cd))
        .withColumn(
            "part2_offer_code",
            F.regexp_replace(col("PMR Offer ID"), r"(?=\d)0(?=\d{5})", ""),
        )
        .withColumn("part3_offer_code", col("Coupon Article").cast("string"))
        .withColumn("leading_2_zeros", F.lit("00"))
        .withColumn(
            "part4_offer_code",
            F.concat(col("leading_2_zeros"), col("Bonus buy")),
        )
        .withColumn(
            "quotient_cpn_nbr",
            F.concat_ws(
                "-", *[F.coalesce(c, F.lit("*")) for c in offer_code_component]
            ),
        )
        .withColumn("offer_id", col("offer_id").cast("long"))
        .withColumn("cpn_class_id", col("cpn_class_id").cast("long"))
        .withColumn("cpn_ah4_cd", col("cpn_ah4_cd").cast("long"))
        .withColumn("cpn_ah5_cd", col("cpn_ah5_cd").cast("long"))
        .withColumn("cpn_dollar_off", col("cpn_dollar_off").cast("double"))
        .withColumn(
            "cpn_dollar_threshold", col("cpn_dollar_threshold").cast("double")
        )
        .withColumn("idx", F.monotonically_increasing_id())
        .withColumn(
            "pipeline_cpn_nbr", F.row_number().over(Window.orderBy("idx")) + 1
        )
        .select(
            "quotient_cpn_nbr",
            "offer_id",
            "cpn_class_id",
            "cpn_ah4_cd",
            "cpn_ah5_cd",
            "cpn_desc",
            "cpn_dollar_off",
            "cpn_dollar_threshold",
            "pipeline_cpn_nbr",
        )
    )

    lkup = offer_code.select(
        offer_code.quotient_cpn_nbr.alias("true_coupon_nbr"),
        offer_code.pipeline_cpn_nbr.alias("cpn_nbr"),
    )
    pipeline = offer_code.select(
        offer_code.pipeline_cpn_nbr.alias("cpn_nbr"),
        "offer_id",
        "cpn_class_id",
        "cpn_ah4_cd",
        "cpn_ah5_cd",
        "cpn_dollar_off",
        "cpn_dollar_threshold",
    )

    lkup.coalesce(1).write.mode("overwrite").csv(f_lkup, header=True)
    pipeline.coalesce(1).write.mode("overwrite").csv(f_pipe, header=True)


def process_app_member(f_raw, f_pipe):
    """
    Process f_raw (raw app member look up file) to meet the required structure
    for the assignment pipeline (f_pipe).

    input:
        f_raw: s3 path to retrieve raw member lookup file (.csv)
                Assumes file structure of at least:
                    'identifier'
        f_pipe: s3 path to place the pipeline formatted member file (.csv)
    output:
        f_pipe has file structure of:
            'MBRSHP_NBR'
    """
    mbr = spark.read.csv(f_raw, header=True)

    mbr = (
        mbr.select(["identifier"])
        .withColumnRenamed("identifier", "MBRSHP_NBR")
        .dropDuplicates(["MBRSHP_NBR"])
    )
    mbr.coalesce(1).write.mode("overwrite").csv(f_pipe, header=True)


# ---- Function Call --- #
process_coupon(
    f_raw="s3://memberanalytics-data-out/ASSIGNMENTS/campaigns/APP1_PRACTICE/raw_inputs/appNovDec_category_coupons.csv",
    f_lkup="s3://memberanalytics-data-out/ASSIGNMENTS/campaigns/APP1_PRACTICE/intermediate/lkup_coupon",
    f_pipe="s3://memberanalytics-data-out/ASSIGNMENTS/campaigns/APP1_PRACTICE/input_coupon_list",
)

process_app_member(
    f_raw="s3://memberanalytics-data-out/ASSIGNMENTS/campaigns/APP1_PRACTICE/raw_inputs/bjs_loyaltynumbers_usercodes_11022018.csv",
    f_pipe="s3://memberanalytics-data-out/ASSIGNMENTS/campaigns/APP1_PRACTICE/input_mail_list",
)
