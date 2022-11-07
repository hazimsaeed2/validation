""".Calculates decile shifts between two bbm scores and also calculates performance of the current bbm score"""
import datetime
from datetime import timedelta
import os
import pandas as pd
import re

from pyspark import SparkContext, SparkConf
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

from pe_member_dna.pipelines.lib.iotools import (
    write_local_to_s3,
    read_s3_to_local,
    split_path_bucket_key,
    is_s3_file,
)
from pe_member_dna.pipelines.lib.utils import lowercase_col_names
from pe_member_dna.pipelines.BBM_propensity_score.QC.lib.bbm_io import (
    load_config,
    get_past_file_date,
    map_file_date,
    get_matching_s3_keys,
)


CONF = SparkConf().setAppName("decile_summary")
SC = SparkContext(conf=CONF)
SPARK = SparkSession.builder.getOrCreate()
SPARK.sparkContext.setLogLevel("WARN")

PARAMS = load_config()
PATHS = PARAMS["paths"]

PROBA_THRESHOLD = 0.50  # a bbm propensity score threshold to determin if a person is a redeemer or not
# 0.50 is chosen arbitrarily because it is in the middle


BUCKET, KEY = split_path_bucket_key(PATHS["BBM_SCORE"])
LIST_OF_SCORE_FILES = get_matching_s3_keys(
    bucket=BUCKET, path="MODELDATA/BBM_PROPENSITY/BBM_SCORES/", contains="MU"
)

SCORE_PATH_DICT = map_file_date(LIST_OF_SCORE_FILES)
CURRENT_DATE = re.search(r"\d{4}-\d{2}-\d{2}", PATHS["BBM_SCORE"]).group()
EARLIEST_COUPON_DATE = (
    (
        datetime.datetime.strptime(CURRENT_DATE, "%Y-%m-%d")
        - timedelta(PARAMS["coupon_window"])
    )
    .date()
    .strftime("%Y-%m-%d")
)

BBM_OUTPUT_PATH = os.path.join(PATHS["BBM_SUMMARY_OUTPUT"], CURRENT_DATE)
ALL_SCORE_SUMMARY_PATH = os.path.join(
    PATHS["BBM_SUMMARY_OUTPUT"],
    "all_scoress",
    "all_scores_deciles_summary.csv",
)
ALL_SCORE_SUMMARY_PATH_AUC = os.path.join(
    PATHS["BBM_SUMMARY_OUTPUT"], "auc", "auc.csv"
)

BBM_SCORE_DATE = get_past_file_date(
    LIST_OF_SCORE_FILES, CURRENT_DATE, n_weeks_apart=PARAMS["bbm_score_lag"]
)

BBM_SCORE_PATH = SCORE_PATH_DICT[BBM_SCORE_DATE]
BBM_SCORE = SPARK.read.csv(BBM_SCORE_PATH, header=True)
DETAILS = SPARK.read.parquet(PATHS["DETAILS"]).select(
    "MBRSHP_SID", "PURCH_DT", "PURCH_HDR_ID", "DISCOUNT_TYPE_CD"
)
DETAILS = lowercase_col_names(DETAILS)
PAYMENTS = SPARK.read.parquet(PATHS["PAYMENTS"]).select(
    "MBRSHP_SID", "PURCH_HDR_ID", "TENDER_TYPE_CD", "PURCH_DT", "CPN_NBR"
)
PAYMENTS = lowercase_col_names(PAYMENTS)
MBR_EXTENDED = SPARK.read.parquet(PATHS["MBR_EXTENDED"]).select(
    "MBRSHP_SID", "MBRSHP_NBR"
)


def get_coupon_redeemers(
    detail, payment, coupons, earliest_coupon_date, current_date
):
    """
    Determines if a member redeemed a ZCOU or ZPAP coupons in the specified
    period of time
    Parameters:
        detail(dataframe) : details table
        payment(dataframe): payment table
        earliest_coupon_date(date): the earlies day a coupon could be redeemed
        current_date(date): the closest date for which we have a bbm score
    Returns:
        Outputs(dataframe): table of members with indicator wheather they redeemed a coupon(ZOU or ZPAP)
        in the last 21 days.
        Note: the 21 days is a parameter
    """
    coupon_detail = (
        detail.filter(F.col("discount_type_cd").isin(coupons))
        .filter(F.col("purch_dt").between(earliest_coupon_date, current_date))
        .select("mbrshp_sid", "purch_dt")
    )

    coupon_payment = (
        payment.filter(F.col("tender_type_cd").isin(coupons))
        .filter(F.col("purch_dt").between(earliest_coupon_date, current_date))
        .select("mbrshp_sid", "purch_dt")
    )

    redeemers_all = (
        coupon_detail.unionAll(coupon_payment)
        .select("mbrshp_sid")
        .drop_duplicates()
        .withColumn("redeemedAtLeastOnce", F.lit(1))
    )
    return redeemers_all


REDEEMERS = get_coupon_redeemers(
    DETAILS, PAYMENTS, PARAMS["coupons"], EARLIEST_COUPON_DATE, CURRENT_DATE
)


DECILE_REDEMPTION = (
    BBM_SCORE.join(MBR_EXTENDED, "mbrshp_nbr", "left")
    .join(REDEEMERS, "mbrshp_sid", "left")
    .fillna({"redeemedAtLeastOnce": 0})
    .withColumn("probability", F.col("score").cast("float"))
    .withColumn("label", F.col("redeemedAtLeastOnce"))
    .withColumn(
        "predicted_label",
        F.when(F.col("probability") >= PROBA_THRESHOLD, 1).otherwise(0),
    )
    .withColumn(
        "true_positive",
        F.when(
            (F.col("label") == 1) & (F.col("predicted_label") == 1), 1.0
        ).otherwise(0.0),
    )
    .withColumn(
        "false_positive",
        F.when(
            (F.col("label") == 0) & (F.col("predicted_label") == 1), 1.0
        ).otherwise(0.0),
    )
    .withColumn(
        "true_negative",
        F.when(
            (F.col("label") == 0) & (F.col("predicted_label") == 0), 1.0
        ).otherwise(0.0),
    )
    .withColumn(
        "false_negative",
        F.when(
            (F.col("label") == 1) & (F.col("predicted_label") == 0), 1.0
        ).otherwise(0.0),
    )
)

PREDICTION_SUMMARY = (
    DECILE_REDEMPTION.select(
        "true_positive", "false_positive", "true_negative", "false_negative"
    )
    .groupBy()
    .agg(
        F.sum("true_positive").alias("TP"),
        F.sum("false_positive").alias("FP"),
        F.sum("false_negative").alias("FN"),
        F.sum("true_negative").alias("TN"),
    )
    .toPandas()
)

TP = PREDICTION_SUMMARY.loc[0, "TP"]
FP = PREDICTION_SUMMARY.loc[0, "FP"]
TN = PREDICTION_SUMMARY.loc[0, "TN"]
FN = PREDICTION_SUMMARY.loc[0, "FN"]

ACCURACY = (TP + TN) / (TP + FP + TN + FN)
PRECISION = TP / (TP + FP)
RECALL = TP / (TP + FN)
F1_SCORE = 2 * (PRECISION * RECALL) / (PRECISION + RECALL)

PREDICTION_AND_LABELS = DECILE_REDEMPTION.select(
    "label", "probability"
).rdd.map(lambda row: (row["probability"], float(row["label"])))
METRICS = BinaryClassificationMetrics(PREDICTION_AND_LABELS)

ROC_DF = pd.DataFrame(
    [
        [
            BBM_SCORE_DATE,
            EARLIEST_COUPON_DATE,
            CURRENT_DATE,
            METRICS.areaUnderROC,
            ACCURACY,
            PRECISION,
            F1_SCORE,
            RECALL,
        ]
    ],
    columns=[
        "bbm_score_date",
        "validation_start_date",
        "validation_end_data",
        "AUC",
        "ACCURACY",
        "PRECISION",
        "F1_SCORE",
        "RECALL",
    ],
)

ROC_DF_BUCKET, ROC_DF_KEY = split_path_bucket_key(ALL_SCORE_SUMMARY_PATH_AUC)
if is_s3_file(ROC_DF_BUCKET, ROC_DF_KEY):
    PREVIOUS_AUC = read_s3_to_local(ALL_SCORE_SUMMARY_PATH_AUC)
    ALL_AUC = pd.concat([ROC_DF, PREVIOUS_AUC])
    ALL_AUC = ALL_AUC.drop_duplicates("bbm_score_date", keep="first")
    write_local_to_s3(ALL_AUC, ALL_SCORE_SUMMARY_PATH_AUC)
else:
    write_local_to_s3(ROC_DF, ALL_SCORE_SUMMARY_PATH_AUC)


DECILE_REDEMPTION_SUMMARY = (
    DECILE_REDEMPTION.groupBy("decile")
    .agg(
        F.countDistinct("mbrshp_sid").alias("mbrs"),
        F.sum("redeemedAtLeastOnce").alias("redemption"),
    )
    .withColumn("redemption_perc", (F.col("redemption") / F.col("mbrs")) * 100)
    .withColumn(
        "redemption_perc_" + CURRENT_DATE, F.round("redemption_perc", 2)
    )
    .withColumn("decile", F.col("decile").cast("int"))
    .drop("redemption_perc")
    .orderBy("decile")
    .toPandas()
)

write_local_to_s3(
    DECILE_REDEMPTION_SUMMARY,
    os.path.join(
        BBM_OUTPUT_PATH,
        "decile_summary_scored_on_{}_validation_period_{}_to_{}.csv".format(
            BBM_SCORE_DATE, EARLIEST_COUPON_DATE, CURRENT_DATE
        ),
    ),
)
BUCKET, KEY = split_path_bucket_key(ALL_SCORE_SUMMARY_PATH)
if is_s3_file(BUCKET, KEY):
    PREVIOUS_DECILE_SUMMARY = read_s3_to_local(ALL_SCORE_SUMMARY_PATH)
    if "redemption_perc_" + CURRENT_DATE in PREVIOUS_DECILE_SUMMARY.columns:
        PREVIOUS_DECILE_SUMMARY.drop(
            columns="redemption_perc_" + CURRENT_DATE, axis=1, inplace=True
        )
    ALL_SUMMARY = PREVIOUS_DECILE_SUMMARY.merge(
        DECILE_REDEMPTION_SUMMARY[
            ["decile", "redemption_perc_" + CURRENT_DATE]
        ],
        on="decile",
        how="outer",
    )
    write_local_to_s3(ALL_SUMMARY, ALL_SCORE_SUMMARY_PATH)
else:
    write_local_to_s3(
        DECILE_REDEMPTION_SUMMARY[
            ["decile", "redemption_perc_" + CURRENT_DATE]
        ],
        ALL_SCORE_SUMMARY_PATH,
    )

SPARK.stop()
