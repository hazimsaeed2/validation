"""
Ad Hoc job to assign email basket offers based on n week average spend.
It is advised to not use this method - it is here just for legacy purposes.
Instead, use the pipeline directly and basket slotting.

To use this script, the following must be added to the config:
shared:
      n_week: 3
      test_proportion: 0.5 # percentage group in the test group
      seed: 8080 # for random split test and control
      cpn_5_off_50_code: 217389
      cpn_10_off_100_code: 217392
      cpn_15_off_150_code: 217393
      cpn_20_off_200_code: 217404
      cpn_25_off_250_code: 217405
"""

import numpy as np
from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col, rank
from pyspark.sql.window import Window

from assn_io import calculate_filepaths, load_config

# ---- Initiate Spark Context --- #
name = "Ad Hoc Email Assignment"

spark = SparkSession.builder.appName(name).getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

# ---- Function Definitions --- #
def get_avg_n_week_spend(
    Transaction_path,
    Member_lookup_path,
    member_dna_path,
    dna_FISCAL_WEEK_END,
    n_week,
    start_date,
    end_date,
):

    ## read and process transaction
    non_merch_codes = ["402030190", "402030191", "203010098", "B01010003"]

    Transaction = spark.read.parquet(Transaction_path)

    n_week_spend = (
        Transaction[~Transaction.MC_CD.isin(non_merch_codes)]
        .withColumn("SALES_QTY", col("SALES_QTY").cast("float"))
        .filter(col("SALES_QTY") > 0)
        .filter(Transaction["PURCH_DT"].between(start_date, end_date))
        .withColumn("MBRSHP_SID", col("MBRSHP_SID").cast("int"))
        .groupBy(["MBRSHP_SID"])
        .agg(sum("EXTENDED_PRC_AMT"))
        .alias("total_spend")
    )

    ## for performance reason presist, o.w. out of memory
    n_week_spend.persist(StorageLevel.DISK_ONLY)
    n_week_spend.count()

    ## read and process Member_lookup, Member_extended
    Member_lookup = (
        spark.read.csv(Member_lookup_path, header=True)
        .withColumn("MBRSHP_SID", col("MBRSHP_SID").cast("int"))
        .dropDuplicates(["MBRSHP_SID"])
    )

    ## read and process member_dna
    DNA = (
        spark.read.parquet(member_dna_path)
        .where(col("FISCAL_WEEK_END") == dna_FISCAL_WEEK_END)
        .withColumn("MBRSHP_SID", col("MBRSHP_SID").cast("int"))
        .dropDuplicates(["MBRSHP_SID"])
    )

    ## merge the target_member number and sid  with the member_dna
    NBR_SID_DNA = Member_lookup.join(DNA, "MBRSHP_SID", "left")

    NBR_SID_DNA.persist(StorageLevel.DISK_ONLY)
    NBR_SID_DNA.count()

    ## Join member list with transaction to only include transactions related to target members
    NBR_SID_DNA_n_week_spend = (
        NBR_SID_DNA.join(n_week_spend, "MBRSHP_SID", "left")
        .fillna(0, subset=["TENURE", "sum(EXTENDED_PRC_AMT)"])
        .withColumn(
            "n_week_spend",
            when(
                col("TENURE") > "365",
                col("sum(EXTENDED_PRC_AMT)") / 52 * n_week,
            ).otherwise(
                col("sum(EXTENDED_PRC_AMT)")
                / 52
                * n_week
                * 365
                / col("TENURE")
            ),
        )
        .fillna(0, subset=["TENURE", "sum(EXTENDED_PRC_AMT)", "n_week_spend"])
    )

    return NBR_SID_DNA_n_week_spend


def split_test_control(assignment_file, test_proportion, seed="8080"):
    test_group, control_group = assignment_file.randomSplit(
        [test_proportion, 1 - test_proportion], seed=seed
    )
    test_group = test_group.withColumn("Cell_name", lit("Test"))
    control_group = control_group.withColumn("Cell_name", lit("Control"))
    test_and_control = test_group.union(control_group)
    return test_and_control


def assign_coupon_number_description(
    assignment_file,
    cpn_5_off_50,
    cpn_10_off_100,
    cpn_15_off_150,
    cpn_20_off_200,
    cpn_25_off_250,
):

    assignment_file = assignment_file.withColumn(
        "coupon_code",
        when(col("n_week_spend") < 75, cpn_5_off_50)
        .when(
            (col("n_week_spend") >= 75) & (col("n_week_spend") < 125),
            cpn_10_off_100,
        )
        .when(
            (col("n_week_spend") >= 125) & (col("n_week_spend") < 175),
            cpn_15_off_150,
        )
        .when(
            (col("n_week_spend") >= 175) & (col("n_week_spend") < 225),
            cpn_20_off_200,
        )
        .when((col("n_week_spend") >= 225), cpn_25_off_250),
    )

    assignment_file = assignment_file.withColumn(
        "coupon_description",
        when(col("n_week_spend") < 75, "FY19_OctPersonalizedEmailTest_5off50")
        .when(
            (col("n_week_spend") >= 75) & (col("n_week_spend") < 125),
            "FY19_OctPersonalizedEmailTest_10off100",
        )
        .when(
            (col("n_week_spend") >= 125) & (col("n_week_spend") < 175),
            "FY19_OctPersonalizedEmailTest_15off150",
        )
        .when(
            (col("n_week_spend") >= 175) & (col("n_week_spend") < 225),
            "FY19_OctPersonalizedEmailTest_20off200",
        )
        .when(
            (col("n_week_spend") >= 225),
            "FY19_OctPersonalizedEmailTest_25off250",
        ),
    )
    return assignment_file


def save_output(file, output_path, type):
    if type == "full":
        file.coalesce(1).write.mode("append").csv(output_path, header=True)
    elif type == "scott":
        file.where(col("Cell_name") == "Test").select(
            ["MBRSHP_NBR", "coupon_code", "coupon_description"]
        ).coalesce(1).write.mode("append").csv(output_path, header=True)
    return


# --- Load config ---- #


cnf, _ = load_config()
PARAMS = dict(list(cnf["shared"].items()) + list(cnf["assignment"].items()))
PATHS = cnf["paths"]
PARAMS, PATHS = calculate_filepaths(PARAMS, PATHS)

# ---- Function Call --- #
member_n_week_sales = get_avg_n_week_spend(
    Transaction_path=PATHS["TRANSACTIONS_PATH"],
    Member_lookup_path=PATHS["EMAIL_MEMBER_LIST_PATH"],
    member_dna_path=PATHS["CUBE"],
    dna_FISCAL_WEEK_END=PARAMS["dna_FISCAL_WEEK_END"],
    n_week=PARAMS["n_week"],
    start_date=PARAMS["start_date"],
    end_date=PARAMS["end_date"],
)
member_n_week_sales.persist(StorageLevel.DISK_ONLY)
member_n_week_sales.count()

member_n_seek_sales_with_test_control = split_test_control(
    assignment_file=member_n_week_sales,
    test_proportion=PARAMS["test_proportion"],
    seed=PARAMS["seed"],
)
member_n_seek_sales_with_test_control.persist(StorageLevel.DISK_ONLY)
member_n_seek_sales_with_test_control.count()
full_member_assignment = assign_coupon_number_description(
    assignment_file=member_n_seek_sales_with_test_control,
    cpn_5_off_50=PARAMS["cpn_5_off_50_code"],
    cpn_10_off_100=PARAMS["cpn_10_off_100_code"],
    cpn_15_off_150=PARAMS["cpn_15_off_150_code"],
    cpn_20_off_200=PARAMS["cpn_20_off_200_code"],
    cpn_25_off_250=PARAMS["cpn_25_off_250_code"],
)
full_member_assignment.persist(StorageLevel.DISK_ONLY)
full_member_assignment.count()


save_output(full_member_assignment, PATHS["EMAIL_OUTPUT_PATH"], type="full")
print("full_email assignment saved")
save_output(full_member_assignment, PATHS["EMAIL_OUTPUT_PATH"], type="scott")
print("scott_file saved")
