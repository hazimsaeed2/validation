from datetime import datetime
from dateutil import parser
import os
import pandas as pd

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
import pyspark.sql.functions as sqlf
from pyspark.sql.window import Window

from pe_member_dna.pipelines.cf_model.lib.models import (
    predict_pf,
    fit_output_scaler,
    evaluate_cf_train,
)
from pe_member_dna.pipelines.cf_model.lib.models import (
    scale_by_group,
    purchase_cycle_normalize,
    cat_size_normalize,
)
from pe_member_dna.pipelines.cf_model.lib.cf_io import (
    load_config,
    calculate_filepaths,
    save_scaler,
)
from pe_member_dna.pipelines.lib.iotools import (
    write_local_to_s3,
    read_s3_to_local,
    copy_file_to_s3,
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

current_output_path = os.path.join(OUTPUT_PATH, "QC", RUN_NAME)

conf = SparkConf().setAppName("cf_combine")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")


columns = ["MBRSHP_SID", "CATEGORY_NAME", "CATEGORY_ID", "prediction"]
lambdas = [f"hs_ind_lambda{lmbd}" for lmbd in CNF["predict"]["lambda"]]
cf_data = spark.read.parquet(input_path)


windowSpec = Window.partitionBy("MBRSHP_SID").orderBy("col2")
top_cat_list = []
for lambda_ in lambdas:
    windowSpec = Window.partitionBy("MBRSHP_SID", lambda_).orderBy(
        sqlf.desc("prediction")
    )
    top_cat = (
        cf_data.select(*columns, lambda_)
        .withColumn("rank", sqlf.row_number().over(windowSpec))
        .filter(sqlf.col("rank") == 1)
        .groupBy("CATEGORY_ID", "CATEGORY_NAME", lambda_)
        .agg(sqlf.countDistinct("MBRSHP_SID").alias("mbrs_count"))
        .withColumn(
            "cat_rank",
            sqlf.row_number().over(
                Window.partitionBy(lambda_).orderBy(sqlf.desc("mbrs_count"))
            ),
        )
        .filter(sqlf.col("cat_rank") <= 5)
        .withColumn("lambda", sqlf.lit(lambda_))
        .withColumnRenamed(lambda_, "hook_stretch")
        .toPandas()
    )
    top_cat_list.append(top_cat)


top_cat_df = pd.concat(top_cat_list)


top_cat_df["lambda"] = top_cat_df["lambda"].apply(lambda x: x[13:])
print(top_cat_df.head(5))


print(current_output_path)
all_top_cat_path = os.path.join(OUTPUT_PATH, "QC", "all", "top_cat.csv")
print(all_top_cat_path)
write_local_to_s3(
    top_cat_df, os.path.join(current_output_path, "top_cat", "top_cat.csv")
)
print(top_cat_df.head())
try:
    previous = read_s3_to_local(all_top_cat_path)
    all_top_cat = previous.merge(
        top_cat_df.drop(columns=["CATEGORY_ID", "mbrs_count"]),
        on=["cat_rank", "lambda", "hook_stretch"],
        how="left",
    )
    print(all_top_cat.head())
    write_local_to_s3(all_top_cat, all_top_cat_path)
except:
    current_output_sub = top_cat_df.drop(columns=["CATEGORY_ID", "mbrs_count"])
    current_output_sub.rename(
        columns={"CATEGORY_NAME": f"CATEGORY_NAME_{RUN_NAME}"}, inplace=True
    )
    print(current_output_sub.head())
    write_local_to_s3(current_output_sub, all_top_cat_path)
