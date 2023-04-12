"""Default script for training and validating a single CF model."""
from datetime import datetime
from dateutil import parser
import os

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql.functions import when

from pe_memberdna.model.cf_model.lib.models import (
    predict_pf,
    fit_output_scaler,
    evaluate_cf_train,
)
from pe_memberdna.model.cf_model.lib.models import (
    scale_by_group,
    purchase_cycle_normalize,
    cat_size_normalize,
    cap_outliers,
)
from pe_memberdna.model.cf_model.lib.cf_io import (
    load_config,
    calculate_filepaths,
    save_scaler,
)
from pe_memberdna.lib.iotools import write_local_to_s3, copy_file_to_s3

# (1) ---- ARGUMENTS ---- #

CNF, CFG_PATH = load_config()
PARAMS = dict(list(CNF["shared"].items()) + list(CNF["train"].items()))
PATHS = CNF["paths"]
PARAMS, PATHS = calculate_filepaths(PARAMS, PATHS)

CNFG_OUTPUT_PATH = os.path.join(
    PATHS["cf_model_output_path"], "conf", "config_train.yml"
)
copy_file_to_s3(CFG_PATH, CNFG_OUTPUT_PATH)
print("conf: ", CNFG_OUTPUT_PATH)
print("model: ", PATHS["model"])

start_dt = parser.parse(PARAMS["start"])
end_dt = parser.parse(PARAMS["end"])
time_period = (end_dt - start_dt).days

# (2) ---- SPARK CONTEXT ---- #
conf = SparkConf().setAppName("cf_train")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# (3) ---- READ DATA ---- #
print("(1/5) starting data read...")
data = spark.read.parquet(PATHS["matrix"]).select(
    "MBRSHP_SID", "CATEGORY_ID", PARAMS["inputtype"]
)

print("data has been read.")

# (4) ---- Filter and Split ---- #
print("(2/5) starting data prep...")
if PARAMS["binary"]:
    data = data.withColumn(
        PARAMS["inputtype"],
        when(data[PARAMS["inputtype"]] > 0, 1).otherwise(0),
    )
if PARAMS["cat_norm"]:
    data = purchase_cycle_normalize(spark, PARAMS, PATHS, data, time_period)
if PARAMS["member_norm"]:
    data = scale_by_group(data, PARAMS["inputtype"], "MBRSHP_SID", "mean")
if PARAMS["size_norm"]:
    data = cat_size_normalize(spark, PARAMS, PATHS, data)

data = cap_outliers(data, PARAMS["inputtype"])

data.cache()
n_examples = data.count()
print("training with: ", n_examples, "examples")


# (5) ---- Train Model ---- #

print("(3/5) starting train...")

als = ALS(
    maxIter=PARAMS["iter"],
    rank=PARAMS["rank"],  # depth of vector to learn
    regParam=PARAMS["lambda"],  # lambda
    alpha=PARAMS["alpha"],  # preference alpha
    userCol="MBRSHP_SID",
    itemCol="CATEGORY_ID",
    numUserBlocks=10,  # 'partitions' of members
    numItemBlocks=10,  # 'partitions' of categories
    checkpointInterval=10,  # interval on which to checkpoint the cache()
    nonnegative=True,
    ratingCol=PARAMS["inputtype"],
    implicitPrefs=True,
    coldStartStrategy="drop",
    intermediateStorageLevel="MEMORY_AND_DISK",
    finalStorageLevel="MEMORY_AND_DISK",
    seed=593367982098446717,
)

model = als.fit(data)

# (6) ---- Evaluate Model ---- #

if PARAMS["rmse"]:
    print("(4/5) evaluating...")
    evals = evaluate_cf_train(
        spark,
        model,
        PATHS,
        eval_col=PARAMS["eval_col"],
        backtest=PARAMS["backtest"],
        level=PARAMS["category"],
        params=PARAMS,
    )
    backtest = evals["backtest"]
    rmse = evals["rmse"]
    mean_score = (evals["mean_score"],)
    count_under05 = (evals["count_under05"],)
    top_cat = (evals["top_cat"],)
    top_cat_ct = (evals["top_cat_ct"],)
    hits_1 = evals["hits_1"]
    hits_05 = evals["hits_05"]
    overall = evals["overall"]
    personal = evals["personal"]
    lut = evals["lut"]
    print("backtest: ", backtest)
    print("mean_score: ", mean_score)
    print("count_under05: ", count_under05)
    print("top_cat: ", top_cat)
    print("top_cat_ct: ", top_cat_ct)
    print("rmse: ", rmse)
    print("hits_1: ", hits_1)
    print("hits_05: ", hits_05)
    print("overall: ", overall)
    print("personal: ", personal)
    print("LUT: ", lut)
else:
    backtest = None
    rmse = None
    mean_score = None
    count_under05 = None
    top_cat = None
    top_cat_ct = None
    hits_1 = None
    hits_05 = None
    overall = None
    personal = None
    lut = None

# (7) ---- Save Model ---- #

print("(5/5) saving results ...")
model.write().overwrite().save(PATHS["model"])

# (8) ---- Train and save Scaler ---- #

if PARAMS["scaler"]:
    print("(6/5) training scaler")
    slate = spark.read.parquet(PATHS["slate"])
    pdata = spark.read.parquet(PATHS["matrix"]).select(
        "MBRSHP_SID", "CATEGORY_ID", PARAMS["eval_col"]
    )
    fdata = spark.read.parquet(PATHS["future"]).select(
        "MBRSHP_SID", "CATEGORY_ID", PARAMS["eval_col"]
    )
    cat_lookup = spark.read.parquet(PATHS["cat"])
    predictions = predict_pf(
        model,
        slate,
        pdata,
        fdata,
        cat_lookup=cat_lookup,
        col_of_interest=PARAMS["eval_col"],
    )
    scaler = fit_output_scaler(sc, predictions)
    print("(7/5) saving scaler")
    save_scaler(sc, scaler, PATHS["slate"])

# (9) --- Write Log and Shut Down --- #

if PARAMS["test"] is False:
    log_line = {
        "run_name": PARAMS["run_name"],
        "date": datetime.now().strftime("%Y%m%d"),
        "data": PARAMS["data"],
        "future": PARAMS["future"],
        "data_version": PARAMS["data_version"],
        "model_version": PARAMS["model_version"],
        "category": PARAMS["category"],
        "num_cats": PARAMS["num_cats"],
        "binary": PARAMS["binary"],
        "backtest": PARAMS["backtest"],
        "member_norm": PARAMS["member_norm"],
        "cat_norm": PARAMS["cat_norm"],
        "size_norm": PARAMS["size_norm"],
        "inputtype": PARAMS["inputtype"],
        "scaler": PARAMS["scaler"],
        "evaluate": PARAMS["evaluate"],
        "eval_col": PARAMS["eval_col"],
        "rank": PARAMS["rank"],
        "lambda": PARAMS["lambda"],
        "alpha": PARAMS["alpha"],
        "iter": PARAMS["iter"],
        "backtest": backtest,
        "mean_score": mean_score,
        "count_under05": count_under05,
        "top_cat": top_cat,
        "top_cat_ct": top_cat_ct,
        "rmse": rmse,
        "hits_1": hits_1,
        "hits_05": hits_05,
        "overall": overall,
        "personal": personal,
        "LUT": lut,
        "cnfg_file": CNFG_OUTPUT_PATH,
    }


write_local_to_s3(log_line, PATHS["TRAIN_LOG"], mode="append")


if PARAMS["rmse"]:
    print("Train with rmse is Done")
else:
    print("Train is Done")

sc.stop()
