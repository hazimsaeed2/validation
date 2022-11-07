"""Default script for making predictions with an ALSmodel and saving them to disk.

NOTE: performance is quite slow for brand data (~2h) given data size

"""
from datetime import datetime
from dateutil import parser
from math import ceil
import os

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import StorageLevel
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession

from pe_member_dna.pipelines.cf_model.lib.cf_io import read_scaler
from pe_member_dna.pipelines.lib.iotools import (
    write_local_to_s3,
    copy_file_to_s3,
)
from pe_member_dna.pipelines.cf_model.lib.busrules import (
    limit_based_on_prior_trips,
    limit_based_on_score,
)
from pe_member_dna.pipelines.cf_model.lib.busrules import (
    apply_bus_rules,
    label_hook_stretch,
)
from pe_member_dna.pipelines.cf_model.lib.models import apply_output_scaling
from pe_member_dna.pipelines.cf_model.lib.models import predict_pf
from pe_member_dna.pipelines.cf_model.lib.models import add_cf_reg
from pe_member_dna.pipelines.cf_model.lib.cf_io import (
    load_config,
    calculate_filepaths,
)
from pe_member_dna.pipelines.lib.utils import top_n

# (1) ---- ARGUMENTS ---- #

CNF, CFG_PATH = load_config()
PARAMS = dict(list(CNF["shared"].items()) + list(CNF["predict"].items()))
PATHS = CNF["paths"]
PARAMS, PATHS = calculate_filepaths(PARAMS, PATHS)
PARAMS["pred_date"] = parser.parse(PARAMS["pred_date"])

CNFG_OUTPUT_PATH = os.path.join(
    PATHS["current_prediction_prediction_path"], "conf", "config_predict.yml"
)
copy_file_to_s3(CFG_PATH, CNFG_OUTPUT_PATH)

# (2) ---- SPARK CONTEXT ---- #

conf = SparkConf().setAppName("cf_predict")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# (3) ---- READ DATA ---- #

print("(1/3) reading data...")
model = ALSModel.read().load(PATHS["model"])
slate = spark.read.parquet(PATHS["slate"])
pdata = spark.read.parquet(PATHS["matrix"])
if PARAMS["future_run"]:
    fdata = spark.read.parquet(PATHS["future"])
cat_lookup = spark.read.parquet(PATHS["cat"])
if PARAMS["test"]:
    cube = spark.read.csv(PATHS["CUBE"], header=True)
else:
    cube = spark.read.parquet(PATHS["CUBE"])

# (4) ---- PREDICT ---- #

print("(2/3) making predictions...")
if PARAMS["future_run"]:
    predictions = predict_pf(
        PARAMS,
        model,
        slate,
        pdata,
        fdata,
        cat_lookup=cat_lookup,
        col_of_interest="TRIPS",
    )
else:
    predictions = predict_pf(
        PARAMS,
        model,
        slate,
        pdata,
        cat_lookup=cat_lookup,
        col_of_interest="TRIPS",
    )
predictions = predictions.repartition("CATEGORY_ID")
predictions.cache()
print("recommending on ", predictions.count(), " raw predictions")

# scale if appropriate
if PARAMS["scale"]:
    scaler_path = PATHS["scaler"]
    scaler = read_scaler(sc, scaler_path)
    predictions = apply_output_scaling(predictions, "prediction", scaler)
# apply business rules to generate output scores
predictions = apply_bus_rules(
    spark,
    PATHS,
    predictions,
    cube,
    "prediction",
    PARAMS["pred_date"],
    PARAMS["category"],
    PARAMS["rel_weighting"],
)

# label hook-stretch
predictions = label_hook_stretch(spark, PARAMS, PATHS, predictions)

# add predictions with empirical insight
predictions = add_cf_reg(spark, PARAMS, predictions)

# subset, cutoffs, score limits, etc...
if PARAMS["stretch"]:
    predictions = predictions[predictions.hs_ind == "stretch"]
if PARAMS["hook"]:
    predictions = predictions[predictions.hs_ind == "hook"]
if PARAMS["subset"] is not None:
    print(PARAMS["subset"])
    predictions = predictions[predictions.MBRSHP_SID.isin(PARAMS["subset"])]
if PARAMS["cutoff"] is not None:
    predictions = limit_based_on_prior_trips(
        spark, PATHS, predictions, PARAMS["data"], PARAMS["cutoff"]
    )
if PARAMS["score_limit"] is not None:
    predictions = limit_based_on_score(
        predictions, "prediction", PARAMS["score_limit"]
    )

predictions = top_n(
    predictions, PARAMS["recommendations"], "prediction", "MBRSHP_SID"
)
if PARAMS["future_run"]:
    predictions = predictions.drop("PAST_TRIPS", "FUTURE_TRIPS")
else:
    predictions = predictions.drop("PAST_TRIPS")
predictions.cache()
print("made ", predictions.count(), "final predictions")

# (5) -- WRITE RESULTS ---- #

print("(3/3) Writing predictions...")
if PARAMS["test"]:
    predictions_path = PATHS["current_prediction_prediction_path"] + ".csv"
    predictions = predictions.repartition(1)
    df = predictions.toPandas()
    write_local_to_s3(df, predictions_path, mode="overwrite")
else:
    predictions.repartition(int(ceil(predictions.count() / 10000000)))
    predictions.persist(StorageLevel.DISK_ONLY)
    pq_path = PATHS["current_prediction_prediction_path"] + "/PARQUET"
    print("writing parquet version")
    predictions.write.parquet(pq_path, mode="overwrite")

    # (9) --- Write Log and Shut Down --- #

    print("writing logs")
    log_line = {
        "run_name": PARAMS["run_name"],
        "date": datetime.now(),
        "data": PARAMS["data"],
        "future": PARAMS["future"],
        "data_version": PARAMS["data_version"],
        "model_version": PARAMS["model_version"],
        "category": PARAMS["category"],
        "num_cats": PARAMS["num_cats"],
        "recommendations": PARAMS["recommendations"],
        "lambda": PARAMS["lambda"],
        "cutoff": PARAMS["cutoff"],
        "scale": PARAMS["scale"],
        "score_limit": PARAMS["score_limit"],
        "stretch": PARAMS["stretch"],
        "hook": PARAMS["hook"],
        "subset": PARAMS["subset"],
        "pred_date": PARAMS["pred_date"],
        "cnfg_file": CNFG_OUTPUT_PATH,
    }
    write_local_to_s3(log_line, PATHS["PREDICT_LOG"], mode="append")

print(
    "CF MODEL PREDICTIONS CAN BE FOUND AT "
    + PATHS["current_prediction_prediction_path"]
)
print("done")
sc.stop()

if PARAMS["future_run"]:
    print("future prediction is Done")
else:
    print("prediction is Done")
