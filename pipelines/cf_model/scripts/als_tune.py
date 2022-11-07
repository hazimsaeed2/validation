"""Grid search job for tuning ALS fits on different models."""
from datetime import datetime
from dateutil import parser
import os

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

from pe_member_dna.pipelines.cf_model.lib.cf_io import (
    load_config,
    calculate_filepaths,
)
from pe_member_dna.pipelines.cf_model.lib.models import evaluate_cf_train
from pe_member_dna.pipelines.cf_model.lib.models import (
    scale_by_group,
    purchase_cycle_normalize,
    cat_size_normalize,
)
from pe_member_dna.pipelines.lib.iotools import (
    write_local_to_s3,
    copy_file_to_s3,
)


# (1) ---- ARGUMENTS ---- #

CNF, CFG_PATH = load_config()
PARAMS = dict(
    list(CNF["shared"].items())
    + list(CNF["grid_search"].items())
    + list(CNF["train"].items())
)
PATHS = CNF["paths"]
PARAMS, PATHS = calculate_filepaths(PARAMS, PATHS)
start_dt = parser.parse(PARAMS["start"])
end_dt = parser.parse(PARAMS["end"])
time_period = (end_dt - start_dt).days

CNFG_OUTPUT_PATH = os.path.join(
    PATHS["cf_model_output_path"], "conf", "config_tune.yml"
)
copy_file_to_s3(CFG_PATH, CNFG_OUTPUT_PATH)

# (2) ---- SPARK CONTEXT ---- #
conf = SparkConf().setAppName("cf_tune")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# (3) ---- Grid Search ---- #
print("starting grid search...")
output = []
for reg in PARAMS["lambdas"]:
    for alpha in PARAMS["alphas"]:
        for iteration in PARAMS["iters"]:
            for rank in PARAMS["ranks"]:
                for binary in PARAMS["binarys"]:
                    for mem_norm in PARAMS["member_norms"]:
                        for cat_norm in PARAMS["cat_norms"]:
                            for size_norm in PARAMS["size_norms"]:
                                for inputtype in PARAMS["inputtypes"]:
                                    print("training param set...")
                                    data = spark.read.parquet(PATHS["matrix"])
                                    data = data.select(
                                        "MBRSHP_SID", "CATEGORY_ID", inputtype
                                    )
                                    if binary:
                                        condition = data[inputtype] > 0
                                        data = data.withColumn(
                                            inputtype,
                                            when(condition, 1).otherwise(0),
                                        )
                                    if cat_norm:
                                        data = purchase_cycle_normalize(
                                            spark,
                                            PARAMS,
                                            PATHS,
                                            data,
                                            time_period,
                                        )
                                    if mem_norm:
                                        data = scale_by_group(
                                            data,
                                            PARAMS["inputtype"],
                                            "MBRSHP_SID",
                                            "mean",
                                        )
                                    if size_norm:
                                        data = cat_size_normalize(
                                            spark, PARAMS, PATHS, data
                                        )
                                    data.cache()
                                    als = ALS(
                                        maxIter=iteration,
                                        rank=rank,  # depth of vector to learn
                                        regParam=reg,  # lambda
                                        alpha=alpha,  # preference alpha
                                        userCol="MBRSHP_SID",
                                        itemCol="CATEGORY_ID",
                                        numUserBlocks=10,  # 'partitions' of members
                                        numItemBlocks=10,  # 'partitions' of categories
                                        checkpointInterval=10,  # interval to checkpoint cache()
                                        nonnegative=True,  # nonnegative sets most results to 0
                                        ratingCol=inputtype,
                                        implicitPrefs=True,
                                        coldStartStrategy="drop",
                                        intermediateStorageLevel="DISK_ONLY",
                                        finalStorageLevel="DISK_ONLY",
                                    )

                                    model = als.fit(data)
                                    print("evaluating param set...")

                                    evals = evaluate_cf_train(
                                        spark,
                                        model,
                                        PATHS,
                                        eval_col=PARAMS["eval_col"],
                                        backtest=PARAMS["backtest"],
                                        level=PARAMS["category"],
                                    )
                                    log_line = {
                                        "run_name": PARAMS["run_name"],
                                        "date": datetime.now().strftime(
                                            "%Y%m%d"
                                        ),
                                        "data": PARAMS["data"],
                                        "future": PARAMS["future"],
                                        "data_version": PARAMS["data_version"],
                                        "model_version": PARAMS[
                                            "model_version"
                                        ],
                                        "category": PARAMS["category"],
                                        "num_cats": PARAMS["num_cats"],
                                        "binary": binary,
                                        "backtest": PARAMS["backtest"],
                                        "member_norm": mem_norm,
                                        "cat_norm": cat_norm,
                                        "size_norm": size_norm,
                                        "inputtype": inputtype,
                                        "eval_col": PARAMS["eval_col"],
                                        "rank": rank,
                                        "lambda": reg,
                                        "alpha": alpha,
                                        "iter": iteration,
                                        "backtest": evals["backtest"],
                                        "mean_score": evals["mean_score"],
                                        "count_under05": evals[
                                            "count_under05"
                                        ],
                                        "top_cat": evals["top_cat"],
                                        "top_cat_ct": evals["top_cat_ct"],
                                        "rmse": evals["rmse"],
                                        "hits_1": evals["hits_1"],
                                        "hits_05": evals["hits_05"],
                                        "overall": evals["overall"],
                                        "personal": evals["personal"],
                                        "LUT": evals["lut"],
                                        "cnfg_file": CNFG_OUTPUT_PATH,
                                    }
                                    write_local_to_s3(
                                        log_line,
                                        PATHS["TUNE_LOG"],
                                        mode="append",
                                    )
                                    # force clear cache and garbage collection to prevent resource buildup
                                    data.unpersist()
                                    spark.catalog.clearCache()
                                    sc._jvm.System.gc()
print("done.")

sc.stop()
