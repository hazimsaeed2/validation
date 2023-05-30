"""Script for combining two sets of predictions."""

from datetime import datetime
from math import ceil
import os
import re

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import StorageLevel
from pyspark.sql import SparkSession

from pe_memberdna.model.cf_model.lib.cf_io import (
    load_config,
    calculate_filepaths,
)
from pe_memberdna.lib.utils import apply_unionall
from pe_memberdna.lib.iotools import (
    write_local_to_s3,
    copy_file_to_s3,
)


# (1) ---- ARGUMENTS ---- #

CNF, CFG_PATH = load_config()
PARAMS = dict(list(CNF["shared"].items()) + list(CNF["combine"].items()))
PATHS = CNF["paths"]
PARAMS, PATHS = calculate_filepaths(PARAMS, PATHS)
# (2) ---- SPARK CONTEXT ---- #

conf = SparkConf().setAppName("cf_combine")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()
# spark.sparkContext.setLogLevel("WARN")

# (3) ---- READ DATA ---- #
print("(1/3) reading data...")
input_type = PARAMS["type"]
if input_type.lower() == "prediction":
    base_path = PATHS["current_prediction_prediction_path"]
else:
    base_path = PATHS["DATA"]

items = PARAMS["items"]
dfs = []
col_sets = []
for item in items:
    path = re.sub(PARAMS["category"], item, base_path)
    df = spark.read.parquet(path + "/PARQUET")
    dfs.append(df)
    col_sets.append(set(df.columns))

# (4) ---- COMBINE ---- #

print("(2/3) combining data...")
# calculate intersection of all column sets
base_set = col_sets[0]
overlap_list = list(base_set.intersection(*col_sets[1:]))

# make a big ol' list of subsetted dfs to join
union_dfs = []
for df in dfs:
    union_dfs.append(df.select(*overlap_list))

combined_df = apply_unionall(*union_dfs)

# (5) -- WRITE OUTPUT --- #

print("(3/3) writing data...")
combined_df = combined_df.repartition(
    int(ceil(combined_df.count() / 10000000))
)
combined_df.persist(StorageLevel.DISK_ONLY)
now = datetime.now().strftime("%Y%m%d")


output_path = os.path.join(
    PATHS["COMBINE_PREDICTION"],
    PARAMS["subtype"] + "-" + now + "-" + "combined",
)
# ---Save config files
CNFG_OUTPUT_PATH = os.path.join(
    output_path, "conf", "config_combine_preds.yml"
)
PATHS["COMBINE_LOG"] = os.path.join(output_path, "LOG", "cf_combine_preds.csv")

print("AAA")
print(PATHS)
print(CNFG_OUTPUT_PATH)
print(output_path)
# copy_file_to_s3(CFG_PATH, CNFG_OUTPUT_PATH)

pq_path = output_path + "/PARQUET"
print("writing parquet version at {}".format(pq_path))
# combined_df.write.parquet(pq_path, mode="overwrite")

# (9) --- Write Log and Shut Down --- #

print("writing logs")
log_line = {
    "run_name": PARAMS["run_name"],
    "date": datetime.now(),
    "type": PARAMS["type"],
    "items": PARAMS["items"],
    "cnfg_file": CNFG_OUTPUT_PATH,
}
# write_local_to_s3(log_line, PATHS["COMBINE_LOG"], mode="append")


print("done")
sc.stop()
