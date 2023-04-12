"""ETL job to create modeling datasets from intermediates for collaborative-filtering."""
# TODO:
#   [] Add support for cubes and strip out intermediates as they become available
from datetime import datetime
import os

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    when,
    countDistinct,
    mean,
    desc,
    sum,
    lit,
    col,
)
from pyspark.sql.types import LongType

from pe_memberdna.model.cf_model.lib.cf_io import (
    load_config,
    calculate_filepaths,
)
from pe_memberdna.lib.iotools import (
    write_local_to_s3,
    copy_file_to_s3,
)
from pe_memberdna.model.cf_model.lib.cf_utils import (
    member_in_range,
    join_category_details,
)
from pe_memberdna.lib.utils import trips_only


# (1) --- Handle Arguments --- #

CNF, CFG_PATH = load_config()
PARAMS = dict(list(CNF["shared"].items()) + list(CNF["etl"].items()))
PATHS = CNF["paths"]
PARAMS, PATHS = calculate_filepaths(PARAMS, PATHS)

CNFG_OUTPUT_PATH = os.path.join(PATHS["CFG_ETL"], "config_etl.yml")
copy_file_to_s3(CFG_PATH, CNFG_OUTPUT_PATH)

# (2) --- Spark Context --- #

conf = (
    SparkConf().setAppName("cf_etl").set("spark.ui.showConsoleProgress", True)
)
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# (3) --- Read Data --- #

print(" 1/13 Reading Data...")
if PARAMS["test"]:
    memberDF = spark.read.csv(PATHS["CUBE"], header=True)
    transactionDF = spark.read.csv(PATHS["TRANS_INT"], header=True)
    itemDF = spark.read.csv(PATHS["ITEM_INT"], header=True)
else:
    memberDF = spark.read.parquet(PATHS["CUBE"])
    transactionDF = spark.read.parquet(PATHS["TRANS_INT"])
    itemDF = spark.read.parquet(PATHS["ITEM_INT"])

# (4) --- Prep Member Data --- #

print("2/13 Prepping Members...")
members = member_in_range(memberDF, PARAMS["start"], PARAMS["end"])
members = members.withColumn("MBRSHP_SID", members.MBRSHP_SID.cast(LongType()))
if PARAMS["sample"] < 1:
    members = members.sample(False, PARAMS["sample"], 42)
n_memb = members.count()

# (5) --- Prep Transaction Details --- #

print("3/13 Prepping Transactions...")
details = transactionDF[transactionDF.PURCH_DT >= PARAMS["start"]]
details = details[details.PURCH_DT <= PARAMS["end"]]
details = trips_only(details)
details = details.withColumn("MBRSHP_SID", details.MBRSHP_SID.cast(LongType()))
if PARAMS["sample"] < 1:
    details = details.sample(False, PARAMS["sample"], 42)
n_trans = details.count()

# (6) --- Prep Items --- #
print("4/13 Prepping Items...")
items = itemDF
if PARAMS["category"] == "BRAND":
    items = items.where(col("BRAND_DESC") != "UNBRANDED")
n_items = items.count()

print("proceeding to tranform data for ", n_memb, " members")
print("and ", n_items, " items")
print("with ", n_trans, " transactions")

# (8) --- Join and Aggregate --- #

print("5/13 Generating Aggregate Data...")
member_trans = members.join(details, "MBRSHP_SID", "inner")
joined = join_category_details(items, member_trans, PARAMS["category"])
aggregated = joined.groupBy("MBRSHP_SID", "CATEGORY_NAME", "CATEGORY_ID").agg(
    countDistinct(joined.PURCH_HDR_ID).alias("TRIPS"),
    sum(joined.NORMAL_PRC_AMT).alias("SALES_AMT"),
    sum(joined.QTY_IN_UNITS).alias("SALES_UNITS"),
)

aggregated = aggregated.withColumn(
    "MBRSHP_SID", aggregated.MBRSHP_SID.cast(LongType())
)
aggregated = aggregated.withColumn(
    "CATEGORY_ID", aggregated.CATEGORY_ID.cast(LongType())
)
aggregated = aggregated[
    aggregated.CATEGORY_NAME != "UNKNOWN"
]  # remove aggregate 'unknown' category
# remove aggregate 'unbranded' category
aggregated = aggregated[aggregated.CATEGORY_NAME != "UNBRANDED"]
aggregated = aggregated.fillna(0)
# remove members with too little data
aggregated.cache()


# (9) ---- Generate Trips by Category --- #

print("6/13 Generating Metrics by Category...")
bycat = aggregated.groupBy("CATEGORY_ID").agg(
    sum("TRIPS").alias("TRIPS"),
    sum("SALES_AMT").alias("SALES_AMT"),
    sum("SALES_UNITS").alias("SALES_UNITS"),
)
bycat = bycat.fillna(0)
bycat = bycat.repartition(20)

print("7/13 Writing Metrics By Category")
bycat_path = PATHS["bycat"]
bycat.write.parquet(bycat_path, mode="overwrite")

# (10) --- Generate Member-Category-Matrix --- #

print("8/13 Generating Member-Category-Matrix...")
# for now, always take top categories by sales dollars
top_cats = (
    bycat.orderBy(desc("SALES_AMT"))
    .limit(int(PARAMS["num_cats"]))
    .select("CATEGORY_ID")
    .collect()
)
top_cats = [str(i.CATEGORY_ID) for i in top_cats]
matrix = aggregated.select(
    ["MBRSHP_SID", "CATEGORY_ID", "TRIPS", "SALES_AMT", "SALES_UNITS"]
)
matrix = matrix[matrix.CATEGORY_ID.isin(top_cats)]
# remove 'UNBRANDED' categories, only appears in brand
matrix = matrix[matrix.CATEGORY_ID != "5265"]
matrix = matrix.fillna(0)
matrix = matrix.repartition("CATEGORY_ID")
matrix.cache()

print("9/13 Writing Member-Category-Matrix...")
matrix_path = PATHS["matrix"]
matrix.write.parquet(matrix_path, mode="overwrite")

# (11) --- Generate Category Lookup --- #

print("10/13 Generating Category Name Lookup...")
catlookup = aggregated.select(["CATEGORY_NAME", "CATEGORY_ID"]).distinct()
catlookup = catlookup.repartition(20)

print("11/13 Writing Category Name lookup...")
lookup_path = PATHS["cat"]
catlookup.write.parquet(lookup_path, mode="overwrite")

# (12) -- Generate full member-categories -- #

print("12/13 Generating Slate table...")
all_members = aggregated.select("MBRSHP_SID").distinct()
all_cats = matrix.select("CATEGORY_ID").distinct()
all_membercats = all_members.crossJoin(all_cats)
all_membercats = all_membercats.repartition("CATEGORY_ID")

print("13/13 Writing Slate table...")
all_membercat_path = PATHS["slate"]
all_membercats.write.parquet(all_membercat_path, mode="overwrite")

# (13) --- Write Log and Shut Down --- #

if PARAMS["test"] is False:
    log_line = {
        "run_name": PARAMS["run_name"],
        "date": datetime.now(),
        "start": PARAMS["start"],
        "end": PARAMS["end"],
        "data_version": PARAMS["data_version"],
        "category": PARAMS["category"],
        "num_cats": PARAMS["num_cats"],
        "sample": PARAMS["sample"],
        "filename": PARAMS["data"],
        "cnfg_file": CNFG_OUTPUT_PATH,
    }
    write_local_to_s3(log_line, PATHS["ETL_LOG"], mode="append")


sc.stop()
