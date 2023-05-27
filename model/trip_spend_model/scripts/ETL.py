import argparse
from datetime import datetime

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, date_sub

from pe_memberdna.lib.iotools import copy_file_to_s3
import pe_memberdna.model.trip_spend_model.lib.spark_general_utilities as util_func
import pe_memberdna.model.trip_spend_model.lib.trip_spend_iotools as iotools


spark = SparkSession.builder.appName("propensity model ETL").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
spark.conf.set(
    "spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED"
)
spark.sparkContext.setLogLevel("WARN")


############# READ IN COMMAND LINE ARGUMENTS #########################
parser = argparse.ArgumentParser(
    description="grid search for trip and spend propensity"
)
parser.add_argument("config", action="store", help="configuration file")
parsed = parser.parse_args()

############## SET VARIABLES FROM CONFIG ###########################
with open(parsed.config, "r") as stream:
    config = yaml.load(stream, Loader=yaml.FullLoader)

############### GLOBAL VARIABLES ####################################
DATE = datetime.today().strftime("%Y%m%d")

HIGH_FREQUENCY_VISITS_LAST_12_WEEKS = config["shared"][
    "high_frequency_visit_threshold"
]
LOW_FREQUENCY_VISITS_LAST_26_WEEKS = config["shared"][
    "low_frequency_visit_threshold"
]
START_WEEK_WINDOW = config["etl"]["start_week_windows"]
BBM_WEEK_WINDOW = config["etl"]["bbm_week_window"]
NUMBER_OF_WEEKS_TO_SAMPLE_FROM_EACH_CUSTOMER = config["etl"]["weeks_to_sample"]
LAST_FISCAL_WEEK_TRAINING = config["etl"]["end_date"]
FIRST_FISCAL_WEEK_TRAINING = config["etl"]["start_date"]
SEGMENTS_TO_FILTER_FOR = [4, 5, 6, 7, 8, 10]
BUCKET = config["shared"]["bucket"]
READ_ONLY_BUCKET = config["shared"]["read_only_bucket"]
BBM = config["etl"]["bbm"]
RUN_NAME = config["shared"]["run_name"]
OUTLIER_COLUMN = config["etl"]["outlier_column"]
OUTPUT_PATH = "s3n://{}/{}/{}/{}/{}-{}".format(
    BUCKET,
    config["shared"]["base_path"],
    config["shared"]["dataset_path"],
    RUN_NAME,
    LAST_FISCAL_WEEK_TRAINING,
    FIRST_FISCAL_WEEK_TRAINING,
)


ETL_OUTPUT_PATH = "{}/transformed_customer_data".format(
    OUTPUT_PATH
)  # etl_{}_{}
CUBE_PATH = "s3n://{}/{}".format(BUCKET, config["shared"]["cube_path"])
FEATURE_PATH = "s3n://{}/{}".format(BUCKET, config["shared"]["feature_path"])
print("AAA ", FEATURE_PATH)
LOOKUP_PATH = "s3n://{}/{}".format(BUCKET, config["etl"]["lookup_path"])
BBM_WEEKS_PATH = "s3n://{}/{}".format(BUCKET, config["etl"]["BBM_weeks_path"])
save_path = OUTPUT_PATH + "/CONF/cnfg_etl.yml"
copy_file_to_s3(parsed.config, save_path)
############# READ IN COMMAND LINE ARGUMENTS #########################
############# DEFINE FUNCTIONS #############################################


def get_bbm_weeks():
    lookup = spark.read.parquet(LOOKUP_PATH)
    lookup = lookup.select("FISCAL_DAY", "FISCAL_WEEK_END")
    lookup = lookup.withColumn(
        "date", to_date(lookup.FISCAL_DAY, "yyyy-MM-dd")
    )
    lookup = lookup.withColumn(
        "fiscal_week_end", to_date(lookup.FISCAL_WEEK_END, "yyyy-MM-dd")
    )

    bbm_dates = iotools.read_csv_spark(spark, BBM_WEEKS_PATH)
    bbm_dates = bbm_dates.withColumn(
        "date", to_date(bbm_dates.min_start_date, "MM/dd/yyy")
    )

    combined = bbm_dates.join(lookup, ["date"], "left_outer")
    combined = combined.withColumn(
        "6_weeks_prior_to_bbm", date_sub(lookup.fiscal_week_end, 42)
    )
    weeks = [i["6_weeks_prior_to_bbm"] for i in combined.collect()]
    weeks = [week.strftime("%Y-%m-%d") for week in weeks]
    return weeks


############# RUN SCRIPT #############################################
BBM_CREATION_DATES = get_bbm_weeks()

# read in cube
print("----------------1/4 read in customer cube------------------")
print("----output Path--{}".format(ETL_OUTPUT_PATH))

customers = iotools.read_parquet_spark(spark, CUBE_PATH)

customers = util_func.create_independent_variables(
    customers,
    LOW_FREQUENCY_VISITS_LAST_26_WEEKS,
    HIGH_FREQUENCY_VISITS_LAST_12_WEEKS,
)
customers = util_func.create_seasonality_fields(customers)

customers = util_func.create_dependent_variables(
    customers, START_WEEK_WINDOW, BBM_WEEK_WINDOW, "bin"
)
customers = util_func.create_dependent_variables(
    customers, START_WEEK_WINDOW, BBM_WEEK_WINDOW, "cont"
)

customers = util_func.filter_customer_cube(
    customers,
    LAST_FISCAL_WEEK_TRAINING,
    FIRST_FISCAL_WEEK_TRAINING,
    BBM_CREATION_DATES,
    START_WEEK_WINDOW,
    BBM_WEEK_WINDOW,
    CUBE_PATH,
)

customers = util_func.remove_outliers(customers, OUTLIER_COLUMN)

# take a subset of oberservaitions
customers = util_func.subset_oberservations(
    customers, NUMBER_OF_WEEKS_TO_SAMPLE_FROM_EACH_CUSTOMER
)
# match subset to segments to get the segments of the customer in the given week


# read in features
print("----------------2/4 read in features----------------")
features = iotools.read_csv_spark(spark, FEATURE_PATH)
categorical_features, continious_features = util_func.get_features_list(
    features
)

# transform data
print("----------------3/4 transforming data----------------")
transformed_customer_data = util_func.prepare_data(
    customers, categorical_features, continious_features
)
transformed_customer_data.persist()

# write transformed data to file
print(
    "----------------4/4 writing ETL file to file print rows %s----------------@ %s"
    % (transformed_customer_data.count(), ETL_OUTPUT_PATH)
)
transformed_customer_data.write.mode("overwrite").parquet(ETL_OUTPUT_PATH)
