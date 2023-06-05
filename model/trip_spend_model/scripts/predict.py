# -*- coding: utf-8 -*-
"""
Created on Mon May 14 11:46:11 2018

@author: Skatteboe Karoline
"""
import argparse
from datetime import datetime
import os
import re

import boto3
import numpy as np
import yaml

import pe_memberdna.lib.iotools as iotools
from pe_memberdna.lib.utils import next_fiscal_week_end
import pe_memberdna.model.trip_spend_model.lib.python_general_utilities as util_func


############# READ IN COMMAND LINE ARGUMENTS #########################
parser = argparse.ArgumentParser(description="Predict on members")
parser.add_argument("config", action="store", help="configuration file")
parser.add_argument("--week", action="store", help="weeks to predict on")
parsed = parser.parse_args()

############## SET VARIABLES FROM CONFIG ###########################
with open(parsed.config, "r") as stream:
    config = yaml.load(stream, Loader=yaml.FullLoader)

############################ DEFINE GLOBAL VARIABLES ###############################
DATE = datetime.today().strftime("%Y%m%d")
START_WINDOW = config["trip"]["start_window"] - 1
HIGH_FREQUENCY_VISITS_LAST_12_WEEKS = config["shared"][
    "high_frequency_visit_threshold"
]
LOW_FREQUENCY_VISITS_LAST_26_WEEKS = config["shared"][
    "low_frequency_visit_threshold"
]
CUBE_PATH = config["shared"]["cube_path"]
BUCKET = config["shared"]["bucket"]
run_name = config["shared"]["run_name"]
MODEL = config["trip"]["model"]
READ_ONLY_BUCKET = config["shared"]["read_only_bucket"]
SPEND_MODEL_DATE = config["predict"]["spend_model_date"]
TRIP_MODEL_DATE = config["predict"]["trip_model_date"]
PATH_PREFIX = config["shared"]["base_path"]
MODEL_PATH_PREFIX = config["shared"]["model_path"]

OUTPUT_PATH = "s3://{}/{}/{}/{}/".format(
    BUCKET,
    config["shared"]["base_path"],
    config["shared"]["model_path"],
    run_name,
)
TRIP_FEATURE_PATH = "%s%s_trip_V1_%s_features_%s_%s_%s.csv" % (
    OUTPUT_PATH,
    MODEL,
    DATE,
    START_WINDOW,
    START_WINDOW + 2,
    "second",
)
SPEND_FEATURE_PATH = "%s%s_spend_V1_%s_features_%s_%s.csv" % (
    OUTPUT_PATH,
    MODEL,
    DATE,
    0,
    2,
)

SPEND_MODEL_PATH = "%s%s_spend_V1_%s.pkl" % (OUTPUT_PATH, MODEL, DATE)
TRIP_MODEL_PATH = "%s%s_trip_V1_%s.pkl" % (OUTPUT_PATH, MODEL, DATE)
PREDICTION_PATH = "s3://{}/{}/{}/{}/".format(
    BUCKET,
    config["shared"]["base_path"],
    config["shared"]["prediction_path"],
    run_name,
)

save_path = PREDICTION_PATH + "CONF/cnfg_predict.yml"
iotools.copy_file_to_s3(parsed.config, save_path)
pub_key = os.environ.get("AWS_ACCESS_KEY_ID")
private_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
if parsed.week is None:
    WEEKS_TO_PREDICT = config["predict"]["weeks_to_predict"]
else:
    WEEKS_TO_PREDICT = parsed.week
WEEKS = []

# The following code is necessary because the script predicts propensity for fiscal weekends only
WEEKS_TO_PREDICT = list(
    set([next_fiscal_week_end(w) for w in WEEKS_TO_PREDICT])
)
########################### DEFINE FUNCTIONS ###############################


def split_features(features):
    categorical = [item for item in features if item.endswith("tmp")]
    continious = [item for item in features if item not in categorical]
    categorical = [item[:-4] for item in categorical]
    return categorical, continious


############################ RUN SCRIPT ##################################
client = boto3.client("s3")
result = client.list_objects(
    Bucket=BUCKET, Prefix="{}/".format(CUBE_PATH), Delimiter="/"
)

for o in result.get("CommonPrefixes"):
    WEEKS += [o.get("Prefix")]

trip_features = iotools.read_s3_to_local(
    TRIP_FEATURE_PATH, ftype="csv", sep=","
)
trip_features = trip_features["Feature_Name"].tolist()

spend_features = iotools.read_s3_to_local(
    SPEND_FEATURE_PATH, ftype="csv", sep=","
)
spend_features = spend_features["Feature_Name"].tolist()

categorical_features, continious_features = split_features(trip_features)


for w in WEEKS_TO_PREDICT:
    WEEK_REGEX = re.compile(r"\={}".format(w))
    week_path = list(filter(WEEK_REGEX.search, WEEKS))
    if len(week_path) == 0:
        continue
    week_path = week_path[0]
    week = re.search("\=(.*)", week_path).group()[1:-1]
    start_time = week
    end_time = next_fiscal_week_end(week)
    print("-----start_time and end_time are: ------", start_time, end_time)
    print("predicting for week {}".format(week))
    data = iotools.read_s3_to_local(
        "s3://{}/{}".format(BUCKET, CUBE_PATH),
        ftype="parquet",
        columns=None,
        sep=",",
        start_time=start_time,
        end_time=end_time,
    )
    print("finished reading in cube {}".format(len(data)))
    print("----------------- read in cubes finshed-------------------")

    # join with segment
    data.set_index(["MBRSHP_SID"], inplace=True, drop=False)
    data[continious_features] = data[continious_features].fillna(value=0)

    data = util_func.create_independent_variables(
        data,
        LOW_FREQUENCY_VISITS_LAST_26_WEEKS,
        HIGH_FREQUENCY_VISITS_LAST_12_WEEKS,
    )
    data = util_func.create_seasonality_fields(data)
    data = util_func.transform_categorical_features(data, categorical_features)

    print(
        "--------------------- transform numpy --------------------------------"
    )
    data_np_array_trip = data[trip_features].values
    data_np_array_trip = np.nan_to_num(data_np_array_trip)

    data_np_array_spend = data[spend_features].values
    data_np_array_spend = np.nan_to_num(data_np_array_spend)

    print("--------------------- load models --------------------------------")
    trip_model = iotools.read_s3_to_local(
        TRIP_MODEL_PATH, ftype="pkl", private_key=private_key, pub_key=pub_key
    )
    spend_model = iotools.read_s3_to_local(
        SPEND_MODEL_PATH, ftype="pkl", private_key=private_key, pub_key=pub_key
    )
    print(
        "--------------------- predict likelihood of trip --------------------------------"
    )
    data["probability_making_a_trip"] = trip_model.predict_proba(
        data_np_array_trip
    )[:, 1]
    data["predicted_make_trip"] = trip_model.predict(data_np_array_trip)
    print(
        "--------------------- predict spend --------------------------------"
    )
    data["predicted_spend"] = spend_model.predict(data_np_array_spend)
    data["predicted_spend"] = np.where(
        data["probability_making_a_trip"] <= 0.08, 0, data["predicted_spend"]
    )
    print(
        "--------------------- write to file --------------------------------"
    )
    iotools.write_local_to_s3(
        data[
            [
                "probability_making_a_trip",
                "predicted_make_trip",
                "MBRSHP_SID",
                "LATEST_PRI_SUPP_FHH_IND",
            ]
        ],
        "{}trip_spend_predictions_{}.csv".format(PREDICTION_PATH, week),
    )
    print(
        "saved prediction at: ",
        "{}trip_spend_predictions_{}.csv".format(PREDICTION_PATH, week),
    )
    WEEKS_TO_PREDICT.remove(start_time)
if len(WEEKS_TO_PREDICT):
    print(
        "NOTE: There is no data for the following week(s): {}".format(
            WEEKS_TO_PREDICT
        )
    )
