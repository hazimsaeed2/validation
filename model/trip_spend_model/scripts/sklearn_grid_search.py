# -*- coding: utf-8 -*-
"""
Created on Thu Apr 12 11:24:53 2018

@author: Skatteboe Karoline
"""
############### IMPORTS #############################################
import argparse
import csv
from datetime import datetime

from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import RandomForestClassifier
import yaml

import pe_memberdna.lib.iotools as iotools
import pe_memberdna.model.trip_spend_model.lib.python_general_utilities as util_func

############### GLOBAL VARIABLES ####################################
############# READ IN COMMAND LINE ARGUMENTS #########################
parser = argparse.ArgumentParser(
    description="grid search for trip and spend propensity"
)
parser.add_argument("config", action="store", help="configuration file")
parser.add_argument("--model", action="store", help="model type")
parsed = parser.parse_args()
############## SET VARIABLES FROM CONFIG ########################################
with open(parsed.config, "r") as stream:
    config = yaml.load(stream, Loader=yaml.FullLoader)

START_WINDOW = config["trip"]["start_window"] - 1
END_WINDOW = config["trip"]["end_window"] - 1
SPLIT_RATE = config["shared"]["split_rate"]
SAMPLE_RATE = config["shared"]["sample_rate"]
BUCKET = config["shared"]["bucket"]
NUMBER_OF_TREES = config["grid_search"]["number_of_trees"]
MAX_DEPTHS = config["grid_search"]["max_depths"]
MAX_FEATURES = config["grid_search"]["max_features"]
MIN_LEAF_SIZES = config["grid_search"]["min_leaf_size"]
MODEL = config["grid_search"]["model"]
DATE = datetime.today().strftime("%Y%m%d")

INPUT_DATA_PATH = "{}/{}/{}".format(
    config["shared"]["base_path"],
    config["shared"]["dataset_path"],
    config["shared"]["etl_path"],
)

FEATURE_PATH = config["shared"]["feature_path"]
OUTPUT_PATH = "grid_search-v1-{}-{}-parameters.csv".format(MODEL, DATE)

if parsed.model is not None:
    MODEL = parsed.model

############## RUN SCRIPT #############################################
if MODEL == "cont":
    label_column = "spend_from_%s_%s" % (str(START_WINDOW), str(END_WINDOW))
if MODEL == "bin":
    label_column = "will_visit_from_%s_%s" % (
        str(START_WINDOW),
        str(END_WINDOW),
    )
    category_cube = iotools.read_s3_to_local(
        "s3://{}/{}".format(BUCKET, FEATURE_PATH), ftype="csv"
    )
dependent_columns = ["will_visit_from_5_7", "spend_from_5_7"]
features = iotools.read_s3_to_local(
    "s3://{}/{}".format(BUCKET, FEATURE_PATH), ftype="csv"
)
(
    column_headers,
    categorical_features,
    continious_features,
) = util_func.get_column_headers(features, dependent_columns)
print("---------------------1/6 read in transformed file---------------------")
data = iotools.read_s3_to_local(
    "s3://{}/{}".format(BUCKET, INPUT_DATA_PATH),
    ftype="parquet",
    columns=column_headers,
)
data = data.fillna(0)
print(
    "---------------------2/6 split data in test and train---------------------"
)

training, testing = util_func.test_and_train_to_pandas(
    data, SPLIT_RATE, SAMPLE_RATE
)

model_metrics_names = (
    config["trip"]["metrics"]
    if parsed.model == "cont"
    else config["spend"]["metrics"]
)

training, testing = util_func.train_initial_model(
    training.drop(columns=dependent_columns),
    training[["will_visit_from_5_7"]],
    testing.drop(columns=dependent_columns),
    SPLIT_RATE,
    testing[["will_visit_from_5_7"]],
    "will_visit_from_5_7",
)


with open(OUTPUT_PATH, "w") as output:
    file_writer = csv.writer(output, delimiter=",", quoting=csv.QUOTE_MINIMAL)

    file_writer.writerow(
        ["max_depth", "max_feature", "numer_estimators", "min_leaf"]
        + model_metrics_names
    )

    for max_depth in MAX_DEPTHS:
        for max_feature in MAX_FEATURES:
            for numer_estimators in NUMBER_OF_TREES:
                for min_sample in MIN_LEAF_SIZES:
                    print(max_depth, max_feature, numer_estimators, min_sample)
                    if parsed.model == "cont":
                        model = RandomForestRegressor(
                            n_jobs=-1,
                            max_depth=max_depth,
                            max_features=max_feature,
                            warm_start=True,
                            n_estimators=numer_estimators,
                            min_samples_leaf=min_sample,
                        )
                        model.fit(
                            training.drop(columns=[label_column]),
                            training[[label_column]].values.ravel(),
                        )
                        predictions = model.predict(
                            testing.drop(columns=[label_column])
                        )
                        model_metrics = (
                            util_func.create_spend_propensity_metric(
                                predictions, testing[[label_column]]
                            )
                        )
                    else:
                        model = RandomForestClassifier(
                            n_jobs=-1,
                            max_depth=max_depth,
                            max_features=max_feature,
                            warm_start=True,
                            n_estimators=numer_estimators,
                            min_samples_leaf=min_sample,
                        )

                        model.fit(
                            training.drop(columns=[label_column]),
                            training[[label_column]].values.ravel(),
                        )
                        predictions = model.predict(
                            testing.drop(columns=[label_column])
                        )
                        model_metrics = (
                            util_func.create_trip_propensity_metric(
                                predictions, testing[[label_column]]
                            )
                        )

                    row = [
                        max_depth,
                        max_feature,
                        numer_estimators,
                        min_sample,
                    ] + model_metrics
                    file_writer.writerow(row)
                    output.flush()
