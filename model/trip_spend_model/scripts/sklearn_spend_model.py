# -*- coding: utf-8 -*-
"""
Created on Thu Apr 12 11:24:53 2018

@author: Skatteboe Karoline
"""
############### IMPORTS #############################################
import argparse
from datetime import datetime

import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import ElasticNet
import yaml

import pe_memberdna.lib.iotools as general_iotools
import pe_memberdna.model.trip_spend_model.lib.python_general_utilities as util_func

############# READ IN COMMAND LINE ARGUMENTS #########################
parser = argparse.ArgumentParser(
    description="grid serach for trip and spend propensity"
)
parser.add_argument("config", action="store", help="configuration file")
parsed = parser.parse_args()

############## SET VARIABLES FROM CONFIG ###########################
with open(parsed.config, "r") as stream:
    config = yaml.load(stream, Loader=yaml.FullLoader)

LAST_FISCAL_WEEK_TRAINING = config["etl"]["end_date"]
FIRST_FISCAL_WEEK_TRAINING = config["etl"]["start_date"]
START_WINDOW = config["spend"]["start_window"] - 1
END_WINDOW = config["spend"]["end_window"] - 1
SPLIT_RATE = config["shared"]["split_rate"]
SAMPLE_RATE = config["shared"]["sample_rate"]
BUCKET = config["shared"]["bucket"]
NUMBER_OF_TREES = config["trip"]["number_of_trees"]
MAX_DEPTH = config["trip"]["max_depth"]
MAX_FEATURES = config["trip"]["max_features"]
MIN_LEAF_SIZE = config["trip"]["min_sample_leaf"]
MODEL = config["trip"]["model"]
DATE = datetime.today().strftime("%Y%m%d")
DATASET_PATH = config["shared"]["dataset_path"]
PATH_PREFIX = config["shared"]["base_path"]
MODEL_PATH_PREFIX = config["shared"]["model_path"]
INPUT_DATA_PATH = "{}/{}/{}/{}".format(
    config["shared"]["base_path"],
    config["shared"]["dataset_path"],
    config["shared"]["run_name"],
    config["shared"]["etl_path"],
)

ETL_OUTPUT_PATH = "{}/{}/{}/{}-{}/transformed_customer_data".format(
    config["shared"]["base_path"],
    config["shared"]["dataset_path"],
    config["shared"]["run_name"],
    LAST_FISCAL_WEEK_TRAINING,
    FIRST_FISCAL_WEEK_TRAINING,
)

FEATURE_PATH = config["shared"]["feature_path"]
REGRESSION_METRICS = config["spend"]["metrics"]
RUN_NAME = config["shared"]["run_name"]

############## DEFINE PATHS #######################################
OUTPUT_PATH = "s3://{}/{}/{}/{}/".format(
    BUCKET,
    config["shared"]["base_path"],
    config["shared"]["model_path"],
    RUN_NAME,
)

FEATURE_IMP_PATH = "%s%s_spend_V1_%s_features_%s_%s.csv" % (
    OUTPUT_PATH,
    MODEL,
    DATE,
    START_WINDOW,
    START_WINDOW + 2,
)

METRIC_PATH = "%s%s_spend_V1_%s_metric_%s_%s.csv" % (
    OUTPUT_PATH,
    MODEL,
    DATE,
    START_WINDOW,
    START_WINDOW + 2,
)
MODEL_PATH = "%s%s_spend_V1_%s.pkl" % (OUTPUT_PATH, MODEL, DATE)
save_path = OUTPUT_PATH + "CONF/cnfg_spend_model.yml"
general_iotools.copy_file_to_s3(parsed.config, save_path)
############# DEFINE FUNCTIONS########################################
def create_rf_model(
    training_X,
    training_y,
    testing_X,
    max_depth,
    max_features,
    n_estimators,
    min_samples_leaf,
    features,
):
    model = RandomForestRegressor(
        n_jobs=-1,
        max_depth=max_depth,
        max_features=max_features,
        warm_start=True,
        n_estimators=n_estimators,
        min_samples_leaf=min_samples_leaf,
    )

    training_X.reset_index(inplace=True)
    testing_X.reset_index(inplace=True)
    training_X = training_X[features]
    testing_X = testing_X[features]
    model.fit(training_X, training_y.values.ravel())
    return model, model.predict(testing_X), model.feature_importances_


def create_lr_model(training_X, training_y, testing_X):
    training_X = training_X[training_X.columns.intersection(features)]
    testing_X = testing_X[training_X.columns.intersection(features)]
    model = ElasticNet(l1_ratio=1, max_iter=1000, warm_start=True)
    model.fit(training_X, training_y.values.ravel())
    return model, model.predict(testing_X), model.coef_


############## RUN SCRIPT #############################################
print("---------------------read in features and data---------------------")
features = general_iotools.read_s3_to_local(
    "s3://{}/{}".format(BUCKET, FEATURE_PATH), ftype="csv"
)
label_column = "spend_from_%s_%s" % (str(START_WINDOW), str(END_WINDOW))
trip_column = "will_visit_from_5_7"
(
    column_headers,
    categorical_features,
    continious_features,
) = util_func.get_column_headers(features, [label_column])
feature_headers = [x for x in column_headers if x != label_column]

INPUT_DATA_PATH = "s3n://{}/{}/{}/{}/etl_{}_{}".format(
    BUCKET,
    config["shared"]["base_path"],
    config["shared"]["dataset_path"],
    RUN_NAME,
    LAST_FISCAL_WEEK_TRAINING,
    FIRST_FISCAL_WEEK_TRAINING,
)

data = general_iotools.read_s3_to_local(
    "s3://{}/{}".format(BUCKET, ETL_OUTPUT_PATH),
    ftype="parquet",
    columns=column_headers + ["will_visit_from_5_7"],
)

print("---------------------split data in test and train---------------------")
training, testing = util_func.test_and_train_to_pandas(
    data, SPLIT_RATE, SAMPLE_RATE
)  # change back to sample rate
training, testing = util_func.train_initial_model(
    training.drop(columns=[trip_column, label_column]),
    training[[trip_column]],
    testing.drop(columns=[trip_column, label_column]),
    SPLIT_RATE,
    testing[[trip_column]],
    label_column,
)
print("---------------------train spend model---------------------")
if MODEL == "rf":
    model, predictions, feature_importance = create_rf_model(
        training.drop(columns=[label_column]),
        training[[label_column]],
        testing.drop(columns=[label_column]),
        MAX_DEPTH,
        MAX_FEATURES,
        NUMBER_OF_TREES,
        MIN_LEAF_SIZE,
        feature_headers,
    )
if MODEL == "lr":
    model, predictions, feature_importance = create_lr_model(
        training.drop(columns=[label_column]),
        training[[label_column]],
        testing.drop(columns=[label_column]),
    )
print("---------------------completed model creation--------------------")
model_metrics = [
    util_func.create_spend_propensity_metric(
        predictions, testing[[label_column]]
    )
]

model_metrics = util_func.get_df_from_list(model_metrics, REGRESSION_METRICS)
print("---------------------print to file---------------------")
general_iotools.write_local_to_s3(
    pd.DataFrame(
        util_func.create_sklearn_features(feature_importance, feature_headers)
    ),
    FEATURE_IMP_PATH,
)
general_iotools.write_local_to_s3(model_metrics, METRIC_PATH)
general_iotools.write_local_to_s3(model, MODEL_PATH)
print("------feature importance is saved at {}".format(FEATURE_IMP_PATH))
print("------model metrics is saved at {}".format(METRIC_PATH))
print("----model is saved at {}".format(MODEL_PATH))
