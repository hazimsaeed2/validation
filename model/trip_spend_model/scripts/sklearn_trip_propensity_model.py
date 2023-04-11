# -*- coding: utf-8 -*-
"""
Trains the trip propensity module. Read in transformed data from
ETL.py. Read in features to train on from s3

Trains the model in two iterations - first iteration on everyone,
second iteration on 10% of the members with >0.95 and <0.05 probabilty
of making a trip and everybody in the middle.
"""
############### IMPORTS #############################################
import argparse
from datetime import datetime

import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import yaml

import pe_memberdna.lib.iotools as general_iotools
import pe_memberdna.model.trip_spend_model.lib.python_general_utilities as util_func


############### GLOBAL VARIABLES ####################################

############# READ IN COMMAND LINE ARGUMENTS #########################
parser = argparse.ArgumentParser(
    description="grid serach for trip and spend propensity"
)
parser.add_argument("config", action="store", help="configuration file")
parsed = parser.parse_args()

############## SET VARIABLES FROM CONFIG ###########################
with open(parsed.config, "r") as stream:
    config = yaml.load(stream, Loader=yaml.FullLoader)

START_WINDOW = config["trip"]["start_window"] - 1
END_WINDOW = config["trip"]["end_window"] - 1
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
LAST_FISCAL_WEEK_TRAINING = config["etl"]["end_date"]
FIRST_FISCAL_WEEK_TRAINING = config["etl"]["start_date"]
INPUT_DATA_PATH = "{}/{}/{}/etl_{}_{}".format(
    config["shared"]["base_path"],
    config["shared"]["dataset_path"],
    config["shared"]["run_name"],
    LAST_FISCAL_WEEK_TRAINING,
    FIRST_FISCAL_WEEK_TRAINING,
)

ETL_OUTPUT_PATH = "{}/{}/{}/{}-{}/transformed_customer_data".format(
    config["shared"]["base_path"],
    config["shared"]["dataset_path"],
    config["shared"]["run_name"],
    LAST_FISCAL_WEEK_TRAINING,
    FIRST_FISCAL_WEEK_TRAINING,
)


FEATURE_PATH = config["shared"]["feature_path"]
run_name = config["shared"]["run_name"]
CLASSIFICATION_METRICS = config["trip"]["metrics"]

############## DEFINE PATHS #######################################
OUTPUT_PATH = "s3://{}/{}/{}/{}/".format(
    BUCKET,
    config["shared"]["base_path"],
    config["shared"]["model_path"],
    run_name,
)

FEATURE_IMP_PATH = "%s%s_trip_V1_%s_features_%s_%s_{}.csv" % (
    OUTPUT_PATH,
    MODEL,
    DATE,
    START_WINDOW,
    START_WINDOW + 2,
)
METRIC_PATH = "%s%s_trip_V1_%s_metric_%s_%s.csv" % (
    OUTPUT_PATH,
    MODEL,
    DATE,
    START_WINDOW,
    START_WINDOW + 2,
)

MODEL_PATH = "%s%s_trip_V1_%s.pkl" % (OUTPUT_PATH, MODEL, DATE)
save_path = OUTPUT_PATH + "CONF/cnfg_trip_model.yml"
general_iotools.copy_file_to_s3(parsed.config, save_path)
############## DEFINE FUNCTIONS########################################


def create_rf_model(
    training_X,
    training_y,
    testing_X,
    max_depth,
    max_features,
    n_estimators,
    min_samples_leaf,
):
    model = RandomForestClassifier(
        n_jobs=-1,
        max_depth=max_depth,
        max_features=max_features,
        warm_start=True,
        n_estimators=n_estimators,
        min_samples_leaf=min_samples_leaf,
    )

    model.fit(training_X, training_y.values.ravel())
    return (
        model,
        model.predict(testing_X),
        model.feature_importances_,
        model.predict_proba(testing_X)[:, 1],
    )


def create_lr_model(training_X, training_y, testing_X):
    training_X = training_X[training_X.columns.intersection(features)]
    testing_X = testing_X[training_X.columns.intersection(features)]
    model = LogisticRegression(n_jobs=-1)
    model.fit(training_X, training_y.values.ravel())
    return (
        model,
        model.predict(testing_X),
        model.coef_,
        model.predict_proba(testing_X)[:, 1],
    )


# ############## RUN SCRIPT #############################################
features = general_iotools.read_s3_to_local(
    "s3://{}/{}".format(BUCKET, FEATURE_PATH), ftype="csv"
)
label_column = "will_visit_from_%s_%s" % (str(START_WINDOW), str(END_WINDOW))
(
    column_headers,
    categorical_features,
    continious_features,
) = util_func.get_column_headers(features, [label_column])
print(
    "---------------------1/6 read in transformed file from {}---------------------".format(
        ETL_OUTPUT_PATH
    )
)
data = general_iotools.read_s3_to_local(
    "s3://{}/{}".format(BUCKET, ETL_OUTPUT_PATH),
    ftype="parquet",
    columns=column_headers,
)

print("---------------------%s--------------------" % len(data))
print(
    "---------------------2/6 split data in test and train---------------------"
)

training, testing = util_func.test_and_train_to_pandas(
    data, SPLIT_RATE, SAMPLE_RATE
)
if MODEL == "rf":
    model, predictions, feature_importance, prediction_prob = create_rf_model(
        training.drop(columns=[label_column]),
        training[[label_column]],
        testing.drop(columns=[label_column]),
        MAX_DEPTH,
        MAX_FEATURES,
        NUMBER_OF_TREES,
        MIN_LEAF_SIZE,
    )
if MODEL == "lr":
    model, predictions, feature_importance, prediction_prob = create_lr_model(
        training.drop(columns=[label_column]),
        training[[label_column]],
        testing.drop(columns=[label_column]),
    )
print("---------------------completed model creation--------------------")
model_metrics = [
    util_func.create_trip_propensity_metric(
        predictions, testing[[label_column]]
    )
]

combined = training.append(testing)
if MODEL == "lr":
    combined = combined[
        combined.columns.intersection(features + [label_column])
    ]

pred = model.predict_proba(combined.drop(columns=[label_column]))
combined["prediction"] = pred[:, 1]
print(
    "----------------initial length of combined data {}---------------".format(
        len(combined)
    )
)
high_predicted_value = combined.loc[combined["prediction"] > 0.95].sample(
    frac=0.1, replace=False
)
low_predicted_value = combined.loc[combined["prediction"] < 0.05].sample(
    frac=0.1, replace=False
)

combined = combined.loc[
    (combined["prediction"] >= 0.05) & (combined["prediction"] <= 0.95)
]

combined = combined.append([high_predicted_value, low_predicted_value])
print(
    "----------------data for second iteration after filtering {}---------------".format(
        len(combined)
    )
)
testing["prediction"] = prediction_prob

combined = combined.drop(columns=["prediction"])
print("---------------------running second iteration---------------------")
#  filter on predictions and rerun model
if len(combined) != 0:
    training, testing = train_test_split(combined, test_size=SPLIT_RATE)
    model, predictions, feature_importance, prediction_prob = create_rf_model(
        training.drop(columns=[label_column]),
        training[[label_column]],
        testing.drop(columns=[label_column]),
        MAX_DEPTH,
        MAX_FEATURES,
        NUMBER_OF_TREES,
        MIN_LEAF_SIZE,
    )
    model_metrics += [
        util_func.create_trip_propensity_metric(
            predictions, testing[[label_column]]
        )
    ]
    general_iotools.write_local_to_s3(
        pd.DataFrame(
            util_func.create_sklearn_features(
                feature_importance, column_headers
            )
        ),
        FEATURE_IMP_PATH.format("second"),
    )

    second_iteration_output_df = testing[[label_column]]

    second_iteration_output_df["prediction"] = predictions
    second_iteration_output_df["prediction_prob"] = prediction_prob

model_metrics = util_func.get_df_from_list(
    model_metrics, CLASSIFICATION_METRICS
)
print(
    "---------Saved feature importance at--------- {}".format(
        FEATURE_IMP_PATH.format("second")
    )
)
print(
    "---------------------print to file---------------------%s "
    % (METRIC_PATH)
)
general_iotools.write_local_to_s3(
    pd.DataFrame(
        util_func.create_sklearn_features(feature_importance, column_headers)
    ),
    FEATURE_IMP_PATH.format("second"),
)
general_iotools.write_local_to_s3(model_metrics, METRIC_PATH)

general_iotools.write_local_to_s3(model, MODEL_PATH)

print("---metrics is saved at {}".format(METRIC_PATH))
print("---model is saved at {}".format(MODEL_PATH))
