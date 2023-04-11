import argparse
import datetime
import os

import yaml

import pe_memberdna.lib.misc as misc
from pe_memberdna.cube.utils.utility import (
    get_max_file_date,
    get_next_file_date,
)
from pe_memberdna.pipelines.cf_model.lib.cf_utils import get_latest_prop_path

parser = argparse.ArgumentParser(description="Update config file.")
parser.add_argument("in_home_date", action="store", help="In Home Date")
parser.add_argument("lambdas", action="store", help="predict -> lambda")
parser.add_argument(
    "prop_path", action="store", help="Trip propensity prediction path"
)
parser.add_argument(
    "pred_root", action="store", help="paths -> PREDICTIONS_ROOT"
)
parser.add_argument(
    "cube_path", action="store", help="Customer cube data path"
)
parser.add_argument(
    "run_type", action="store", help="The run type for this execution"
)
args = parser.parse_args()

curr_dir = os.path.abspath(os.path.dirname(__file__))

for AH_type in ["AH4", "AH5"]:

    yaml_path = curr_dir + "/conf/config_" + AH_type + ".yml"

    with open(yaml_path) as config_file:
        config = yaml.load(config_file, Loader=yaml.Loader)

    if args.in_home_date == "automatic":
        last_cube_date_str = get_max_file_date(
            "memberanalytics-data-out-prod", "CUBES/"
        )
        last_cube_date = datetime.datetime.strptime(
            last_cube_date_str, "%Y-%m-%d"
        )
        config["predict"]["pred_date"] = (
            last_cube_date + datetime.timedelta(6 * 7)
        ).strftime("%Y-%m-%d")
    else:
        last_date = (
            datetime.datetime.strptime(args.in_home_date, "%Y-%m-%d")
            - datetime.timedelta(6 * 7)
        ).strftime("%Y-%m-%d")
        last_cube_date_str = get_next_file_date(
            "memberanalytics-data-out-prod", "CUBES/", last_date
        )
        if not last_cube_date_str:
            print(
                "No data file within six weeks of the In-Home date. \n \
                Using the latest data file instead."
            )
            last_cube_date_str = get_max_file_date(
                "memberanalytics-data-out-prod", "CUBES/"
            )
        config["predict"]["pred_date"] = args.in_home_date
        last_cube_date = datetime.datetime.strptime(
            last_cube_date_str, "%Y-%m-%d"
        )

    (
        config["shared"]["start"],
        config["shared"]["end"],
    ) = misc.get_previous_fiscal_weekend(365, last_cube_date_str)

    (
        config["shared"]["f_start"],
        config["shared"]["f_end"],
    ) = misc.get_previous_fiscal_weekend(3 * 365 / 12, last_cube_date_str)

    curr_dt = datetime.datetime.now().strftime("%Y-%m-%d")
    config["shared"]["run_name"] = args.run_type + "_" + curr_dt
    version = args.run_type + "_" + curr_dt.replace("-", "_")
    config["shared"]["data_version"] = version
    config["shared"]["model_version"] = version

    if args.prop_path == "automatic":
        prop_path = get_latest_prop_path(
            "memberanalytics-data-out-prod",
            "v2/MODELDATA/PREDICTIONS/TRIP_SPEND_MODELS/",
            run_type=args.run_type,
        )
        config["paths"]["PROPENSITY_PREDICTIONS"] = (
            "s3://memberanalytics-data-out-prod/" + prop_path
        )
    else:
        config["paths"]["PROPENSITY_PREDICTIONS"] = args.prop_path

    config["paths"]["CUBE"] = args.cube_path
    config["paths"][
        "ETL_LOG"
    ] = "s3://memberanalytics-data-out-prod/Code_and_Data_repo/MODELDATA/LOGS/cf_etl.csv"
    config["paths"][
        "TRAIN_LOG"
    ] = "s3://memberanalytics-data-out-prod/Code_and_Data_repo/MODELDATA/LOGS/als_train.csv"
    config["paths"][
        "PREDICT_LOG"
    ] = "s3://memberanalytics-data-out-prod/Code_and_Data_repo/MODELDATA/LOGS/cf_predict.csv"
    config["paths"][
        "COMBINE_LOG"
    ] = "s3://memberanalytics-data-out-prod/Code_and_Data_repo/MODELDATA/LOGS/cf_combine_preds.csv"
    config["paths"][
        "TUNE_LOG"
    ] = "s3://memberanalytics-data-out-prod/Code_and_Data_repo/MODELDATA/LOGS/als_tune.csv"

    lambdas = args.lambdas.split(",")
    config["predict"]["lambda"] = []
    for l in lambdas:
        config["predict"]["lambda"].append(l)

    config["paths"]["PREDICTIONS_ROOT"] = args.pred_root

    with open(yaml_path, "w") as config_file:
        yaml.dump(config, config_file)
