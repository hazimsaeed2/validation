import argparse
import datetime
import os
import yaml

from pe_memberdna.lib.utility import get_max_file_date, get_next_file_date
import pe_memberdna.lib.misc as misc

parser = argparse.ArgumentParser(description="Update config file.")
parser.add_argument("in_home_date", action="store", help="In Home Date")
parser.add_argument(
    "cube_path", action="store", help="Customer cube data path"
)
parser.add_argument(
    "run_type", action="store", help="The run type for this execution"
)
parser.add_argument("git_branch", action="store", help="Git branch")
args = parser.parse_args()

curr_dir = os.path.abspath(os.path.dirname(__file__))

run_type = args.run_type
git_branch = "branch/" + args.git_branch.replace("origin/", "")
config_suffix = ""
s3_input_prefix = ""
s3_output_prefix = ""

if run_type == "dev" or run_type == "stage":
    config_suffix = f"_{run_type}"
    s3_input_prefix = f"{run_type}/ref/"
    s3_output_prefix = f"{run_type}/{git_branch}/"

yaml_path = curr_dir + f"/conf/config{config_suffix}.yml"

with open(yaml_path) as config_file:
    config = yaml.load(config_file, Loader=yaml.FullLoader)

config["shared"]["run_name"] = "{}_{}".format(
    run_type, datetime.date.today().strftime("%Y_%m_%d")
)

if args.in_home_date == "automatic":
    print("AAAA" + f"{s3_input_prefix}CUBES/")
    last_cube_date_str = get_max_file_date(
        "memberanalytics-data-out-prod", f"{s3_input_prefix}CUBES/"
    )
else:
    last_date = (
        datetime.datetime.strptime(args.in_home_date, "%Y-%m-%d")
        - datetime.timedelta(6 * 7)
    ).strftime("%Y-%m-%d")
    last_cube_date_str = get_next_file_date(
        "memberanalytics-data-out-prod", f"{s3_input_prefix}CUBES/", last_date
    )
    if not last_cube_date_str:
        print(
            "No data file within six weeks of the In-Home date. \n \
            Using the latest data file instead."
        )
        last_cube_date_str = get_max_file_date(
            "memberanalytics-data-out-prod", f"{s3_input_prefix}CUBES/"
        )

last_cube_date = datetime.datetime.strptime(last_cube_date_str, "%Y-%m-%d")

config["shared"]["cube_path"] = args.cube_path

(
    config["etl"]["start_date"],
    config["etl"]["end_date"],
) = misc.get_previous_fiscal_weekend(13 * 365 / 12, last_cube_date_str)

config["predict"]["weeks_to_predict"] = [config["etl"]["end_date"]]

config["predict"]["trip_model_date"] = datetime.date.today().strftime("%Y%m%d")
config["predict"]["spend_model_date"] = datetime.date.today().strftime(
    "%Y%m%d"
)

with open(yaml_path, "w") as config_file:
    yaml.dump(config, config_file)
