"""Functions and paths to help with general io for all modeling purposes."""

# TODO:
#   [] Add support to automatically route to the most recent cube, assuming backwards compatibility
import argparse
import ast
import os
from datetime import datetime

import yaml
from dateutil import rrule

from pe_memberdna.lib.utils import next_fiscal_week_end


def load_config():
    """Read in configuration file.

    Reads in confuguration file given path
    and retuns dictionary of full config.

    Parameters:
        path (str): local path to config file

    Returns:
        cfg (dict): dictionary representation of config
    """
    cur_file_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..")
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("--config")
    parser.add_argument("--test", action="store_true")
    parser.add_argument("--future", action="store_true")
    parser.add_argument("--rmse", action="store_true")
    args = parser.parse_args()

    if not os.path.isfile(args.config):
        msg = "--config: invalid file path: {}".format(args.config)
        raise parser.error(msg)
    elif args.test:
        cfg_path = "./conf/test_cnf.yml"
    else:
        cfg_path = args.config
    with open(cfg_path, "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.Loader)
    cfg["shared"]["future_run"] = args.future
    cfg["shared"]["rmse"] = args.rmse
    return cfg, cfg_path


def make_formatted_date_range(start, end):
    """Convert input dates to formatted string date range.

    Parameters
        start (str): start date in YYYY-MM-DD format
        end (str): end date in YYYY-MM-DD format

    Returns
        daterange(str): date range in YYYYMMDD-YYYYMMDD format
    """
    start_fmt = start.replace("-", "")
    end_fmt = end.replace("-", "")
    daterange = start_fmt + "-" + end_fmt
    return daterange


def make_dataset_filename(start, end, n_cats, category, version):
    """Convert dataset parameters to filename.

    Takes the bounds and parameters that define
    a dataset, and converts them to a filename
    following conventions for storing.

    Parameters:
        start (str): start date in YYYY-MM-DD format
        end (str): end date in YYYY-MM-DD format
        n_cats (str): number of 'top' cats in dataset
        category (str): category-level of data on item heirarchy
        version (str): version of dataset
    Returns:
        filename (str): full filename
    """
    dates = make_formatted_date_range(start, end)
    params = [dates, category, str(n_cats), version]
    filename = "_".join(params)
    return filename


def make_predictions_filename(model_type, model_version, name):
    """Convert prediction parameters to filename.

    Takes the bounds and parameters that define
    a set of predictions, and converts them to a filename
    following conventions for storing.

    Parameters:
        model_type (str): type of the preeictions (i.e. 'cf')
        model_version (str): version number of model
        name (str): name of predictions file
    Returns:
        filename (str): full filename
    """
    filename = model_type
    filename += "-"
    filename += model_version
    filename += "-"
    filename += name
    return filename


def calculate_filepaths(params, paths):
    """Calculate additional derived parameters and filepaths.

    Takes the bounds and parameters that define a pipeline
    run and calculates derived parameters for use throughout
    the run.

    Parameters:
        params (dict): dictionary of baseline parameters
        paths (dict): dictionary of baseline filepaths
    Returns:
        params (dict): extended dictionary of parameters
        paths (dict): extended dictionary of paths
    """
    if params["future_run"] or params["rmse"]:
        params["start"] = params["f_start"]
        params["end"] = params["f_end"]

    daterange = make_formatted_date_range(params["start"], params["end"])
    params["data"] = make_dataset_filename(
        params["start"],
        params["end"],
        params["num_cats"],
        params["category"],
        params["data_version"],
    )

    params["future"] = make_dataset_filename(
        params["f_start"],
        params["f_end"],
        params["num_cats"],
        params["category"],
        params["data_version"],
    )

    etl_output_path = os.path.join(
        paths["DATA"], params["run_name"], params["category"], daterange
    )

    for name, prefix, suffix in (
        ("matrix", "matrix_", params["data"]),
        ("slate", "slate_", params["data"]),
        ("cat", "cat_lookup_", params["data"]),
        ("bycat", "data_by_cat_", params["data"]),
        ("future", "matrix_", params["future"]),
    ):
        paths[name] = os.path.join(etl_output_path, prefix + suffix)

    paths["ETL_LOG"] = os.path.join(etl_output_path, "LOGS", "cf_etl.csv")
    paths["CFG_ETL"] = os.path.join(etl_output_path, "CONF")
    paths["cf_model_output_path"] = os.path.join(
        paths["MODEL"], params["run_name"], params["category"], daterange
    )

    paths["model"] = os.path.join(
        paths["cf_model_output_path"],
        "cf_model_"
        + params["model_version"]
        + "_"
        + params["category"]
        + "_"
        + daterange,
    )

    paths["scaler"] = os.path.join(
        paths["cf_model_output_path"],
        "cf_scaler_"
        + params["model_version"]
        + "_"
        + params["category"]
        + "_"
        + daterange,
    )

    paths["TRAIN_LOG"] = os.path.join(
        paths["cf_model_output_path"], "LOGS", "als_train.csv"
    )

    paths["cf_prediction_output_path"] = os.path.join(
        paths["PREDICTIONS_ROOT"],
        params["run_name"],
        params["category"],
        daterange,
    )

    paths["current_prediction_prediction_path"] = os.path.join(
        paths["cf_prediction_output_path"],
        make_predictions_filename(
            "cf", params["model_version"], params["run_name"]
        ),
    )

    paths["PREDICT_LOG"] = os.path.join(
        paths["cf_prediction_output_path"], "LOGS", "cf_predict.csv"
    )

    paths["COMBINE_PREDICTION"] = os.path.join(
        paths["PREDICTIONS_ROOT"], params["run_name"], daterange + "-combined"
    )

    return (params, paths)


def read_partial(
    sparkcontext, base_path, start_time, end_time, partition_range="daily"
):
    """Read efficient intermediates for a time-range. Requires partitioned data.

    We store most intermediate data in parquet format partitioned by date. This function allows
    partial read of date partitioned parquet data for a given time range. It can handle different partition
    sizes (daily,weekly, monthly), but requires the partition naming scheme to contain PURCH_DT= followed
    by the formatted partition identifier.

    Parameters:
        base_path (str): base path of parquet file to read
        start_time (str): start of date partitioned parquet file range in 'YYYY-MM-dd' format
        end_time (str): end of date partitioned parquet file range in 'YYYY-MM-dd' format
        partition_range (str): time interval of date partitioning, currently supports 'daily' 'weekly'
        and 'monthly'

    Returns:
        s (int or string): int of string if possible else input string
    """
    paths = []
    if partition_range == "daily":
        rule = rrule.DAILY
    elif partition_range == "weekly":
        rule = rrule.WEEKLY
        start_time = next_fiscal_week_end(start_time)
    elif partition_range == "montly":
        rule = rrule.MONTLY
    else:
        raise ValueError(
            "only daily, weekly, and monthly partitions are supported"
        )
    for dt in rrule.rrule(
        rule,
        dtstart=datetime.strptime(start_time, "%Y-%m-%d"),
        until=datetime.strptime(end_time, "%Y-%m-%d"),
    ):
        date = dt.strftime("%Y-%m-%d")
        paths.append(base_path + "FISCAL_WEEK_END=" + date + "/*")
    return sparkcontext.read.parquet(*paths)


def save_scaler(sparkcontext, scaler, path):
    """Save scaler.

    Saves a scaler broadcast object at an S3 path.

    Parameters:
        sparkcontext (pyspark.SparkContext): The spark context to return the scaler to
        scaler (pyspark.broadcast): broadcast object containing scaling parameters
        path (str): string representing location to save scaler

    Returns:
        None!
    """
    rdd = sparkcontext.parallelize([scaler.value])
    rdd.saveAsTextFile(path)


def read_scaler(sparkcontext, path):
    """Read scaler broadcast variable from path.

    Read in a broadcast scaler to use with predictions

    Parameters:
        sparkcontext (pyspark.SparkContext): The spark context to return the scaler to
        path (str): path to find scaler

    Returns:
        scaler (pyspark.broadcast): broadcast object containing scaling parameters
    """
    rdd = sparkcontext.textFile(path)
    dictionary = ast.literal_eval(rdd.collect()[0])
    scaler = sparkcontext.broadcast(dictionary)
    return scaler
