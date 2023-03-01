import argparse
import datetime as dt
import logging
import os

import pe_memberdna.etl.lib.validations_ETL as validations
from pe_memberdna.lib.job_manager import JobManager
from pyspark.sql.types import *


def next_weekday(d, weekday):
    """
    Returns next weekday. If date matches weekday, returns same date.
    0 = Monday, 1=Tuesday, 2=Wednesday...
    """
    day_gap = weekday - d.weekday()
    if day_gap < 0:
        day_gap += 7
    return d + dt.timedelta(days=day_gap)


def prior_weekday(d, weekday):
    """
    Returns prior weekday. If date matches weekday, returns same date.
    0 = Monday, 1=Tuesday, 2=Wednesday...
    """
    day_gap = weekday - d.weekday()
    if day_gap > 0:
        day_gap -= 7
    return d + dt.timedelta(days=day_gap)


def find_true_date_range(start_date, end_date):
    """
    Finds closest fiscal start day to start_date within time range and closest
    fiscal week start to end_date within the time range. In other words, assuming
    M-Sa fiscal week, shifts start_date to the next Sunday and shifts back
    end_date to prior Saturday, defining even time range.
    """
    fiscal_week_start = 6  # Sunday
    fiscal_week_end = 5  # Saturday

    true_start_date = next_weekday(start_date, fiscal_week_start)
    true_end_date = prior_weekday(end_date, fiscal_week_end)

    return true_start_date, true_end_date


def lookback_date(fw_end, num_weeks):
    """
    Calculates starting FWE for lookback period incl. N weeks
    """
    # subtract 1, since we're counting current week
    return fw_end - dt.timedelta(weeks=num_weeks - 1)


def iterate_fiscal_weeks(start_date, end_date):
    """
    Generates the tuple for the provided time range.
    The tuple is of the form (fiscal_day, fiscal_week_start, fiscal_week_end,
    fiscal_l4w_end, ...). List of tuples in Python is natural structure for
    transformation to Spark Dataframe. Accomplished via iteration through each
    fiscal week in the time range and adding fiscal days tuple, which includes fiscal
    week in which that fiscal day occurred. Additionally adds fiscal week end
    dates of previous fiscal weeks 4, 8, 12, 26, and 52 weeks back.
    """

    start_date, end_date = find_true_date_range(start_date, end_date)

    end_of_week_incr = dt.timedelta(days=6)
    next_week_incr = dt.timedelta(weeks=1)

    date_list = []

    while start_date < end_date:
        fw_end = start_date + end_of_week_incr
        for day_num in range(0, 7):
            fiscal_day = start_date + dt.timedelta(days=day_num)
            date_tuple = (
                fiscal_day,
                start_date,
                fw_end,
                lookback_date(fw_end, 4),
                lookback_date(fw_end, 8),
                lookback_date(fw_end, 12),
                lookback_date(fw_end, 26),
                lookback_date(fw_end, 52),
            )
            date_list.append(date_tuple)
        start_date = start_date + next_week_incr
    return date_list


def convert_to_spark_dataframe(job, date_list):
    schema = StructType(
        [
            StructField("FISCAL_DAY", DateType(), True),
            StructField("FISCAL_WEEK_START", DateType(), True),
            StructField("FISCAL_WEEK_END", DateType(), True),
            StructField("FISCAL_L4W_END", DateType(), True),
            StructField("FISCAL_L8W_END", DateType(), True),
            StructField("FISCAL_L12W_END", DateType(), True),
            StructField("FISCAL_L26W_END", DateType(), True),
            StructField("FISCAL_L52W_END", DateType(), True),
        ]
    )
    return job.spark.createDataFrame(date_list, schema=schema)


def main(job, data_paths, config_validation):
    logging.info("Starting processing table fiscal_days")

    date_format = "%Y-%m-%d"
    start_date = dt.datetime.strptime(
        data_paths["fiscal_days"]["start"], date_format
    )
    end_date = dt.datetime.strptime(
        data_paths["fiscal_days"]["end"], date_format
    )

    out = data_paths["intermediate"]["fiscal_days"]

    date_list = iterate_fiscal_weeks(start_date, end_date)
    fiscal_days = convert_to_spark_dataframe(job, date_list)

    validations.validate_table(
        job.spark,
        "intermediate",
        "fiscal_days",
        config_validation,
        fiscal_days,
    )

    logging.info("Saving the intermediate file " + out)

    fiscal_days.repartition(1).write.parquet(out, mode="overwrite")


job = JobManager("fiscal_days")
parser = argparse.ArgumentParser()

parser.add_argument(
    "--config_path",
    type=str,
    default=os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "../configs/config.yaml",
    ),
    help=(
        """
        path to the config file
        """
    ),
)
args = parser.parse_args()
config = job.load_config(args)
data_paths, club_square_config, config_validation = job.split_config(config)
main(job, data_paths, config_validation)
