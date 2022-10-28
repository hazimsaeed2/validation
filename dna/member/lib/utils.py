"""
Helper functions that help tackle repetitive problems or to deflect
responsability
"""
import datetime
import gc
import re

import pyspark
import pyspark.sql.functions as sqlf
import pyspark.sql.window as W

import memberdna.lib.misc as misc
import memberdna.pipelines.lib.iotools as iotools


def set_default_value(dna, features, value=None):
    """
    Set default values for the specified features

    Parameters:
        dna (pyspark.DataFrame): dna dataframe
        features (list(str)): the list of feature columns for which to add
            default values
        value (object): the value to be added as default

    Returns:
        (pyspark.DataFrame): dna with default values for the specified features
    """
    df = dna
    if value is None:
        types = dict(df.dtypes)
        columns = df.columns
        for column in features:
            if types[column] not in ["float", "double"]:
                continue
            df = df.withColumn(
                column,
                sqlf.when(sqlf.isnan(column), sqlf.lit(None)).otherwise(
                    sqlf.col(column)
                ),
            )
        df = df.select(*columns)
    else:
        df = df.na.fill(value, subset=features)

    return df


def window_dates(job):
    """
    Generate the dates used to window the fiscal weekends

    Parameters:
        job (JobManager): a helper object for accessing data and config

    Returns:
        (str, str, str): A tuple of three string formatted dates, %Y-%m-%d,
            representing:
                Start date of the past 14 month window
                End date of the window, the most recent fiscal weekend
                Start date of the 52 week lookback window for the first
                week in the 14 month window
    """
    cube_config = job.config.params["params"] or {}
    if not ({"start_date", "end_date"} - cube_config.keys()):
        start_date = cube_config["start_date"]
        end_date = cube_config["end_date"]
    elif len({"start_date", "end_date"} - cube_config.keys()) == 2:
        start_date, end_date = misc.get_last_fiscal_weekend()
        job.config.params["params"]["start_date"] = start_date
        job.config.params["params"]["end_date"] = end_date
    else:
        raise Exception(
            """In the config file you have to either specify
        both start_date and end_date or neighter of them."""
        )

    lookback_start_date = str(
        (
            datetime.datetime.strptime(start_date, "%Y-%m-%d")
            - datetime.timedelta(days=364)
        )
    )[:10]

    return start_date, end_date, lookback_start_date


def next_fiscal_week_end(job):
    """
    Calculate next future saturday from the start_date.
    Parameters:
        job (JobManager): a helper object for accessing data and config
    Returns:
        (str): string formatted date, %Y-%m-%d
    """
    start_date, _, _ = window_dates(job)
    start_datetime = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    # Saturday is indexed as 5 (int) so the difference between start date and
    # the next saturday is 5 - start_date index
    days_to_sat = datetime.timedelta(days=5 - start_datetime.weekday())
    first_fiscal_week_end = start_datetime + days_to_sat
    # Return as string since we have been filtering as dates as strings
    return str(first_fiscal_week_end)


def get_latest_path(config_path):
    """
    Return the latest file path based on the path date pattern.

    config_path contains YYYYmmdd which is used to match s3 paths.

    Parameters:
        config_path (str): the config path containing the pattern
    Returns:
        (str): the latest file path for matching the config path pattern
    """
    pattern = "YYYYmmdd"
    pattern_position = config_path.find(pattern)
    if pattern_position < 0:
        raise ValueError("config path does not have YYYYmmdd pattern")

    bucket, prefix = iotools.split_path_bucket_key(
        config_path[:pattern_position]
    )
    pattern = re.compile(config_path.replace(pattern, "[0-9]{8}"))

    valid_paths = filter(
        lambda path: pattern.fullmatch(path) is not None,
        iotools.list_s3_files(bucket, prefix),
    )
    path = sorted(
        valid_paths,
        key=lambda path: path.split("_")[-1].split(".")[0],
        reverse=True,
    )[0]

    return path


def remove_spark_df(df):
    """
    Unpersist and delete the df(s) and run garbage collection.

    Parameters:
        df (spark.sql.DataFrame/[]): df(s) to remove
    Returns:
        None
    """
    if not isinstance(df, (list, tuple)):
        df = [df]

    for dat in df:
        dat.unpersist()
        del dat

    gc.collect()


def cache_df(df, level=pyspark.StorageLevel.MEMORY_ONLY):
    """
    Persist the dataframe in one of the Spark accepted levels.

    Parameters:
        df (pyspark.sql.DataFrame): dataframe to cache
        level (pyspark.storagelevel.StorageLevel): how to persist
    Returns:
        (pyspark.sql.DataFrame): dataframe which has been persisted
    """
    df = df.persist(level)
    df.count()  # spark action to trigger the cache
    return df


def fiscal_week_feature_name(weeks, feature_name):
    """
    Builds conventional feature name based on weeks and base feature names.

    Parameters:
        weeks (int): The number of weeks the feature is relevant for
        feature_name (str) : The base feature name e.g (SPEND)

    Returns:
        (str) : The feature name with the naming convention
    """
    if weeks < 1:
        raise Exception("Weeks cannot be less than 1")
    elif weeks == 1:
        return "FW_{}".format(feature_name)
    else:
        return "L{}W_{}".format(weeks, feature_name)


def create_window(weeks):
    """
    Builds window to window {weeks} back

    Args:
        c (cube.core.cube) : The current member cube
        weeks (int) : The number of weeks to window back

    Returns
        w (spark.sql.window.Window) : The generated window
    """
    return (
        W.Window.partitionBy("MBRSHP_SID")
        .orderBy("FISCAL_WEEK_END")
        .rowsBetween(-(weeks - 1), 0)
    )


def create_epoch_window(lb_weeks, cols=None):
    """
    Create time window.

    Parameters:
        lb_weeks (int): number of weeks to use in the window
    Returns:
        (pyspark.window.Window): a time window to use for partitioning members
    """
    if not cols:
        cols = ["MBRSHP_SID"]

    lb_epoch = weeks_to_epoch(lb_weeks)

    window = (
        W.Window.partitionBy(*cols).orderBy("EPOCH").rangeBetween(-lb_epoch, 0)
    )

    return window


def weeks_to_epoch(weeks):
    """
    Convert weeks to seconds
    Parameters:
        weeks (int): number of weeks
    Return:
        (int): number of seconds representing the weeks
    """

    days_per_week = 7
    hours_per_day = 24
    minutes_per_hour = 60
    seconds_per_minute = 60

    final_scalar = (
        days_per_week * hours_per_day * minutes_per_hour * seconds_per_minute
    )

    return weeks * final_scalar


def to_epoch(col_name):
    """
    Convert the date column to an epoch (cast as long)
    Parameters:
        col_name (str): name of column containing readable dates

    Returns:
        (pyspark.sql.column): column containing corresponding epoch values
    """
    epoch_col = sqlf.col(col_name).cast("timestamp").cast("long")
    return epoch_col
