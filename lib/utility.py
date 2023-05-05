import boto3
from datetime import datetime as dt
import gc
import re

from pe_memberdna.lib.iotools import (
    split_path_bucket_key,
    list_s3_files,
)


def remove_spark_df(df):
    """
    Unpersist and delete the df(s) and run garbage collection.

    Args:
        df (spark.sql.DataFrame/[]): df(s) to remove
    """
    if not isinstance(df, (list, tuple)):
        df = [df]

    for dat in df:
        dat.unpersist()
        del dat

    gc.collect()


def get_max_file_date(bucket, prefix, format="%Y-%m-%d", hist_date=0):
    """
    Return the maximum file date found within bucket/prefix.

    Assumes the path structure ends in =<date>

    Args:
        bucket (str): an s3 bucket name
        prefix (str): an s3 object prefix (e.g. path) of form pre1/pre2/etc/
    Returns:
        str in the same format of the path's <date>
    """
    client = boto3.client("s3")
    objs = client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter="/")

    print(prefix)
    print(objs)
    max_dt = dt.strptime("2000-01-01", "%Y-%m-%d")
    max_dt_minus_1 = max_dt

    for i, obj in enumerate(objs["CommonPrefixes"]):

        if (
            ("customer_cube_" in obj["Prefix"])
            & (not ("customer_cube_full" in obj["Prefix"]))
            & (not (":" in obj["Prefix"]))
        ):
            d = obj["Prefix"].split("/")[-2].split("customer_cube_")[-1]

            d = dt.strptime(d, format)

            if d > max_dt:
                max_dt_minus_1 = max_dt
                max_dt = d

    if hist_date == 0:
        return dt.strftime(max_dt, format)
    if hist_date == 1:
        return dt.strftime(max_dt_minus_1, format)
    else:
        raise Exception(
            'incorrect value passed for hist_date: ""' + str(hist_date) + '"'
        )


def get_next_file_date(bucket, prefix, date, format="%Y-%m-%d"):
    """
    Return the next cube date found within bucket/prefix after the given date.

    Assumes the path structure ends in =<date>

    Args:
        bucket (str): an s3 bucket name
        prefix (str): an s3 object prefix (e.g. path) of form pre1/pre2/etc/
        date (str): user provided date
    Returns:
        str in the same format of the path's <date> after the given date
    """

    client = boto3.client("s3")
    objs = client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter="/")

    date = dt.strptime(date, format)
    next_dt = dt.strptime("3000-01-01", "%Y-%m-%d")

    for i, obj in enumerate(objs["CommonPrefixes"]):

        if (
            ("customer_cube_" in obj["Prefix"])
            & (not ("customer_cube_full" in obj["Prefix"]))
            & (not (":" in obj["Prefix"]))
        ):
            d = obj["Prefix"].split("/")[-2].split("customer_cube_")[-1]

            d = dt.strptime(d, format)

            if d >= date and d < next_dt:
                next_dt = d

    if next_dt == dt.strptime("3000-01-01", "%Y-%m-%d"):
        return None

    return dt.strftime(next_dt, format)


def get_quotient_path(config_path):
    """
    Return the latest quotient file based on the path date pattern.

    config_path contains YYYYddmm which is used to match s3 paths.

    Args:
        config_path (str): the config value from quotient_id_path
    Returns:
        quotient_id_path (str): the latest path for quotient_id
    """
    quotient_id_pattern = "YYYYmmdd"
    pattern_position = config_path.find(quotient_id_pattern)
    if pattern_position < 0:
        raise Exception("quotient_id_path does not have YYYYmmdd pattern")

    bucket, prefix = split_path_bucket_key(config_path[:pattern_position])
    pattern = re.compile(config_path.replace(quotient_id_pattern, "[0-9]{8}"))

    valid_paths = filter(
        lambda path: pattern.fullmatch(path) is not None,
        list_s3_files(bucket, prefix),
    )
    quotient_id_path = sorted(
        valid_paths,
        key=lambda path: path.split("_")[-1].split(".")[0],
        reverse=True,
    )[0]

    return quotient_id_path
