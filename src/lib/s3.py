"""
Generic S3 helper functions.

split_path_bucket_key()
list_s3_dir()
remove_file_name_from_s3_path()
get_s3_api_response()
get_s3_relative_timestamp()
s3_folder_existence_check()
s3_file_recency_check()
get_s3_matched_paths()
input_data_validator()
"""
import os
import re
from datetime import date
from urllib.parse import urlparse

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import col, count, isnan, sum, when

from datetime import datetime, timezone, timedelta
from typing import Iterable, List, Optional, Tuple, Union

from pyspark.sql import functions as F

from pyspark.dbutils import DBUtils

def _init_dbutils_spark(spark_session):
    global dbutils
    global spark
    spark = spark_session
    dbutils = DBUtils(spark)

def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)

def _from_epoch_seconds(epoch_s: int) -> datetime:
    return datetime.fromtimestamp(epoch_s, tz=timezone.utc)

def _is_temp_or_marker(path: str) -> bool:
    name = path.rsplit("/", 1)[-1]
    return (
        name in {"_SUCCESS", "_temporary"} or
        "/_temporary/" in path or
        name.endswith(".crc")
    )

def _spark_list_binary(paths: Union[str, List[str]]):
    """
    Efficiently list files (recursively) with modification time using Spark's binaryFile.
    Accepts a single path with/without globs, or a list of paths.
    """
    df = (
        spark.read.format("binaryFile")
        .option("recursiveFileLookup", "true")
        .load(paths)
        .select(
            F.col("path").alias("path"),
            # seconds since epoch (Spark: cast(timestamp as long) -> seconds)
            F.col("modificationTime").cast("timestamp").cast("long").alias("mtime_s"),
            F.col("length").alias("size"),
        ))
    return df.filter(~F.col("path").rlike(r"/_temporary/|/_SUCCESS($|/)")).filter(~F.col("path").endswith(".crc"))

def _first_existing_child(path: str) -> Optional[str]:
    """
    Quick existence probe for non-glob paths using dbutils.fs.ls on the driver.
    Returns the first child path if any, else None.
    """
    try:
        entries = dbutils.fs.ls(path)
        if entries:
            return entries[0].path
    except Exception:
        return None
    return None



def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def _fmt_uc_name(table_full_name: str) -> str:
    """
    Wrap each identifier part in backticks to safely handle special chars.
    Works with 1-, 2-, or 3-part names (catalog.schema.table).
    """
    parts = [p.strip("`") for p in table_full_name.split(".")]
    return ".".join(f"`{p}`" for p in parts)

def etl_input_table_validator(
    *tables_list,
    recency_lookback_duration: Union[int, dict],
    spark
) -> None:
    """
    Validate a Unity Catalog managed Delta table:
      1) The table exists and is Delta.
      2) The table contains at least one row.
      3) The last commit time (latest version in DESCRIBE HISTORY) is within the given lookback.

    Args:
        table_full_name: Fully-qualified UC name, e.g. 'catalog.schema.table'
        recency_lookback_duration: int days OR dict {'days': <int>, ...} (order ignored)
        spark: SparkSession
    Raises:
        Exception with a clear message when a validation fails.
    """
    if isinstance(recency_lookback_duration, dict):
        days = int(recency_lookback_duration.get("global", 0))
    else:
        days = int(recency_lookback_duration)
    threshold_dt = _now_utc() - timedelta(days=days)

    for table_full_name in tables_list:
        ident = _fmt_uc_name(table_full_name)

        try:
            detail = spark.sql(f"DESCRIBE DETAIL {ident}")
        except:
            raise Exception(f"Table not found: {table_full_name}")

        detail_row = detail.limit(1).collect()[0]
        fmt = detail_row["format"]
        if fmt is None or fmt.lower() != "delta":
            raise Exception(f"Table {table_full_name} is not a Delta table (format={fmt}).")

        has_data = spark.read.table(table_full_name).limit(1).count() > 0
        if not has_data:
            raise Exception(f"No data found in table {table_full_name}")

        try:
            hist = spark.sql(f"DESCRIBE HISTORY {ident}")
        except:
            raise Exception(f"Cannot read history for table {table_full_name}")

        last_ts = (
            hist.orderBy(F.col("version").desc())
                .select("timestamp")
                .limit(1)
                .collect()[0][0]
        )
        if last_ts is None:
            raise Exception(f"No history entries found for table {table_full_name}")

        last_update_dt = _as_utc(last_ts)

        if last_update_dt < threshold_dt:
            raise Exception(
                f"Data is stale for table {table_full_name}. "
                f"Last table update time {last_update_dt.isoformat()} is older than "
                f"threshold {threshold_dt.isoformat()} (lookback={days}d)."
            )




def split_path_bucket_key(path):
    """Split a full path into a bucket and key for s3 writes.

    Does what it says.

    Parameters:
        path (str): full s3 path
    Returns:
        bucket (str): bucket portion of s3 path
        key (str): key portion of s3 path
    """
    parsed = urlparse(path)
    bucket = parsed.netloc
    key = parsed.path[1:]
    return (bucket, key)

def get_s3_api_response(path):
    """
    Retrieve/extract S3 boto API response for a given path
    Can be used to fetch any given S3 attribute
    Use pagination for get s3 records
    to be used for any further logic
    Args:
        path (str) - s3 path which is subjected to existence check
    Returns:
        response(dict)
    """
    bucket, key = split_path_bucket_key(path)
    try:
        s3_client = boto3.client("s3")

        paginator = s3_client.get_paginator("list_objects")
        pages = paginator.paginate(Bucket=bucket, Prefix=key)
        response = {}
        for page in pages:
            if "ResponseMetadata" not in response:
                response.update({"ResponseMetadata": page["ResponseMetadata"]})
            if "Contents" not in response:
                response.update({"Contents": page["Contents"]})
            else:
                response["Contents"].extend(page["Contents"])
        return response
    except:
        raise Exception("Incorrect S3 path or permission denied")


def calculate_null_percentages(df, cols):
    """
    To Calculate the null % in the current df
    Args:
        df (spark df) - Whose Null values to be verified.
        cols (list) - List of columns to be checked for null values.
    Returns:
        % nulls in df
    """
    total_count = df.count()
    null_counts = df.select([
        (sum(when(col(c).isNull() | isnan(c), 1).otherwise(0)) * 100 / total_count).alias(c)
        for c in cols
    ])
    return null_counts

def join_missing_path_s3(path):
    """
    To make in sync with prod s3 paths. 
    We are generating it if already present its valiadted.
    Args:
        path - Which is to be valiadted.
    Returns:
        verified s3 path 
    """
    bucket, key = split_path_bucket_key(path)
    s3 = boto3.client('s3')
    result = s3.list_objects_v2(Bucket=bucket, Prefix=key)
    if 'Contents' in result:
        s3_path = path
    else:
        s3_path = path.replace("Driving_Dist/","Driving_Dist/Code_and_Data_repo/")

    return s3_path


def list_s3_dir(bucket, path):
    """
        Lists all S3 objects which are inside this path. Similar to listing
        files in a directory.
        IMPORTANT: Directories will not be listed. They are not S3 objects.

    Parameters:
        bucket: string representing the s3 bucket
        path: string prefix
    Returns:
        all_keys([str, str,...]): a list containing full S3 uri
    """
    config = Config(connect_timeout=5)
    s3 = boto3.resource("s3", config=config)
    bucket_obj = s3.Bucket(bucket)

    hard_prefix = path if path.endswith("/") else path + "/"
    all_keys = [
        "s3://{bucket}/{key}".format(
            bucket=summary.bucket_name, key=summary.key
        )
        for summary in bucket_obj.objects.filter(Prefix=hard_prefix)
    ]
    return all_keys


def remove_file_name_from_s3_path(s3_path):
    """
    Remove file name from S3 path

    Args:
        s3_path (str) - s3 path which is subjected to existence check
    Returns:
        s3_path(str)
    """
    new_path = s3_path.split("/")
    if "." in new_path[-1]:
        new_path[-1] = ""
    s3_path = "/".join(new_path)
    return s3_path


def get_s3_api_response(path):
    """
    Retrieve/extract S3 boto API response for a given path
    Can be used to fetch any given S3 attribute
    Use pagination for get s3 records
    to be used for any further logic
    Args:
        path (str) - s3 path which is subjected to existence check
    Returns:
        response(dict)
    """
    bucket, key = split_path_bucket_key(path)
    try:
        s3 = boto3.client("s3")

        paginator = s3.get_paginator("list_objects")
        pages = paginator.paginate(Bucket=bucket, Prefix=key)
        response = {}
        for page in pages:
            if "ResponseMetadata" not in response:
                response.update({"ResponseMetadata": page["ResponseMetadata"]})
            if "Contents" not in response:
                response.update({"Contents": page["Contents"]})
            else:
                response["Contents"].extend(page["Contents"])
        return response
    except:
        raise Exception("Incorrect S3 path or permission denied")


def get_s3_relative_timestamp(response, param="latest"):
    """
    Retrieve/extract timestamp related information from a given API response
    For multiple files we can choose if we need the most recent/oldest timestamp
    Args:
        response (dict) - S3 API response
        param (str) - argument to decide computation of oldest/recent
        timestamp for a given set of file(s)
    Returns:
        response(dict)
    """
    if response:
        if "Contents" in response:
            if param == "latest":
                last_modified_timestamp = max(
                    [
                        res["LastModified"].replace(tzinfo=None)
                        for res in response["Contents"]
                    ]
                )
                return last_modified_timestamp
            elif param == "oldest":
                last_modified_timestamp = min(
                    [
                        res["LastModified"].replace(tzinfo=None)
                        for res in response["Contents"]
                    ]
                )
                return last_modified_timestamp
            else:
                raise Exception("Incorrect param argument")
        else:
            raise Exception("Corrupt response API, missing 'Contents'")
    else:
        raise Exception("Empty API response")


def s3_folder_existence_check(s3_path: str) -> None:
    """
    Raise if the folder/file does not exist or is currently being written (/_temporary).
    Matches previous behavior: checks that at least one concrete object exists.
    """
    if "*" not in s3_path and "{" not in s3_path and "}" not in s3_path and "?" not in s3_path:
        first = _first_existing_child(s3_path)
        if not first:
            raise Exception(f"No paths found under {s3_path}")
        if "/_temporary/" in first:
            raise Exception(f"Path {s3_path} appears to be in-progress (_temporary present)")
        return

    df = _spark_list_binary(s3_path).limit(1)
    rows = df.collect()
    if not rows:
        raise Exception(f"No paths found matching pattern {s3_path}")
    if any("/_temporary/" in r["path"] for r in rows):
        raise Exception(f"Path {s3_path} appears to be in-progress (_temporary present)")


def s3_file_recency_check(
    recency_lookback_duration: Union[int, dict],
    s3_path: str
) -> None:
    """
    Validate that files under s3_path meet recency threshold.

    recency_lookback_duration can be:
      - int: number of days (backwards from now), using the legacy default ordering
      - dict: {'days': <int>, 'order': 'latest' | 'oldest'}  # preserves your code's semantics

    Behavior:
      * If 'order' == 'latest' -> compare the NEWEST file's mtime against now - days
      * If 'order' == 'oldest' -> compare the OLDEST file's mtime against now - days
    """
    if isinstance(recency_lookback_duration, dict):
        days = int(recency_lookback_duration.get("days", 0))
        order = str(recency_lookback_duration.get("order", "oldest")).lower()
    else:
        days = int(recency_lookback_duration)
        order = "oldest"

    threshold_dt = _now_utc() - timedelta(days=days)

    df = _spark_list_binary(s3_path)

    if df.select("path").limit(1).count() == 0:
        raise Exception(f"No objects to validate under {s3_path}")

    agg_col = F.min("mtime_s") if order == "oldest" else F.max("mtime_s")
    mtime_s = df.agg(agg_col.alias("ts")).collect()[0]["ts"]
    candidate_dt = _from_epoch_seconds(mtime_s)
    # print(f"Validating {s3_path}: {candidate_dt.isoformat()}")
    if candidate_dt < threshold_dt:
        raise Exception(
            f"Data is stale for {s3_path}. "
            f"{order.capitalize()} object time {candidate_dt.isoformat()} is older than "
            f"threshold {threshold_dt.isoformat()} (lookback={days}d)."
        )


def get_most_recent_s3_object(s3_glob_path: str) -> List[str]:
    """
    For a wildcard path, return a single-element list with the most recent object's path.
    Ignores _SUCCESS/_temporary markers.
    """
    df = (
        _spark_list_binary(s3_glob_path)
        .orderBy(F.col("mtime_s").desc())
        .limit(1)
    )
    rows = df.collect()
    return [rows[0]["path"]] if rows else []


def check_if_file_exists(path):
    """
    Check for existence of path on s3 location
    Args:
        path (str) -  s3 path which is subjected to existence check
    Returns:
        Bool (True/False) - Whether path exist or not
    """
    s3 = boto3.client("s3")
    bucket, key = split_path_bucket_key(path)
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def update_most_recent_input_paths(
    path_dir, data_paths, most_recent_input_paths_list
):
    """
    Retrieve/extract timestamp related information from a given API response
    For multiple files we can choose if we need the most recent timestamp
    Args:
        data_paths - dict structure containing the source and intermediate paths
        most_recent_input_paths_list (list) - paths which are subjected to existence
                      and update most recent paths
        path_dir (str) - path dir
    """
    if most_recent_input_paths_list is not None:
        for table_name in most_recent_input_paths_list:
            table_path = data_paths[path_dir][table_name]
            run_date = "*20"
            regex_p = r"\d{4}\d{2}\d{2}"
            path_exist = check_if_file_exists(table_path)
            if not path_exist:
                if len(run_date) > 0 and run_date in table_path:
                    split_key = run_date[0]
                    split_path = table_path.split(split_key)
                    bucket, key = split_path_bucket_key(split_path[0])
                    s3 = boto3.client("s3")
                    paginator = s3.get_paginator("list_objects_v2")
                    get_last_modified = lambda obj: int(
                        obj["LastModified"].strftime("%s")
                    )
                    page_iterator = paginator.paginate(
                        Bucket=bucket, Prefix=key
                    )
                    all_matched_paths = []
                    for page in page_iterator:
                        if "Contents" in page:
                            if (
                                "*" in split_path[-1]
                                or "nobs" in split_path[-1]
                            ):
                                file_name = split_path[-1].split(".")
                                file_name = rf"(?P<str>.*)\.{file_name[-1]}$"
                            else:
                                file_name = split_path[-1]
                            for res in page["Contents"]:
                                key = res["Key"]
                                match_path = f"s3://{bucket}/{key}"
                                regex_path = (
                                    split_path[0] + regex_p + file_name
                                )
                                check_regex = re.findall(
                                    rf"{regex_path}",
                                    match_path,
                                )
                                if check_regex:
                                    all_matched_paths.append(res)
                    matched_paths = [
                        obj["Key"]
                        for obj in sorted(
                            all_matched_paths,
                            key=get_last_modified,
                            reverse=True,
                        )
                    ]
                    matched_path = ""
                    if len(matched_paths) > 0:
                        matched_path = f"s3://{bucket}/{matched_paths[0]}"
                        data_paths[path_dir][table_name] = matched_path
                        print(
                            f"Path {table_path} got replaced by {matched_path}"
                        )
                    else:
                        print("No paths found matching pattern {table_path}")

            else:
                print(f"No paths on s3 {table_path}")


def input_data_validator(final_recency_duration: Union[int, dict], data_path: str) -> None:
    """
    Entry point used by pe_memberdna.etl.lib.s3.etl_input_data_validator.
    Matches the legacy control flow:

      - If data_path contains a wildcard: pick the single most recent matched object,
        then run existence + recency on that concrete object; if nothing matched, log/skip recency.
      - If no wildcard: validate the folder/file directly.
    """
    if "*" in data_path or "{" in data_path or "}" in data_path or "?" in data_path:
        matched_paths = get_most_recent_s3_object(data_path)
        if matched_paths:
            s3_folder_existence_check(matched_paths[0])
            s3_file_recency_check(final_recency_duration, matched_paths[0])
        else:
            print(f"No paths found matching pattern {data_path}. Hence skipping checking recency.")
    else:
        s3_folder_existence_check(data_path)
        s3_file_recency_check(final_recency_duration, data_path)
