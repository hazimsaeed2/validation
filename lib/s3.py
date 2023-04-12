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


def s3_folder_existence_check(s3_path):
    """
    Check for existence of file on s3 location
    Particularly checks for existence followed by number of files
    being greater than 1 and ensures to check if no other process is writing
    to s3_path by checking for '_temporary'

    Args:
        s3_path (str) - s3 path which is subjected to existence check
    """
    s3_path = remove_file_name_from_s3_path(s3_path)
    bucket, key = split_path_bucket_key(s3_path)
    list_s3 = list_s3_dir(bucket, key)

    if len(list_s3) < 1 or any(
        [os.path.join(s3_path, "_temporary") in x for x in list_s3]
    ):
        raise Exception(
            "One of the following has occured:\n"
            + "1. Recent folder on S3 does not exist"
            + "\n2. Recent folder exists but is empty"
            + " at the S3 location: "
            + str(s3_path)
        )


def s3_file_recency_check(recency_lookback_duration, path):
    """
    Check for recency and last Modified timestamp of
    file on s3 location against the threshold

    Args:
        recency_lookback_duration (int) - recency threshold in number of days
        path (str) - path which is subjected to recency
                        and freshness check
    """
    if recency_lookback_duration > 0:
        response = get_s3_api_response(path)
        last_modified_timestamp = get_s3_relative_timestamp(response, "oldest")
        today = date.today()
        elapsed_time = today - last_modified_timestamp.date()
        age_in_days = elapsed_time.days

        if age_in_days > recency_lookback_duration:
            raise Exception(
                f"Data present on S3 for dataset '{path}' is stale. "
                + f"Last modified on {age_in_days} day(s) ago! "
                + f"Requested tolerance: {recency_lookback_duration} day(s)."
            )
    else:
        print(
            f"WARN:Skipping freshness check for the S3 path {path} as the value for recency_lookback_duration is <= 0 "
        )


def get_most_recent_s3_object(path):
    """
    Get most recent path from file
    If Inside folder two files, one is end with _SUCCESS then return single useful file.

    Args:
        path (str): s3 path we want to remove * and get matched paths from s3

    Returns:
        matched(str): matched paths
    """
    split_path = path.split("*")
    bucket, prefix = split_path_bucket_key(split_path[0])
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    get_last_modified = lambda obj: int(obj["LastModified"].strftime("%s"))
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    all_matched_paths = []
    for page in page_iterator:
        if "Contents" in page:
            for res in page["Contents"]:
                key = res["Key"]
                match_path = f"s3://{bucket}/{key}"
                if match_path[len(split_path[0]) + 1].isdigit():
                    all_matched_paths.append(res)
    matched_paths = [
        obj["Key"]
        for obj in sorted(
            all_matched_paths, key=get_last_modified, reverse=True
        )
    ]
    matched_path = ""
    if len(matched_paths) > 0:
        matched_path = f"s3://{bucket}/{matched_paths[0]}"
    latest_path = [matched_path if matched_path else None]
    return latest_path


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
            regex_p = "\d{4}\d{2}\d{2}"
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
                                file_name = f"(?P<str>.*)\.{file_name[-1]}$"
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


def input_data_validator(final_recency_duration, data_path):
    """
    Check for existence and last Modified timestamp of
    file on s3 location against the threshold

    Args:

        final_recency_duration (int) - recency threshold in number of days
        data_path - dict structure containing the source and intermediate paths
    """
    if "*" in data_path:
        matched_paths = get_most_recent_s3_object(data_path)
        if matched_paths:
            s3_folder_existence_check(matched_paths[0])
            s3_file_recency_check(final_recency_duration, matched_paths[0])
        else:
            print(
                f"No paths found matching pattern {data_path}. Hence skipping checking recency."
            )
    else:
        s3_folder_existence_check(data_path)
        s3_file_recency_check(final_recency_duration, data_path)
