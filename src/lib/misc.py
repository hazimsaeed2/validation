import os
import datetime

def get_weekday_abbreviation(str_date):
    """
    Takes date represented by string in the format YYY-MM-DD returns
    the corresponding weekday

    Args:
        str_date: string in the format YYYY-MM-DD

    Returns:
        Three-letter abreviation of the corresponding weekdays
    """

    datetime_obj = datetime.datetime.strptime(str_date, "%Y-%m-%d")

    weekday_str_list = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    return weekday_str_list[datetime_obj.weekday()]


def get_last_fiscal_weekend(start_date_delta=31 * 14, end_date_str_in=None, use_this_as_today = None):
    """
    Calculates end_date - the last fiscal week end date. This is equivalent to
    finding the date of last saturday. On saturday this functions returns
    saturday of the previous week.

    In addition to this the function also returns start_date - the date
    preceeding the end_date by start_date_delta days

    Args:
        start_date_delta(optional) - difference between start_date_str and
            end_date_str in days
        end_date_str_in (format YYYY-MM-DD) - if this argument is non-None this
            function will use it as value for end_date and calculate
            start_date as start_date = end_date_str_in - start_date_delta
    Returns:
        start_date_str: string representing start_date in the format YYYY-MM-DD
        end_date_str: string representing end_date in the format YYYY-MM-DD
    """

    

    if end_date_str_in:
        end_date = datetime.datetime.strptime(end_date_str_in, "%Y-%m-%d")
    else:
        if use_this_as_today:
            today = use_this_as_today
        else:
            today = today_helper()

        idx = (today.weekday() + 1) % 7
        end_date = today - datetime.timedelta(7 + idx - 6)

    start_date_str = "{:%Y-%m-%d}".format(
        end_date - datetime.timedelta(start_date_delta)
    )
    end_date_str = "{:%Y-%m-%d}".format(end_date)

    return start_date_str, end_date_str


def get_previous_fiscal_weekend(start_date_delta, end_date_str_in):
    """
    Calculates end_date - the last fiscal week end date before the given end
    date. This is equivalent to finding the date of last saturday before the
    end date. If the end date is a saturday this functions returns same date.

    In addition to this the function also returns start_date - the date
    preceeding the end_date by start_date_delta days

    Args:
        start_date_delta - difference between start_date_str and end_date_str
            in days
        end_date_str_in (format YYYY-MM-DD) - This function will use it to
            calculte the previous fiscal weekend
    Returns:
        start_date_str: string representing start_date in the format YYYY-MM-DD
        end_date_str: string representing end_date in the format YYYY-MM-DD
    """
    end_date = datetime.datetime.strptime(end_date_str_in, "%Y-%m-%d")
    if end_date.weekday() != 5:
        idx = (end_date.weekday() + 1) % 7
        end_date = end_date - datetime.timedelta(7 + idx - 6)

    start_date_str = "{:%Y-%m-%d}".format(
        end_date - datetime.timedelta(start_date_delta)
    )
    end_date_str = "{:%Y-%m-%d}".format(end_date)

    return start_date_str, end_date_str


def today_helper():
    return datetime.date.today()


def print_table(lst):
    """
    Take a list of iterables and print them as a nicely formatted table.
    All values must be convertible to a str, or else a ValueError will
    be raised.

    Parameters:
        lst: list of iterables
    Generates: prints to stdout
    """
    pad = 2
    maxlens = [0] * len(lst[0])

    for row in lst[1:]:
        for i, col in enumerate(row):
            maxlens[i] = max(maxlens[i], len(str(col)) + pad)

    def pad_cell(i, val):
        spaces = maxlens[i] - len(str(val))
        return "%s%s|" % (val, " " * spaces)

    for row in lst:
        line = ""
        for i, col in enumerate(row):
            line += pad_cell(i, col)
        print(line)
        print("-" * sum(maxlens))

# def get_max_fiscal_week(dbutils, path, date_format="%Y-%m-%d"):
#     """
#     Finds the maximum fiscal week from partition folders using dbutils.

#     This function reads a directory where data is partitioned in Hive format,
#     like '.../FISCAL_WEEK_END=YYYY-MM-DD/'. It uses dbutils.fs.ls() and is
#     compatible with raw cloud URIs (s3://, abfss://) and DBFS paths.

#     Args:
#         path (str): The file path to the directory (e.g., 's3://bucket/path/',
#                     '/Volumes/cat/sch/vol/table/').
#         date_format (str): The string format of the date in the folder names.

#     Returns:
#         str: The most recent date found, formatted as a string, or None if
#              no valid date-partitioned folders are found.
#     """
#     all_dates = []
#     partition_prefix = "FISCAL_WEEK_END="

#     try:
#         # List all file/directory objects in the given path using dbutils
#         for file_info in dbutils.fs.ls(path):
#             # The name attribute includes the trailing slash, e.g., 'folder/'
#             folder_name = file_info.name.strip('/')

#             if folder_name.startswith(partition_prefix):
#                 try:
#                     # Extract the date part of the folder name
#                     date_str = folder_name.split('=')[-1]
#                     # Convert to a datetime object and add to our list
#                     all_dates.append(datetime.datetime.strptime(date_str, date_format))
#                 except (ValueError, IndexError):
#                     # Ignore folders that don't match the expected format
#                     continue
#     except Exception as e:
#         # dbutils can raise a Java-based exception if the path doesn't exist
#         print(f"⚠️ Error accessing path '{path}': {e}")
#         return None

#     if not all_dates:
#         return None

#     # Find the maximum date and format it as a string
#     max_date = max(all_dates)
#     return datetime.datetime.strftime(max_date, date_format)


    # bucket, key = split_path_bucket_key(path)
    # if key[-1] == "/":
    #     prefix = key
    # else:
    #     prefix = f"{key}/"
    # client = boto3.client("s3")
    # objs = client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter="/")
    # old_date = datetime.datetime(2000, 1, 1)
    # max_date = old_date

    # for obj in objs["CommonPrefixes"]:
    #     date_str = obj["Prefix"].split("/")[-2].split("FISCAL_WEEK_END=")[-1]
    #     date = datetime.datetime.strptime(date_str, date_format)
    #     max_date = max(date, max_date)

    # if max_date == old_date:
    #     max_date_str = None
    # else:
    #     max_date_str = datetime.datetime.strftime(max_date, date_format)

    # return max_date_str




import re
from pyspark.sql import functions as F

def get_latest_path(spark, config_path: str) -> str:
    """
    Return the latest file path based on the YYYYmmdd date pattern embedded in config_path.
    - Input:  s3://.../some_name_YYYYmmdd.txt
    - Output: s3://.../some_name_20250823.txt   (the most recent date match)

    Databricks-native: uses Spark 'binaryFile' for recursive listing (no boto3 / no RDD / serverless-safe).
    """
    token = "YYYYmmdd"
    pos = config_path.find(token)
    if pos < 0:
        raise ValueError("config path does not have YYYYmmdd pattern")

    before = config_path[:pos]
    slash_idx = before.rfind("/")
    if slash_idx < 0:
        raise ValueError(f"config path must include a directory before {token}")
    base_dir = before[:slash_idx + 1]  

    df = (spark.read.format("binaryFile")
            .option("recursiveFileLookup", "true")
            .load(base_dir)
            .select(F.col("path")))
    df = (df.filter(~F.col("path").rlike(r"/_temporary/|/_SUCCESS($|/)"))
           .filter(~F.col("path").endswith(".crc")))

    esc = re.escape(config_path)
    esc = esc.replace(r"\*", ".*").replace(r"\?", ".")
    regex_full = f"^{esc.replace(token, r'([0-9]{8})')}$"

    candidates = (df
        .filter(F.col("path").rlike(regex_full))
        .withColumn("yyyymmdd", F.regexp_extract(F.col("path"), regex_full, 1))
        .withColumn("yyyymmdd_dt", F.to_date("yyyymmdd", "yyyyMMdd")))

    if candidates.select("path").limit(1).count() == 0:
        raise Exception(f"No matching path found for pattern {config_path}")

    latest = (candidates
        .orderBy(F.col("yyyymmdd_dt").desc(), F.col("path").desc())
        .limit(1)
        .select("path")
        .collect()[0]["path"])

    return latest
