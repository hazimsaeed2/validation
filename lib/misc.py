"""
Miscellaneous helper functions that don't fit elsewhere.

Includes:
    print_table()
    get_last_fiscal_weekend(start_date_delta=31*14)
    today_helper()
"""

import boto3
import datetime

from memberdna.pipelines.lib.iotools import split_path_bucket_key


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


def get_last_fiscal_weekend(start_date_delta=31 * 14, end_date_str_in=None):
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


def get_max_fiscal_week(path, date_format="%Y-%m-%d"):
    """
    Return the maximum fiscal week found within bucket/prefix.

    Args:
        path (str): a path with FISCAL_WEEK_END folders
        date_format (str): format for return date and path dates
    Returns:
        str in the same format of the path's <date>
    """

    bucket, key = split_path_bucket_key(path)
    if key[-1] == "/":
        prefix = key
    else:
        prefix = f"{key}/"
    client = boto3.client("s3")
    objs = client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter="/")
    old_date = datetime.datetime(2000, 1, 1)
    max_date = old_date

    for obj in objs["CommonPrefixes"]:
        date_str = obj["Prefix"].split("/")[-2].split("FISCAL_WEEK_END=")[-1]
        date = datetime.datetime.strptime(date_str, date_format)
        max_date = max(date, max_date)

    if max_date == old_date:
        max_date_str = None
    else:
        max_date_str = datetime.datetime.strftime(max_date, date_format)

    return max_date_str
