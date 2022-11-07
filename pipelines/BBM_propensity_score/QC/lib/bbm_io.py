"""Functions and paths to help with general io for all modeling purposes."""

import argparse
import datetime
from datetime import timedelta
import numpy as np
import os
import pandas as pd
import re

import boto3
import yaml

from pe_member_dna.pipelines.lib.iotools import list_s3_dir


def load_config():
    """
    Returns configuration file from args.config as a dictionary.
    Parameters:
        None

    Returns:
        (dict) configuration specs
    """
    cur_file_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..")
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("--config")
    args = parser.parse_args()
    if not os.path.isfile(args.config):
        msg = "--config: invalid file path: {}".format(args.config)
        raise parser.error(msg)
    with open(args.config, "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

    return cfg


def get_matching_s3_keys(bucket, path, contains):
    """
    gets lists files in the specified path and filters based on certain criteria.
    Parameters:
        bucket(str): bucket of the files ae are interested in
        path(str): path of the folder that contains the files
        contains([str]): any string that uniquely identifies the list
    Output:
        list of file paths(list)
    """
    list_of_score_files = []
    for key in list_s3_dir(bucket, path):
        if contains in key:
            list_of_score_files.append(key)

    return list_of_score_files


def map_file_date(files):
    """
    Returns list of files as a dict, where key is the date and value is the file.

    Assumes date format of "YYYY-mm-dd". If the same date appears in more than one file, only the last file will be used as the dict value.

    Parameters:
        files ([str]): list of files which include date in the name

    Generates:
        (dict): date:file mapping
    """
    out = {}
    for file in files:
        dt = re.search(r"\d{4}-\d{2}-\d{2}", file).group()
        out[dt] = file
    return out


def get_past_file_date(files, cur_date, n_weeks_apart=3):
    """
    Return most recent date from files that has at least n_weeks lag to cur_date.

    Parameters:
        files ([str]): list of file names
        cur_date (str): date to look back from, in %Y-%m-%d format
        n_weeks_apart (int): number of weeks lag to look back for past file date
    """
    dt_threshold = datetime.datetime.strptime(
        cur_date, "%Y-%m-%d"
    ) - timedelta(weeks=n_weeks_apart)

    max_dt = None
    for dt in map_file_date(files).keys():
        dt = datetime.datetime.strptime(dt, "%Y-%m-%d")
        if dt == dt_threshold:
            return datetime.datetime.strftime(dt, "%Y-%m-%d")

        if dt < dt_threshold:
            if max_dt:
                max_dt = max(max_dt, dt)
            else:
                max_dt = dt

    if max_dt:
        max_dt = datetime.datetime.strftime(max_dt, "%Y-%m-%d")
    return max_dt
