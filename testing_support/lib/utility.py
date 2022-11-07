import datetime
import os
import re
import socket
import subprocess
import traceback
import unittest
from urllib.parse import urlparse

import boto3
import yaml

from pe_memberdna.pipelines.lib.iotools import (
    split_path_bucket_key,
    s3_copy,
    s3_delete,
    write_yaml_to_s3,
    copy_file_to_s3,
)

import pe_memberdna.etl.lib.validations_ETL as validations


class DqCheckUnittest(unittest.TestCase):
    def set_config(self):
        """
        Load the unit test config and set s3_stat_path.
        """
        response = get_s3_object(self.conf_path)
        conf = yaml.load(response, Loader=yaml.FullLoader)

        self.conf = conf["validation"]
        if self.tmp_stats is not None:
            self.conf["s3_stat_path"] = self.tmp_stats

    def create_stats(self, empty=False):
        """
        Create a temp version of the unit test stats, leaving empty if specified.
        """

        dynamic = self.conf["s3_stat_path"]
        print(dynamic)
        if empty:
            bucket, key = split_path_bucket_key(dynamic)
            write_yaml_to_s3(bucket, key, dict())
        else:
            bucket, key_source = split_path_bucket_key(self.expected_path)
            _, key_dest = split_path_bucket_key(dynamic)
            s3_copy(bucket, key_source, key_dest)

    def remove_stats(self):
        """Remove the temp version of unit test stats."""
        f = self.conf["s3_stat_path"]
        bucket, key = split_path_bucket_key(f)
        s3_delete(bucket, key, allowed_paths=["STATS/ETL/", "STATS/DNA/"])

    def validation_report_pass(self, method, val_func):
        """Ensure the method succeeds when it is supposed to."""
        res = val_func(
            spark=self.spark,
            tabletype=self.tabletype,
            tablename=self.tablename,
            config_validation=self.conf,
            df=self.df,
            check_list=[method],
            archive=False,
            throw_errors=False,
        )
        self.assertEqual(res[0]["error_count"], 0)

    def validation_report_fail(self, method, val_func):
        """Ensure the method succeeds when it is supposed to."""
        res = val_func(
            spark=self.spark,
            tabletype=self.tabletype,
            tablename=self.tablename,
            config_validation=self.conf,
            df=self.df,
            check_list=[method],
            archive=False,
            throw_errors=False,
        )

        self.assertGreater(res[0]["error_count"], 0)

    def validation_pass(self, method, val_func):
        """Ensure the method succeeds when it is supposed to."""
        try:
            val_func(
                spark=self.spark,
                tabletype=self.tabletype,
                tablename=self.tablename,
                config_validation=self.conf,
                df=self.df,
                check_list=[method],
            )
        except Exception as e:
            traceback.print_exc()
            self.fail("Unit test of passing {} failed".format(method))

    def validation_fail(self, method, val_func):
        """Ensure the method succeeds when it is supposed to."""
        msg = "Unit test of failing {} failed".format(method)
        with self.assertRaises(validations.DataQualityException, msg=msg):
            res = val_func(
                spark=self.spark,
                tabletype=self.tabletype,
                tablename=self.tablename,
                config_validation=self.conf,
                df=self.df,
                check_list=[method],
            )

    def set_up(self, empty_stats=False):
        """
        Initialize config, stats, and data for a unit test.
        """
        self.set_config()
        self.df.registerTempTable("df")
        self.create_stats(empty=empty_stats)

    def validation_write(self, method, val_func, testname=None):
        """
        Ensure the method writes out correct stats when not present.

        Args:
            tabletype (str): type of the table - source, intermediate, or dna
            method (class): method to execute for validation
            testname (str): desirable to set to subclass name when a class is being
                called with parameters (e.g. CompareColAggregatesBoth)
                {Default: method.__name__}
        """
        if testname is None:
            testname = method.__name__
        fail_msg = "Unit test of writing stats for {} failed".format(method)
        try:
            val_func(
                spark=self.spark,
                tabletype=self.tabletype,
                tablename=self.tablename,
                config_validation=self.conf,
                df=self.df,
                check_list=[method],
            )
        except Exception as e:
            traceback.print_exc()
            self.fail(fail_msg)

        response_expected = get_s3_object(self.expected_path)
        expected = yaml.load(response_expected, Loader=yaml.FullLoader)[
            self.tabletype
        ]["df"][testname]
        response_stat = get_s3_object(self.conf["s3_stat_path"])
        written = yaml.load(response_stat, Loader=yaml.FullLoader)[
            self.tabletype
        ]["df"][testname]

        self.assertEqual(expected, written, msg=fail_msg)


def get_s3_object(path):
    """
    Reads the path and returns data from S3

        Args:
            path: contains bucket name and key

        Returns:
            Body of the object containing actual data
    """
    s3_client = boto3.client("s3")
    parse_file = urlparse(path)
    parse_bucket = parse_file.netloc
    parse_path = parse_file.path
    response = s3_client.get_object(Bucket=parse_bucket, Key=parse_path[1:])
    return response["Body"]


def copy_local_to_s3(in_path, out_path):
    """
    Copy local file to S3
    """

    if "/unit_test_data/" in out_path:
        copy_file_to_s3(in_path, out_path)
    else:
        raise Exception(
            'The path "'
            + out_path
            + '" does not contain the string "/unit_test_data/"'
        )


def remove_from_s3(path):
    """
    Delete an s3 file or files
    """

    bucket, key = split_path_bucket_key(path)
    s3_delete(bucket, key, allowed_paths=["unit_test_data/"])


def get_unique_id(test_name):
    """
    Create a unique id for each test run.
    Args:
        test_name (str): the name of the test being run

    Returns:
        (str): a unique id composed by user, machine ip and test name

    """

    cwd = os.getcwd()
    person = re.search("(?<=/home/hadoop/).*", cwd).group(0).split("/")[0]
    ip = socket.gethostbyname(socket.gethostname())
    return "<{}-{}-{}>".format(person, ip, test_name)


def make_paths_unique(local_path, test_name):
    """
    Replace all occurrences of "UNIQUE_ID" with calculated unique_id

    Args:
        local_path(str): local file with templated paths
        test_name(str): the name of the test being run
    Returns:
         unique_content (str): content with unique paths
    """
    with open(local_path, "rb") as f:
        content = f.read()
        unique_content = content.replace(
            b"UNIQUE_ID", bytes(get_unique_id(test_name), "utf-8")
        )

    return unique_content
