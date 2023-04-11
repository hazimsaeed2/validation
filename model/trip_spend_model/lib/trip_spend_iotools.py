# -*- coding: utf-8 -*-
"""
Library for reading and writing to file.
"""
import csv
import io


import boto3
from botocore.client import Config
import pandas as pd

import joblib


def write_list_to_csv(path, input_list, headers):
    """
    Write python list to .csv

    Write python list to .csv by iterating through elements and appending them
    to output file. Overwrites if the file already exists.
    Args:
        path (String): path of output file
        list (arrray): array of elements to write to file
        headers (array<string>): array of header strings
    Returns:
    """

    with open(path, "w") as output:
        file_writer = csv.writer(
            output, delimiter=",", quoting=csv.QUOTE_MINIMAL
        )
        file_writer.writerow(headers)
        file_writer.writerows(input_list)
    return


def read_csv_pandas_s3(bucket, path):
    """
    Read csv file to Pandas dataframe from s3

    Create Boto bucket, read in csv file from the specified location
    Args:
        bucket (string): name of s3 bucket
        path (string): path for input file
    Returns:
        data (Pandas.DataFrame)
    """
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=path)
    data = pd.read_csv(io.BytesIO(obj["Body"].read()))
    return data


def read_specific_columns_in_parquet_file(bucket, key, columns):
    """
    Read single parquet file to Pandas dataframe from s3 specifying what columns to read

    Read given columns from parquet file on s3 to python Pandas
    Args:
        bucket (string): name of s3 bucket
        key (string): path for input file
        columns (array<string>): list of column headers
    Returns:
        data (Pandas.DataFrame)
    """
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pq.read_table(io.BytesIO(obj["Body"].read()), columns).to_pandas()
    return df


def read_parquet_file(bucket, key):
    """
    Read single parquet file to Pandas dataframe from s3

    Args:
        bucket (string): name of s3 bucket
        key (string): path for input file
    Returns:
        data (Pandas.DataFrame)
    """
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pq.read_table(io.BytesIO(obj["Body"].read())).to_pandas()
    return df


def read_parquet_pandas_s3(bucket, path, columns=[]):
    """
    Read folder of parquet files

    Gets a list of all file headers in folder,
    call the correct subfunction to read in specfic file.
    Return the combination of all sub-files.

    Args:
        bucket (string): name of s3 bucket
        key (string): path for input file
        columns (string<array>): columns to read in
    Returns:
        data (Pandas.DataFrame)
    """
    config = Config(connect_timeout=5, retries={"max_attempts": 0})
    client = boto3.client("s3", config=config)
    objects_dict = client.list_objects_v2(Bucket=bucket, Prefix=path)
    s3_keys = [
        item["Key"]
        for item in objects_dict["Contents"]
        if item["Key"].endswith(".parquet")
    ]
    if len(columns) == 0:
        dfs = [read_parquet_file(bucket, key) for key in s3_keys]
    else:
        dfs = [
            read_specific_columns_in_parquet_file(bucket, key, columns)
            for key in s3_keys
        ]
    df = pd.concat(dfs, ignore_index=True)
    return df


def read_specific_week_customer_cube(bucket, path, week, columns=[]):
    """
    Save ROC plot

    Args:
        bucket (string): S3 bucket of data
        week (string):
        columns (array<string>): list of columns to read from s3
        path (string): path of input file
    Returns:
        DataFrame
    """
    client = boto3.client("s3")
    objects_dict = client.list_objects_v2(
        Bucket=bucket, Prefix="%s/FISCAL_WEEK_END=%s" % (path, week)
    )
    s3_keys = [
        item["Key"]
        for item in objects_dict["Contents"]
        if item["Key"].endswith(".parquet")
    ]
    if len(columns) == 0:
        dfs = [read_parquet_file(bucket, key) for key in s3_keys]
    else:
        dfs = [
            read_specific_columns_in_parquet_file(bucket, key, columns)
            for key in s3_keys
        ]
    df = pd.concat(dfs, ignore_index=True)
    return df


def read_multiple_csv(bucket, path):
    """
    read every csv in an s3 folder

    read all the files in the specified folder.
    iterate through the files and read them one by one.
    append the result.

    Args:
        bucket (string): location on s3
        path (string): input path for folder
    Returns:
        data (DataFrame)
    """
    client = boto3.client("s3")
    objects_dict = client.list_objects_v2(Bucket=bucket, Prefix=path)
    s3_keys = [
        item["Key"]
        for item in objects_dict["Contents"]
        if item["Key"].endswith(".csv")
    ]
    dfs = [read_csv_pandas_s3(bucket, key) for key in s3_keys]
    return pd.concat(dfs, ignore_index=True)


def read_csv_spark(spark, path):
    """
    Read in csv

    Read in csv from specified path and saves it as as a pyspark dataframe

    Args:
        spark (spark): spark instance.
        path (string): path to input file.
    Returns:
        data (DataFrame)
    """
    return spark.read.csv(path, inferSchema=True, header=True)


def read_parquet_spark(spark, path):
    """
    Read in parquet file

    Read in file from specified path and saves it as as a pyspark dataframe

    Args:
        spark (spark): spark instance.
        path (string): path to input file.
    Returns:
        data (DataFrame)
    """
    return spark.read.load(path)


def write_to_csv(data, bucket, path):
    """
    Write pandas DF to CSV on S3

    Creates boto instance and writes pandas dataframe to CSV hosted on S3.

    Args:
        data (DataFrame): data to write to csv
        path (string): path for file
        bucket (string): string with S3 bucket name
    Returns:
    """
    csv_buffer = io.BytesIO()
    data.to_csv(csv_buffer)
    s3_resource = boto3.resource("s3")
    s3_resource.Object(bucket, path).put(Body=csv_buffer.getvalue())
    return


def dump_job_lib_to_s3(data, path, bucket):
    """
    Dumps joblib file to s3

    Args:
        data (DataFrame): data to write to csv
        path (string): path for file
        bucket (string): string with S3 bucket name
    Returns:
    """
    my_mock_file = io.StringIO()
    joblib.dump(data, my_mock_file)
    data = my_mock_file.read()
    s3_client = boto3.client("s3")
    s3_client.Object(bucket, path).put(Body=data.getvalue())
    return
