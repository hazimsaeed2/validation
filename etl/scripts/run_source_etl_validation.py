import csv
import io
import logging
from argparse import ArgumentParser
from urllib.parse import urlparse

import boto3
import pe_memberdna.etl.utils.validations_ETL as validations
import yaml
from pyspark.sql.types import *

parser = ArgumentParser()
parser.add_argument(
    "--config",
    help="path to the member DNA unit test configuration file",
    default="../configs/config.yaml",
)

args = parser.parse_args()


def run_DQ_tests():

    with open(args.config) as config_file:
        config = yaml.load(config_file, Loader=yaml.FullLoader)

    out_path = config["data_paths"]["dq"]

    res = []

    for table_name in config["data_paths"]["intermediate"]:

        df = spark.read.parquet(
            config["data_paths"]["intermediate"][table_name]
        )

        res += validations.validate_table(
            spark=spark,
            tabletype="intermediate",
            tablename=table_name,
            config_validation=config["validation"],
            df=df,
            check_list=[
                validations.CompareColAggregatesPrior,
                validations.TestOutlierDays,
                validations.TestColNames,
                validations.TestDuplicates,
                validations.TestControlTable,
            ],
            archive=False,
            throw_errors=False,
        )

    keys = res[0].keys()

    parsed = urlparse(out_path)
    bucket = parsed.netloc
    key = parsed.path[1:]

    csv_buffer = io.StringIO()

    dict_writer = csv.DictWriter(csv_buffer, keys)
    dict_writer.writeheader()
    dict_writer.writerows(res)

    s3_resource = boto3.resource("s3")
    s3_resource.Object(bucket, key).put(
        Body=csv_buffer.getvalue(), ServerSideEncryption="aws:kms"
    )


if __name__ == "__main__":
    import os
    import sys

    import findspark

    findspark.init()
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import Row, SparkSession

    name = "DQ_check_tests"

    conf = SparkConf().setAppName(name)
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logging.getLogger().setLevel(logging.INFO)
    run_DQ_tests()
