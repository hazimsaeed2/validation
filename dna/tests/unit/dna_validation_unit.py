import argparse
import datetime
import logging
import os
import numpy as np
import pandas as pd
import subprocess
import sys
import traceback
import unittest
import unittest.mock as mock

import findspark
import xmlrunner
import yaml
import pyspark
import pyspark.sql as sql
import pyspark.sql.functions as sqlf

import memberdna.dna.lib.validations_DNA as validations_DNA
import memberdna.testing_support.lib.utility as test_utils
import memberdna.pipelines.lib.iotools as iotools


parser = argparse.ArgumentParser()
parser.add_argument(
    "--tmp_stats", help="s3 path to temporarily place unit test stats"
)
args = parser.parse_args()

LOCAL_STATS = "unittests/data/dna_validations_unit_stats.yaml"
EXPECTED = (
    "s3://memberanalytics-data-out-prod/STATS/DNA/UNIQUE_ID/"
    "dna_validations_unit_stats.yaml"
)
S3_CONF_PATH = (
    "s3://memberanalytics-data-out-prod/STATS/DNA/UNIQUE_ID/"
    "config_validations_unit.yaml"
)
LOCAL_CONF_PATH = "unittests/data/config_validations_unit.yaml"

ARCHIVE_PATH = "STATS/DNA/{}/archive/stats_{}"


def create_data():
    """ Create sample test data """
    data = [
        (1, 10, 1.0, "2019-03-02"),
        (1, 10, 2.0, "2019-02-23"),
        (1, 10, 3.0, "2019-02-16"),
        (2, 20, 1.0, "2019-03-02"),
        (2, 20, 2.0, "2019-02-23"),
        (2, 20, 3.0, "2019-02-16"),
        (3, 30, 1.0, "2019-03-02"),
        (3, 30, 2.0, "2019-02-23"),
        (3, 30, 3.0, "2019-02-16"),
    ]
    rdd = sc.parallelize(data)
    df = rdd.toDF(("MBRSHP_SID", "var1", "var2", "FISCAL_WEEK_END"))
    return df


class DqCheckUnittestDna(test_utils.DqCheckUnittest):
    """ Parent class for all DNA DQ check unit test classes """

    def setUp(self):
        test_name = self.__class__.__name__
        unique_id = test_utils.get_unique_id(test_name)
        self.spark = spark
        self.expected_path = EXPECTED.replace("UNIQUE_ID", unique_id)
        self.conf_path = S3_CONF_PATH.replace("UNIQUE_ID", unique_id)
        self.tabletype = "DNA"
        self.tablename = "DNA"
        self.tmp_stats = args.tmp_stats
        self.df = create_data()

        local_conf = test_utils.make_paths_unique(LOCAL_CONF_PATH, test_name)
        bucket, key = test_utils.split_path_bucket_key(self.conf_path)
        iotools.write_text_to_s3(bucket, key, local_conf)

        local_stats = test_utils.make_paths_unique(LOCAL_STATS, test_name)
        bucket, key = test_utils.split_path_bucket_key(self.expected_path)
        iotools.write_text_to_s3(bucket, key, local_stats)

        self.set_up()

        patcher = mock.patch(
            "memberdna.lib.data_quality.get_archive_path",
            return_value=ARCHIVE_PATH.format(
                unique_id,
                datetime.datetime.strftime(datetime.datetime.today(), "%Y%m%d")
            )
        )
        self.get_archive_path = patcher.start()
        self.addCleanup(patcher.stop)

    def tearDown(self):
        self.remove_stats()


class TestOutlierWeeks(DqCheckUnittestDna):
    """ Ensure TestOutlierWeeks passes and fails on respective data """

    def test_pass(self):
        self.df = self.df.select("MBRSHP_SID", "var1", "FISCAL_WEEK_END")
        self.validation_pass(
            validations_DNA.TestOutlierWeeks, validations_DNA.validate_table
        )
        self.validation_report_pass(
            validations_DNA.TestOutlierWeeks, validations_DNA.validate_table
        )

    def test_fail(self):
        self.df = self.df.select("MBRSHP_SID", "var1", "FISCAL_WEEK_END")
        update_func = sqlf.when(
            sqlf.col("FISCAL_WEEK_END") == "2019-02-23",
            sqlf.col("var1") * 10000,
        ).otherwise(sqlf.col("var1"))
        self.df = self.df.withColumn("var1", update_func)
        self.validation_fail(
            validations_DNA.TestOutlierWeeks, validations_DNA.validate_table
        )
        self.validation_report_fail(
            validations_DNA.TestOutlierWeeks, validations_DNA.validate_table
        )


class TestOldDNAvsNewDNAColAggregates(DqCheckUnittestDna):
    """
    Ensure TestOldDNAvsNewDNAColAggregates passes and fails on
    respective data
    """

    def test_pass(self):
        self.df = self.df.select("MBRSHP_SID", "var1", "FISCAL_WEEK_END")
        self.validation_pass(
            validations_DNA.TestOldDNAvsNewDNAColAggregates,
            validations_DNA.validate_table,
        )
        self.validation_report_pass(
            validations_DNA.TestOldDNAvsNewDNAColAggregates,
            validations_DNA.validate_table,
        )

    def test_fail(self):
        self.df = self.df.select("MBRSHP_SID", "var1", "FISCAL_WEEK_END")
        update_func = sqlf.when(
            sqlf.col("FISCAL_WEEK_END") == "2019-02-23",
            sqlf.col("var1") * 1000,
        ).otherwise(sqlf.col("var1"))
        self.df = self.df.withColumn("var1", update_func)
        self.validation_fail(
            validations_DNA.TestOldDNAvsNewDNAColAggregates,
            validations_DNA.validate_table,
        )
        self.validation_report_fail(
            validations_DNA.TestOldDNAvsNewDNAColAggregates,
            validations_DNA.validate_table,
        )


class TestColNames(DqCheckUnittestDna):
    """ Ensure TestColNames passes and fails on respective data """

    def test_pass(self):
        self.validation_pass(
            validations_DNA.TestColNames, validations_DNA.validate_table
        )
        self.validation_report_pass(
            validations_DNA.TestColNames, validations_DNA.validate_table
        )

    def test_fail(self):
        self.df = self.df.select("MBRSHP_SID", "var1", "FISCAL_WEEK_END")
        self.validation_fail(
            validations_DNA.TestColNames, validations_DNA.validate_table
        )
        self.validation_report_fail(
            validations_DNA.TestColNames, validations_DNA.validate_table
        )


class TestCountsPerFiscalWeekEnd(DqCheckUnittestDna):
    """ Ensure TestCountsPerFiscalWeekEnd passes and fails on respective data """

    def test_pass(self):
        self.validation_pass(
            validations_DNA.TestCountsPerFiscalWeekEnd,
            validations_DNA.validate_table,
        )
        self.validation_report_pass(
            validations_DNA.TestCountsPerFiscalWeekEnd,
            validations_DNA.validate_table,
        )

    def test_fail(self):
        dup = spark.createDataFrame([(4, 30, 3.0, "2019-02-16")])
        self.df = self.df.union(dup)
        self.validation_fail(
            validations_DNA.TestCountsPerFiscalWeekEnd,
            validations_DNA.validate_table,
        )
        self.validation_report_fail(
            validations_DNA.TestCountsPerFiscalWeekEnd,
            validations_DNA.validate_table,
        )


class TestCountsPerMbrshpSid(DqCheckUnittestDna):
    """ Ensure TestCountsPerMbrshpSid passes and fails on respective data """

    def test_pass(self):
        self.validation_pass(
            validations_DNA.TestCountsPerMbrshpSid,
            validations_DNA.validate_table,
        )
        self.validation_report_pass(
            validations_DNA.TestCountsPerMbrshpSid,
            validations_DNA.validate_table,
        )

    def test_fail(self):
        dup = spark.createDataFrame([(1, 30, 3.0, "2019-02-16")])
        self.df = self.df.union(dup)
        self.validation_fail(
            validations_DNA.TestCountsPerMbrshpSid,
            validations_DNA.validate_table,
        )
        self.validation_report_fail(
            validations_DNA.TestCountsPerMbrshpSid,
            validations_DNA.validate_table,
        )


if __name__ == "__main__":
    findspark.init()

    name = "DQ checks DNA validation - unittests"
    conf = pyspark.SparkConf().setAppName(name)
    sc = pyspark.SparkContext(conf=conf)
    spark = sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logging.getLogger().setLevel(logging.INFO)
    unittest.main(
        testRunner=xmlrunner.XMLTestRunner(output="unit-test-reports"),
        # these make sure that some options that are not applicable
        # remain hidden from the help menu.
        failfast=False,
        buffer=False,
        catchbreak=False,
    )
