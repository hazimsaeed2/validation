import logging
from argparse import ArgumentParser
from unittest import mock

import findspark
import xmlrunner
from pe_memberdna.pipelines.lib.iotools import (
    split_path_bucket_key,
    write_text_to_s3,
)
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

import pe_memberdna.etl.utils.validations_ETL as validations
from pe_memberdna.testing_support.lib.utility import *

parser = ArgumentParser()
parser.add_argument(
    "--tmp_stats", help="s3 path to temporarily place unit test stats"
)
args = parser.parse_args()

LOCAL_STATS = "unittests/data/etl_validations_unit_stats.yaml"
EXPECTED = (
    "s3://memberanalytics-data-out-prod/STATS/ETL/UNIQUE_ID/"
    "etl_validations_unit_stats.yaml"
)
S3_CONF_PATH = (
    "s3://memberanalytics-data-out-prod/STATS/ETL/UNIQUE_ID/"
    "config_validations_unit.yaml"
)
LOCAL_CONF_PATH = "unittests/data/config_validations_unit.yaml"
ARCHIVE_PATH = "STATS/ETL/{}/archive/stats_{}"


def create_data():
    data = [
        ("1", "foo1", 10, 15, "2019-01-01"),
        ("2", "foo2", 10, 15, "2019-01-02"),
        ("3", "foo3", 11, 15, "2019-01-03"),
        ("4", "foo4", 11, 15, "2019-01-04"),
        ("5", "foo5", 12, 15, "2019-01-05"),
        ("6", "foo6", 10, 15, "2019-01-06"),
        ("7", "foo7", 10, 15, "2019-01-07"),
        ("8", "foo8", 11, 15, "2019-01-08"),
        ("9", "foo9", 11, 15, "2019-01-09"),
        ("10", "foo10", 12, 15, "2019-01-10"),
    ]
    rdd = sc.parallelize(data)
    df = rdd.toDF(("id", "desc", "to_cnt", "to_avg", "PURCH_DT"))
    return df


class DqCheckUnittestEtl(DqCheckUnittest):
    """Parent class for all ETL DQ check unit test classes"""

    def setUp(self):
        test_name = self.__class__.__name__
        unique_id = get_unique_id(test_name)

        self.spark = spark
        self.expected_path = EXPECTED.replace("UNIQUE_ID", unique_id)
        self.conf_path = S3_CONF_PATH.replace("UNIQUE_ID", unique_id)
        self.tabletype = "source"
        self.tablename = "df"
        self.tmp_stats = args.tmp_stats
        self.df = create_data()

        local_conf = make_paths_unique(LOCAL_CONF_PATH, test_name)
        bucket, key = split_path_bucket_key(self.conf_path)
        write_text_to_s3(bucket, key, local_conf)

        local_stats = make_paths_unique(LOCAL_STATS, test_name)
        bucket, key = split_path_bucket_key(self.expected_path)
        write_text_to_s3(bucket, key, local_stats)

        self.set_up()

        patcher = mock.patch(
            "memberdna.lib.data_quality.get_archive_path",
            return_value=ARCHIVE_PATH.format(
                unique_id,
                datetime.datetime.strftime(
                    datetime.datetime.today(), "%Y%m%d"
                ),
            ),
        )
        self.get_archive_path = patcher.start()
        self.addCleanup(patcher.stop)

    def tearDown(self):
        self.remove_stats()


class TestWrites(DqCheckUnittest):
    """Ensure each applicable method correctly writes stats when not present."""

    def setUp(self):
        test_name = self.__class__.__name__
        unique_id = get_unique_id(test_name)
        self.spark = spark
        self.expected_path = EXPECTED.replace("UNIQUE_ID", unique_id)
        self.conf_path = S3_CONF_PATH.replace("UNIQUE_ID", unique_id)
        self.tabletype = "intermediate"
        self.tablename = "df"
        self.tmp_stats = args.tmp_stats
        self.df = create_data()

        local_conf = make_paths_unique(LOCAL_CONF_PATH, test_name)
        bucket, key = split_path_bucket_key(self.conf_path)
        write_text_to_s3(bucket, key, local_conf)

        local_stats = make_paths_unique(LOCAL_STATS, test_name)
        bucket, key = split_path_bucket_key(self.expected_path)
        write_text_to_s3(bucket, key, local_stats)

        self.set_up(empty_stats=True)

        patcher = mock.patch(
            "memberdna.lib.data_quality.get_archive_path",
            return_value=ARCHIVE_PATH.format(
                unique_id,
                datetime.datetime.strftime(
                    datetime.datetime.today(), "%Y%m%d"
                ),
            ),
        )
        self.get_archive_path = patcher.start()
        self.addCleanup(patcher.stop)

    def test_compare_col_agg_prior_write(self):
        self.tabletype = "intermediate"
        self.validation_write(
            validations.CompareColAggregatesPrior, validations.validate_table
        )

    def test_compare_col_agg_both1_write(self):
        self.tabletype = "source"
        self.validation_write(
            validations.CompareColAggregatesBoth,
            validations.validate_table,
            testname="CompareColAggregatesPrior",
        )

    def test_compare_col_agg_both2_write(self):
        self.tabletype = "intermediate"
        self.validation_write(
            validations.CompareColAggregatesBoth,
            validations.validate_table,
            testname="CompareColAggregatesPrior",
        )

    def test_colnames_write(self):
        self.tabletype = "source"
        self.validation_write(
            validations.TestColNames, validations.validate_table
        )

    def tearDown(self):
        self.remove_stats()


class TestCompareColAggregatesSource(DqCheckUnittestEtl):
    """Ensure CompareColAggregatesSource passes and fails on respective data"""

    def test_compare_col_agg_source_pass(self):
        self.validation_pass(
            validations.CompareColAggregatesSource, validations.validate_table
        )
        self.validation_report_pass(
            validations.CompareColAggregatesSource, validations.validate_table
        )

    def test_compare_col_agg_source_fail(self):
        self.df = spark.sql(
            """
            SELECT * FROM df
            UNION ALL
            SELECT * FROM df
            """
        )
        self.validation_fail(
            validations.CompareColAggregatesSource, validations.validate_table
        )
        self.validation_report_fail(
            validations.CompareColAggregatesSource, validations.validate_table
        )


class TestCompareColAggregatesPrior(DqCheckUnittestEtl):
    """Ensure CompareColAggregatesPrior passes and fails on respective data"""

    def test_compare_col_agg_prior_pass(self):
        self.validation_pass(
            validations.CompareColAggregatesPrior, validations.validate_table
        )
        self.validation_report_pass(
            validations.CompareColAggregatesPrior, validations.validate_table
        )

    def test_compare_col_agg_prior_fail(self):
        self.df = spark.sql(
            """
            SELECT * FROM df
            UNION ALL
            SELECT * FROM df
            """
        )
        self.validation_fail(
            validations.CompareColAggregatesPrior, validations.validate_table
        )
        self.validation_report_fail(
            validations.CompareColAggregatesPrior, validations.validate_table
        )


class TestCompareColAggregatesBoth(DqCheckUnittestEtl):
    """Ensure CompareColAggregatesBoth passes and fails on respective data"""

    def test_compare_col_agg_both_pass(self):
        self.validation_pass(
            validations.CompareColAggregatesBoth, validations.validate_table
        )
        self.validation_report_pass(
            validations.CompareColAggregatesBoth, validations.validate_table
        )

    def test_compare_col_agg_both_fail(self):
        self.df = spark.sql(
            """
            SELECT * FROM df
            UNION ALL
            SELECT * FROM df
            """
        )
        self.validation_fail(
            validations.CompareColAggregatesBoth, validations.validate_table
        )
        self.validation_report_fail(
            validations.CompareColAggregatesBoth, validations.validate_table
        )


class TestTestOutlierDays(DqCheckUnittestEtl):
    """Ensure TestOutlierDays passes and fails on respective data"""

    def test_outlier_days_pass(self):
        self.validation_pass(
            validations.TestOutlierDays, validations.validate_table
        )
        self.validation_report_pass(
            validations.TestOutlierDays, validations.validate_table
        )

    def test_TestOutlierDays_fail(self):
        outlier = spark.createDataFrame(
            [("11", "foo11", 10, 1500, "2019-01-10")]
        )
        self.df = self.df.union(outlier)

        self.validation_fail(
            validations.TestOutlierDays, validations.validate_table
        )
        self.validation_report_fail(
            validations.TestOutlierDays, validations.validate_table
        )


class TestTestColNames(DqCheckUnittestEtl):
    """Ensure TestColNames passes and fails on respective data"""

    def test_colname_pass(self):
        self.validation_pass(
            validations.TestColNames, validations.validate_table
        )
        self.validation_report_pass(
            validations.TestColNames, validations.validate_table
        )

    def test_colname_fail(self):
        self.df = self.df.withColumnRenamed("desc", "Desc")

        self.validation_fail(
            validations.TestColNames, validations.validate_table
        )
        self.validation_report_fail(
            validations.TestColNames, validations.validate_table
        )


class TestTestDuplicates(DqCheckUnittestEtl):
    """Ensure TestDuplicates passes and fails on respective data"""

    def test_duplicates_pass(self):
        self.validation_pass(
            validations.TestDuplicates, validations.validate_table
        )
        self.validation_report_pass(
            validations.TestDuplicates, validations.validate_table
        )

    def test_duplicates_fail(self):
        dup = spark.createDataFrame([("10", "foo11", 10, 1500, "2019-01-10")])
        self.df = self.df.union(dup)

        self.validation_fail(
            validations.TestDuplicates, validations.validate_table
        )
        self.validation_report_fail(
            validations.TestDuplicates, validations.validate_table
        )


class TestTestControlTable(DqCheckUnittestEtl):
    """Ensure TestControlTable passes and fails on respective data"""

    def test_control_table_pass(self):

        data = [(None, None), (0.0, 0.0), (100.0, 100.0), (101.0, 101.0)]

        rdd = sc.parallelize(data)

        self.df = rdd.toDF(("engine_col_1", "redshift_col_1"))

        self.validation_pass(
            validations.TestControlTable, validations.validate_table
        )

        self.validation_report_pass(
            validations.TestControlTable, validations.validate_table
        )

        data = [
            (None, None),
            (
                datetime.datetime.strptime("10/10/2019", "%m/%d/%Y"),
                datetime.datetime.strptime("10/10/2019", "%m/%d/%Y"),
            ),
            (
                datetime.datetime.strptime("01/01/2010", "%m/%d/%Y"),
                datetime.datetime.strptime("01/01/2010", "%m/%d/%Y"),
            ),
        ]

        rdd = sc.parallelize(data)

        self.df = rdd.toDF(("engine_col_1", "redshift_col_1"))

        self.validation_pass(
            validations.TestControlTable, validations.validate_table
        )

        self.validation_report_pass(
            validations.TestControlTable, validations.validate_table
        )

    def test_control_table_fail(self):

        data = [
            (None, 1.0),
            (0.0, None),
            (0.0, 10.0),
            (10.0, 0.0),
            (0.1, 10.0),
            (10.0, 0.1),
        ]

        rdd = sc.parallelize(data)

        self.df = rdd.toDF(("engine_col_1", "redshift_col_1"))

        self.validation_fail(
            validations.TestControlTable, validations.validate_table
        )

        self.validation_report_fail(
            validations.TestControlTable, validations.validate_table
        )

        data = [
            (None, datetime.datetime.strptime("01/01/2019", "%m/%d/%Y")),
            (datetime.datetime.strptime("01/01/2019", "%m/%d/%Y"), None),
            (
                datetime.datetime.strptime("01/01/2019", "%m/%d/%Y"),
                datetime.datetime.strptime("10/10/2010", "%m/%d/%Y"),
            ),
            (
                datetime.datetime.strptime("10/10/2010", "%m/%d/%Y"),
                datetime.datetime.strptime("01/01/2019", "%m/%d/%Y"),
            ),
        ]

        rdd = sc.parallelize(data)

        self.df = rdd.toDF(("engine_col_1", "redshift_col_1"))

        self.validation_fail(
            validations.TestControlTable, validations.validate_table
        )

        self.validation_report_fail(
            validations.TestControlTable, validations.validate_table
        )


if __name__ == "__main__":
    findspark.init()

    name = "DQ checks source ETL validation - unittests"
    conf = SparkConf().setAppName(name)
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.getOrCreate()
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
