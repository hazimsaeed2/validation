from mock import Mock, patch
import unittest
import xmlrunner
import os
import pandas as pd
from unittest import mock as mock

from pyspark.sql import SparkSession, SQLContext
import pyspark.sql.functions as sqlf
import pyspark.sql.types as sqlt
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pe_member_dna.pipelines.assignment.lib.assn_io import JobManager
from pe_member_dna.pipelines.assignment.lib.qctests import (
    QCTestRunner,
    count_oob_cell_sizes,
    count_null_estimated_sizes,
    check_sensitive_content,
)

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")


class AvgSpendDecrTestCase(unittest.TestCase):
    """Unit tests for avg_spend_decr"""

    def setUp(self):
        self.job = JobManager("test_avg_spend_decr", "text_avg_spend_decr")

        self.qcTestRunner = QCTestRunner(self.job, ["avg_spend_decr"])
        self.mail_list_columns = ["MBRSHP_SID", "MBRSHP_NBR", "decile"]

        self.dna_columns = [
            "MBRSHP_SID",
            "TENURE",
            "EXP_DT",
            "MBRSHP_EXP_DT",
            "MBRSHP_RNWL_DT",
        ]

        dna_pdf = pd.DataFrame(
            columns=self.dna_columns,
            data=[
                # 2019-05-25 is end of a fiscal week
                ["101", 0, "2019-05-25", "2019-05-25", "2019-05-25"],
                ["102", 0, "2019-05-25", "2019-05-25", "2019-05-25"],
                ["103", 0, "2019-05-25", "2019-05-25", "2019-05-25"],
                ["1", 1000, "2019-05-25", "2019-05-25", "2019-05-25"],
                ["29", 1000, "2019-05-25", "2019-05-25", "2019-05-25"],
                ["3", 1000, "2019-05-25", "2019-05-25", "2019-05-25"],
                ["42", 1000, "2019-05-25", "2019-05-25", "2019-05-25"],
                ["5", 1000, "2019-05-25", "2019-05-25", "2019-05-25"],
                ["6", 1000, "2019-05-25", "2019-05-25", "2019-05-25"],
                ["61", 1000, "2019-05-25", "2019-05-25", "2019-05-25"],
                ["62", 1000, "2019-05-25", "2019-05-25", "2019-05-25"],
                ["727", 1000, "2019-05-25", "2019-05-25", "2019-05-25"],
                ["8", 1000, "2019-05-25", "2019-05-25", "2019-05-25"],
                ["9000", 1000, "2019-05-25", "2019-05-25", "2019-05-25"],
                ["10", 1000, "2019-05-25", "2019-05-25", "2019-05-25"],
                ["11", 1000, "2019-05-25", "2019-05-25", "2019-05-25"],
                ["12", 1000, "2019-05-25", "2019-05-25", "2019-05-25"],
            ],
        )

        self.job.data.tables["dna"] = self.job.spark.createDataFrame(dna_pdf)

        mail_list_pdf = pd.DataFrame(
            columns=self.mail_list_columns,
            data=[
                ["101", "1000101", "new_member"],
                ["102", "1000102", "new_member"],
                ["103", "1000103", "new_member"],
                ["1", "10001", "1"],
                ["29", "100029", "1"],
                ["3", "10003", "2"],
                ["42", "100042", "3"],
                ["5", "10005", "4"],
                ["6", "10006", "5"],
                ["61", "100061", "5"],
                ["62", "100062", "5"],
                ["727", "1000727", "6"],
                ["8", "10008", "7"],
                ["9000", "10009000", "8"],
                ["10", "100010", "9"],
                ["11", "100011", "10"],
                ["12", "100012", "10"],
            ],
        )
        self.job.data.tables["mail_list"] = self.job.spark.createDataFrame(
            mail_list_pdf
        )

        self.raw_member_columns = [
            "MBRSHP_SID",
            "MBRSHP_NBR",
            "MKT_CD",
            "MBRSHP_FEE_INC",
        ]
        raw_member_pdf = pd.DataFrame(
            columns=self.raw_member_columns,
            data=[
                ["101", "1000101", "a", 10],
                ["102", "1000102", "a", 10],
                ["103", "1000103", "a", 10],
                ["1", "10001", "a", 10],
                ["29", "100029", "a", 10],
                ["3", "10003", "a", 10],
                ["42", "100042", "a", 10],
                ["5", "10005", "a", 10],
                ["6", "10006", "a", 10],
                ["61", "100061", "a", 10],
                ["62", "100064", "a", 10],
                ["727", "1000727", "a", 10],
                ["8", "10008", "a", 10],
                ["9000", "10009000", "a", 10],
                ["10", "100010", "a", 10],
                ["11", "100011", "a", 10],
                ["12", "100012", "a", 10],
            ],
        )
        self.job.data.tables["raw_member"] = self.job.spark.createDataFrame(
            raw_member_pdf
        )

    def test_valid_avg_spend_decr(self):

        self.base_columns = [
            "MBRSHP_SID",
            "LFOURW_SPEND_IN_STORE",
            "LTWELVEW_SPEND_IN_STORE",
            "LFIFTY-TWOW_SPEND_IN_STORE",
        ]

        base_pdf = pd.DataFrame(
            columns=self.base_columns,
            data=[
                ["101", 10, 11, 12],
                ["102", 100, 101, 102],
                ["103", 1000, 1001, 1002],
                ["1", 140, 141, 142],
                ["29", 130, 131, 132],
                ["3", 120, 121, 122],
                ["42", 110, 111, 112],
                ["5", 100, 101, 102],
                ["6", 90, 91, 92],
                ["61", 80, 81, 82],
                ["62", 70, 71, 72],
                ["727", 60, 61, 62],
                ["8", 50, 51, 52],
                ["9000", 41, 40, 42],
                ["10", 30, 31, 32],
                ["11", 20, 21, 22],
                ["12", 10, 12, 12],
            ],
        )
        self.job.data.tables["base"] = self.job.spark.createDataFrame(base_pdf)
        actual_output = self.qcTestRunner.avg_spend_decr(base=None)
        self.assertEqual(actual_output, "Pass")

    def test_invalid_avg_spend_decr(self):
        self.base_columns = [
            "MBRSHP_SID",
            "LFOURW_SPEND_IN_STORE",
            "LTWELVEW_SPEND_IN_STORE",
            "LFIFTY-TWOW_SPEND_IN_STORE",
        ]

        base_pdf = pd.DataFrame(
            columns=self.base_columns,
            data=[
                ["101", 10, 11, 12],
                ["102", 100, 101, 102],
                ["103", 1000, 1001, 1002],
                ["1", 140, 141, 142],
                ["29", 130, 131, 132],
                ["3", 120, 121, 122],
                ["42", 110, 111, 112],
                ["5", 1, 2, 3],
                ["6", 90, 91, 92],
                ["61", 80, 81, 82],
                ["62", 70, 71, 72],
                ["727", 60, 61, 62],
                ["8", 50, 51, 52],
                ["9000", 41, 40, 42],
                ["10", 30, 31, 32],
                ["11", 20, 21, 22],
                ["12", 10, 12, 12],
            ],
        )
        self.job.data.tables["base"] = self.job.spark.createDataFrame(base_pdf)
        actual_output = self.qcTestRunner.avg_spend_decr(base=None)
        self.assertEqual(actual_output, "Fail")


class CellSizeTestCase(unittest.TestCase):
    """Unit tests related to estimated_size column added to cells.csv"""

    def setUp(self):
        good_rdd = sc.parallelize([(100, 110), (230, 200), (50, 50), (60, 75)])
        self.good = good_rdd.toDF(["cell_size", "estimated_size"])

        bad_rdd = sc.parallelize(
            [(200, 300), (400, 500), (50, 100), (100, 50)]
        )
        self.bad = bad_rdd.toDF(["cell_size", "estimated_size"])

        blank_rdd = sc.parallelize(
            [(200, None), (400, None), (50, 100), (100, 50)]
        )
        self.blank = blank_rdd.toDF(["cell_size", "estimated_size"])

    def test_cell_size_expected(self):
        """
        check that actual cell_size is within +/- 25% of estimate or 100000
        """
        total_oob = count_oob_cell_sizes(self.good)

        self.assertEqual(total_oob, 0)

    def test_cell_size_unexpected(self):
        """
        check that actual cell_size is not within +/- 25% of estimate or 100000
        """

        total_wb = self.bad.count() - count_oob_cell_sizes(self.bad)

        self.assertEqual(total_wb, 1)

    def test_empty_estimate(self):
        total_nulls = count_null_estimated_sizes(self.blank)

        self.assertEqual(total_nulls, 2)


class SensitiveContentTestCase(unittest.TestCase):
    """Unit tests for sensitive cetegory coupon assignments."""

    def setUp(self):
        self.job = JobManager("test_sensitive_content")
        self.job.tables = {}

        self.job.data.tables["dna"] = sc.parallelize(
            [
                ("mbr_none", 0, 0, 0, 0),
                ("mbr_both", 1, 1, 0, 0),
                ("mbr_pet", 1, 0, 0, 0),
                ("mbr_baby", 0, 1, 0, 0),
            ]
        ).toDF(
            [
                "MBRSHP_SID",
                "L52W_HAS_BOUGHT_PET",
                "L52W_HAS_BOUGHT_BABY",
                "L52W_HAS_BOUGHT_WOMEN",
                "L52W_HAS_BOUGHT_CHILDREN",
            ]
        )

        self.job.data.tables["exclusion_rules"] = sc.parallelize(
            [
                ("AH4_CD", 100, "exclude", "SENSITIVE", "PET"),
                ("AH5_CD", 200, "exclude", "SENSITIVE", "BABY"),
            ]
        ).toDF(
            [
                "CATEGORY_TYPE",
                "CATEGORY_CD",
                "INCLUDE_OR_EXCLUDE",
                "EXCLUSION_TYPE",
                "EXCLUSION_SUBTYPE",
            ]
        )

        self.job.data.tables["coup_map"] = sc.parallelize(
            [(1, 10), (2, 20), (3, 30), (3, 31), (4, 40), (4, 41), (5, 50)]
        ).toDF(["cpn_nbr", "article_nbr"])

        self.job.data.tables["article_map"] = sc.parallelize(
            [
                (10, 100, 999),  # AH4 exclusion
                (20, 999, 200),  # AH5 exclusion
                (30, 100, 999),
                (31, 999, 999),  # inclusion over-ride
                (40, 999, 200),
                (41, 999, 999),  # inclusion over-ride
                (50, 999, 999),
            ]
        ).toDF(["article_nbr", "AH4_CD", "AH5_CD"])

        self.job.data.tables["cf_original"] = sc.parallelize(
            [
                ("mbr_none", "AH4_CD", 100, -0.5),
                ("mbr_none", "AH5_CD", 200, -0.5),
                ("mbr_none", "AH4_CD", 999, 0.5),
                ("mbr_both", "AH4_CD", 100, 0.5),
                ("mbr_both", "AH5_CD", 200, 0.5),
                ("mbr_pet", "AH4_CD", 100, 0.5),
                ("mbr_pet", "AH5_CD", 200, -0.5),
                ("mbr_pet", "AH5_CD", 999, 0.5),
                ("mbr_baby", "AH4_CD", 100, -0.5),
                ("mbr_baby", "AH5_CD", 200, 0.5),
                ("mbr_baby", "AH4_CD", 999, 0.5),
            ]
        ).toDF(["MBRSHP_SID", "CATEGORY_LVL", "CATEGORY_ID", "PREDICTION"])

    @patch("memberdna.pipelines.lib.iotools.is_s3_path")
    @patch("memberdna.pipelines.lib.iotools.s3_delete")
    def test_cleanup_delete(self, s3_delete, is_s3_path):
        "Test that old assignment is deleted when nothing is passed"
        is_s3_path.return_value = True

        self.job.data.tables["exclusion_rules"] = sc.parallelize(
            [
                ("AH4_CD", 100, "exclude", "NOT_SENSITIVE", "PET"),
                ("AH5_CD", 200, "exclude", "NOT_SENSITIVE", "BABY"),
            ]
        ).toDF(
            [
                "CATEGORY_TYPE",
                "CATEGORY_CD",
                "INCLUDE_OR_EXCLUDE",
                "EXCLUSION_TYPE",
                "EXCLUSION_SUBTYPE",
            ]
        )

        output_path = "s3://bucket/key"
        self.job.config.paths["SENSITIVE_QC"] = output_path

        rtn = check_sensitive_content(self.job)
        s3_delete.assert_called_with(
            "bucket", "key", allowed_paths=output_path
        )

    @patch("memberdna.pipelines.lib.iotools.is_s3_path")
    @patch("memberdna.pipelines.lib.iotools.s3_delete")
    def test_no_sensitive_exclusions(self, s3_delete, is_s3_path):
        """
        Test that old output is deleted when no sensitive exclusions exist and
        the test returns 'NA'
        """
        exc_cols = sqlt.StructType(
            [
                sqlt.StructField("CATEGORY_TYPE", sqlt.StringType(), False),
                sqlt.StructField("CATEGORY_CD", sqlt.IntegerType(), False),
                sqlt.StructField(
                    "INCLUDE_OR_EXCLUDE", sqlt.StringType(), False
                ),
                sqlt.StructField("EXCLUSION_TYPE", sqlt.StringType(), False),
                sqlt.StructField("EXCLUSION_SUBTYPE", sqlt.StringType(), True),
            ]
        )

        self.job.data.tables["exclusion_rules"] = spark.createDataFrame(
            sc.emptyRDD(), exc_cols
        )

        output_path = "s3://bucket/key"
        self.job.config.paths["SENSITIVE_QC"] = output_path
        is_s3_path.return_value = True  # old sensitive output exists

        rtn = check_sensitive_content(self.job)
        self.assertEqual(rtn, "NA")
        s3_delete.assert_called_with(
            "bucket", "key", allowed_paths=output_path
        )

    @patch("memberdna.pipelines.lib.iotools.is_s3_path")
    def test_no_sensitive_assignments(self, is_s3_path):
        """
        Test that no action is taken and test returns 'NA' when there are no
        sensitive coupons assigned.
        """
        output_path = "s3://bucket/key"
        self.job.config.paths["SENSITIVE_QC"] = output_path
        is_s3_path.return_value = False  # old sensitive output does not exist

        self.job.data.tables["assignment"] = sc.parallelize(
            [("mbr_none", "999", 5)]
        ).toDF(["mbrshp_sid", "bf_construct", "cpn_nbr"])

        rtn = check_sensitive_content(self.job)
        self.assertEqual(rtn, "NA")

    @patch("memberdna.pipelines.lib.iotools.is_s3_path")
    @patch("memberdna.pipelines.lib.iotools.s3_delete")
    def test_pass_with_cf_eligible(self, s3_delete, is_s3_path):
        """
        Test for no sensitive assignments after CF eligible is accounted for
        """
        is_s3_path.return_value = True
        output_path = "s3://bucket/key"
        self.job.config.paths["SENSITIVE_QC"] = output_path

        self.job.data.tables["assignment"] = sc.parallelize(
            [("mbr_baby", "999", 3), ("mbr_pet", "999", 4)]
        ).toDF(["mbrshp_sid", "bf_construct", "cpn_nbr"])

        rtn = check_sensitive_content(self.job)
        self.assertEqual(rtn, "Pass")

        s3_delete.assert_called_with(
            "bucket", "key", allowed_paths=output_path
        )

    @patch("memberdna.pipelines.lib.iotools.is_s3_path")
    @patch("memberdna.pipelines.lib.iotools.s3_delete")
    def test_pass_has_bought(self, s3_delete, is_s3_path):
        """
        Test for sensitive assignments which has purchases in the last 52 weeks
        """
        is_s3_path.return_value = True

        output_path = "s3://bucket/key"
        self.job.config.paths["SENSITIVE_QC"] = output_path

        self.job.data.tables["assignment"] = sc.parallelize(
            [
                ("mbr_pet", "999", 1),
                ("mbr_baby", "999", 2),
                ("mbr_both", "999", 1),
                ("mbr_both", "999", 2),
            ]
        ).toDF(["mbrshp_sid", "bf_construct", "cpn_nbr"])

        rtn = check_sensitive_content(self.job)
        self.assertEqual(rtn, "Pass")

        s3_delete.assert_called_with(
            "bucket", "key", allowed_paths=output_path
        )

    @patch("pyspark.sql.dataframe.DataFrameWriter")
    def test_fail_not_bought(self, writer):
        """
        Test for sensitive assignments which do not have
        purchases in the last 52 weeks
        """
        df_writer = mock.MagicMock()
        df_writer.csv.return_value = None

        writer.return_value = df_writer

        output_path = "s3://bucket/key"
        self.job.config.paths["SENSITIVE_QC"] = output_path
        self.job.data.tables["assignment"] = sc.parallelize(
            [("mbr_none", "999", 1)]
        ).toDF(["mbrshp_sid", "bf_construct", "cpn_nbr"])

        rtn = check_sensitive_content(self.job)
        self.assertEqual(rtn, "Fail")

        df_writer.csv.assert_called_with(
            output_path, header=True, mode="overwrite"
        )

    @patch("memberdna.pipelines.lib.iotools.is_s3_path")
    @patch("memberdna.pipelines.lib.iotools.s3_delete")
    def test_exclusion_rules_are_optional(self, s3_delete, is_s3_path):
        """
        Test that EXCLUSIONS can be NULL
        """
        is_s3_path.return_value = True

        output_path = "s3://bucket/key"
        self.job.config.paths["SENSITIVE_QC"] = output_path
        self.job.data.tables.pop("exclusion_rules")
        self.job.data.tables["assignment"] = sc.parallelize(
            [("mbr_none", "999", 1)]
        ).toDF(["mbrshp_sid", "bf_construct", "cpn_nbr"])

        rtn = check_sensitive_content(self.job)
        self.assertEqual(rtn, "NA")

        s3_delete.assert_called_with(
            "bucket", "key", allowed_paths=output_path
        )


if __name__ == "__main__":

    unittest.main(
        testRunner=xmlrunner.XMLTestRunner(output="unit-test-reports"),
        failfast=False,
        buffer=False,
        catchbreak=False,
    )
