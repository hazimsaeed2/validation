"""Integration Test for coupon creation file."""
import unittest
from datetime import datetime

import pandas
import xmlrunner
from mock import Mock, patch

from pe_memberdna.assignment.lib.checks import check_prod_status
from pe_memberdna.assignment.lib.coupon_utils import (
    clean_cpg_coupon_file,
    remove_exclusions,
)


class TestUtils(unittest.TestCase):
    """Unit tests for coupon utility functions."""

    def test_clean_cpg_coupon_file(self):
        """Test clean coupon file."""
        l = [
            (
                "37.3",
                "1000169113",
                "(1) A1 Steak Sauce 2 pk./15 oz.OR Lea & Perrins Worcestershire Sauce2 pk./15 oz.",
                "26775, 202975,",
                "5.49 - 8.29",
                "8.29",
                "$1.75",
                "FY19_BBM12_A1_LEA & PERRINS_SAVE$1.75",
                "2002356",
                "1",
                "1",
                "1",
                "Paper Coupon",
                "07/14/2018",
                "08/14/2018",
                1,
            )
        ]
        h = [
            "Page #",
            "Promo #",
            "Full Offer Description",
            "Article Number",
            "Chain Level Retail P",
            "Promo Price",
            "Discount Value",
            "Promo Description",
            "PMR Offer ID",
            "Quantity Threshold",
            "Eligible for MMPC",
            "Eligible for ATC",
            "Promo Type",
            "Valid From",
            "Valid To",
            "self_funded_flag",
        ]
        df = sc.parallelize(l).toDF(h)
        coupon_types = ["Paper Coupon", "Clipless Coupon"]
        cleaned = clean_cpg_coupon_file(df, coupon_types).toPandas()
        desired_cols = [
            "cpn_desc",
            "article_nbr",
            "cpn_dollar_threshold",
            "cpn_dollar_off",
            "cpn_nbr",
            "cpn_qty_threshold",
            "cpn_class_id",
            "offer_id",
        ]
        headers = [item in cleaned.columns for item in desired_cols]

        first_cpnid = cleaned.iloc[0]["cpn_nbr"]

        self.assertEqual(len(cleaned), 2)
        self.assertEqual(len(headers), 8)
        self.assertEqual(first_cpnid, 2002356)

    def test_remove_exclusions(self):
        """Test coupon_utils.remove_exclusions()."""
        coup_bdy = [(1, "desc1", 2), (2, "desc2", 3), (3, "desc3", 1)]
        coup_hdr = ["cpn_nbr", "cpn_desc", "category"]
        cat_dna = [
            (1, "cat_desc1", "exclude", "funtimes"),
            (2, "cat_desc2", "exclude", "complicated"),
            (3, "cat_desc3", None, None),
        ]
        cat_hdr = [
            "category",
            "category_desc",
            "INCLUDE_OR_EXCLUDE",
            "EXCLUSION_TYPE",
        ]
        art_bdy = [
            (1, "ah4_desc1", "ah5_desc1"),
            (2, "ah4_desc2", "ah5_desc2"),
            (3, "ah4_desc3", "ah5_desc3"),
        ]
        art_hdr = ["article_nbr", "ah4_desc", "ah5_desc"]

        coups = sc.parallelize(coup_bdy).toDF(coup_hdr)
        cat_dna = sc.parallelize(cat_dna).toDF(cat_hdr)
        art = sc.parallelize(art_bdy).toDF(art_hdr)
        excl_types = ["funtimes"]
        removed, _ = remove_exclusions(coups, cat_dna, art, excl_types, None)
        removed = removed.toPandas()
        # changed assertion as we are only outputting the excluded ones for now
        self.assertEqual(len(removed), 2)
        self.assertEqual(len(removed.columns), 3)


class TestCheckProdStatus(unittest.TestCase):
    """unit tests for check_prod_status"""

    def setUp(self):
        self.coupon_log_for_job_finish = pandas.DataFrame(
            columns=["campaign", "log_event", "run_name", "run_type", "time"],
            data=[
                [
                    "mmpc_test",
                    "END",
                    "test_run",
                    "prod",
                    "2018-12-12-05-13-04",
                ],
                [
                    "mmpc_test",
                    "END",
                    "test_run",
                    "prod",
                    "2018-12-12-05-15-44",
                ],
            ],
        )

        self.coupon_log_for_job_rerun = pandas.DataFrame(
            columns=["campaign", "log_event", "run_name", "run_type", "time"],
            data=[
                [
                    "mmpc_test",
                    "END",
                    "test_run",
                    "prod",
                    "2018-12-12-05-13-04",
                ],
                [
                    "TestCheckProdStatus",
                    "END",
                    "test-rerun",
                    "prod",
                    "2018-12-12-05-15-44",
                ],
            ],
        )

        self.coupon_log_for_dead_job = pandas.DataFrame(
            columns=["campaign", "log_event", "run_name", "run_type", "time"],
            data=[
                [
                    "mmpc_test",
                    "END",
                    "test_run",
                    "prod",
                    "2018-12-12-05-13-04",
                ],
                [
                    "TestCheckProdStatus",
                    "START",
                    "test-dead",
                    "prod",
                    "2018-12-12-05-15-44",
                ],
            ],
        )

        self.coupon_log_for_collision = pandas.DataFrame(
            columns=["campaign", "log_event", "run_name", "run_type", "time"],
            data=[
                [
                    "mmpc_test",
                    "END",
                    "test_run",
                    "prod",
                    "2018-12-12-05-13-04",
                ],
                [
                    "TestCheckProdStatus",
                    "START",
                    "test-dead",
                    "prod",
                    str(datetime.now().strftime("%Y-%m-%d-%H-%M-%S")),
                ],
            ],
        )

    @patch("memberdna.pipelines.assignment.lib.checks.get_datetime_now")
    @patch("memberdna.pipelines.assignment.lib.checks.write_local_to_s3")
    @patch("memberdna.pipelines.assignment.lib.checks.read_s3_to_local")
    def test_last_job_is_finished(
        self, read_s3_to_local, write_local_to_s3, get_datetime_now
    ):
        now = datetime.now()
        get_datetime_now.return_value = now
        read_s3_to_local.return_value = self.coupon_log_for_job_finish

        job = Mock(
            config=Mock(
                paths={
                    "COUPON_LOG_FOLDER": "/path/logs",
                    "COUPON_LOG": "this.log",
                },
                params={
                    "run_name": "test-can-start",
                    "campaign": "TestCheckProdStatus",
                    "run_type": "prod",
                },
            )
        )

        check_prod_status(job)

        write_local_to_s3.assert_called_once_with(
            {
                "run_type": "prod",
                "time": str(now.strftime("%Y-%m-%d-%H-%M-%S")),
                "log_event": "START",
                "campaign": "TestCheckProdStatus",
                "run_name": "test-can-start",
            },
            "this.log",
        )

    @patch("memberdna.pipelines.assignment.lib.checks.get_datetime_now")
    @patch("memberdna.pipelines.assignment.lib.checks.write_local_to_s3")
    @patch("memberdna.pipelines.assignment.lib.checks.read_s3_to_local")
    def test_last_job_rerun(
        self, read_s3_to_local, write_local_to_s3, get_datetime_now
    ):
        now = datetime.now()
        get_datetime_now.return_value = now
        read_s3_to_local.return_value = self.coupon_log_for_job_rerun

        job = Mock(
            config=Mock(
                paths={
                    "COUPON_LOG_FOLDER": "/path/logs",
                    "COUPON_LOG": "this.log",
                },
                params={
                    "run_name": "test-rerun",
                    "campaign": "TestCheckProdStatus",
                    "run_type": "prod",
                },
            )
        )

        check_prod_status(job)

        write_local_to_s3.assert_called_once_with(
            {
                "run_type": "prod",
                "time": str(now.strftime("%Y-%m-%d-%H-%M-%S")),
                "log_event": "START",
                "campaign": "TestCheckProdStatus",
                "run_name": "test-rerun",
            },
            "this.log",
        )

    @patch("memberdna.pipelines.assignment.lib.checks.get_datetime_now")
    @patch("memberdna.pipelines.assignment.lib.checks.warnings.warn")
    @patch("memberdna.pipelines.assignment.lib.checks.write_local_to_s3")
    @patch("memberdna.pipelines.assignment.lib.checks.read_s3_to_local")
    def test_last_job_is_dead(
        self, read_s3_to_local, write_local_to_s3, log, get_datetime_now
    ):
        now = datetime.now()
        get_datetime_now.return_value = now
        read_s3_to_local.return_value = self.coupon_log_for_dead_job

        job = Mock(
            config=Mock(
                paths={
                    "COUPON_LOG_FOLDER": "/path/logs",
                    "COUPON_LOG": "this.log",
                },
                params={
                    "run_name": "test-can-start",
                    "campaign": "TestCheckProdStatus",
                    "run_type": "prod",
                },
            )
        )

        check_prod_status(job)

        write_local_to_s3.assert_called_once_with(
            {
                "run_type": "prod",
                "time": str(now.strftime("%Y-%m-%d-%H-%M-%S")),
                "log_event": "START",
                "campaign": "TestCheckProdStatus",
                "run_name": "test-can-start",
            },
            "this.log",
        )

        log.assert_called_with(
            "WARNING: the previous coupon etl scripts started over 120"
            " minutes ago without reaching to the end"
        )

    @patch("memberdna.pipelines.assignment.lib.checks.write_local_to_s3")
    @patch("memberdna.pipelines.assignment.lib.checks.read_s3_to_local")
    def test_job_collision(self, read_s3_to_local, write_local_to_s3):
        read_s3_to_local.return_value = self.coupon_log_for_collision
        job = Mock(
            config=Mock(
                paths={
                    "COUPON_LOG_FOLDER": "/path/logs",
                    "COUPON_LOG": "this.log",
                },
                params={
                    "run_name": "test-cannot-start",
                    "campaign": "TestCheckProdStatus",
                    "run_type": "prod",
                },
            )
        )

        with self.assertRaises(ValueError):
            check_prod_status(job)

        write_local_to_s3.assert_not_called()


if __name__ == "__main__":
    import findspark

    findspark.init()
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import Row, SparkSession

    name = "coupon_creation_unit_test"

    spark = SparkSession.builder.appName(name).getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    unittest.main(
        testRunner=xmlrunner.XMLTestRunner(output="unit-test-reports"),
        # these make sure that some options that are not applicable
        # remain hidden from the help menu.
        failfast=False,
        buffer=False,
        catchbreak=False,
    )
