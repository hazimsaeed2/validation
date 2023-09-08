"""
Unit test for the file
pipelines/assignment/scripts/create_closure.py

TODO: This test is not comprehensive and does not test all data
"""

import os
import unittest

import pyspark.sql.functions as sqlf
import xmlrunner

import pe_memberdna.assignment.scripts.create_closure as create_closure
from pe_memberdna.assignment.lib.assn_io import JobManager


class TestCreateClosure(unittest.TestCase):
    def setUp(self):
        """
        Prepare test data
        """

        conf_path = os.path.join(
            os.path.abspath(os.path.dirname(os.path.dirname(__file__))),
            "conf/config_template.yml",
        )

        self.job = JobManager(
            "create_closure_unit",
            "create_closure_unit",
            conf_path_in=conf_path,
        )

        self.job.config.params["assignment_date"] = "2019-04-01"

        self.job.data.tables["closure_input"] = (
            self.job.spark.sparkContext.parallelize(
                [
                    [201, "301, 304", 26],
                    [202, "302", 52],
                    [203, "303", 52],
                    [204, "777", 52],
                ]
            )
            .toDF(["PMR Offer ID", create_closure.CAT_LIST, "closure_window"])
            .repartition(1)
        )

        self.job.data.tables["transactions"] = (
            self.job.spark.sparkContext.parallelize(
                [
                    [501, 1, "2019-01-01", 1, 301, 10000],
                    [502, 1, "2019-02-01", 2, 304, 10001],
                    [503, 1, "2019-03-01", 3, 302, 10002],
                    [508, 1, "2018-01-01", 3, 302, 10002],
                    [504, 2, "2019-03-01", 3, 303, 10003],
                    [505, 1, "2019-03-01", 4, 0, 10004],
                    [506, 1, "2019-03-01", 5, 0, 10004],
                    [507, 1, "2019-03-01", 6, 0, 10004],
                ]
            )
            .toDF(
                [
                    "PURCH_HDR_ID",
                    "QTY_IN_UNITS",
                    "PURCH_DT",
                    "MBRSHP_SID",
                    create_closure.CAT_HRRCHY,
                    "ARTICLE_NBR",
                ]
            )
            .repartition(1)
        )

        self.job.data.tables["item"] = (
            self.job.spark.sparkContext.parallelize(
                [
                    [10000, 301, "2019-01-01"],
                    [10000, 101, None],
                    [10001, 304, "2019-02-01"],
                    [10001, 104, "2019-01-01"],
                    [10002, 302, "2019-03-01"],
                    [10002, 102, None],
                    [10002, 305, "2019-01-01"],
                    [10004, 777, "2019-03-01"],
                    [10004, 0, None],
                ]
            )
            .toDF(["ARTICLE_NBR", create_closure.CAT_HRRCHY, "EXP_DT"])
            .repartition(1)
        )

        self.job.data.tables["mail_list"] = (
            self.job.spark.sparkContext.parallelize(
                [[401], [402], [403], [404], [405], [406]]
            )
            .toDF(["MBRSHP_NBR"])
            .repartition(1)
        )

        self.job.data.tables["member_extended"] = (
            self.job.spark.sparkContext.parallelize(
                [[401, 1], [402, 2], [403, 3], [404, 4], [405, 5], [406, 6]]
            )
            .toDF(["MBRSHP_NBR", "MBRSHP_SID"])
            .repartition(1)
        )

    def test_process_tbls(self):
        """
        Execute coupon closure script and verify
        closure flag for two members.

        Member 5 was chosed because she/he
        has not purchased AH5s 301, 304 and
        is therefore eligable.

        Member 3 has purchased AH5 302 and
        is therefore ineligable for the offer.
        """
        create_closure.process_tbls(self.job)

        self.assertEqual(
            self.job.data.tables["closure_input"].count()
            * self.job.data.tables["mail_list"].count(),
            self.job.data.tables["closure_coupon"].count(),
        )

        self.assertEqual(
            self.job.data.tables["closure_coupon"]
            .where(
                (sqlf.col("MBRSHP_SID") == 5)
                & (sqlf.col("cpn_nbr") == 201)
                & (sqlf.col(create_closure.CAT_HRRCHY) == "301, 304")
            )
            .select("closure_flag")
            .collect()[0][0],
            1,
        )

        self.assertEqual(
            self.job.data.tables["closure_coupon"]
            .where(
                (sqlf.col("MBRSHP_SID") == 3)
                & (sqlf.col("cpn_nbr") == 202)
                & (sqlf.col(create_closure.CAT_HRRCHY) == "302")
            )
            .select("closure_flag")
            .collect()[0][0],
            0,
        )

        expected_ahcd_set = set()
        for row in self.job.data.tables["closure_input"].collect():
            expected_ahcd_set.update(
                row[create_closure.CAT_LIST].replace(" ", "").split(",")
            )

        actual_ahcd_set = set()
        for row in self.job.data.tables["closure_coupon"].collect():
            actual_ahcd_set.update(
                row[create_closure.CAT_HRRCHY].replace(" ", "").split(",")
            )

        self.assertEqual(expected_ahcd_set, actual_ahcd_set)

        self.assertEqual(
            self.job.data.tables["closure_coupon"]
            .filter(
                (sqlf.col(create_closure.CAT_HRRCHY) == "777")
                & (sqlf.col("closure_flag") == 0)
            )
            .count(),
            3,
        )


if __name__ == "__main__":

    unittest.main(
        testRunner=xmlrunner.XMLTestRunner(output="unit-test-reports"),
        failfast=False,
        buffer=False,
        catchbreak=False,
    )
