import unittest
import warnings

import pandas as pd
import xmlrunner

from pyspark.sql import SparkSession

from pe_memberdna.dna.member.lib.coupon_digital_features import (
    feature_cpn_channel,
)
from pe_memberdna.pipelines.assignment.lib.assn_io import JobManager


class CouponDigitalFeaturesTestCase(unittest.TestCase):
    """Unit test for coupon digital features"""

    def setUp(self):
        self.job = JobManager("test_coupon_digital_features")
        self.job.tables = {}
        self.job.config.params["params"] = {}

        # Setup input and expected data
        self.dna = sc.parallelize(
            [
                ("1001", "2021-07-03"),
                ("1002", "2021-07-03"),
                ("1003", "2021-07-03"),
                ("1004", "2021-07-03"),
            ]
        ).toDF(
            [
                "MBRSHP_SID",
                "FISCAL_WEEK_END",
            ]
        )

        self.job.data.tables["detail"] = sc.parallelize(
            [
                ("1001", "2021-07-03", "ZPAP", "1"),
                ("1001", "2021-07-03", "ZCOU", "2"),
                ("1002", "2021-07-03", "ZCOU", "2"),
                ("1003", "2021-07-03", "ZPAP", "3"),
                ("1004", "2021-07-03", "", ""),
            ]
        ).toDF(
            [
                "MBRSHP_SID",
                "FISCAL_WEEK_END",
                "DISCOUNT_TYPE_CD",
                "VECTOR_OFFER_ID",
            ]
        )

        dna_expected = sc.parallelize(
            [
                ("1001", "2021-07-03", "dual"),
                ("1002", "2021-07-03", "digital"),
                ("1003", "2021-07-03", "paper"),
                ("1004", "2021-07-03", "no_cpns"),
            ]
        ).toDF(
            [
                "MBRSHP_SID",
                "FISCAL_WEEK_END",
                "cpn_channel",
            ]
        )
        self.dna_expected = dna_expected.toPandas()
        self.dna_expected.reset_index(inplace=True, drop=True)

    def test_feature_cpn_channel(self):
        """
        Compare actual and expected dna
        """
        dna_actual = feature_cpn_channel(self.job, self.dna)
        dna_actual = dna_actual.toPandas()
        dna_actual.sort_values("MBRSHP_SID", inplace=True)
        dna_actual.reset_index(inplace=True, drop=True)
        pd.testing.assert_frame_equal(dna_actual, self.dna_expected)


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
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
