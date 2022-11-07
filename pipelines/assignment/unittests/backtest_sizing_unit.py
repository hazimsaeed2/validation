import unittest

import xmlrunner
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, date_add, lit
from pyspark.sql.functions import max as fmax
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import sum as fsum
from pyspark.sql.functions import when

from pe_member_dna.pipelines.assignment.lib.assn_utils import (
    read_subset_and_cast,
)
from pe_member_dna.pipelines.assignment.scripts.backtest_sizing import (
    _flag_qualifiers,
)
from pe_member_dna.pipelines.lib.spark_util import get_logger


class BacksizeTesting(unittest.TestCase):
    "Unit tests for backtest_sizing"

    def setUp(self):
        rdd = sc.parallelize(
            [
                ("1", "5", 1.0, "3", 0.5, 1),
                ("2", "6", 0.1, "4", 5.0, None),
                ("2", "6", 0.1, "4", 5.0, None),
                ("1", "5", 1.0, "3", 0.25, None),
            ]
        )
        self.df = rdd.toDF(
            [
                "MBRSHP_SID",
                "CPN_NBR",
                "CPN_DOLLAR_THRESHOLD",
                "PURCH_HDR_ID",
                "EXTENDED_PRC_AMT",
                "OFFER_ID",
            ]
        )

    def test__flag_qualifiers(self):
        """Check that values are correct in this step (for the 'qualifiers' column),
        which is being sent to downstream calculations"""
        self.df = _flag_qualifiers(self.df)
        self.assertEqual(
            self.df.columns, ["MBRSHP_SID", "CPN_NBR", "is_qualified"]
        )
        self.assertEqual(self.df.count(), 2)

        not_qualified = self.df.filter(self.df.CPN_NBR == "5")
        self.assertEqual(
            not_qualified.select("is_qualified").collect()[0][0], 0
        )
        qualified = self.df.filter(self.df.CPN_NBR == "6")
        self.assertEqual(qualified.select("is_qualified").collect()[0][0], 1)


if __name__ == "__main__":

    name = "backtest_sizing - unittests"
    log = get_logger(name)

    spark = SparkSession.builder.appName(name).getOrCreate()
    sc = spark.sparkContext

    unittest.main(
        testRunner=xmlrunner.XMLTestRunner(output="unit-test-reports"),
        # these make sure that some options that are not applicable
        # remain hidden from the help menu.
        failfast=False,
        buffer=False,
        catchbreak=False,
    )
