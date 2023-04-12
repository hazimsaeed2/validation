from pyspark.sql.functions import col, length, lit, when

import pe_memberdna.testing_support.regression_framework.regression as regression


class CouponUpdate(regression.RegressionTest):
    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)

        MAILFILE = self.spark.read.option("header", "true").csv(
            self.config.paths["MAILFILE"]
        )
        MAILFILE = MAILFILE.withColumn(
            "correct", when(length(col("CPN1")) == 7, lit(0)).otherwise(lit(1))
        )
        self.assertEqual(
            MAILFILE.select(["correct"]).groupBy().sum().collect()[0][0], 0
        )


if __name__ == "__main__":
    CouponUpdate.execute_test()
