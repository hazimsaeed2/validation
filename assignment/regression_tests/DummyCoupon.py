from pyspark.sql.functions import col

import memberdna.testing_support.regression_framework.regression as regression


class DummyCoupon(regression.RegressionTest):
    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)

        MAILFILE = self.spark.read.option("header", "true").csv(
            self.config.paths["MAILFILE"]
        )
        coupon = MAILFILE.where(col("MBRSHP_SID") == 61011282).collect()[0][
            4:17
        ]
        self.assertEqual(len(set(coupon)), len(coupon))


if __name__ == "__main__":
    DummyCoupon.execute_test()
