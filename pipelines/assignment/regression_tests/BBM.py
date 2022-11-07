from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StringType, StructField, StructType

import memberdna.testing_support.regression_framework.regression as regression


class BBM(regression.RegressionTest):
    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def assert_tests_tab(self, qc_tests_tab):
        qc_tests_tab = qc_tests_tab.withColumn(
            "Failed", when(col("outcome") == "Fail", lit(1)).otherwise(lit(0))
        )

        total_failures = qc_tests_tab.where(col("Failed") == 1).count()

        self.assertEqual(total_failures, 0)

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)

        MAILFILE = self.spark.read.option("header", "true").csv(
            self.config.paths["MAILFILE"]
        )
        MAIL_LIST = self.spark.read.option("header", "true").csv(
            self.config.paths["MAIL_LIST"]
        )
        MAILFILE = MAILFILE.withColumn(
            "VERSION_num", col("VERSION").cast("int")
        )

        MAILFILE = MAILFILE.withColumn(
            "correct",
            when(col("VERSION_num").isNull(), lit(0)).otherwise(lit(1)),
        )

        num_member_in_MAILFILE = (
            MAILFILE.select(["MBRSHP_NBR"]).distinct().count()
        )
        num_member_in_MAIL_LIST = (
            MAIL_LIST.select(["MBRSHP_NBR"]).distinct().count()
        )

        self.assertEqual(
            MAILFILE.select(["correct"]).groupBy().sum().collect()[0][0], 0
        )
        self.assertEqual(num_member_in_MAILFILE, num_member_in_MAIL_LIST)

        schema = StructType(
            [
                StructField("test", StringType(), True),
                StructField("outcome", StringType(), True),
            ]
        )

        qc_tests_tab = self.spark.read.csv(
            self.config.paths["QC_REPORT"], header=False, schema=schema
        )

        self.assert_tests_tab(qc_tests_tab)

    def execute_assignment_scripts(self):
        super(BBM, self).execute_assignment_scripts(
            _scripts=[
                "create_coupons.py",
                "assign_offers.py",
                "backtest_sizing.py",
                "generate_output_file.py",
                "qc_assignments.py",
            ]
        )


if __name__ == "__main__":
    BBM.execute_test()
