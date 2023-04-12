from pyspark.sql.functions import col

import pe_memberdna.testing_support.regression_framework.regression as regression


class ForceBackIn(regression.RegressionTest):
    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def test_regression(self):
        super(ForceBackIn, self).run_test_scripts_and_prepare_data()

        MAILFILE = self.spark.read.option("header", "true").csv(
            self.config.paths["MAILFILE"]
        )

        self.assertEqual(
            MAILFILE.where(col("MBRSHP_SID") == 33193978)
            .select(["CPN1"])
            .collect()[0][0],
            "2017244",
        )

    def execute_assignment_scripts(self):
        regression.RegressionTest.execute_assignment_scripts(self)


if __name__ == "__main__":
    ForceBackIn.execute_test()
