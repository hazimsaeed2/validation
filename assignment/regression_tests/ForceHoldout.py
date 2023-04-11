import pyspark.sql.functions as sqlf

import pe_memberdna.testing_support.regression_framework.regression as regression


class ForceHoldout(regression.RegressionTest):
    """
    Split the population into two cells, where cell 0 is mailed and cell 1
    is not.
    """

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)
        self.TOTAL_MEMBERS = 100

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)

        mailfile = self.spark.read.csv(
            self.config.paths["INPUT_MAILHOUSE"], header=True
        )

        mailed = mailfile.filter(sqlf.col("mail_flag") == "CIRC")
        mailed_cells = mailed.select("cell_id").distinct()
        mailed_cells = [row["cell_id"] for row in mailed_cells.collect()]

        nomail = mailfile.filter(sqlf.col("mail_flag") == "NO MAIL")
        nomail_cells = nomail.select("cell_id").distinct()
        nomail_cells = [row["cell_id"] for row in nomail_cells.collect()]

        self.assertEqual(mailed_cells, ["0"])
        self.assertEqual(nomail_cells, ["1"])
        self.assertEqual(mailed.count() + nomail.count(), self.TOTAL_MEMBERS)


if __name__ == "__main__":
    ForceHoldout.execute_test()
