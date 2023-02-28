import unittest
import xmlrunner

import pyspark.sql.functions as sqlf

import memberdna.testing_support.regression_framework.regression as regression


class AnniversaryContent(regression.RegressionTest):
    """
        The regression test checks anniverasry slot function:
        1. in combination with waterfall slot function:
            It uses slot 13 to assign gas(B), non gas(A) and, as a third option
            C for members with trips in the last 3 months or D for members
            wihtout trips in the last 3 months.
        2. using multiple renewable dates:
            It uses slot 13 to assign gas(B), non gas(A).

        The campaign uses 2 cell with multi constructs:
        1. (1 category + 11 articles 1 + dummy(waterfall(anniversary)))
            + default backfill
        2. (1 category + 11 articles 1 + dummy(anniversary))
            + default backfill

        Cell 2 uses construct satisfied filter to force members in.
    """

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)

        input_assignments = (
            self.spark.read.option("header", "true")
            .csv(self.config.paths["INPUT_ASSIGNMENTS"])
            .filter(sqlf.col("slot_nbr") == 13)
        )

        prev = (
            self.spark.read.option("header", "true")
            .csv(self.config.paths["ASSN_LOC"] + "assignments_reference/")
            .filter(sqlf.col("slot_nbr") == 13)
        )

        same = input_assignments.join(prev, input_assignments.columns, "inner")

        self.assertEqual(prev.count(), same.count())

    def execute_assignment_scripts(self):
        super(AnniversaryContent, self).execute_assignment_scripts(
            _scripts=[
                "create_coupons.py",
                "assign_offers.py"
            ]
        )


if __name__ == "__main__":
    AnniversaryContent.execute_test()
