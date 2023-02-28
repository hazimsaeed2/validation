import pyspark.sql.functions as sqlf

import memberdna.pipelines.assignment.lib.filters as filters

import memberdna.testing_support.regression_framework.regression as regression


class MultiSegments(regression.RegressionTest):

    """
    The purpose of the test is to check the basic use cases for
    multi segments.

    The test data is split into 8 cells:
        0. contains members from (october and new members) or gas.
        1. contains from november which have purchased gas
        2. contains members from september.
        3. contains members from october.
        4. contains members from november.
        5. contains members from september or october.
        6. contains members from october or november.
        7. contains the rest of member. has no segment.
    """

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)

        input_assignments = self.spark.read.option("header", "true").csv(
            self.config.paths["INPUT_ASSIGNMENTS"]
        )
        mail_list = self.spark.read.option("header", "true").csv(
            self.config.paths["MAIL_LIST"]
        )

        cube = self.spark.read.parquet(self.config.paths["CUBE"])
        cube = cube.select(
            "MBRSHP_SID",
            "L_FIFTY-TWOW_GAS_TRIPS",
            "PREFERRED_CLUB_HAS_GAS",
            "TENURE"
        )

        cell_assignment = input_assignments.select(
            "MBRSHP_SID",
            "cell_id"
        ).distinct()

        cell_assignment = cell_assignment.join(
            mail_list, "MBRSHP_SID", "inner"
        )
        cell_assignment = cell_assignment.join(cube,  "MBRSHP_SID", "inner")
        cell_assignment = filters.new(cell_assignment, "new_member")
        cell_assignment = filters.gas(
            cell_assignment,
            "has_gas_purchase",
            ["PREFERRED_CLUB_HAS_GAS", "L_FIFTY-TWOW_GAS_TRIPS"]
        )

        self.assertEqual(
            cell_assignment.filter(sqlf.col("cell_id") == 0).count(),
            cell_assignment.filter(
                (sqlf.col("cell_id") == 0)
            ).filter(
                (
                    (sqlf.col("october") == 1)
                    & (sqlf.col("new_member") == 1)
                )
                | (sqlf.col("has_gas_purchase") == 1)
            ).count()
        )

        self.assertEqual(
            cell_assignment.filter(sqlf.col("cell_id") == 1).count(),
            cell_assignment.filter(
                (sqlf.col("cell_id") == 1)
            ).filter(
                (sqlf.col("november") == 1)
                & (sqlf.col("has_gas_purchase") == 1)
            ).count()
        )

        self.assertEqual(
            cell_assignment.filter(sqlf.col("cell_id") == 2).count(),
            cell_assignment.filter(
                (sqlf.col("cell_id") == 2)
                & (sqlf.col("september") == 1)
            ).count()
        )

        self.assertEqual(
            cell_assignment.filter(sqlf.col("cell_id") == 3).count(),
            cell_assignment.filter(
                (sqlf.col("cell_id") == 3)
                & (sqlf.col("october") == 1)
            ).count()
        )

        self.assertEqual(
            cell_assignment.filter(sqlf.col("cell_id") == 4).count(),
            cell_assignment.filter(
                (sqlf.col("cell_id") == 4)
                & (sqlf.col("november") == 1)
            ).count()
        )

        self.assertEqual(
            cell_assignment.filter(sqlf.col("cell_id") == 5).count(),
            cell_assignment.filter(
                (sqlf.col("cell_id") == 5)
            ).filter(
                (sqlf.col("september") == 1)
                | (sqlf.col("october") == 1)
            ).count()
        )

        self.assertEqual(
            cell_assignment.filter(sqlf.col("cell_id") == 6).count(),
            cell_assignment.filter(
                (sqlf.col("cell_id") == 6)
            ).filter(
                (sqlf.col("october") == 1)
                | (sqlf.col("november") == 1)
            ).count()
        )

    def execute_assignment_scripts(self):
        super(MultiSegments, self).execute_assignment_scripts(
            _scripts=[
                "create_coupons.py",
                "assign_offers.py"
            ]
        )


if __name__ == "__main__":
    MultiSegments.execute_test()
