from pyspark.sql.functions import col, when

import memberdna.testing_support.regression_framework.regression as regression


class BacktoBack(regression.RegressionTest):
    """
    The purpose of this test is to check that avoid back to back feature is
    not assigning the same coupons to the same members and also that
    members get their next best category offer.

    IMPORTANT: avoid back to back only works correct for frontfill category.

    To be able to implement a test that is deterministic and only looks into
    the feature itself changes had to be made to the data:

    1. We created simple a construct based on 2 slots(1 category, 1 basket).
    2. For category we chose rank_by_col "prediction" in order to reduce test
    complexity.
    3. The category.csv has been changed to 1 coupon/category in order to be
    able to predict the next assignment for a member.
    4. CF scores have been updated to values greater than 0 in order for
    the engine to ingest them.
    5. Each coupon from category.csv belonging to a member has the following
    layout:
        first coupon has ID_1 and CF score X
        second coupon has ID_2 and CF score Y

        ID_1 + 1 = ID_2 and X > Y

        Which is why we can assert:
        col('previous_cpn_nbr') + 1 == col('current_cpn_nbr')
    """

    def __init__(self, methodName):
        regression.RegressionTest.__init__(self, methodName)

    def assert_correct_next_category(self):
        total_expected_assignments = 4
        previous_cell_id = 1
        current_cell_id = 2
        slot_under_test = 1

        previous_assignment = self.spark.read.parquet(
            self.config.paths["CDSA_LOC"] + "assignments"
        )

        print("Previous assignment:")
        previous_assignment.show(50)

        previous_cell = previous_assignment.where(
            (col("cell_id") == previous_cell_id)
            & (col("slot_nbr") == slot_under_test)
        )
        previous_cell = previous_cell.select(
            "mbrshp_sid", col("cpn_nbr").alias("previous_cpn_nbr")
        )

        current_assignment = self.spark.read.option("header", "true").csv(
            self.config.paths["INPUT_ASSIGNMENTS"]
        )

        print("Current assignment:")
        current_assignment.show(50)

        current_cell = current_assignment.where(
            (col("cell_id") == current_cell_id)
            & (col("slot_nbr") == slot_under_test)
        )
        current_cell = current_cell.select(
            "mbrshp_sid", col("cpn_nbr").alias("current_cpn_nbr")
        )

        compare_cell = previous_cell.join(current_cell, "mbrshp_sid", "inner")

        print("Previous + current assignment:")
        compare_cell.show(50)

        compare_cell = compare_cell.withColumn(
            "is_expected_coupon",
            when(
                col("previous_cpn_nbr") + 1 == col("current_cpn_nbr"), 1
            ).otherwise(0),
        )
        total_assignments = compare_cell.count()
        total_ok_assignments = compare_cell.where(
            col("is_expected_coupon") == 1
        ).count()

        self.assertEqual(total_assignments, total_expected_assignments)
        self.assertEqual(total_assignments, total_ok_assignments)

    def assert_hardest_basket(self):
        total_expected_assignments = 4
        slot_under_test = 2
        current_cell_id = 2
        hardest_basket = "2025509"

        current_assignment = self.spark.read.option("header", "true").csv(
            self.config.paths["INPUT_ASSIGNMENTS"]
        )

        print("Current assignment:")
        current_assignment.show(50)

        total_ok_assignments = current_assignment.where(
            (col("cell_id") == current_cell_id)
            & (col("slot_nbr") == slot_under_test)
            & (col("cpn_nbr") == hardest_basket)
        )

        self.assertEqual(
            total_ok_assignments.count(), total_expected_assignments
        )

    def test_regression(self):
        regression.RegressionTest.run_test_scripts_and_prepare_data(self)
        self.assert_correct_next_category()
        self.assert_hardest_basket()

    def execute_assignment_scripts(self):
        super(BacktoBack, self).execute_assignment_scripts(
            _scripts=["create_coupons.py", "assign_offers.py"]
        )


if __name__ == "__main__":
    BacktoBack.execute_test()